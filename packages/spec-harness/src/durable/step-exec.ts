import type { TestStepResultStatus } from "@cucumber/messages"
import { AttachmentContentEncoding, TestStepResultStatus as Status } from "@cucumber/messages"
import { Data, Effect } from "effect"
import { DataTable } from "./data-table.ts"
import type { CompiledStep, World } from "./support.ts"
import type { CapturedAttachment, InvokeRequest, StepOutcome } from "./types.ts"

/**
 * Running one step body → a `StepOutcome`. Shared by both executors (the
 * in-process `directExec` and the durable per-scenario object), so "what it
 * means to run a step" lives in exactly one place. Pure with respect to
 * durability: the World is a transient capture buffer for the call, and the
 * outcome (status + attachments) is the value journaled by the durable path.
 */

const LOG_MEDIA_TYPE = "text/x.cucumber.log+plain"
const URI_LIST_MEDIA_TYPE = "text/uri-list"

const isReadableStream = (value: unknown): value is NodeJS.ReadableStream =>
  typeof value === "object" && value !== null && typeof (value as { pipe?: unknown }).pipe === "function"

const streamToBuffer = (stream: NodeJS.ReadableStream): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    const chunks: Array<Buffer> = []
    stream.on("data", (chunk: Buffer | string) => chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk))
    stream.on("end", () => resolve(Buffer.concat(chunks)))
    stream.on("error", reject)
  })

const makeWorld = (): { readonly world: World; readonly captured: ReadonlyArray<CapturedAttachment> } => {
  const captured: Array<CapturedAttachment> = []
  const attach = async (data: unknown, options?: string | { mediaType: string; fileName?: string }): Promise<void> => {
    const resolved = typeof options === "string" ? { mediaType: options, fileName: undefined } : options
    const mediaType = resolved?.mediaType ?? "application/octet-stream"
    const fileName = resolved?.fileName
    const base = typeof data === "string"
      ? { body: data, contentEncoding: AttachmentContentEncoding.IDENTITY as string }
      : {
        body: (isReadableStream(data) ? await streamToBuffer(data) : Buffer.from(data as Uint8Array)).toString("base64"),
        contentEncoding: AttachmentContentEncoding.BASE64 as string,
      }
    captured.push({ ...base, mediaType, ...(fileName === undefined ? {} : { fileName }) })
  }
  const world: World = {
    attach,
    log: (text) => attach(text, LOG_MEDIA_TYPE),
    link: (uri) => attach(uri, URI_LIST_MEDIA_TYPE),
  }
  return { world, captured }
}

class StepThrew extends Data.TaggedError("StepThrew")<{ readonly cause: unknown }> {}

const isPromiseLike = (value: unknown): value is PromiseLike<unknown> =>
  typeof value === "object" && value !== null && typeof (value as { then?: unknown }).then === "function"

const isGeneratorObject = (value: unknown): boolean =>
  typeof value === "object" && value !== null &&
  typeof (value as { next?: unknown }).next === "function" &&
  typeof (value as { throw?: unknown }).throw === "function"

const interpretReturn = (value: unknown): Effect.Effect<TestStepResultStatus, StepThrew> => {
  if (Effect.isEffect(value)) {
    return Effect.as((value as Effect.Effect<unknown, unknown>).pipe(Effect.mapError((cause) => new StepThrew({ cause }))), Status.PASSED)
  }
  if (isPromiseLike(value)) {
    return Effect.as(Effect.tryPromise({ try: () => Promise.resolve(value), catch: (cause) => new StepThrew({ cause }) }), Status.PASSED)
  }
  if (isGeneratorObject(value)) {
    return Effect.fail(new StepThrew({ cause: new Error("step body returned a generator; lift it with Effect.fn(...)") }))
  }
  if (value === "pending") return Effect.succeed(Status.PENDING)
  if (value === "skipped") return Effect.succeed(Status.SKIPPED)
  return Effect.succeed(Status.PASSED)
}

const toFailure = (error: unknown): { readonly type: string; readonly message: string; readonly stackTrace?: string } => {
  const c = error as { readonly _tag?: unknown; readonly name?: unknown; readonly message?: unknown; readonly stack?: unknown }
  const type = typeof c?._tag === "string" ? c._tag : typeof c?.name === "string" ? c.name : "Error"
  const message = typeof c?.message === "string" ? c.message : String(error)
  const stackTrace = typeof c?.stack === "string" ? c.stack : undefined
  return stackTrace === undefined ? { type, message } : { type, message, stackTrace }
}

// Re-bind args to the World at invoke time (parameter-type transforms may use `this`).
const bindArguments = (step: CompiledStep, text: string, world: unknown): ReadonlyArray<unknown> => {
  const args = step.expression.match(text)
  return args === null ? [] : args.map((arg) => arg.getValue(world))
}

const trailingArg = (request: InvokeRequest): ReadonlyArray<unknown> => {
  if (request.dataTable !== undefined) return [DataTable.from(request.dataTable)]
  if (request.docString !== undefined) return [request.docString]
  return []
}

/** A FAILED outcome with no attachments, for host/wiring errors (unknown step id, etc.). */
export const failOutcome = (message: string): StepOutcome => ({
  status: Status.FAILED,
  attachments: [],
  error: { type: "Error", message },
})

/** Run one matched step body against a fresh World, capturing attachments. */
export const executeStep = (step: CompiledStep, request: InvokeRequest): Effect.Effect<StepOutcome> =>
  Effect.suspend(() => {
    const { captured, world } = makeWorld()
    const args = [...bindArguments(step, request.text, world), ...trailingArg(request)]
    return Effect.try({ try: () => step.fn.apply(world, args), catch: (cause) => new StepThrew({ cause }) }).pipe(
      Effect.flatMap(interpretReturn),
      Effect.map((status): StepOutcome => ({ status, attachments: [...captured] })),
      Effect.catch((error) => Effect.succeed<StepOutcome>({ status: Status.FAILED, attachments: [...captured], error: toFailure(error.cause) })),
    )
  })
