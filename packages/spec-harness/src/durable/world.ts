import type { TestStepResultStatus } from "@cucumber/messages"
import { AttachmentContentEncoding, TestStepResultStatus as Status } from "@cucumber/messages"
import { Data, Effect } from "effect"
import { object } from "effect-s2-durable"
import { DataTable } from "./data-table.ts"
import { invocationArguments } from "./matcher.ts"
import type { SupportBundle, World } from "./support.ts"
import type { BeginScenarioInput, CapturedAttachment, StepInvocation, StepOutcome } from "./types.ts"

/**
 * The step-definition host + scenario World, as a durable virtual object keyed
 * by scenario-attempt id (cucumber-js's parallel worker, made durable). The
 * support bundle is **captured in this object's handler closures** at definition
 * time via `makeWorldObject(support)` — a deployment dependency, not a global
 * lookup — so on recovery the rebuilt layer re-establishes the same closures.
 *
 * Each `invoke` is the per-step durable boundary: its `StepOutcome` (status +
 * captured attachments) is journaled on the owner stream, so a replay returns
 * the recorded outcome without re-running the body or its side effects. Per-
 * scenario `state(...)` written by step bodies is isolated by the object key.
 * It mints no Cucumber ids; the runner owns those and maps outcomes to envelopes.
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

// The in-process World handed to a step body as `this`. Attachments are captured
// into a transient buffer for the duration of one (journaled) `invoke`, then
// serialized into the StepOutcome — they never live as durable object state.
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

// ── step-body invocation ──────────────────────────────────────────────────

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

const trailingArg = (invocation: StepInvocation): ReadonlyArray<unknown> => {
  if (invocation.dataTable !== undefined) return [DataTable.from(invocation.dataTable)]
  if (invocation.docString !== undefined) return [invocation.docString]
  return []
}

const executeStep = (support: SupportBundle, invocation: StepInvocation): Effect.Effect<StepOutcome> =>
  Effect.suspend(() => {
    const step = support.steps[invocation.stepIndex]
    if (step === undefined) {
      return Effect.succeed<StepOutcome>({
        status: Status.FAILED,
        attachments: [],
        error: { type: "Error", message: `no step definition at index ${invocation.stepIndex}` },
      })
    }
    const { captured, world } = makeWorld()
    const args = [...invocationArguments(support, invocation.stepIndex, invocation.text, world), ...trailingArg(invocation)]
    return Effect.try({ try: () => step.fn.apply(world, args), catch: (cause) => new StepThrew({ cause }) }).pipe(
      Effect.flatMap(interpretReturn),
      Effect.map((status): StepOutcome => ({ status, attachments: [...captured] })),
      Effect.catch((error) => Effect.succeed<StepOutcome>({ status: Status.FAILED, attachments: [...captured], error: toFailure(error.cause) })),
    )
  })

// ── the durable object ──────────────────────────────────────────────────────

/** Build the `world` step-host object bound to a specific support bundle. */
export const makeWorldObject = (support: SupportBundle) =>
  object({
    name: "cucumber-effect/world",
    handlers: {
      // wire `begin_scenario`: scenario lifecycle anchor (Before hooks / the
      // firegrid.scenario span attach here next).
      *beginScenario(_input: BeginScenarioInput) {
        return { ok: true as const }
      },
      // wire `invoke`: run one matched step body, journaled per step.
      *invoke(input: { readonly invocation: StepInvocation }) {
        return yield* executeStep(support, input.invocation)
      },
      // wire `end_scenario`: teardown (After hooks land here next).
      *endScenario(_input: { readonly tags: ReadonlyArray<string> }) {
        return { ok: true as const }
      },
    },
  })

export type WorldDefinition = ReturnType<typeof makeWorldObject>
