import { DataTable } from "@cucumber/core"
import type { TestStepResult, TestStepResultStatus } from "@cucumber/messages"
import { AttachmentContentEncoding, IdGenerator, TestStepResultStatus as Status } from "@cucumber/messages"
import { Data, Effect, Schema } from "effect"
import { object, state } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { buildSupportLibrary } from "./support.ts"
import type { BeginScenarioInput, CapturedAttachment, StepInvocation, StepOutcome } from "./types.ts"

/**
 * The step-definition host — the Cucumber wire "step host" + the scenario World,
 * as a durable virtual object keyed by scenario-attempt id. It owns the support
 * code (looked up from the registered bundle) and per-scenario `state(...)`. Each
 * `invoke` is the per-step durable boundary: its result is journaled on the
 * owner stream, so on replay a completed step returns its recorded outcome and
 * its side effects (incl. proof reads) are not re-run.
 *
 * It mints no Cucumber ids; the runner owns those and maps these outcomes onto
 * envelopes.
 */

class WorldCtx extends Table<WorldCtx>("worldCtx")({
  id: Schema.String.pipe(primaryKey),
  supportName: Schema.String,
}) {}

// ── in-process World handed to a step body as `this` (attach/log/link) ────────

interface World {
  attach(data: unknown, options: string | { mediaType: string; fileName?: string }): Promise<void>
  log(text: string): Promise<void>
  link(uri: string): Promise<void>
}

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
  const attach = async (data: unknown, options: string | { mediaType: string; fileName?: string }): Promise<void> => {
    const { mediaType, fileName } = typeof options === "string" ? { mediaType: options, fileName: undefined } : options
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

const okResult = (status: TestStepResultStatus): TestStepResult => ({ status, duration: { seconds: 0, nanos: 0 } })

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

const callArgsFor = (invocation: StepInvocation): ReadonlyArray<unknown> => {
  if (invocation.dataTable !== undefined) return [...invocation.argValues, DataTable.from(invocation.dataTable)]
  if (invocation.docString !== undefined) return [...invocation.argValues, invocation.docString]
  return invocation.argValues
}

const executeStep = (supportName: string, invocation: StepInvocation): Effect.Effect<StepOutcome> =>
  Effect.suspend(() => {
    const library = buildSupportLibrary(supportName, IdGenerator.incrementing())
    const matches = library.findAllStepsBy(invocation.text)
    if (matches.length !== 1) {
      return Effect.succeed<StepOutcome>({
        status: Status.FAILED,
        attachments: [],
        error: { type: "Error", message: `expected exactly one step match for ${JSON.stringify(invocation.text)}, found ${matches.length}` },
      })
    }
    const { world, captured } = makeWorld()
    const fn = matches[0]!.def.fn
    return Effect.try({ try: () => fn.apply(world, [...callArgsFor(invocation)]) as unknown, catch: (cause) => new StepThrew({ cause }) }).pipe(
      Effect.flatMap(interpretReturn),
      Effect.map((status): StepOutcome => ({ status, attachments: [...captured] })),
      Effect.catch((error) => Effect.succeed<StepOutcome>({ status: Status.FAILED, attachments: [...captured], error: toFailure(error.cause) })),
    )
  })

// ── the durable object ──────────────────────────────────────────────────────

export const world = object({
  name: "cucumber-effect/world",
  handlers: {
    // wire `begin_scenario`: establish the scenario context (and, later, run
    // Before hooks / open the firegrid.scenario span).
    *beginScenario(input: BeginScenarioInput) {
      yield* state(WorldCtx).set({ id: "ctx", supportName: input.supportName })
      return { ok: true as const }
    },
    // wire `invoke`: run one matched step body, journaled per step.
    *invoke(input: { readonly invocation: StepInvocation }) {
      const ctx = yield* state(WorldCtx).get("ctx")
      if (ctx._tag === "None") {
        return {
          status: Status.FAILED,
          attachments: [],
          error: { type: "Error", message: "invoke before beginScenario" },
        } satisfies StepOutcome
      }
      return yield* executeStep(ctx.value.supportName, input.invocation)
    },
    // wire `end_scenario`: teardown (and, later, After hooks).
    *endScenario(_input: { readonly tags: ReadonlyArray<string> }) {
      yield* state(WorldCtx).delete("ctx")
      return { ok: true as const }
    },
  },
})

export type WorldDefinition = typeof world
export { okResult }
