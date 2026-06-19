import type { AssembledTestStep, PreparedStep } from "@cucumber/core"
import { DataTable } from "@cucumber/core"
import type { Envelope, TestStepResult, TestStepResultStatus } from "@cucumber/messages"
import { TestStepResultStatus as Status } from "@cucumber/messages"
import { Data, Effect } from "effect"
import { object } from "effect-s2-durable"
import { type AssembledRun, assembleRun } from "./assembly.ts"
import {
  ambiguousResult,
  attachmentEnvelope,
  failedResult,
  passedResult,
  pendingResult,
  skippedResult,
  testCaseFinished,
  testCaseStarted,
  testStepFinished,
  testStepStarted,
  undefinedResult,
} from "./messages.ts"
import type { SupportModule } from "./support.ts"
import type { ScenarioAttemptInput, ScenarioAttemptResult } from "./types.ts"
import { type CapturedAttachment, makeWorld, type World } from "./world.ts"

/**
 * The keyed, stateful scenario actor. Its key is the scenario attempt id
 * (`${testCaseId}:${attempt}`) so a retry is always a fresh key, never a reuse
 * of a prior attempt's durable state. It re-assembles the run deterministically
 * to recover the live `AssembledTestCase` (with its `prepare()` closures) — the
 * ids line up with the coordinator's emitted test-case envelopes because
 * assembly is a pure function of `(sources, support)`.
 */

// Carries whatever a step body threw or rejected with through the Effect error
// channel as a typed failure, so the catch boundary stays typed end to end.
class StepThrew extends Data.TaggedError("StepThrew")<{ readonly cause: unknown }> {}

const isPromiseLike = (value: unknown): value is PromiseLike<unknown> =>
  typeof value === "object" && value !== null && typeof (value as { then?: unknown }).then === "function"

const isGeneratorObject = (value: unknown): boolean =>
  typeof value === "object" && value !== null &&
  typeof (value as { next?: unknown }).next === "function" &&
  typeof (value as { throw?: unknown }).throw === "function"

const toFailure = (error: unknown): { readonly type: string; readonly message: string; readonly stackTrace?: string } => {
  const candidate = error as { readonly _tag?: unknown; readonly name?: unknown; readonly message?: unknown; readonly stack?: unknown }
  const type = typeof candidate?._tag === "string"
    ? candidate._tag
    : typeof candidate?.name === "string"
    ? candidate.name
    : "Error"
  const message = typeof candidate?.message === "string" ? candidate.message : String(error)
  const stackTrace = typeof candidate?.stack === "string" ? candidate.stack : undefined
  return stackTrace === undefined ? { type, message } : { type, message, stackTrace }
}

const buildArgs = (prepared: PreparedStep, world: World): ReadonlyArray<unknown> => {
  const exprArgs = prepared.args.map((arg) => arg.getValue(world))
  if (prepared.dataTable !== undefined) return [...exprArgs, DataTable.from(prepared.dataTable)]
  if (prepared.docString !== undefined) return [...exprArgs, prepared.docString.content]
  return exprArgs
}

// A step body may be synchronous, return a Promise, or return an Effect (so it
// can reach for durable primitives). A returned generator is a missed lift and
// is surfaced loudly as a failure rather than mistaken for a pass.
const interpretReturn = (value: unknown): Effect.Effect<TestStepResult, StepThrew> => {
  if (Effect.isEffect(value)) {
    return Effect.as(
      (value as Effect.Effect<unknown, unknown>).pipe(Effect.mapError((cause) => new StepThrew({ cause }))),
      passedResult(),
    )
  }
  if (isPromiseLike(value)) {
    return Effect.as(
      Effect.tryPromise({ try: () => Promise.resolve(value), catch: (cause) => new StepThrew({ cause }) }),
      passedResult(),
    )
  }
  if (isGeneratorObject(value)) {
    return Effect.succeed(
      failedResult({ type: "Error", message: "step body returned a generator; lift it with Effect.fn(...)" }),
    )
  }
  if (value === "pending") return Effect.succeed(pendingResult())
  if (value === "skipped") return Effect.succeed(skippedResult())
  return Effect.succeed(passedResult())
}

const executePreparedStep = (prepared: PreparedStep, world: World): Effect.Effect<TestStepResult> =>
  Effect.try({
    try: () => prepared.fn.apply(world, [...buildArgs(prepared, world)]) as unknown,
    catch: (cause) => new StepThrew({ cause }),
  }).pipe(
    Effect.flatMap(interpretReturn),
    Effect.catch((error) => Effect.succeed(failedResult(toFailure(error.cause)))),
  )

const runStep = (step: AssembledTestStep, world: World): Effect.Effect<TestStepResult> => {
  const prepared = step.prepare()
  switch (prepared.type) {
    case "undefined":
      return Effect.succeed(undefinedResult())
    case "ambiguous":
      return Effect.succeed(
        ambiguousResult(
          `Multiple step definitions match: ${prepared.matches.map((match) => String(match.expression.raw)).join(", ")}`,
        ),
      )
    case "prepared":
      return executePreparedStep(prepared, world)
  }
}

interface StepFold {
  readonly mode: "run" | "skip"
  readonly envelopes: ReadonlyArray<Envelope>
  readonly statuses: ReadonlyArray<TestStepResultStatus>
}

const foldStep = (
  acc: StepFold,
  step: AssembledTestStep,
  world: World,
  captured: ReadonlyArray<CapturedAttachment>,
  testCaseStartedId: string,
): Effect.Effect<StepFold> => {
  if (acc.mode === "skip" && !step.always) {
    const result = skippedResult()
    return Effect.succeed({
      mode: "skip",
      envelopes: [...acc.envelopes, testStepFinished({ testCaseStartedId, testStepId: step.id, testStepResult: result })],
      statuses: [...acc.statuses, result.status],
    })
  }

  const before = captured.length
  return runStep(step, world).pipe(
    Effect.map((result) => {
      const stepAttachments = captured.slice(before).map((attachment) =>
        attachmentEnvelope({ testCaseStartedId, testStepId: step.id, ...attachment }),
      )
      const nextMode: "run" | "skip" = acc.mode === "run" && result.status === Status.PASSED ? "run" : "skip"
      return {
        mode: nextMode,
        envelopes: [
          ...acc.envelopes,
          testStepStarted({ testCaseStartedId, testStepId: step.id }),
          ...stepAttachments,
          testStepFinished({ testCaseStartedId, testStepId: step.id, testStepResult: result }),
        ],
        statuses: [...acc.statuses, result.status],
      }
    }),
  )
}

/**
 * Execute exactly one scenario attempt against an already-assembled run. Pure
 * with respect to the durable backend (it only emits envelopes), so it is the
 * shared core run both by the durable worker handler and by direct callers.
 */
export const executeScenario = (
  assembled: AssembledRun,
  params: { readonly testCaseId: string; readonly testCaseStartedId: string; readonly attempt: number },
): Effect.Effect<ScenarioAttemptResult> => {
  const testCase = assembled.byId.get(params.testCaseId)
  if (testCase === undefined) {
    return Effect.succeed({ envelopes: [], statuses: [] })
  }

  const { world, captured } = makeWorld()
  const testCaseStartedId = params.testCaseStartedId

  const initial: StepFold = {
    mode: "run",
    envelopes: [testCaseStarted({ id: testCaseStartedId, testCaseId: testCase.id, attempt: params.attempt })],
    statuses: [],
  }

  return testCase.testSteps
    .reduce(
      (accEffect, step) => accEffect.pipe(Effect.flatMap((acc) => foldStep(acc, step, world, captured, testCaseStartedId))),
      Effect.succeed(initial),
    )
    .pipe(
      Effect.map((folded) => ({
        envelopes: [...folded.envelopes, testCaseFinished({ testCaseStartedId, willBeRetried: false })],
        statuses: folded.statuses,
      })),
    )
}

/** Build the per-run worker object, closing over the run's support module. */
export const makeWorker = (support: SupportModule) =>
  object({
    name: "cucumber-effect/worker",
    handlers: {
      *runScenario(input: ScenarioAttemptInput) {
        return yield* executeScenario(assembleRun({ sources: input.sources, support }), {
          testCaseId: input.testCaseId,
          testCaseStartedId: input.testCaseStartedId,
          attempt: input.attempt,
        })
      },
    },
  })

export type WorkerDefinition = ReturnType<typeof makeWorker>
