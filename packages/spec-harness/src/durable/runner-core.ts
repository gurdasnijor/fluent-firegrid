import type { AttachmentContentEncoding, Envelope, TestStepResult, TestStepResultStatus } from "@cucumber/messages"
import { TestStepResultStatus as Status } from "@cucumber/messages"
import { Effect, Ref, Stream } from "effect"
import { assemble } from "./assembly.ts"
import {
  ambiguousResult,
  attachmentEnvelope,
  failedResult,
  metaEnvelope,
  passedResult,
  pendingResult,
  skippedResult,
  testCaseFinished,
  testCaseStarted,
  testRunFinished,
  testRunStarted,
  testRunSuccess,
  testStepFinished,
  testStepStarted,
  undefinedResult,
} from "./messages.ts"
import { executeStep, failOutcome } from "./step-exec.ts"
import type { StepHost } from "./step-host.ts"
import type { CapturedAttachment, InvokeRequest, PreparedScenario, PreparedStep, StepOutcome } from "./types.ts"

/**
 * The cucumber runner core — the protocol, from scratch. Given parsed features,
 * a `StepHost`, and an `Executor`, it emits the canonical cucumber `Envelope`
 * stream **in order** (the data plane), as facts produced incrementally. It owns
 * parse → assemble → emit and the skip-after-failure rule; it knows nothing about
 * durability or S2. `testRunFinished`/`success` is folded from the very events it
 * emits (the data plane is the source of truth), not threaded through an
 * accumulator.
 *
 * HOW a step runs is the injected `Executor` — the wire `begin`/`invoke`/`end`.
 * `directExec` runs bodies in-process (the pure CCK gate); the durable path
 * injects an executor backed by the per-scenario object's command handlers.
 */

export interface Executor<E = never, R = never> {
  readonly beginScenario: (scenario: PreparedScenario) => Effect.Effect<void, E, R>
  readonly invoke: (scenario: PreparedScenario, request: InvokeRequest) => Effect.Effect<StepOutcome, E, R>
  readonly endScenario: (scenario: PreparedScenario) => Effect.Effect<void, E, R>
}

const resultFor = (outcome: StepOutcome): TestStepResult => {
  switch (outcome.status) {
    case Status.PASSED:
      return passedResult()
    case Status.SKIPPED:
      return skippedResult()
    case Status.PENDING:
      return pendingResult()
    default:
      return failedResult(outcome.error ?? { type: "Error", message: "step failed" })
  }
}

const attachmentEnvelopes = (
  testCaseStartedId: string,
  testStepId: string,
  attachments: ReadonlyArray<CapturedAttachment>,
): ReadonlyArray<Envelope> =>
  attachments.map((attachment) =>
    attachmentEnvelope({
      testCaseStartedId,
      testStepId,
      body: attachment.body,
      mediaType: attachment.mediaType,
      contentEncoding: attachment.contentEncoding as AttachmentContentEncoding,
      ...(attachment.fileName === undefined ? {} : { fileName: attachment.fileName }),
    }),
  )

type Mode = "run" | "skip"

// Decide a step's result + attachments, honouring the skip-after-failure rule.
// Once skipping, every following step (incl. undefined/ambiguous) is SKIPPED.
const stepResult = <E, R>(
  scenario: PreparedScenario,
  step: PreparedStep,
  exec: Executor<E, R>,
  mode: Ref.Ref<Mode>,
): Effect.Effect<{ readonly result: TestStepResult; readonly attachments: ReadonlyArray<CapturedAttachment> }, E, R> =>
  Effect.gen(function*() {
    if ((yield* Ref.get(mode)) === "skip") return { result: skippedResult(), attachments: [] }
    switch (step._tag) {
      case "undefined":
        yield* Ref.set(mode, "skip")
        return { result: undefinedResult(), attachments: [] }
      case "ambiguous":
        yield* Ref.set(mode, "skip")
        return { result: ambiguousResult(step.message), attachments: [] }
      case "invoke": {
        const outcome = yield* exec.invoke(scenario, step.request)
        if (outcome.status !== Status.PASSED) yield* Ref.set(mode, "skip")
        return { result: resultFor(outcome), attachments: outcome.attachments }
      }
    }
  })

const stepEnvelopes = <E, R>(
  scenario: PreparedScenario,
  step: PreparedStep,
  exec: Executor<E, R>,
  mode: Ref.Ref<Mode>,
  statuses: Ref.Ref<ReadonlyArray<TestStepResultStatus>>,
): Effect.Effect<ReadonlyArray<Envelope>, E, R> =>
  stepResult(scenario, step, exec, mode).pipe(
    Effect.tap(({ result }) => Ref.update(statuses, (all) => [...all, result.status])),
    Effect.map(({ attachments, result }) => [
      testStepStarted({ testCaseStartedId: scenario.testCaseStartedId, testStepId: step.testStepId }),
      ...attachmentEnvelopes(scenario.testCaseStartedId, step.testStepId, attachments),
      testStepFinished({ testCaseStartedId: scenario.testCaseStartedId, testStepId: step.testStepId, testStepResult: result }),
    ]),
  )

const scenarioEvents = <E, R>(
  scenario: PreparedScenario,
  exec: Executor<E, R>,
  statuses: Ref.Ref<ReadonlyArray<TestStepResultStatus>>,
): Stream.Stream<Envelope, E, R> =>
  Stream.unwrap(Effect.gen(function*() {
    const mode = yield* Ref.make<Mode>("run")
    yield* exec.beginScenario(scenario)
    const head = Stream.make(testCaseStarted({ id: scenario.testCaseStartedId, testCaseId: scenario.testCaseId, attempt: 0 }))
    const body = Stream.flatMap(Stream.fromIterable(scenario.steps), (step) =>
      Stream.unwrap(stepEnvelopes(scenario, step, exec, mode, statuses).pipe(Effect.map(Stream.fromIterable))))
    const tail = Stream.unwrap(
      exec.endScenario(scenario).pipe(
        Effect.as(Stream.make(testCaseFinished({ testCaseStartedId: scenario.testCaseStartedId, willBeRetried: false }))),
      ),
    )
    return head.pipe(Stream.concat(body), Stream.concat(tail))
  }))

/** Emit the canonical cucumber `Envelope` stream for a run. The data plane. */
export const runFeatures = <E, R>(
  sources: Parameters<typeof assemble>[0],
  host: StepHost,
  exec: Executor<E, R>,
): Stream.Stream<Envelope, E, R> =>
  Stream.unwrap(Effect.gen(function*() {
    const assembled = assemble(sources, host)
    const statuses = yield* Ref.make<ReadonlyArray<TestStepResultStatus>>([])
    const framing = Stream.fromIterable([
      metaEnvelope(),
      ...assembled.discoveryEnvelopes,
      ...assembled.supportEnvelopes,
      testRunStarted(assembled.testRunStartedId),
      ...assembled.testCaseEnvelopes,
    ])
    const scenarios = Stream.flatMap(Stream.fromIterable(assembled.scenarios), (scenario) =>
      scenarioEvents(scenario, exec, statuses))
    // testRunFinished is a projection over the emitted step statuses — evaluated
    // after the scenario stream drains (concat is sequential), so it sees them all.
    const finished = Stream.unwrap(
      Ref.get(statuses).pipe(
        Effect.map((all) =>
          Stream.make(testRunFinished({ testRunStartedId: assembled.testRunStartedId, success: testRunSuccess(all) })),
        ),
      ),
    )
    return framing.pipe(Stream.concat(scenarios), Stream.concat(finished))
  }))

/** In-process executor: runs step bodies directly. No durability, no S2 — the pure CCK gate. */
export const directExec = (host: StepHost): Executor => ({
  beginScenario: () => Effect.void,
  endScenario: () => Effect.void,
  invoke: (_scenario, request) => {
    const step = host.resolve(request.stepDefId)
    return step === undefined ? Effect.succeed(failOutcome(`unknown step definition ${request.stepDefId}`)) : executeStep(step, request)
  },
})
