import type { AttachmentContentEncoding, Envelope, TestStepResult, TestStepResultStatus } from "@cucumber/messages"
import { TestStepResultStatus as Status } from "@cucumber/messages"
import { Effect } from "effect"
import { type DurableExecutionError, type DurableExecutionRuntime, objectClient, run, service } from "effect-s2-durable"
import { RunEnvelopes } from "./streams.ts"
import { assembleRun } from "./assembly.ts"
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
import type { CapturedAttachment, PlannedScenario, PlannedStep, RunInput, RunResult, StepOutcome } from "./types.ts"
import { world } from "./world.ts"

/**
 * The Cucumber runner, as a stateless durable service. Modelled on the wire
 * protocol's runner: it parses + orders, owns **all** Cucumber ids/envelopes,
 * and drives the `world` step-host over durable RPC
 * (`beginScenario` -> per-step `invoke` -> `endScenario`), mapping each response
 * onto envelopes. It holds no step code and no world services.
 */

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

interface StepFold {
  readonly mode: "run" | "skip"
  readonly envelopes: ReadonlyArray<Envelope>
  readonly statuses: ReadonlyArray<TestStepResultStatus>
}

const emitStep = (
  testCaseStartedId: string,
  testStepId: string,
  acc: StepFold,
  result: TestStepResult,
  attachments: ReadonlyArray<CapturedAttachment>,
): StepFold => ({
  mode: acc.mode === "run" && result.status === Status.PASSED ? "run" : "skip",
  envelopes: [
    ...acc.envelopes,
    testStepStarted({ testCaseStartedId, testStepId }),
    ...attachmentEnvelopes(testCaseStartedId, testStepId, attachments),
    testStepFinished({ testCaseStartedId, testStepId, testStepResult: result }),
  ],
  statuses: [...acc.statuses, result.status],
})

export const runner = service({
  name: "cucumber-effect/runner",
  handlers: {
    *run(input: RunInput) {
      const assembled = assembleRun({ sources: input.sources, supportName: input.supportName })

      type StepEffect = Effect.Effect<StepFold, DurableExecutionError, DurableExecutionRuntime>

      const foldStep = (testCaseStartedId: string, attemptKey: string) =>
      (acc: StepFold, step: PlannedStep): StepEffect => {
        if (acc.mode === "skip" && !step.always) {
          return Effect.succeed(emitStep(testCaseStartedId, step.testStepId, acc, skippedResult(), []))
        }
        switch (step.kind._tag) {
          case "undefined":
            return Effect.succeed(emitStep(testCaseStartedId, step.testStepId, acc, undefinedResult(), []))
          case "ambiguous":
            return Effect.succeed(emitStep(testCaseStartedId, step.testStepId, acc, ambiguousResult(step.kind.message), []))
          case "prepared": {
            const invocation = step.kind.invocation
            return objectClient(world, attemptKey).invoke({ invocation }).pipe(
              Effect.map((outcome) => emitStep(testCaseStartedId, step.testStepId, acc, resultFor(outcome), outcome.attachments)),
            )
          }
        }
      }

      const runScenario = (scenario: PlannedScenario): StepEffect => {
        const attemptKey = `${scenario.testCaseId}:0`
        const fold = scenario.steps.reduce<StepEffect>(
          (accEffect, step) => accEffect.pipe(Effect.flatMap((acc) => foldStep(scenario.testCaseStartedId, attemptKey)(acc, step))),
          Effect.succeed({
            mode: "run",
            envelopes: [testCaseStarted({ id: scenario.testCaseStartedId, testCaseId: scenario.testCaseId, attempt: 0 })],
            statuses: [],
          }),
        )
        return objectClient(world, attemptKey)
          .beginScenario({ supportName: input.supportName, scenarioId: scenario.scenarioId, tags: scenario.tags })
          .pipe(
            Effect.flatMap(() => fold),
            Effect.flatMap((acc) =>
              objectClient(world, attemptKey).endScenario({ tags: scenario.tags }).pipe(Effect.as({
                mode: acc.mode,
                envelopes: [...acc.envelopes, testCaseFinished({ testCaseStartedId: scenario.testCaseStartedId, willBeRetried: false })],
                statuses: acc.statuses,
              })),
            ),
          )
      }

      const scenarioFolds = yield* Effect.forEach(assembled.scenarios, runScenario, {
        concurrency: input.options.scenarioConcurrency ?? 1,
      })

      const statuses = scenarioFolds.flatMap((fold) => [...fold.statuses])
      const success = testRunSuccess(statuses)
      const envelopes: ReadonlyArray<Envelope> = [
        metaEnvelope(),
        ...assembled.discoveryEnvelopes,
        ...assembled.supportEnvelopes,
        testRunStarted(assembled.testRunStartedId),
        ...assembled.testCaseEnvelopes,
        ...scenarioFolds.flatMap((fold) => [...fold.envelopes]),
        testRunFinished({ testRunStartedId: assembled.testRunStartedId, success }),
      ]

      // Publish the ordered envelopes as facts on the run's durable stream — the
      // canonical output consumers read/tail. Journaled via `run(...)` so a
      // handler replay re-reads the ack instead of re-appending (no duplicates).
      yield* run(
        "publish-envelopes",
        RunEnvelopes.open(input.runId).pipe(
          Effect.flatMap((stream) => stream.appendBatch(envelopes)),
          Effect.asVoid,
        ),
      )

      return { success } satisfies RunResult
    },
  },
})

export type RunnerDefinition = typeof runner
