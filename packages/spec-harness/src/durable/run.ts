import type { Envelope } from "@cucumber/messages"
import { Effect } from "effect"
import { type AssembledRun, assembleRun } from "./assembly.ts"
import { metaEnvelope, testRunFinished, testRunStarted, testRunSuccess } from "./messages.ts"
import type { SupportModule } from "./support.ts"
import type { RunOptions, RunResult, ScenarioAttemptResult, SourceInput } from "./types.ts"
import { executeScenario } from "./worker.ts"

/**
 * Shared run-shaping logic used by both the durable coordinator and the
 * in-process local runner, so they produce byte-identical envelope streams.
 * The coordinator routes scenarios to durable worker objects; {@link runFeaturesLocal}
 * runs them in-process. Either way the ordering and ids are the same.
 */

export interface ScenarioAttemptPlan {
  readonly testCaseId: string
  readonly attempt: number
  readonly attemptKey: string
  readonly testCaseStartedId: string
}

/** Mint one attempt per assembled test case, in order, with deterministic ids. */
export const planAttempts = (assembled: AssembledRun): ReadonlyArray<ScenarioAttemptPlan> =>
  assembled.testCases.map((testCase) => ({
    testCaseId: testCase.id,
    attempt: 0,
    attemptKey: `${testCase.id}:0`,
    testCaseStartedId: assembled.newId(),
  }))

/** Assemble the canonical ordered envelope stream from the run pieces. */
export const buildRunEnvelopes = (
  assembled: AssembledRun,
  scenarioEnvelopes: ReadonlyArray<Envelope>,
  success: boolean,
): ReadonlyArray<Envelope> => [
  metaEnvelope(),
  ...assembled.discoveryEnvelopes,
  ...assembled.supportEnvelopes,
  testRunStarted(assembled.testRunStartedId),
  ...assembled.testCaseEnvelopes,
  ...scenarioEnvelopes,
  testRunFinished({ testRunStartedId: assembled.testRunStartedId, success }),
]

export const foldRunResult = (
  assembled: AssembledRun,
  scenarioResults: ReadonlyArray<ScenarioAttemptResult>,
): RunResult => {
  const statuses = scenarioResults.flatMap((result) => [...result.statuses])
  const success = testRunSuccess(statuses)
  return {
    envelopes: buildRunEnvelopes(assembled, scenarioResults.flatMap((result) => [...result.envelopes]), success),
    statuses,
    success,
  }
}

/**
 * Run features in-process without the durable backend. Same assembly, same
 * scenario execution, and same envelope ordering as the durable coordinator —
 * useful for environments without an S2 backend and as the CCK message gate.
 */
export const runFeaturesLocal = (
  sources: ReadonlyArray<SourceInput>,
  support: SupportModule,
  options: RunOptions,
): Effect.Effect<RunResult> =>
  Effect.gen(function*() {
    const assembled = assembleRun({ sources, support })
    const attempts = planAttempts(assembled)
    const scenarioResults = yield* Effect.forEach(
      attempts,
      (attempt) =>
        executeScenario(assembled, {
          testCaseId: attempt.testCaseId,
          testCaseStartedId: attempt.testCaseStartedId,
          attempt: attempt.attempt,
        }),
      { concurrency: options.scenarioConcurrency ?? 1 },
    )
    return foldRunResult(assembled, scenarioResults)
  })
