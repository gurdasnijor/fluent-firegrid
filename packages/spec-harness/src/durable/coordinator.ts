import { Effect } from "effect"
import { objectClient, service } from "effect-s2-durable"
import { assembleRun } from "./assembly.ts"
import { foldRunResult, planAttempts } from "./run.ts"
import type { SupportModule } from "./support.ts"
import type { RunInput, RunResult } from "./types.ts"
import type { WorkerDefinition } from "./worker.ts"

/**
 * The stateless durable coordinator — the top-level Cucumber runner. It owns
 * run ordering (meta -> discovery -> support -> testRunStarted -> testCases ->
 * scenarios -> testRunFinished), parses + assembles the plan, routes each test
 * case to a keyed worker object, folds statuses, and returns the canonical
 * ordered `Envelope[]`.
 *
 * Run-level `BeforeAll`/`AfterAll` hooks are not yet executed here (no CCK
 * sample in the current gate uses them); scenario-level `Before`/`After` hooks
 * are assembled into each test case and run by the worker.
 */
export const makeCoordinator = (support: SupportModule, worker: WorkerDefinition) =>
  service({
    name: "cucumber-effect/coordinator",
    handlers: {
      *run(input: RunInput) {
        const assembled = assembleRun({ sources: input.sources, support })
        const concurrency = input.options.scenarioConcurrency ?? 1

        // Mint a testCaseStarted id per case up front, in order, so ids stay
        // deterministic regardless of the execution concurrency below.
        const attempts = planAttempts(assembled)

        const scenarioResults = yield* Effect.forEach(
          attempts,
          (attempt) =>
            objectClient(worker, attempt.attemptKey).runScenario({
              sources: input.sources,
              options: input.options,
              testCaseId: attempt.testCaseId,
              testCaseStartedId: attempt.testCaseStartedId,
              attempt: attempt.attempt,
            }),
          { concurrency },
        )

        return foldRunResult(assembled, scenarioResults) satisfies RunResult
      },
    },
  })

export type CoordinatorDefinition = ReturnType<typeof makeCoordinator>
