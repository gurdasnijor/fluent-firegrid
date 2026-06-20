import type { Envelope } from "@cucumber/messages"
import { TestStepResultStatus as Status } from "@cucumber/messages"
import { Effect, Stream } from "effect"
import type { Executor } from "../durable/runner-core.ts"
import { runFeatures } from "../durable/runner-core.ts"
import { failOutcome, runStepBody } from "../durable/step-exec.ts"
import { stepHost } from "../durable/step-host.ts"
import type { StepHost } from "../durable/step-host.ts"
import { readSources } from "../durable/sources.ts"
import type { SupportBundle } from "../durable/support.ts"
import { loadProofs, type ProofBlock, type ProofResult, runProof, type SpecWorld, type SqlProofError } from "./proofs.ts"
import { SpecTracing, type WorldServices } from "./runtime.ts"

/**
 * The firegrid executor + entrypoint. Step bodies run in-process against a World
 * shared across the scenario's steps (they key per-scenario state on `this`),
 * each wrapped in the `firegrid.scenario` span so the product durable engine's
 * spans are captured under that scenario's trace. After each scenario the spans
 * are flushed and its `@sql:` proofs are evaluated against chDB.
 */

interface ScenarioContext {
  readonly world: SpecWorld
  readonly proofs: ReadonlyArray<ProofBlock>
}

const makeSpecWorld = (scenarioId: string): SpecWorld => ({
  scenarioId,
  attach: () => Promise.resolve(),
  log: () => Promise.resolve(),
  link: () => Promise.resolve(),
})

const firegridExec = (
  host: StepHost,
  contexts: Map<string, ScenarioContext>,
  results: Array<ProofResult>,
): Executor<SqlProofError, WorldServices> => ({
  beginScenario: (scenario) =>
    loadProofs(scenario).pipe(
      Effect.map((proofs) => {
        contexts.set(scenario.testCaseId, { world: makeSpecWorld(scenario.scenarioId), proofs })
      }),
    ),
  invoke: (scenario, request) => {
    const context = contexts.get(scenario.testCaseId)
    const step = host.resolve(request.stepDefId)
    if (context === undefined || step === undefined) {
      return Effect.succeed(failOutcome(`firegrid: no context/step for ${request.stepDefId}`))
    }
    return runStepBody(step, request, context.world, []).pipe(
      Effect.withSpan("firegrid.scenario", { attributes: { "firegrid.scenario.id": scenario.scenarioId } }),
    )
  },
  endScenario: (scenario) =>
    Effect.gen(function*() {
      const context = contexts.get(scenario.testCaseId)
      if (context === undefined || context.proofs.length === 0) return
      yield* (yield* SpecTracing).flush
      const scenarioResults = yield* Effect.forEach(context.proofs, (proof) => runProof(proof, scenario.scenarioId))
      results.push(...scenarioResults)
      contexts.delete(scenario.testCaseId)
    }),
})

export interface FiregridResult {
  readonly envelopes: ReadonlyArray<Envelope>
  readonly proofs: ReadonlyArray<ProofResult>
}

/** Run firegrid feature files in-process over the real World services, evaluating their `@sql:` proofs. */
export const runFiregrid = (
  paths: ReadonlyArray<string>,
  support: SupportBundle,
): Effect.Effect<FiregridResult, SqlProofError, WorldServices> =>
  Effect.gen(function*() {
    const sources = yield* readSources(paths).pipe(Effect.orDie)
    const host = stepHost(support)
    const contexts = new Map<string, ScenarioContext>()
    const results: Array<ProofResult> = []
    const envelopes = yield* Stream.runCollect(
      runFeatures(sources, host, firegridExec(host, contexts, results), {
        // The `proofs` profile: only run scenarios carrying an `@sql:` trace proof.
        selectScenario: (scenario) => scenario.tags.some((tag) => tag.startsWith("@sql:")),
      }),
    ).pipe(Effect.map((chunk) => Array.from(chunk) as ReadonlyArray<Envelope>))
    return { envelopes, proofs: results }
  })

/** All step statuses across the run (as strings), for asserting a clean pass. */
export const statusesOf = (result: FiregridResult): ReadonlyArray<string> =>
  result.envelopes.flatMap((envelope) =>
    envelope.testStepFinished === undefined ? [] : [String(envelope.testStepFinished.testStepResult.status)])

/** First failing step result (message), if any — for diagnostics. */
export const firstFailure = (result: FiregridResult): string | undefined =>
  result.envelopes
    .flatMap((envelope) => (envelope.testStepFinished === undefined ? [] : [envelope.testStepFinished.testStepResult]))
    .find((r) => r.status !== Status.PASSED && r.status !== Status.SKIPPED)?.message
