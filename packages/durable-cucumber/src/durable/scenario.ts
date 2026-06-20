import { Effect } from "effect"
import { type DurableExecutionError, type DurableExecutionRuntime, object, objectClient } from "effect-s2-durable"
import type { Executor } from "./runner-core.ts"
import { executeStep, failOutcome } from "./step-exec.ts"
import { stepHost } from "./step-host.ts"
import type { SupportBundle } from "./support.ts"
import type { BeginScenarioInput, InvokeRequest, PreparedScenario } from "./types.ts"

/**
 * The per-scenario command handler — the cucumber-wire "connection", as a
 * durable virtual object keyed by scenario-attempt id. Its handlers are the wire
 * commands (`begin` / `invoke` / `end`); the support bundle is captured in the
 * handler closures (a deployment dependency), so on recovery the rebuilt layer
 * re-establishes it. Each `invoke` is the per-step durable boundary: its
 * `StepOutcome` is journaled on the owner stream, so a replay returns the
 * recorded outcome without re-running the body. Step bodies may use `state(...)`
 * (per-scenario, isolated by key) and drive product durable defs.
 */

export const makeScenario = (support: SupportBundle) => {
  const host = stepHost(support)
  return object({
    name: "cucumber-effect/scenario",
    handlers: {
      // wire `begin_scenario`: lifecycle anchor (Before hooks / firegrid.scenario span land here).
      *begin(_input: BeginScenarioInput) {
        return { ok: true as const }
      },
      // wire `invoke`: resolve the host-owned step id and run the body, journaled per step.
      *invoke(request: InvokeRequest) {
        const step = host.resolve(request.stepDefId)
        if (step === undefined) return failOutcome(`unknown step definition ${request.stepDefId}`)
        return yield* executeStep(step, request)
      },
      // wire `end_scenario`: teardown (After hooks land here).
      *end(_input: { readonly tags: ReadonlyArray<string> }) {
        return { ok: true as const }
      },
    },
  })
}

export type ScenarioDefinition = ReturnType<typeof makeScenario>

const scenarioKey = (scenario: PreparedScenario): string => `${scenario.testCaseId}:0`

/**
 * The durable executor: each wire command is a durable object call to the
 * scenario's command handler (deterministic child id → replay-stable). This is
 * the control plane — `begin`/`invoke`/`end` as commands the durable engine
 * journals.
 */
export const durableExec = (scenario: ScenarioDefinition): Executor<DurableExecutionError, DurableExecutionRuntime> => ({
  beginScenario: (s) =>
    objectClient(scenario, scenarioKey(s)).begin({ scenarioId: s.scenarioId, tags: s.tags }).pipe(Effect.asVoid),
  invoke: (s, request) => objectClient(scenario, scenarioKey(s)).invoke(request),
  endScenario: (s) => objectClient(scenario, scenarioKey(s)).end({ tags: s.tags }).pipe(Effect.asVoid),
})
