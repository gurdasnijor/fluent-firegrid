import type { Envelope } from "@cucumber/messages"
import { Effect, Stream } from "effect"
import { run, service } from "effect-s2-durable"
import { durableExec, type ScenarioDefinition } from "./scenario.ts"
import { runFeatures } from "./runner-core.ts"
import { stepHost } from "./step-host.ts"
import { RunEnvelopes } from "./streams.ts"
import type { SupportBundle } from "./support.ts"
import type { RunInput, RunResult } from "./types.ts"

/**
 * The run coordinator, as a thin durable service. It runs the pure cucumber core
 * (`runFeatures`) with the durable executor — so step execution crosses the
 * control plane as per-scenario `invoke` commands, each journaled on the scenario
 * object's owner stream (the durable boundary that matters). It then publishes
 * the produced envelopes onto the run's durable event stream as facts, in one
 * journaled `appendBatch` (`run(...)`), so a handler replay re-reads the ack
 * rather than re-appending. The in-memory collection is transient and
 * deterministically rebuilt on replay (step outcomes replay from the journal).
 */

export const makeRunner = (support: SupportBundle, scenario: ScenarioDefinition) => {
  const host = stepHost(support)
  const exec = durableExec(scenario)
  return service({
    name: "cucumber-effect/runner",
    handlers: {
      *run(input: RunInput) {
        const envelopes: ReadonlyArray<Envelope> = yield* Stream.runCollect(runFeatures(input.sources, host, exec))
          .pipe(Effect.map((chunk) => Array.from(chunk)))
        yield* run(
          "publish-envelopes",
          RunEnvelopes.open(input.runId).pipe(Effect.flatMap((stream) => stream.appendBatch(envelopes)), Effect.asVoid),
        )
        const success = envelopes.some((envelope) => envelope.testRunFinished?.success === true)
        return { success } satisfies RunResult
      },
    },
  })
}

export type RunnerDefinition = ReturnType<typeof makeRunner>
