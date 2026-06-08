import { defineSimulation } from "../../types.ts"
import { driver } from "./driver.ts"
import { host } from "./host.ts"

export default defineSimulation({
  id: "fluent-durable-sleep",
  description:
    "Durable sleep over real fluent-runtime timer machinery (Store.scheduleTurnTimer + Sources.fireDueTurnTimers) against a DurableStreamTestServer: TimerScheduled before park, durable TimerFired source append, idempotent journal-resolved re-drive — no process-local timer.",
  host,
  driver,
  coverage: {
    // Verdict is computed from forge-proof host-substrate spans. A process-local
    // `Clock.sleep` mutation (the feature's red scenario) emits none of these —
    // no scheduled append, no timer source, no durable fire — so it flips red.
    gates: [
      {
        id: "fluent_durable_sleep.scheduled",
        description: "timer intent (TimerScheduled) was durably appended before park",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.turn.timer.schedule\"))",
      },
      {
        id: "fluent_durable_sleep.timer_source",
        description: "the timer source materialized the wake (read journal, fired due timers)",
        claim: "spans.exists(s, named(s, \"fluent_runtime.sources.timer.fire_due\"))",
      },
      {
        id: "fluent_durable_sleep.fired",
        description:
          "a durable TimerFired was appended by the store — distinguishable from a driver-forged observation",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.turn.timer.fire\"))",
      },
      {
        id: "fluent_durable_sleep.durable_write",
        description: "the timer facts were written over real Durable Streams HTTP",
        claim: "spans.exists(s, named(s, \"firegrid.durable_streams.http.request\"))",
      },
    ],
    corroborations: [
      {
        id: "fluent_durable_sleep.host_ran",
        description: "the host ran the schedule -> fire -> idempotent re-drive sequence",
        claim: "spans.exists(s, named(s, \"firegrid.sim.fluent_durable_sleep.host.run\"))",
      },
      {
        id: "fluent_durable_sleep.driver_asserted",
        description: "the driver asserted product-visible timer facts from the turn stream",
        claim: "spans.exists(s, named(s, \"firelab.fluent_durable_sleep.driver\"))",
      },
    ],
  },
})
