import { defineSimulation } from "../../types.ts"
import { driver } from "./driver.ts"
import { host } from "./host.ts"

export default defineSimulation({
  id: "fluent-worker-redrive",
  description:
    "Drives fluent-runtime post-claim redrive over the real Durable Streams named-consumer pull-wake substrate.",
  host,
  driver,
  coverage: {
    gates: [
      {
        id: "fluent_worker_redrive.host_ran",
        description: "the host drove the fluent worker redrive scenario",
        claim: "spans.exists(s, named(s, \"firegrid.sim.fluent_worker_redrive.host\"))",
      },
      {
        id: "fluent_worker_redrive.consumer_acquire",
        description: "fluent-runtime acquired real Durable Streams named-consumer claims",
        claim: "spans.exists(s, named(s, \"fluent_runtime.worker_redrive.consumer.acquire\"))",
      },
      {
        id: "fluent_worker_redrive.product_before_ack",
        description: "a durable L2 product outcome was appended before the Durable Streams ack",
        claim: "spans.exists(outcome, named(outcome, \"fluent_runtime.store.session.append_event\") && attr(outcome, \"firegrid.session.event.name\") == \"fluent.worker_redrive.l2_outcome\" && spans.exists(ack, named(ack, \"fluent_runtime.worker_redrive.consumer.ack\") && startMs(outcome) <= startMs(ack)))",
      },
      {
        id: "fluent_worker_redrive.release",
        description: "fluent-runtime released/doned the DS claim after ack",
        claim: "spans.exists(s, named(s, \"fluent_runtime.worker_redrive.consumer.release\"))",
      },
      {
        id: "fluent_worker_redrive.materialized_session",
        description: "fluent-runtime materialized committed session facts while driving under claim",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.session.collect\"))",
      },
    ],
    corroborations: [
      {
        id: "fluent_worker_redrive.driver_asserted_product_facts",
        description: "the driver asserted product-visible stream facts and DS wake events",
        claim: "spans.exists(s, named(s, \"firelab.fluent_worker_redrive.driver\"))",
      },
    ],
  },
})

