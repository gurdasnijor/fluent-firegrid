import { defineSimulation } from "../../types.ts"
import { driver } from "./driver.ts"
import { host } from "./host.ts"

export default defineSimulation({
  id: "fluent-durable-wait",
  description:
    "Drives provider event ingress into a post-claim fluent session authority that materializes waits, commits L2 outcomes, and acks DS wakes.",
  host,
  driver,
  coverage: {
    gates: [
      {
        id: "fluent_durable_wait.host_ran",
        description: "the host drove the durable wait scenario",
        claim: "spans.exists(s, named(s, \"firegrid.sim.fluent_durable_wait.host\"))",
      },
      {
        id: "fluent_durable_wait.intent_before_park",
        description: "wait intent was durably recorded before the park fact",
        claim: "spans.exists(reg, named(reg, \"fluent_runtime.store.turn.wait.register\") && spans.exists(park, named(park, \"fluent_runtime.store.session.append_event\") && attr(park, \"firegrid.session.event.name\") == \"fluent.durable_wait.turn.parked\" && startMs(reg) <= startMs(park)))",
      },
      {
        id: "fluent_durable_wait.provider_ingress",
        description: "provider events became queryable session facts before wake handling",
        claim: "spans.exists(ingress, named(ingress, \"firegrid.sim.fluent_durable_wait.provider_event_ingress\") && attr(ingress, \"firegrid.fluent_durable_wait.delivery_id\") == \"pr-e1\" && hasDescendant(ingress, \"fluent_runtime.store.state_change.append_fenced\") && spans.exists(handle, named(handle, \"firegrid.sim.fluent_durable_wait.session_authority.handle_wake\") && attr(handle, \"firegrid.fluent_durable_wait.delivery_id\") == attr(ingress, \"firegrid.fluent_durable_wait.delivery_id\") && endMs(ingress) <= startMs(handle)))",
      },
      {
        id: "fluent_durable_wait.claim_before_handle",
        description: "the post-claim actor acquires a real DS claim before driving the session authority",
        claim: "spans.exists(claim, named(claim, \"firegrid.sim.fluent_durable_wait.session_authority.claim_acquired\") && hasDescendant(claim, \"fluent_runtime.worker_redrive.consumer.acquire\") && spans.exists(handle, named(handle, \"firegrid.sim.fluent_durable_wait.session_authority.handle_wake\") && attr(handle, \"firegrid.fluent_durable_wait.delivery_id\") == attr(claim, \"firegrid.fluent_durable_wait.delivery_id\") && endMs(claim) <= startMs(handle) && hasDescendant(handle, \"fluent_runtime.sources.wait.match_pending\")))",
      },
      {
        id: "fluent_durable_wait.catchup_match",
        description: "a catch-up event matched the parked review wait",
        claim: "spans.exists(outcome, named(outcome, \"firegrid.sim.fluent_durable_wait.session_authority.wait_outcome\") && attr(outcome, \"firegrid.fluent_durable_wait.delivery_id\") == \"review-e-catchup\" && attr(outcome, \"fluent_runtime.wait.id\") == \"review-posted\" && attr(outcome, \"fluent_runtime.wait.outcome\") == \"matched\" && spans.exists(match, named(match, \"fluent_runtime.store.turn.wait.match\") && attr(match, \"fluent_runtime.wait.id\") == \"review-posted\" && startMs(match) <= startMs(outcome)))",
      },
      {
        id: "fluent_durable_wait.non_match_pending",
        description: "a non-matching provider event left the main wait pending",
        claim: "spans.exists(outcome, named(outcome, \"firegrid.sim.fluent_durable_wait.session_authority.wait_outcome\") && attr(outcome, \"firegrid.fluent_durable_wait.delivery_id\") == \"issue-e-nonmatch\" && attr(outcome, \"fluent_runtime.wait.id\") == \"pr-merged\" && attr(outcome, \"fluent_runtime.wait.outcome\") == \"not_matched\")",
      },
      {
        id: "fluent_durable_wait.match_journaled",
        description: "the matching PR wake resolved and journaled the wait",
        claim: "spans.exists(outcome, named(outcome, \"firegrid.sim.fluent_durable_wait.session_authority.wait_outcome\") && attr(outcome, \"firegrid.fluent_durable_wait.delivery_id\") == \"pr-e1\" && attr(outcome, \"fluent_runtime.wait.id\") == \"pr-merged\" && attr(outcome, \"fluent_runtime.wait.outcome\") == \"matched\" && spans.exists(match, named(match, \"fluent_runtime.store.turn.wait.match\") && attr(match, \"fluent_runtime.wait.id\") == \"pr-merged\" && startMs(match) <= startMs(outcome)))",
      },
      {
        id: "fluent_durable_wait.already_matched_replay",
        description: "redrive served the journaled match instead of selecting the newer satisfying event",
        claim: "spans.exists(outcome, named(outcome, \"firegrid.sim.fluent_durable_wait.session_authority.wait_outcome\") && attr(outcome, \"firegrid.fluent_durable_wait.delivery_id\") == \"pr-e2\" && attr(outcome, \"fluent_runtime.wait.id\") == \"pr-merged\" && attr(outcome, \"fluent_runtime.wait.outcome\") == \"already_matched\")",
      },
      {
        id: "fluent_durable_wait.product_before_ack",
        description: "durable product outcome was written before DS ack",
        claim: "spans.exists(product, named(product, \"firegrid.sim.fluent_durable_wait.session_authority.product_outcome_append\") && attr(product, \"firegrid.fluent_durable_wait.delivery_id\") == \"pr-e1\" && hasDescendant(product, \"fluent_runtime.store.session.append_event\") && spans.exists(ack, named(ack, \"fluent_runtime.worker_redrive.consumer.ack\") && endMs(product) <= startMs(ack)))",
      },
      {
        id: "fluent_durable_wait.release",
        description: "the DS claim was released after durable ack",
        claim: "spans.exists(release, named(release, \"firegrid.sim.fluent_durable_wait.session_authority.release_claim\") && attr(release, \"firegrid.fluent_durable_wait.delivery_id\") == \"pr-e1\" && hasDescendant(release, \"fluent_runtime.worker_redrive.consumer.release\") && spans.exists(ack, named(ack, \"fluent_runtime.worker_redrive.consumer.ack\") && endMs(ack) <= startMs(release)))",
      },
    ],
    corroborations: [
      {
        id: "fluent_durable_wait.driver_observed_completion",
        description: "the driver observed the product-visible completion fact and annotated row counts",
        claim: "spans.exists(s, named(s, \"firelab.fluent_durable_wait.driver\") && attr(s, \"firegrid.fluent_durable_wait.completion_observed\") == \"true\")",
      },
    ],
  },
})
