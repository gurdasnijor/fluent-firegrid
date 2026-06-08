import { defineSimulation } from "../../types.ts"
import { driver } from "./driver.ts"
import { host } from "./host.ts"

export default defineSimulation({
  id: "fluent-acp-conductor-binding",
  description:
    "Drives a real ACP SDK editor-side client over acp.Stream into connectFiregridAcpConductor, backed by makeConductorSessionPortFromRuntime + FluentRuntimeLive; verifies newSession/prompt/cancel become durable fluent-runtime session facts (no faked port).",
  host,
  driver,
  coverage: {
    // Verdict is computed from forge-proof FluentStore substrate spans: those
    // names are emitted ONLY by the host-side store (the driver reads over HTTP
    // and never instantiates it), so their existence proves the conductor's ACP
    // calls reached fluent-runtime — not driver-only assertions.
    gates: [
      {
        id: "fluent_acp_conductor.session_create",
        description: "editor newSession reached FluentStore.createSession through the conductor",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.session.create\"))",
      },
      {
        id: "fluent_acp_conductor.event_append",
        description:
          "editor prompt/cancel reached FluentStore.appendSessionEvent through the conductor",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.session.append_event\"))",
      },
      {
        id: "fluent_acp_conductor.durable_write",
        description: "the conductor binding drove a real durable-streams HTTP append",
        claim: "spans.exists(s, named(s, \"firegrid.durable_streams.http.request\"))",
      },
    ],
    corroborations: [
      {
        id: "fluent_acp_conductor.host_round_trip",
        description: "the host ran the editor->conductor ACP round-trip",
        claim:
          "spans.exists(s, named(s, \"firegrid.sim.fluent_acp_conductor_binding.host.run\"))",
      },
    ],
  },
})
