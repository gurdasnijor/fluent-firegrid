import { defineSimulation } from "../../types.ts"
import { driver } from "./driver.ts"
import { host } from "./host.ts"

export default defineSimulation({
  id: "fluent-acp-client-binding",
  description:
    "Drives FiregridAcpClient through a real ACP process stream and verifies fluent-runtime Store L1/L2 facts.",
  host,
  driver,
  coverage: {
    gates: [
      {
        id: "fluent_acp_client_binding.host_ran",
        description: "the firelab host drove the ACP client binding scenario",
        claim: "spans.exists(s, named(s, \"firegrid.sim.fluent_acp_client_binding.host\"))",
      },
      {
        id: "fluent_acp_client_binding.store_session_create",
        description: "the host created the fluent session stream",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.session.create\"))",
      },
      {
        id: "fluent_acp_client_binding.store_session_append",
        description: "the host persisted ACP callback facts through FluentStore",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.session.append_event\"))",
      },
      {
        id: "fluent_acp_client_binding.durable_http",
        description: "the host used Durable Streams HTTP while persisting facts",
        claim: "spans.exists(s, named(s, \"firegrid.durable_streams.http.request\"))",
      },
    ],
    corroborations: [
      {
        id: "fluent_acp_client_binding.driver_asserted_store",
        description: "the driver asserted product-visible L1/L2 facts from the session stream",
        claim: "spans.exists(s, named(s, \"firelab.fluent_acp_client_binding.driver\"))",
      },
    ],
  },
})
