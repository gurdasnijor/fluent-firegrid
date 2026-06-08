import { defineSimulation } from "../../types.ts"
import { driver } from "./driver.ts"
import { host } from "./host.ts"

export default defineSimulation({
  id: "fluent-runtime-workbench",
  description:
    "Launches @firegrid/fluent-runtime against firelab's DurableStreamTestServer and verifies Store/Sources/EventIngress commit observable stream facts.",
  host,
  driver,
  coverage: {
    gates: [
      {
        id: "fluent_runtime.store.session_create",
        description: "the launched host created a fluent session stream",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.session.create\"))",
      },
      {
        id: "fluent_runtime.store.wait_register",
        description: "the launched host registered a durable wait before matching",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.turn.wait.register\"))",
      },
      {
        id: "fluent_runtime.ingress.external",
        description: "the launched host ingested an external event through FluentEventIngress",
        claim: "spans.exists(s, named(s, \"fluent_runtime.event_ingress.external\"))",
      },
      {
        id: "fluent_runtime.store.wait_match",
        description: "the launched host matched the registered wait through the store",
        claim: "spans.exists(s, named(s, \"fluent_runtime.store.turn.wait.match\"))",
      },
    ],
  },
})
