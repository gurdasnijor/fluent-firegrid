# 37. References

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

* Anthropic Engineering, “Scaling Managed Agents: Decoupling the brain from the hands,” which motivates the six primitive framing used by this RFC: https://www.anthropic.com/engineering/managed-agents.
* Durable Streams Protocol draft, which specifies an HTTP-based protocol for durable append-only byte streams with replay, live reads, offsets, closure, and idempotent producer semantics.
* Fireline “The Log” canon concept, which states that the durable log is the unifying abstraction and distinguishes the log from queues, RPC, databases, and workflow engines.
* Fireline Observation Model, which defines observation as durable stream append -> materialized live collections -> subscription.
* Fireline Durable Promises, which defines awakeables as promise-shaped durable waits over completion keys, not a separate workflow engine.
* Fireline Durable Streams canon, which defines append/replay/project/resume, offset-as-cursor, append-order authority, and projections as derived views: `vault/canon/concepts/durable-streams.md`.
* Fireline Durable Channels canon, which defines sync `spawn`/`call` over awakeables and async state inserts plus `state.changes` waits as durable channel modes over streams: `vault/canon/concepts/durable-channels.md`.
* Fireline Sessions and ACP, which defines the relationship between ACP sessions, prompt identity, session/update, durable pause/resume, and canonical identifiers.
* Historical Fireline managed-agents mapping, retained as reasoning evidence and not as current implementation status truth: `vault/explorations/managed-agents-mapping.md`.
* Fireline choreography vs. orchestration canon, which defines Fireline's choreography-first application-layer posture and rejects workflow orchestration SDKs as the substrate model: `vault/canon/concepts/choreography-vs-orchestration.md`.
* Fireline runtime stream-first cleanup SDD: `vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md`.
* Runtime architecture codebase inventory: `vault/explorations/runtime-architecture-codebase-inventory-2026-04-30.md`.
* Runtime cleanup sequencing draft: `vault/explorations/runtime-cleanup-sequencing-draft-2026-04-30.md`.
* Agent 1 direct ACP escape hatch inventory: `vault/explorations/agent1-direct-acp-escape-hatch-inventory-2026-04-30.md`.
* Agent 2 runtime launch/prompt/restart callgraph: `vault/explorations/agent2-runtime-launch-prompt-restart-callgraph-2026-04-30.md`.
* Agent 3 conductor side-cache projection audit: `vault/explorations/agent3-conductor-side-cache-projection-audit-2026-04-30.md`.
* Agent 4 TS client schema/replay audit: `vault/explorations/agent4-ts-client-schema-replay-audit-2026-04-30.md`.
* Agent 5 CLI/topology/host wiring inventory: `vault/explorations/agent5-cli-topology-host-wiring-inventory-2026-04-30.md`.
* Agent 6 examples/tests blast-radius audit: `vault/explorations/agent6-examples-tests-blast-radius-2026-04-30.md`.
* Agent 7 provider/sandbox/transport boundary audit: `vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md`.
* Tool attachment canon: `vault/canon/concepts/tool-attachment.md`.
* Fireline tool publication path canon: `vault/canon/concepts/fireline-tool-publication-path.md`.
* Client middleware API canon: `vault/canon/api/client-middleware.md`.
* Middleware composition canon: `vault/canon/concepts/middleware-composition.md`.
* Mailbox plane canon: `vault/canon/concepts/mailbox-plane.md`.
* Mailbox client API canon: `vault/canon/api/mailbox-client.md`.
* Mailbox MCP tools API canon: `vault/canon/api/mailbox-tools.md`.
* Live ACP metadata contract canon: `vault/canon/protocols/meta-fireline.md`.
* SQL persistence canon: `vault/canon/concepts/sql-persistence.md`.
* Fireline verification canon: `vault/canon/verification.md`.
* Fireline Quint model canon: `vault/canon/verification/quint-models.md`.
* Fireline architecture drift guards canon: `vault/canon/verification/architecture-drift-guards.md`.
* Oxide RFD 60, “Storage Architecture Considerations,” used as a system-RFD structure reference: https://rfd.shared.oxide.computer/rfd/0060.
* Oxide RFD 63, “Network Architecture,” used as a system-RFD structure reference: https://rfd.shared.oxide.computer/rfd/0063.
* Restate durable execution concepts: https://docs.restate.dev/concepts/durable_execution/.
* Temporal durable execution overview: https://temporal.io/.
* NATS JetStream concepts: https://docs.nats.io/nats-concepts/jetstream.
* Cloudflare Workflows overview: https://developers.cloudflare.com/workflows/.
* ACP Initialization: https://agentclientprotocol.com/protocol/initialization.
* ACP Session Setup, including `session/new`, `session/load`, `session/resume`, `session/close`, and `sessionId`: https://agentclientprotocol.com/protocol/session-setup.
* ACP Prompt Turn, including `session/prompt`, `session/update`, `toolCallId`, stop reasons, permission request references, and cancellation: https://agentclientprotocol.com/protocol/prompt-turn.
* ACP Content Blocks: https://agentclientprotocol.com/protocol/content.
* ACP Tool Calls: https://agentclientprotocol.com/protocol/tool-calls.
* ACP Transports: https://agentclientprotocol.com/protocol/transports.
