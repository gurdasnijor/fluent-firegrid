# Prior Art / Existing Systems

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

This RFC intentionally borrows from several existing system families while choosing a different center of gravity.

| System | What it contributes | Where this RFC differs |
| --- | --- | --- |
| Anthropic Managed Agents | The six primitive interface set: Session, Orchestration, Harness, Sandbox, Resources, Tools. | This RFC adds stream authority, projection rebuildability, claim-first side effects, restart recovery, and choreography-first observation around those primitives. |
| ACP and other agent protocols | Session, prompt/turn, update, tool-call, permission, cancellation, and load/reattach concepts for protocol adapters. | ACP is one adapter peer among stdio, HTTP, gRPC, vendor APIs, MCP-capable agents, and in-process agents. Protocol concepts do not become substrate truth unless represented in durable records. |
| Restate | Durable execution via a journal, durable steps, idempotent invocations, timers, and observable retries. | Restate centers handler/workflow durable execution. This RFC centers protocol-neutral managed-agent sessions and stream-derived observation, with choreography tools rather than a required workflow function. |
| Temporal | Event-history replay, deterministic workflow code, activities for side effects, timers, signals, retries, and long-running execution visibility. | Temporal is a workflow orchestration engine. This RFC rejects a substrate workflow SDK and instead makes the model own sequencing through durable choreography primitives. |
| NATS JetStream | Persistent streams, replay, live consumption, retention, idempotent stream definition, key/value and object-store patterns. | JetStream is a messaging/streaming substrate. This RFC adds agent session lifecycle, live ownership, promptability, adapter contracts, and terminal state semantics over durable logs. |
| Cloudflare Workflows | Durable multi-step execution, retries, waits for external events/approvals, and managed state. | Cloudflare Workflows is a hosted step/workflow engine. This RFC keeps workflow engines external integrations and specifies the lower-level managed-agent substrate. |

The prior art motivates the split in this document: durable logs and projections provide the recovery/observation substrate; managed-agent primitives define the agent boundary; choreography tools give the model durable control-flow affordances without forcing a developer-authored DAG.

---
