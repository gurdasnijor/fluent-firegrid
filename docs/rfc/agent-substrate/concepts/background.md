# Background

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

This document specifies a stream-first architecture for building agent runtimes, control planes, observation systems, durable workflow primitives, and protocol adapters around a single durable append-only log.

The organizing goal has three pillars:

```txt
Anthropic managed-agent primitives define the substrate interface set.
Streams-as-truth define the substrate semantics.
Choreography-first design defines the application-layer posture.
```

The result is a system in the style of Anthropic's Managed Agents framework, enriched with streams-as-source-of-truth so its primitives become durable, replayable, observable, and restart-safe, while avoiding workflow-orchestration SDKs that put hand-coded control flow above the model.

The system sits in a gap between several existing shapes:

```txt
stateless agent gateways that proxy requests but lose durable session truth
workflow engines that durably execute pre-authored control flow
message logs that replay events but do not own live agent session semantics
session-bound agent runtimes that stream progress but lose ownership on restart
```

This RFC defines the substrate between those shapes: protocol-neutral managed-agent primitives, durable channel/log semantics, explicit live ownership, projection-first observation, and choreography-first application behavior.

The core principle is:

```txt
Durable ordered facts are the source of truth.
Everything else is a projection, live resource, adapter, or operator over that log.
```

A system implementing this RFC provides a way for applications, agents, runtimes, human approvers, webhooks, schedulers, and observability consumers to coordinate by appending and reading durable events. It derives higher-level capabilities from that foundation:

```txt
durable state
materialized projections
live observation
durable promises / awaitables
durable claims
agent session lifecycle
prompt dispatch
human approval
restart-safe waits
resource/sandbox provisioning
transport/protocol adaptation
audit and replay
```

The RFC is intentionally not tied to any specific implementation family, programming language, runtime, effect system, agent protocol, transport, or database. ACP is treated as one possible agent protocol. Local stdio agents, HTTP agents, gRPC agents, MCP-capable agents, in-process agents, and future protocols can all participate through adapters, as long as their observable lifecycle is represented in the durable log.

This document uses normative RFC language. The words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, **MAY**, and **OPTIONAL** are to be interpreted in the usual RFC 2119 / RFC 8174 sense.

---

# Problem Context

Agent systems need more than transport. They need a shared, durable account of what happened.

A user may start an agent session. The runtime may provision a sandbox. The agent may emit streaming updates. A tool call may require human approval. A timer may fire after a day. A peer agent may complete a subtask. A dashboard may reconnect. A host may crash and restart. An audit consumer may replay the entire run.

Without a shared durable log, each of those features tends to create its own coordination mechanism:

```txt
RPC for prompt dispatch
websocket messages for UI updates
database rows for current state
queues for work delivery
ad-hoc tables for approvals
timers for sleeps
logs for audit
side channels for child agents
```

This RFC specifies a single architectural base:

```txt
append durable facts
read durable facts
derive everything else
```

The durable log is the unifying abstraction; it is not a message queue, not RPC, not a database, and not a workflow engine. It is the ordered source of truth from which multiple consumers can replay, observe, and derive current state.

The observation model is stream-derived: append durable state, materialize it into live projections, then subscribe to those projections. The durable stream is the truth; the materialized view is replaceable.

This RFC generalizes that architecture and aligns it with the six managed-agent primitives described by Anthropic: Session, Orchestration, Harness, Sandbox, Resources, and Tools. The six primitives are the organizing surface; streams-as-truth is the substrate contribution; choreography-first is the application-layer rule that the model owns sequence, branching, parallelism, and recovery through durable tools.

---

# Goals

A conforming system should enable:

```txt
1. Stream-first application clients.
2. Durable replay of agent activity.
3. Materialized read models for applications and dashboards.
4. Restart-safe prompt/session/approval/timer flows.
5. Claim-first execution of effectful work.
6. Separation between durable identity and live process ownership.
7. Protocol-neutral agent connectivity.
8. Local and remote agent support.
9. Transport-neutral runtime design.
10. Language-neutral implementation.
11. Choreography-first agent applications where the model owns control flow.
12. Agent-side introspection over durable execution history.
```

The design is deliberately minimal at the core. Higher-level features are derived from the durable log.

---

# Non-Goals

This RFC does not specify:

```txt
- a particular storage engine
- a particular HTTP API
- a particular programming language
- a particular agent protocol
- a particular process supervisor
- a particular schema language
- a particular UI framework
- a particular sandbox technology
- a particular approval UI
- a workflow orchestration SDK or DAG language
```

It also does not require ACP. ACP is a useful reference protocol, especially for session/prompt/update semantics, but this RFC treats it as one possible wire protocol among many.

---
