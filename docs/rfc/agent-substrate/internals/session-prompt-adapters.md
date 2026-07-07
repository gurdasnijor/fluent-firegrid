# 14. Session Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A session is the durable identity of an agent conversation.

This section is the detailed specification for the Session primitive introduced in §6.1.1.

If the underlying agent wire format defines sessions, the substrate SHOULD preserve that adapter session identity.

A session lifecycle MAY include:

```txt
requested
creating
active
closed
failed
loaded
reattached
```

These lifecycle states are substrate states. An adapter may map them to ACP `session/new`, `session/load`, `session/resume`, or to equivalent stdio/HTTP/gRPC/vendor lifecycle operations.

## 14.1 Live Promptability

A session is promptable only when the runtime owns or can obtain a live promptable resource.

A conforming system **MUST** distinguish:

```txt
logical session exists
live session resource exists
```

This is especially important after restart.

Live promptability requires all of:

```txt
durable session identity exists
runtime owns or has reattached a live adapter session
adapter reports the session can accept prompts
runtime owner is not fenced by a newer owner or failed lease
session is not terminally closed, failed, or cancelled
```

A ready launch row, session-ready row, or equivalent client-visible ready state **MUST** be emitted only after the runtime owns a live promptable session for the returned session id. If readiness is historical or imported rather than live, the row **MUST** say so and clients **MUST NOT** treat it as promptable.

Runtime, host, process, heartbeat, launch-stop, or liveness records **MUST NOT** by themselves terminalize the durable session row. They may affect promptability, ownership, recovery, or runtime projections, but the session lifecycle may enter a terminal state only through a domain-valid session terminal record or adapter/profile rule.

## 14.2 Reattach / Load

If an agent protocol supports session load or reattach, the runtime MAY use it to convert a durable session identity back into a live session resource.

Each adapter **MUST** declare one reattach profile:

| Profile | Contract |
| --- | --- |
| `no_reattach_must_fail` | Durable session identity cannot become live after owner loss; prompt dispatch fails with typed not-live state. |
| `load_via_protocol` | Adapter can load/resume by protocol identity and must prove promptability before ready state. |
| `reprovision_replacement` | Runtime can create a semantically valid replacement resource and must record replacement identity/lineage. |
| `supervised_reattach` | Runtime can recover a still-live resource through a supervisor, lease, or provider control plane. |

If no reattach is available, the runtime **MUST** either reprovision a semantically valid replacement under a declared profile or fail with a typed durable failure.

The substrate enforces the same invariant for all profiles: durable session identity is not live promptability proof. A runtime **MUST NOT** silently return stale session handles as promptable.

## 14.3 Session Interface

A session service SHOULD expose semantics equivalent to:

```txt
create_session(launch_id, adapter, spec) -> SessionReady | SessionError
load_session(session_id, adapter, options) -> SessionReady | SessionError
mark_not_live(session_id, reason) -> TerminalOrRecoverableState
stop_session(session_id, reason) -> StopResult
owns_promptable(session_id) -> OwnershipState
```

`create_session` and `load_session` MUST append durable lifecycle records for application-visible outcomes. `owns_promptable` MAY consult process-local live registries, but its result **MUST NOT** be persisted as evergreen truth.

## 14.4 Restart Recovery

After restart, a runtime **MUST** reconstruct session projections from the log before accepting prompt dispatch for existing session ids. For every previously active session, it **MUST** classify the session as:

```txt
reattached_promptable
reprovisioned_promptable
not_live
closed_or_terminal
unknown_pending_check
```

Prompt dispatch is allowed only for promptable classifications. `unknown_pending_check` **MUST** block dispatch until resolved. If a duplicate idempotent launch finds a durable session row but no live promptable owner, the runtime **MUST** return or append a typed not-live result rather than allowing the client to append prompt work against the stale session.

---

# 15. Prompt / Turn Model

A prompt or turn is an agent request inside a session.

ACP's Prompt Turn documentation is one concrete adapter reference for `session/prompt`, `session/update`, tool call updates, stop reasons, and cancellation. This RFC generalizes those concepts to any adapter that can accept a request and emit ordered updates.

In the six-primitive model, a prompt is not a seventh primitive. It is the common interaction where a Client appends Session intent, a Harness yields effects, Tools or Sandboxes may execute work, and the Session records chunks plus terminal state.

Prompt input **MUST** come from an explicit prompt/session intent or an explicit async-to-session bridge decision. Async mailbox, webhook, or subscriber payloads **MUST NOT** be smuggled into a session as hidden prompt text without origin, payload, policy, and current-state validation.

A prompt lifecycle MAY include:

```txt
requested
claimed
active
chunked
completed
failed
cancelled
```

A prompt request record SHOULD include:

```txt
session id
request id
input payload
started/requested timestamp
idempotency key, if applicable
metadata
```

A prompt completion record SHOULD include:

```txt
session id
request id
terminal state
stop reason
error, if any
completion timestamp
```

Streaming updates SHOULD be represented as durable chunk/update records keyed by session and request. Chunk ordering **MUST** be derived from append order or projection cursor within the documented ordering boundary. Timestamps **MUST NOT** be authoritative for chunk order. Sequence numbers or adapter event ids MAY be used for validation, duplicate detection, or presentation, but the stream/projection cursor is the agreement mechanism.

For protocols that represent tool progress as session updates, tool-call and tool-call-update messages **MAY** be persisted as session chunk/update records rather than as a separate durable tool-invocation row family. In that profile, approval linkage and terminal behavior are keyed by `(session id, tool call id)`, and the session chunk cursor remains authoritative for what the client and agent observed.

## 15.1 Prompt Ordering and Idempotency

Prompt dispatch **MUST** follow this order:

```txt
1. prompt intent is durably appended
2. prompt operator reaches live processing
3. prompt operator appends or observes winning claim
4. runtime verifies live promptability
5. adapter performs prompt side effect
6. adapter/runtime appends chunks and required actions as they occur
7. operator appends terminal prompt state
```

Prompt chunks **MUST** be ordered within a prompt. Ordering **MUST** follow append order or projection cursor. Sequence numbers, adapter event ids, and timestamps MAY supplement ordering for validation or diagnostics, but they cannot override the authoritative stream/projection order unless the implementation has documented that those ids are the source of its projection cursor. If the agent wire format can deliver duplicate updates, the adapter **MUST** dedupe them or mark duplicates deterministically.

If a prompt is cancelled while tool progress is in flight, the adapter/runtime **MUST** append a durable prompt terminal and a durable terminal or failed update for any externally visible in-flight tool progress it owns. After the winning prompt terminal is visible, the adapter **MUST NOT** continue streaming session updates for that prompt except duplicate/conflict records allowed by the terminal-winner policy.

Exactly one terminal prompt state **MUST** win for a `(session id, request id)` pair. Prompt terminals follow the first-valid-terminal-wins semantics in §10.7.

Prompt idempotency keys **SHOULD** be scoped to the session and logical request. For duplicate prompt attempts:

```txt
same payload, in progress -> observe existing work
same payload, terminal -> return current terminal result
conflicting payload -> IdempotencyConflict
stale session not live -> typed not-live failure and no new prompt intent for that stale session
```

## 15.2 Rich Prompt Content

The prompt payload SHOULD NOT be restricted to plain text.

Implementations SHOULD support a protocol-neutral content model, such as:

```txt
text
image
file reference
tool result
structured JSON
protocol-specific content block
```

Plain text MAY be a convenience API.

## 15.3 Terminal Completion Guarantees

A prompt terminal record **MUST** include:

```txt
session id
request id
terminal state
completion or failure time
producer/operator identity
causation id or claim id when applicable
```

When terminal state is visible in the projection snapshot, clients **SHOULD** resolve from the snapshot before opening a live subscription. This snapshot-first rule makes terminal completion restart-safe and prevents missed completions.

---

# 16. Agent Protocol Adapter Model

Agent protocols are adapters, not the substrate.

Adapters are the wire-format boundary for the Session and Harness primitives. ACP, stdio, HTTP, gRPC, vendor APIs, MCP-capable agents, and in-process calls are peers at this layer.

A conforming implementation MAY support any number of agent protocols.

Examples:

```txt
ACP
local stdio JSON-RPC
MCP-like tool protocol
HTTP request/response
gRPC streaming
vendor-specific API
in-process function call
serverless invocation
```

An adapter’s job is to translate between:

```txt
durable substrate semantics
protocol-specific wire semantics
```

Adapter-private metadata is request-local extension data. It may carry protocol lineage, load/reattach errors, or adapter feature hints when the wire format needs them, but durable/queryable facts belong in the log. An adapter **MUST NOT** rely on live metadata as the only record of suspension, promptability, lineage, permission state, or restart recovery.

Adapters that use stdio or another single-purpose protocol stream **MUST** keep that stream protocol-clean. Logs, traces, metrics, and diagnostic export **MUST** use separate channels or structured protocol messages so they do not corrupt agent-protocol framing.

## 16.1 Adapter Requirements

An agent adapter SHOULD expose:

```txt
initialize / capability negotiation, if supported
create or load session, if supported
send prompt/request
receive updates
handle tool/permission requests
cancel/stop
close
```

An adapter MUST emit durable records for application-observable events.

An adapter MUST NOT hide meaningful state transitions solely in process memory.

The abstract adapter contract is:

```txt
initialize(config) -> capabilities | AdapterError
create_session(session_spec) -> session_handle | AdapterError
load_session(session_id, options) -> session_handle | AdapterError
send_prompt(session_handle, prompt_request) -> prompt_handle | AdapterError
receive_updates(session_handle, cursor) -> update | EOF | AdapterError
handle_required_action(session_handle, action, resolution) -> action_result | AdapterError
cancel(session_handle, request_id, reason) -> cancel_result | AdapterError
close(session_handle, reason) -> close_result | AdapterError
```

`initialize` **MUST** negotiate capabilities before those capabilities are exposed to clients or middleware. `create_session` **MUST** return an adapter session identity when the agent wire format has one. `load_session` **MUST** either produce a live promptable session handle or return a typed unsupported/not-found/not-live error.

`send_prompt` **MUST** be called only after the prompt intent and claim requirements in §15.1 are satisfied. `receive_updates` **MUST** surface updates in agent wire order or include sequence metadata sufficient to restore order. `cancel` **SHOULD** append or cause a durable cancellation/terminal state when cancellation affects application-visible state.

Adapter errors **SHOULD** distinguish:

```txt
unsupported operation
protocol initialization failed
session not found
session not promptable
prompt rejected
required action failed
transport unavailable
adapter bug / unexpected failure
```

An adapter MAY use protocol-specific transports internally. Transport endpoints, process ids, and handles **MUST NOT** leak into the application client as required prompt inputs.

## 16.2 ACP as Reference Adapter

ACP is one suitable reference adapter protocol because it is well specified. It is not the canonical substrate protocol and is not required by this RFC. ACP defines initialization, session setup/load/resume/close, prompt turns, content blocks, tool calls, cancellation, and transports in its protocol documentation; see §37 for URLs.

```txt
initialize
session/new
session/load
session/prompt
session/update
permission request
session cancel/close
```

Other adapters such as local stdio, HTTP, gRPC, vendor APIs, MCP-capable agents, and in-process agents are equally valid when they satisfy the substrate contracts.

## 16.3 Local stdio Agents

A local stdio agent is a first-class adapter target.

A stdio adapter typically:

```txt
spawns a child process
connects stdin/stdout to a framing protocol
performs protocol initialization
creates/loads session
sends prompts
reads updates
cleans up process resources
```

The process handle, pipes, and PID are live resources. They MUST NOT be treated as durable truth.

The durable log SHOULD record:

```txt
agent launch requested
process/provider provisioned
session created
prompt requested
updates emitted
terminal status
process/session stopped or failed
```

## 16.4 Network Agents

Network agents are also adapter targets.

Examples:

```txt
WebSocket ACP server
HTTP streaming agent
gRPC agent service
remote vendor API
```

Network endpoints are adapter configuration, not application-level truth.

A client SHOULD NOT need to know the network endpoint to prompt an agent through the substrate.

---
