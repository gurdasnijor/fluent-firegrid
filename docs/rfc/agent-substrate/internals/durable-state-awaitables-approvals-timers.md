# 19. Durable State

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Durable state is state represented by log records and projections.

A conforming system SHOULD model state changes as append-only facts or state-protocol operations.

State operations MAY include:

```txt
insert
update
delete
patch
append child row
resolve
terminalize
```

A state operation MUST be replayable.

If state updates are compacted or collapsed, the system SHOULD preserve enough history for required audit/replay guarantees or document retention limits.

Durable coordination patterns, including mailbox-like behavior, **MUST** be expressible as state operations plus projection waits when possible:

```txt
state.insert(...)
wait_for(state.changes(query).onInsert)
```

An implementation that introduces a separate mailbox class or coordination abstraction **MUST** justify why composition from canonical state records and projection waits is insufficient. A mailbox abstraction **MUST NOT** become a second source of coordination truth beside the durable log.

Async delivery completion **MUST** be bound to the claimed work identity when claims exist. A bridge that converts async work into session work may ack/fail the async item only after the chosen durable side effect is accepted or the bridge intentionally decides no side effect should occur. Acking before durable prompt/session acceptance risks lost work; failing without recording bridge reason risks invisible retry loops.

Durable state is not the same thing as query storage. SQL tables, search indexes, object archives, and analytics sinks are projection consumers. Their schemas may evolve, lag, or apply backpressure, but those conditions **MUST NOT** redefine the durable record contract or allow writes that bypass the log.

---

# 20. Durable Promises / Awaitables

A durable promise is a wait expressed in the log.

It consists of:

```txt
wait record
completion key
completion record
subscriber or awaiter
```

A durable promise MUST be reconstructable from the log.

A durable promise MUST NOT depend solely on an in-memory promise/future.

An awaitable implementation is a promise-shaped handle over durable waits, keyed by canonical completion keys and resolved by appending matching completion records.

## 20.1 Completion Keys

Completion keys SHOULD be canonical and domain-specific.

Examples:

```txt
session:<session-id>
prompt:<session-id>:<request-id>
tool:<session-id>:<tool-call-id>
timer:<timer-id>
webhook:<delivery-id>
```

Completion keys **MUST** be unique within their declared domain. A domain **MUST** define the tuple that makes the key unique, for example `(session id, request id)` for prompt completion or `(permission id)` for approval resolution.

The completion key namespace **SHOULD** be versioned or typed so unrelated domains cannot accidentally resolve each other.

## 20.2 First Resolution Wins

Durable promises **MUST** follow the first-valid-terminal-wins rule in §10.7 unless a stricter domain policy rejects all duplicates before projection. The winning resolution is the first valid completion record in log order for the completion key. Later completions with the same key:

```txt
same semantic resolution -> MAY be treated as idempotent duplicate
different semantic resolution -> MUST be recorded or surfaced as duplicate/conflict without changing the winner
invalid resolution -> MUST NOT resolve the awaitable
```

## 20.3 Replay Reconstruction

Durable promises **MUST** be reconstructable by replaying wait records and completion records. Reconstructing a promise after restart MUST NOT require an in-memory waiter to have survived.

If a completion record appears before a waiter subscribes, snapshot-first reconstruction **MUST** resolve the wait from the existing completion. If a wait record exists without completion, the awaiter MAY subscribe after the snapshot cursor and wait for future completion.

Cancellation of an awaiter does not delete the durable wait unless the domain appends a durable cancellation record.

Composite waits such as races **MUST** distinguish the composite result from each child awaitable. A race may resolve the composite to the first child completion, but it **MUST NOT** consume, delete, or terminalize the losing child awaitables unless it appends explicit cancellation records for those child keys. Later waiters for a losing child key must still be able to observe that child's eventual completion through the normal snapshot/subscription path.

Composite waits with timeout/timer branches **MUST** define losing-branch
cancellation or suppression semantics. Late child completions remain auditable
and **MUST NOT** mutate the winning composite result except through the
terminal-conflict policy.

---

# 21. Human Approval and Required Actions

Human approval is a durable wait, not a hidden callback.

A required action flow SHOULD be represented as:

```txt
agent/tool requests permission
runtime/middleware appends required-action record
agent turn suspends or waits
external UI observes projection
human/app appends resolution record
runtime resumes or completes the wait
```

ACP's prompt-turn permission request is one adapter-level example of this pattern; the canonical substrate model is the durable permission record/projection described below.

Approval UIs should not need privileged access to agent process memory.

They should observe durable projections and append durable decisions.

## 21.1 Canonical Required-Action Records

A conforming required-action domain SHOULD define these records:

```txt
permission.requested
permission.resolved
permission.timed_out
permission.cancelled
```

`permission.timed_out` and `permission.cancelled` MAY be represented as terminal states of `permission.resolved` if the schema makes the terminal reason explicit. The implementation **MUST** choose one durable representation; timeout MUST NOT exist only as an in-memory failure.

A permission request record **MUST** include:

```txt
permissionId
sessionId
scope
state = requested
requestedAt
```

It **SHOULD** include:

```txt
requestId
toolCallId
options
defaultOptionId
expiresAt
requestedBy
reason
metadata
```

A tool-call-scoped permission **MUST** identify the tool call by the canonical `(session id, tool call id)` tuple. A prompt-scoped fallback permission **MUST** identify the prompt by the canonical `(session id, request id)` tuple. These tuples are the linkage between permission state and the blocked work; implementations **SHOULD NOT** require a separate foreign-key row to associate a permission with the tool call or prompt it gates.

A permission resolution record **MUST** include:

```txt
permissionId
sessionId
state
resolvedAt
```

It **SHOULD** include:

```txt
requestId
toolCallId
selectedOptionId
resolvedBy
resolutionReason
metadata
```

Valid terminal states SHOULD include:

```txt
approved
denied
timed_out
cancelled
failed
```

The abstract lifecycle is:

```txt
requested -> resolved(approved | denied | timed_out | cancelled | failed)
```

An implementation MAY encode terminal states as separate record types or as a `permission.resolved` record with a terminal mode. The projection **MUST** expose a single logical state machine with `requested` as non-terminal and `approved`, `denied`, `timed_out`, `cancelled`, and `failed` as terminal. Permission terminals follow the first-valid-terminal-wins semantics in §10.7.

The projection row **SHOULD** separate lifecycle state from terminal outcome when the schema supports it. For example, a row may expose `state = resolved` and `outcome = approved|denied|timed_out|cancelled|failed`, while the abstract state machine remains `requested -> terminal(outcome)`. Separating state and outcome makes idempotent resolution, conflict reporting, and UI filtering explicit without making a Fireline-specific field name mandatory.

## 21.2 Required-Action Projection Rows

The required-action projection row SHOULD include:

```txt
permissionId
sessionId
requestId?
toolCallId?
scope
state
options
selectedOptionId?
requestedBy?
resolvedBy?
requestedAt
expiresAt?
resolvedAt?
updatedAt
```

The projection **MUST** be rebuildable from permission records. A `requested` permission is the pending/non-terminal row. A pending row becomes terminal only through a durable resolution, timeout, cancellation, or failure record.

## 21.3 Approval Execution Semantics

The approval lifecycle is:

```txt
1. agent/tool/runtime requests permission
2. approval gate appends permission.requested
3. tool call or prompt is suspended
4. approver/client observes pending projection row
5. approver/client appends permission.resolved or equivalent terminal record
6. waiting operator observes terminal state through snapshot/subscription
7. runtime resumes, denies, cancels, or fails the waiting operation
8. runtime appends terminal prompt/tool state affected by the decision
```

The waiting operation **MUST** resume through durable state observation, not a private callback. A process-local notification MAY wake the waiter, but correctness **MUST** derive from the durable permission row.

Timeout behavior is normative: when a permission expires, the system **MUST** append a durable terminal permission state, normally `timed_out` or `denied` with reason `timeout`. The waiting operator then observes that terminal state and appends the corresponding prompt/tool terminal state. A timeout that only fails an in-memory waiter is non-conforming because it cannot be reconstructed after restart.

The timeout terminal record MAY be appended by one of these roles:

| Timeout appender | Invariants |
| --- | --- |
| Approval gate | Gate owns the wait and appends timeout after observing expiry. It must use durable time state or restart-safe timer reconstruction. |
| Timer operator | Timer domain appends a timer completion that causes a permission timeout record. The permission projection must not terminalize until the permission timeout record exists. |
| Permission operator | Dedicated permission worker observes expired requests and appends timeout terminals. It must claim or otherwise dedupe expiry work. |

Whichever pattern is chosen, timeout authority **MUST** be documented, idempotent by permission id, and compatible with first-valid-terminal-wins.

## 21.4 Tool-Call Versus Prompt Approval

The target approval boundary is concrete tool calls. Middleware with scope `tool_calls` SHOULD gate each tool call with its own `toolCallId` and permission id.

Tool-call-scoped approval **MUST** gate real tool calls only. A prompt that does not reach a concrete tool call **MUST NOT** be blocked by tool-call approval middleware merely because the session or prompt exists.

Prompt-level approval is transitional. It MAY be used only when the adapter or protocol cannot intercept concrete tool calls. In that case:

```txt
scope MUST identify that approval is prompt-level fallback
permission row SHOULD reference sessionId and requestId
projection/audit SHOULD make the fallback visible
implementations SHOULD migrate to tool-call gating when interception becomes available
```

Prompt-level fallback **MUST NOT** be described as the primary model when tool-call interception is available.

For ACP-like adapters, one acceptable denial pattern is: the approval resolution selects a reject/deny option, the adapter returns that denial to the agent as the tool result or required-action resolution, the agent marks the tool call failed or declined according to protocol semantics, and the prompt may still complete. This is an adapter contract, not a substrate requirement for every protocol. Protocols with different denial semantics **MUST** document how denied approval maps to tool failure, prompt failure, or continued prompt execution.

## 21.5 Approval Idempotency and Authorization

`permissionId` is the idempotency key for approval resolution. Duplicate identical resolutions are idempotent. Conflicting resolutions after a terminal state **MUST** be rejected or recorded as conflicts without changing the winning terminal state.

Only authorized approvers may append resolution records. Authorization policy MAY depend on tenant, session, tool, scope, requested option, or external identity, but the decision **SHOULD** be auditable.

---

# 22. Timers and Scheduling

Timers are durable waits.

A timer flow SHOULD be represented as:

```txt
timer scheduled record
timer operator observes schedule
timer fired record
awaitable or subscriber resolves
```

Timers MUST be replay/restart safe.

A timer implementation MAY use process-local timers, but the scheduled intent MUST be durable.

On restart, the timer operator MUST reconstruct outstanding timers from the log.

## 22.1 Timer Semantics

A timer schedule record SHOULD include:

```txt
timerId
completionKey
scheduledAt
fireAt or delay
clock source
tenant/namespace
payload or causation metadata
```

A timer fired record SHOULD include:

```txt
timerId
completionKey
scheduledAt
firedAt
operatorId
```

Timer ids or completion keys **MUST** be unique within the timer domain. Duplicate schedule attempts with the same idempotency key **SHOULD** resolve to the existing timer.

Timer cancellation is terminal-order-sensitive. A cancellation appended before
the timer fires **MUST** suppress the fired record and **MUST** be idempotent for
the timer key. A cancellation appended after a durable fired record **MUST NOT**
suppress or rewrite the fired record; it SHOULD report an already-fired terminal
result to the caller or projection.

Timeout-owned timers **MUST** use schedule-discriminated durable keys so a
restart can replay the original timer intent. Restart replay **MUST** preserve
the original fire time or deadline and **MUST NOT** reset timeout duration based
on restart wall-clock time.

## 22.2 Clock Skew, Drift, and Restart

Timer implementations **MUST** document clock assumptions. Distributed implementations SHOULD tolerate clock skew by using one of:

```txt
single authoritative scheduler clock
lease/fencing around scheduler ownership
monotonic clock plus durable wall-clock conversion
late-fire tolerance window
```

Timers SHOULD fire no earlier than `fireAt` according to the scheduler's documented clock. They MAY fire late. Implementations SHOULD document expected drift bounds, for example scheduling granularity, maximum polling interval, and retry delay after restart.

On restart, the timer operator **MUST** replay schedules and completions before firing overdue timers. It **MUST NOT** fire a timer that already has a winning completion/fired record. If multiple timer operators exist, firing **MUST** be protected by durable claim, idempotent completion, or another fencing mechanism.

---
