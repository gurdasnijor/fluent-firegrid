# Effect S2 durable consolidation SDD

Status: **active correction** · Scope: `effect-s2-durable` · Date: 2026-06-16

This SDD reframes the object actor work around S2's native guarantees:

> Keep `DurableExecutionRuntime` as the only runtime. Implement object calls as S2-native owner
> streams: durable append acknowledgements, ordered reads/tails, `check-tail`, conditional appends,
> command-record trim/fence, and resident projections.

This SDD supersedes the removed `object-actor-model-sdd.md` narrative and the abandoned horizontal
`actor/*` runtime path. Do not preserve or extend direct `Actor.admit` / `Actor.drain` product
paths, validation-only facades, or a public `Actor` namespace. Reintroduce reusable internals only
when they are wired through `DurableExecutionRuntime` and delete the corresponding legacy object
path in the same PR.

The actor requirements remain specified in:

- [`features/effect-s2-durable/stateful-execution.feature.yaml`](../../features/effect-s2-durable/stateful-execution.feature.yaml)
- [`effect-durable-execution-sdd.md`](./effect-durable-execution-sdd.md)

This document owns the integration shape: how object semantics enter the existing runtime without
adding another public product path or rebuilding weaker coordination above S2.

## Public Boundary

The public API is already settled:

```txt
object(...)
client(object, key).method(input)
sendClient(object, key).method(input)
attach(callId)
poll(callId)
resolveSignal(callId, ...)
```

Those calls enter:

```txt
service.ts public API
  -> DurableExecutionRuntime
  -> ActiveInvocation
  -> InvocationStore
```

The object owner stream is the S2-backed persistence and coordination mechanism behind
`InvocationStore`. It is not a second runtime, facade, or authoring API.

## Why This Exists

The current object implementation spreads one object call across several durable homes:

```txt
ObjectInboxRow / ObjectStateDb   object admission and state
WorkflowDb                       per-call primitive journal
RosterDb                         completion/result/recovery index
running map / waiters            resident fibers and attach/signal wakeups
object drain loop                per-key single-writer execution
```

That split creates the seams this work should delete:

- completion and inbox advancement are separate facts;
- signal ingress depends on the target invocation being resident in memory;
- recovery reconciles object rows, per-call streams, roster rows, and in-memory state;
- durable primitive facts for one object call live in too many stores.

The replacement should not add a new actor framework. It should move object coordination onto the
S2 stream that already gives durable order, append acknowledgement, read cursors, conditional
append, and stream-control command records.

## S2 Guarantees To Use

The object design should lean on these S2 properties instead of recreating them:

- **Append acknowledgement is the durable commit point.** S2 acknowledges only after records are
  durable and returns the assigned sequence range plus current tail. A runtime must not acknowledge
  admission, state mutation, primitive journal, or completion until the relevant append is acked.
- **S2 `seq_num` is the order.** Admission order, replay order, wait resolution order, and
  checkpoint cursors come from S2 sequence numbers. Do not invent an application sequence field.
- **Reads are ordered from a cursor.** Recovery uses bounded ordered reads; resident owners should
  use a read session / tailing consumer to maintain projection state incrementally.
- **`check-tail` is the cheap freshness boundary.** A projection with `lastAppliedSeqNum` can use
  `check-tail` to determine whether it is caught up before serving a strong `attach`/`poll`/state
  view.
- **Conditional appends are coordination.** `match_seq_num` provides optimistic first-writer-wins
  admission/completion where needed. Fencing tokens provide cooperative write exclusion for
  ownership/checkpoint protocols.
- **Leases are log-backed, not TTL-backed.** Multi-worker ownership requires a lease-renewal fact in
  the owner log plus a cooperative S2 fence token. A worker that cannot renew before lease expiry
  must self-demote; a would-be owner campaigns only after it has observed the prior lease expire.
- **Command records are stream-control facts.** S2 command records currently support `fence` and
  `trim`. They consume sequence numbers and appear in reads, but they are interpreted by S2 and
  must be filtered or handled separately from typed `ActorEvent`s.
- **Trim is explicit, not retention.** Trimming is a command record that moves the trim point.
  Actor checkpoints must make older records redundant before issuing trim.

## Core Model

One object key maps to one owner stream:

```txt
owner key schema
  -> owner stream name
  -> S2 append/read/tail/check-tail/command records
  -> resident projection at lastAppliedSeqNum
  -> InvocationStore projection interface
```

There are two layers that must not be conflated:

```txt
InvocationStore
  primitive/projection interface used by DurableExecutionRuntime

S2 owner stream
  object backend source of truth, ordered by S2 seq_num
```

The interface is shaped around what durable primitives need to ask:

```txt
run/sleep/attach/deferred   have we recorded this primitive fact?
state                       what is the current projected value?
signal                      has this wait been resolved?
complete                    can we record the terminal result?
```

For service calls, those questions may continue to be answered by today's `WorkflowDb` / `RosterDb`
implementation.

For object calls, those questions are answered by the resident owner projection, which is folded
from the owner stream. Writes append records to S2 and become visible to other calls only after the
append ack and subsequent projection application. The running handler may observe its own writes
through a local overlay immediately after the write is planned; completion and external visibility
still wait for the append ack.

```txt
handler code
  -> run/sleep/state/signal/deferred/attach primitives
  -> DurableExecutionRuntime
  -> ActiveInvocation.store
  -> ObjectInvocationStore
  -> owner stream append / resident projection
```

This resolves the apparent tension: `InvocationStore` may expose point-query-shaped operations,
while the object backend remains an ordered log. The point reads are projection views over the
owner stream, not a claim that the source of truth is a latest-value table.

## Owner Stream Records

The owner stream contains two classes of records.

### ActorEvents

Typed application/runtime facts, decoded by `effect-s2.readDecoded(ActorEvent)`:

```txt
Accepted        call admitted, ordered by S2 seq_num
Journaled       durable primitive facts, including replay-stable reads
StateChanged    persistent object state mutation
SignalResolved  durable ingress/wakeup fact
LeaseRenewed    owner-loop lease renewal for multi-worker safety
Completed       result/done fact
Checkpointed    bounded replay and idempotency horizon
```

These are the durable facts folded into the actor projection.

Every ActorEvent carries a producer schema/runtime version. A reader that encounters a newer
unsupported version must stop folding and alarm rather than reinterpret the record. Rolling deploys
must preserve recovery by checkpointing at the oldest compatible running version until all readers
understand the newer event vocabulary.

### S2 command records

S2-interpreted stream-control directives:

```txt
fence           set/clear cooperative fencing token
trim            move trim point after checkpoint coverage exists
```

Command records are not ActorEvents. They consume `seq_num` and are returned to reads, so the actor
read path must handle them intentionally:

```txt
S2 record
  if command record -> stream-control handling / skip from ActorEvent decode
  else              -> decode ActorEvent and fold projection
```

This division is important:

- application semantics stay typed as `ActorEvent`;
- stream ownership/checkpoint control uses native S2 command records;
- checkpoints and trims can share one stream order without pretending a trim is an application
  event.

## Resident Owner Loop

The production shape should not repeatedly read the whole log for every primitive operation.

A resident object owner should maintain:

```txt
owner stream name
read cursor
lastAppliedSeqNum
ActorProjection
local waiter registry
optional fencing/lease metadata
local write overlay for the running handler
```

The loop:

```txt
open/read from checkpoint cursor
fold ActorEvents and command records in seq_num order
tail the stream with a read session
serve InvocationStore reads from the projection
append new ActorEvents for writes
apply appended records as the tail observes them
use check-tail when a caller needs a caught-up strong view
```

Bounded `readDecoded(...).runCollect` remains acceptable for tests, recovery scans, and first
bootstrapping. It should not be the long-term hot-path model for a resident owner.

Once a resident owner holds a valid lease/fence and has applied its own acknowledged writes, it can
serve owner-local strong reads from its projection without a `check-tail` round-trip. Non-owner reads
and reads after uncertain ownership still need a freshness boundary.

## StreamDb Boundary

`effect-s2-stream-db` remains the latest-value `ChangeMessage` projection layer. It is useful for
service streams, schema-owned key enumeration, and reusable latest-value fold mechanics. It is not a
generic ordered event log.

Object streams must not be opened through `StreamDb.open`, because `StreamDb` preloads by decoding
`ChangeMessage` rows and folding latest values. Object streams store `ActorEvent`s and S2 command
records.

If `ActorEvent`s are forced into `StreamDb` records, either:

1. the latest-value fold collapses history and loses admission/replay order; or
2. each event becomes a fake table row, which reimplements an event log inside `StreamDb` while
   still needing ordered reads, CAS admission, actor replay, waits, command-record handling, and
   checkpointing.

The reusable part of `StreamDb` is the latest-value-per-key fold mechanism. The actor projection may
reuse/refactor that mechanism for the `StateChanged` subset. It must not feed object streams through
`StreamDb.open`.

## InvocationStore Shape

The concrete interface should be introduced from the runtime call sites, not frozen from this SDD.
The boundary should follow these rules:

- no delimiter-composed `table: string` + `key: string` identity at the store boundary;
- durable primitive keys must be schema-derived, typed operation identities or opaque encoded keys;
- free primitives keep flowing through `DurableExecutionRuntime`;
- object calls use `ObjectInvocationStore`; services may keep `ServiceInvocationStore`;
- implementation-specific event/log/fence details stay behind the store.

Illustrative shape:

```ts
interface InvocationStore {
  readonly readPrimitive: (
    key: DurablePrimitiveKey,
  ) => Effect.Effect<Option.Option<unknown>, DurableExecutionError>

  readonly writePrimitive: (
    key: DurablePrimitiveKey,
    value: unknown,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly readState: (
    key: DurableStateKey,
  ) => Effect.Effect<Option.Option<unknown>, DurableExecutionError>

  readonly writeState: (
    key: DurableStateKey,
    value: unknown,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly deleteState: (
    key: DurableStateKey,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly complete: (
    exit: DurableInvocationExit,
  ) => Effect.Effect<void, DurableExecutionError>
}
```

`DurablePrimitiveKey`, `DurableStateKey`, and `DurableInvocationExit` are placeholders for
schema-owned runtime identities, not permission to introduce new string encodings.

For object calls, representative translations are:

```txt
writePrimitive(run/sleep/attach/deferred fact) -> append Journaled
readPrimitive(...)                             -> projection lookup over Journaled facts
readState(...)                                 -> projection lookup over StateChanged facts
writeState(...)                                -> append StateChanged
deleteState(...)                               -> append StateChanged delete
complete(...)                                  -> append Completed
```

The write path may coalesce same-turn durable facts into one S2 append batch to amortize append
latency. The batch still preserves record order, and none of its facts are externally durable until
S2 acknowledges the append range.

## Durable Waits

A durable wait is not "a fiber is waiting in memory." It is:

> the owner stream contains a pending journal fact whose wake/result fact is not yet present.

Example signal wait:

```txt
Accepted(call-a)
Journaled(call-a, signal.await "approved")
```

The handler parks. Later:

```txt
resolveSignal(call-a, "approved", payload)
  -> decode callId to owner key
  -> append SignalResolved(call-a, "approved", payload)
  -> best-effort poke resident waiter
```

If the process crashes, recovery folds the owner stream. The projection sees that `call-a` is still
the pending head and that `approved` is now resolved, so the runtime can re-drive the handler.
In-memory waiters are acceleration only.

The same shape applies to:

- `sleep`: journal timer intent, later append timer-fired/resolution fact;
- `run`: journal side-effect result; replay returns the stored result;
- `attach`: journal target invocation, wait until target completion is observable;
- `deferred`: journal promise creation/resolution facts.

The exact wait event vocabulary is still an implementation detail, but it must preserve the
invariant above.

## Admission And Draining

Object submission changes from inbox rows to S2 append/CAS:

```txt
sendClient(object, key).method(input)
  -> DurableExecutionRuntime.submit(... object key ...)
  -> derive owner stream from schema-owned key/callId
  -> check resident projection or bounded read
  -> CAS append Accepted when callId is unknown
  -> append ack returns seq_num
  -> ensure one resident owner loop for that key
  -> return callId
```

The object drainer remains part of the object branch inside `DurableExecutionRuntime`:

```txt
owner loop maintains projection by tailing S2
find pending head from projection
run existing handler machinery with ActiveInvocation.store = ObjectInvocationStore
handler calls existing free primitives
free primitives call DurableExecutionRuntime
ObjectInvocationStore appends Journaled / StateChanged / SignalResolved
on exit append Completed
tail applies Completed and advances the projection
```

This keeps execution semantics in one place while changing where object primitive facts are stored.

## Checkpointing, Trim, And Fencing

Checkpointing should use S2's native stream-control tools instead of inventing an external GC
protocol.

Target shape:

```txt
projection at cursor C
  -> append Checkpointed snapshot/fingerprint ActorEvent
  -> once checkpoint coverage is durable, append trim command record before C
```

Important constraints:

- `Checkpointed` is an ActorEvent because it is application/runtime projection metadata.
- `trim` is an S2 command record because it changes the stream trim point.
- command records appear in reads and consume sequence numbers, so replay must skip or separately
  handle them.
- trimming is eventually consistent, so replay must tolerate older records briefly remaining
  visible after trim.
- trimming cannot precede durable checkpoint coverage.

Fencing is the native direction for future cross-process ownership/checkpoint coordination:

```txt
campaign from a caught-up projection after prior lease expiry
append fence command record with owner token
append LeaseRenewed ActorEvents before lease expiry
perform protected owner/checkpoint writes with that fencing token
self-demote if renewal or token-protected append fails
clear fence when intentionally releasing ownership
```

Fencing is cooperative. Appends that do not specify a fencing token are still allowed by S2, so any
fenced protocol must ensure all protected writers use the expected token. Until that discipline is
implemented and proven, multi-worker object execution must not be advertised.

Checkpoints must be verifiable:

```txt
Checkpointed {
  coveredSeqNum
  projectionFingerprint
  snapshot
}
```

Firelab should be able to fold from a fresh read and reproduce the checkpoint fingerprint before any
trim below `coveredSeqNum` is considered valid. Checkpoint scheduling should be freshness-based
(tail distance, write throughput, state size), not a vague background cleanup. Off-box snapshotting
is not part of the default design; consider it only when object state is large enough that inline
serialization would stall the single owner loop.

## Recovery Registry

Recovery should not depend on a racy "new stream appears in listStreams quickly enough" property.
Use an append-only owner registry stream per basin/namespace as the authoritative set of known
object keys:

```txt
owner-key registry stream
  -> append owner key before the first Accepted for that key
  -> fold registry to discover owner keys
  -> fold each owner stream from checkpoint cursor
  -> start only keys with a pending head
```

This is not the deleted roster. The registry is a monotonic set of owner keys, not a mutable per-call
status/result index. Crash windows are safe:

```txt
crash before registry append              -> nothing was promised
crash after registry, before Accepted     -> orphan key, empty/no-pending owner stream, compactable
crash after Accepted ack                  -> key is discoverable and call is recoverable
```

The caller is acknowledged only after the owner-stream `Accepted` append lands. A duplicate producer
then dedups through the owner projection. `StreamDb.list()` can remain a supporting/debugging
affordance, but it should not be the sole correctness source for boot recovery once object recovery
is production-critical.

## External Lessons

Restate appears simpler because its runtime is already the durable invocation store: `ctx.run`,
`ctx.sleep`, `ctx.get`, `ctx.set`, promises, calls, replay, waits, idempotency, and recovery all
delegate to the Restate runtime. In this repo, S2 provides append/read/seq_num/trim/fence and
`StreamDb` provides latest-value rows. `effect-s2-durable` still needs the primitive-to-S2-fact
adapter.

S2 examples point to the same model:

- long-lived entities/sessions/rooms use one granular stream as the durable timeline;
- resident processes maintain projections from ordered reads/tails;
- append acknowledgements define durable commit points;
- command records provide stream-level control;
- checkpoints bound replay and trims discard covered history;
- leases plus fences coordinate checkpoint or writer ownership, but only if all protected writers
  participate;
- versioned records protect rolling deploys from silent mis-folds;
- batching same-turn writes is the main lever for reducing the per-primitive append tax.

## Vertical Slice Rule

Each implementation PR must replace one public object behavior and delete or disable the old object
machinery for that same behavior.

Acceptable first slice:

```txt
sendClient(counter, "acct").add(5)
attach(callId)
```

That slice must route through `DurableExecutionRuntime` and prove:

- `Accepted` is appended to the object owner stream and acked with an S2 `seq_num`;
- the resident owner loop/projection sees the append;
- the object drainer runs the existing handler machinery;
- `state.get` / `state.set` use `ObjectInvocationStore`;
- `Completed` is appended and applied to the projection;
- public `attach(callId)` returns the result.

The same slice should remove the corresponding old path from object execution. If the old path
cannot be removed, the slice is not vertical enough.

## Slice A Specifics

The first object cutover PR must settle these details explicitly.

### callId routing

`attach(callId)` and `poll(callId)` are load-bearing. Today they receive a plain string and resolve
through `RosterDb`. For object calls, the id must be a schema-owned call id that carries enough
owner identity to derive the owner stream without residency or a side index:

```txt
object call id
  -> decode { owner, method, nonce | idempotencyKey }
  -> encode owner through the object owner-key codec
  -> read the owner projection for Completed/Pending/Expired
```

The by-id APIs branch by id kind:

```txt
object call id  -> owner stream projection
service id      -> existing service roster/result path
```

`service.ts` must mint object ids in this form. Do not encode object identity as the legacy
delimiter string `objectName:key`.

### owner identity threading

The cutover must stop treating `objectKey` as a prebuilt `"name:key"` string. Thread the object
name and typed key through submit/admission, then derive the stream name only by encoding through
the owner schema. This is in scope for Slice A because path identity is part of the public object
call path.

### temporary recovery gap

Slice A may temporarily disable and annotate the existing object boot-recovery tests if it deletes
the old drainer before the S2 owner-stream recovery path is implemented. That gap is acceptable for
one named slice only:

```txt
Slice A: public call + attach through owner stream
Slice B: recovery enumeration and recovered-head restart
```

Do not silently break or leave the old recovery path half-active.

### primitive feature scope

Slice A only has to support the object-handler primitives it proves through the public path. The
initial regression may be state + completion only:

```txt
state.get / state.set / attach / completion
```

`run`, `sleep`, `signal`, `deferred`, and child attach must fail clearly or remain on the legacy path
until their vertical slices land. Do not silently drop capabilities for handlers that typecheck.

### no mergeable horizontal store/executor PR

Do not land an `ObjectLogStore`, `ObjectExecutor`, or similar layer by itself. Introducing the
object-backed store and wiring it into `DurableExecutionRuntime` must happen in the same PR that
replaces a public object behavior and deletes/disables the old machinery for that behavior.

## Deletion Targets

As public object behavior moves to the object-backed `InvocationStore`, delete:

- `ObjectInboxRow`;
- `ObjectStateDb` as the object state store;
- object-path use of per-call `WorkflowDb`;
- object-path use of `RosterDb` for completion;
- object-specific `drainLoop` / `drainOne` / `ensureDrainerLocked`;
- window-2 idempotent guard;
- residency-retry signal behavior.

Services may keep the current `WorkflowDb` / `RosterDb` model until a separate
stateless-execution pass.

## Firelab Rule

Firelab must drive public behavior:

```txt
given object(...)
when sendClient/client/attach/resolveSignal is used
then the public result is correct
and spans show S2 owner-stream-backed object persistence
```

Firelab may inspect owner streams as supporting evidence. It must not call `Actor.admit`,
`Actor.drain`, or a validation-only facade as the product path.

## Review Checklist

Before merging any object actor PR, answer:

1. Which public `object(...)` behavior changed?
2. Which old object code path was deleted or disabled?
3. Do free primitives still flow through `DurableExecutionRuntime`?
4. Is `InvocationStore` a projection interface over durable facts?
5. Are store keys schema-owned rather than delimiter-composed strings?
6. Does the object backend use S2 owner-stream ordering and append acknowledgements?
7. Are command records filtered/handled separately from ActorEvents?
8. Does any fenced protocol ensure all protected writers use the token?
9. Does recovery use the append-only owner registry, or is any temporary stream-list fallback
   explicitly scoped and non-production?
10. Does Firelab avoid direct `Actor.*` product-path calls?

If the PR adds a layer without deleting an old object path, reject it.
