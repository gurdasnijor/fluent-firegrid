# Effect S2 durable execution runtime

Status: **canonical planning SDD** · Scope: `effect-s2-durable` · Date: 2026-06-16

This is the top-level design document for the durable execution runtime in
`packages/effect-s2-durable`. It replaces the older Durable Streams-oriented
`effect-durable-execution` design as the canonical entry point for this repository.

The detailed contracts remain in the feature files and layer SDDs linked below. This doc owns the
runtime map, semantic target, build order, and cross-layer guardrails so agents do not have to infer
the whole plan from several lower-level documents.

## Canonical references

Normative contracts:

- [`features/effect-s2-durable/object-actor-model.feature.yaml`](../../features/effect-s2-durable/object-actor-model.feature.yaml)
  — virtual-object actor model, admission, execution, completion, ingress, recovery,
  checkpointing, workflow specialization.
- [`features/effect-s2-stream-db/storage-primitives.feature.yaml`](../../features/effect-s2-stream-db/storage-primitives.feature.yaml)
  — storage primitives consumed by services and the actor runtime.

Narrative sub-SDDs:

- [`effect-s2-durable-consolidation-sdd.md`](./effect-s2-durable-consolidation-sdd.md) — the active
  implementation correction: keep `DurableExecutionRuntime` as the only runtime and implement
  object calls as S2-native owner streams with resident projections and an object-backed
  `InvocationStore`.
- [`s2-resource-provisioning-sdd.md`](./s2-resource-provisioning-sdd.md) — the storage/resource
  layer beneath the runtime: `effect-s2`, `effect-s2-stream-db`, stream config, enumeration,
  ordered reads, checkpoints, and deferred provisioning policy.

Supporting executable validation:

- [`packages/firelab`](../../packages/firelab) validates feature requirements against real
  system behavior and OpenTelemetry evidence.
- `pnpm --filter firelab validate:proofs check effect-s2-durable/object-actor-model --allow-missing`
  reports which actor-model requirements still need proofs.

## Purpose

`effect-s2-durable` is the Effect-native durable execution runtime over S2. It provides the public
authoring surface:

```txt
service(...)
object(...)
workflow(...)

client(...)
sendClient(...)
attach(...)
poll(...)
resolveSignal(...)

run(...)
sleep(...)
state(...)
signal(...)
awakeable(...)
deferred(...)
```

The package should feel close to Restate's semantic model, but it should not clone Restate's
TypeScript SDK internals. Effect already gives us typed effects, schemas, layers, scopes, retries,
fibers, streams, and test tools. The runtime should spend its complexity budget on durable
semantics: replay, single-writer object execution, durable primitive journals, ingress,
checkpointing, recovery, and validation.

## Layer map

```txt
user code
  service(...), object(...), workflow(...), run/sleep/state/signal primitives

effect-s2-durable
  public authoring API
  service runtime: ephemeral one-stream-per-call execution
  object runtime: per-key S2 owner stream + resident projection + drainer
  workflow runtime: object specialization with run-once admission
  durable primitive semantics: run, sleep, signal, deferred, state, attach, poll
  DurableStore port and recovery/checkpoint policies

effect-s2-stream-db
  schema-owned stream keys
  ChangeMessage latest-value table projection
  service execution streams
  stream enumeration by key codec
  table transactions and table checkpoint/trim primitives

effect-s2
  typed S2 client
  readDecoded preserving seq_num and metadata
  append/read/read-session/list/checkTail/trim/fence/resource operations

S2
  append acknowledgement, seq_num order, read cursors, check-tail, command records, durability
```

The most important boundary is this:

- object streams are **not** opened as `StreamDb` table instances;
- object streams are S2 owner streams: typed `ActorEvent` records plus S2 command records for
  stream-control operations such as trim/fence;
- resident owner loops maintain projections from ordered reads/tails and track `lastAppliedSeqNum`;
- `check-tail` is the freshness boundary before serving strong projection views;
- `effect-s2-stream-db` remains the latest-value `ChangeMessage` projection layer for service
  streams and reusable table-fold mechanics;
- schema-owned codecs derive stream identity. Hand-built path parsing is not part of the model.

## Runtime taxonomy

### Services

A `service` is stateless from the caller's perspective. Each call uses an ephemeral execution stream
that can be dropped after the result is retained long enough for `attach`/`poll` and idempotency.
Services continue to use `effect-s2-stream-db`'s `ChangeMessage` table projection where it is the
right fit: one stream per call, latest-value facts, result retention, and recovery enumeration.

### Objects

An `object` is a keyed actor. Each object key maps through the owning Effect Schema codec to one S2
stream. That stream is the single system of record for:

- accepted exclusive calls;
- per-call durable primitive facts;
- signal/timer/deferred ingress;
- completed results and idempotency metadata;
- persistent user state changes;
- checkpoints.

The object runtime maintains a resident owner projection from the S2 stream. Historical reads fold
records without executing actions; the live owner loop tails new records, drives the pending head,
and serves `InvocationStore` point reads from the projection. Recovery replays from the latest
checkpoint cursor, then starts the recovered pending head if no resident fiber exists.

Implementation details live in
[`effect-s2-durable-consolidation-sdd.md`](./effect-s2-durable-consolidation-sdd.md).

### Workflows

A `workflow` is not a third runtime. It is an object specialization:

- `run` is an exclusive handler admitted at most once per workflow id;
- `signal` and `query` are shared handlers;
- durable promises, timers, child work, and state are scoped to the workflow's actor log.

This gives workflow semantics without adding another storage topology.

## Settled decisions

1. **One authoritative S2 owner stream per object key.** The object stream contains typed
   `ActorEvent` records and S2 command records. Projection buckets such as pending calls, results,
   signals, and user state are derived views, not independent storage tables.
2. **S2 `seq_num` is the order.** Admission order and replay order come from S2. Do not invent an
   app-level sequence field to recover order from a latest-value projection.
3. **Append acknowledgement is the commit point.** Admission, state mutation, primitive journal,
   ingress, and completion are not durable until S2 acknowledges the append and assigns `seq_num`.
4. **Done is derived.** A call is done iff a `Completed` event exists. There is no mutable inbox
   `status`, no dequeue row, and no atomic result/status transaction to get wrong.
5. **Ingress is an append.** `resolveSignal(callId, ...)` routes from the call id to the owner
   stream and appends a durable event. In-process waiter wakeups are best-effort only.
6. **Call ids self-route through schemas.** A call id carries a schema-decodable owner identity.
   The owner becomes an S2 path only through the owner key codec.
7. **Checkpointing is explicit.** Persistent object streams cannot rely on age retention because
   they hold permanent state. The owner loop writes `Checkpointed` as an ActorEvent and trims with
   an S2 `trim` command record only after checkpoint coverage is durable.
8. **Command records are not ActorEvents.** S2 `trim`/`fence` records consume sequence numbers and
   appear in reads; the actor read path must filter or handle them separately from typed
   `ActorEvent` decoding.
9. **Replay is fold-only.** Historical events rebuild the snapshot but do not execute actions.
   Only the recovered head and live-tail events are interpreted.
10. **StreamDb is not a generic event log.** `StreamDb.open` folds `ChangeMessage` rows. Object
    owner streams use S2 ordered reads/tails, typed `ActorEvent` decoding, and actor-specific
    checkpointing.

## Semantic coverage target

The runtime should cover these semantics before the old object implementation is removed:

| Area | Target semantics |
|---|---|
| Durable calls | Submit, idempotent admission, attach, poll, result normalization, duplicate handling. |
| Durable steps | `run` memoizes terminal success/failure facts and never re-runs a recorded step. |
| Timers | `sleep` persists timer intent and resumes after process restart. |
| Signals / awakeables / deferreds | External resolution is durable, residency-independent, and safe before or after a waiter parks. |
| State | `state(Table)` supports get/set/delete with replay-safe reads and object-persistent writes. |
| Objects | One exclusive writer per key; shared handlers are concurrent and read-only over user state. |
| Workflows | Run-once `run`, shared signal/query handlers, and durable primitive scope under the workflow id. |
| Recovery | Boot enumeration, replay, recovered-head restart, checkpoint resume, and no re-running completed calls. |
| Checkpoint / trim | Bounded replay, idempotency horizon, Expired result view, and trim only after durable checkpoint. |
| Error / interruption | Success, typed failure, defect, interrupt, timeout/cancel policy, and retry policy are explicit durable outcomes. |
| Observability | Firelab proofs require behavioral assertions plus spans emitted by production code paths. |

The current actor-model feature file covers the object/workflow core. Some broader service and
durable-primitive lifecycle semantics are still implemented by the legacy runtime and should receive
their own feature groups or follow-up feature files as they are moved onto the actor architecture.

## Build order

The active build plan is S2-first and vertical. Do not add horizontal actor helpers unless they are
introduced through a public `object(...)` behavior and delete or disable the corresponding legacy
object path in the same PR.

### Foundation — storage primitives

Status: implemented in the storage layer.

Purpose: give the runtime reliable lower affordances without smuggling policy into them.

- `StreamDb.open(key, { config })` for stream config at creation.
- `StreamDb.list()` for schema-decoded stream enumeration.
- `StreamDb.openExisting()` for non-creating service-stream reads.
- `checkpoint`/`trim` for latest-value table streams.
- `effect-s2.readDecoded` for metadata-preserving ordered reads.
- `S2Client` append/read/check-tail/trim/fence affordances with centralized tracing.

Relevant contract: `storage-primitives`.

### Gate — recovery enumeration visibility

Before object cutover depends on boot enumeration, prove the empirical S2 property:

> A just-created owner stream is visible to the enumeration path soon enough for crash recovery.

This is the `listStreams` / `StreamDb.list()` visibility gate. A silent enumeration miss strands
durable work, so this gate is not optional.

### Slice 1 — vertical object call path

Purpose: move one public object call through the S2 owner-stream path.

Required product path:

```txt
sendClient(counter, "acct").add(5)
attach(callId)
```

Required behavior:

- `DurableExecutionRuntime.submit(... object key ...)` derives the owner stream through schemas.
- Admission CAS-appends `Accepted`; the S2 append ack is the commit point.
- A resident owner loop/projection sees the appended record.
- The existing handler machinery runs with `ActiveInvocation.store = ObjectInvocationStore`.
- `state.get` / `state.set` use the object-backed `InvocationStore`.
- Completion appends `Completed`; `attach(callId)` returns the result from the projection.
- The corresponding legacy object inbox/state/roster path for this behavior is removed or disabled.

### Slice 2 — durable waits and ingress

Purpose: prove that waits are durable owner-stream facts, not resident-fiber facts.

- `signal`, `deferred`, `sleep`, and `attach` record pending journal facts.
- Wake/result facts append to the owner stream.
- In-memory waiter wakeups remain best-effort.
- Recovery folds the owner stream and re-drives the pending head without residency retries.

### Slice 3 — resident projection freshness

Purpose: stop treating bounded whole-log reads as the hot path.

- Resident owner loops maintain `lastAppliedSeqNum`.
- Owner loops follow S2 reads/tails from a cursor.
- Strong `attach`/`poll`/state views use `check-tail` when they need a caught-up projection.
- Command records are filtered or handled separately from typed `ActorEvent` records.

### Slice 4 — checkpoint, trim, and idempotency horizon

Purpose: bound replay using S2-native stream-control operations.

- Write `Checkpointed` as an ActorEvent only after the projection is internally consistent.
- Issue S2 `trim` command records only after durable checkpoint coverage exists.
- Tolerate eventual trim visibility during replay.
- Retain completed-call metadata for an idempotency horizon and return `Expired` after it.
- Track S2 fencing as the native direction for future cross-process checkpoint/ownership, but keep
  cross-process ownership deferred until every protected writer participates in the token protocol.

### Slice 5 — shared handlers and workflow specialization

Purpose: finish the object/workflow semantic model.

- Shared handlers run concurrently over a caught-up snapshot/projection.
- Shared handlers cannot mutate user state at the type level.
- Shared handlers may append system ingress events.
- Workflow `run` is admitted at most once per workflow id.
- Workflow signal/query handlers are shared handlers.

Definition of done: existing durable/recovery tests pass on the S2 owner-stream object path,
Firelab checks the object-actor feature without `--allow-missing`, and no object call depends on the
legacy two-stream/roster coordination path.

## Post-cutover semantic passes

These are not blockers for deleting the object two-stream seam, but they are needed to get closer
to the intended durable-execution semantic envelope:

- **Durable primitive parity:** re-check `run`, `sleep`, `state`, `signal`, `awakeable`, and
  `deferred` under the actor runtime rather than only the service runtime.
- **Invocation lifecycle:** delayed send, cancellation, interruption, timeout/deadline, attach/poll
  status transitions, and terminal retention.
- **Retry/error policy:** retryable vs terminal failures, typed failure encoding, defect encoding,
  and user-visible status normalization.
- **Durable concurrency guardrails:** document and enforce which Effect concurrency forms are
  durable-safe inside handlers, and where named branches or durable scopes are required.
- **Agent-style validation:** a long-running workflow/object scenario with LLM/tool-call shaped
  steps, signals, timers, and restart recovery.

## Deferred work

These remain explicitly out of the current actor build:

- cross-process per-key leasing/fencing;
- object lifecycle APIs such as `clearAll` / destroy;
- framed or chunked checkpoints beyond a single S2 batch;
- multi-basin tenancy and encryption policy;
- a public Effect-native S2 resource reconciler beyond existing control-plane operations.

## Process guardrails

- Feature YAML is the contract; SDD prose explains it.
- Firelab proofs should map directly to feature requirement ids.
- Evidence spans must come from production code paths, not validation-only instrumentation.
- When a lower layer already exposes an affordance, use it. Do not reimplement pagination, stream
  listing, key decoding, or tracing in the durable layer.
- If a new abstraction is needed, first state which feature requirement it satisfies and which layer
  owns it.
- Prefer narrow spec edits over new docs when a decision is already settled by a normative feature
  requirement.
