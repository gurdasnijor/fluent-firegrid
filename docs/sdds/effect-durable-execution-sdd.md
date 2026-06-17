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
11. **Recoverable ownership should be structural.** Recovery should not depend on a timing-sensitive
    stream-list visibility property once object recovery is production-critical. A basin-level
    append-only owner registry is the preferred recovery source; it records owner keys before their
    first acknowledged `Accepted` event.
12. **Multi-worker safety requires leases and fences.** A single in-process drainer is enough only
    for one worker. Before multi-worker object execution is supported, owner loops must acquire a
    log-backed lease/fence, renew it, self-demote on renewal failure, and write protected records
    with the active token.
13. **Handlers read their own writes.** Inside one object handler, `state.set` / `state.delete`
    should be visible through a local overlay as soon as the write is planned; external visibility,
    completion, and other handlers remain gated on the S2 append acknowledgement and projection
    application.
14. **Actor records are versioned.** Permanent owner streams outlive deploys. Every appended
    ActorEvent must carry a producer schema/runtime version, and a reader that sees a newer
    unsupported version must halt rather than mis-fold it.
15. **Append latency is the dominant primitive cost.** The `InvocationStore` write path should be
    able to coalesce same-turn durable facts into one S2 append batch while preserving per-record
    order and commit semantics.

## Semantic Coverage Target

The runtime is complete when the public API below has one coherent durable meaning across services,
objects, and workflows. Implementation may keep different storage backends for services and objects,
but users should not see partial semantics depending on which authoring surface they choose.

| Area | Target semantics |
|---|---|
| Durable calls | Submit, idempotent admission, attach, poll, result normalization, duplicate handling. |
| Durable steps | `run` memoizes terminal success/failure facts and never re-runs a recorded step. |
| Timers | `sleep` persists timer intent and resumes after process restart. |
| Signals / awakeables / deferreds | External resolution is durable, residency-independent, and safe before or after a waiter parks. |
| State | `state(Table)` supports get/set/delete with replay-safe reads and object-persistent writes. |
| Objects | One exclusive writer per key; shared handlers are concurrent and read-only over user state. |
| Workflows | Run-once `run`, shared signal/query handlers, and durable primitive scope under the workflow id. |
| Recovery | Owner-key registry discovery, replay, recovered-head restart, checkpoint resume, and no re-running completed calls. |
| Checkpoint / trim | Bounded replay, idempotency horizon, Expired result view, and trim only after durable checkpoint. |
| Error / interruption | Success, typed failure, defect, interrupt, timeout/cancel policy, and retry policy are explicit durable outcomes. |
| Observability | Firelab proofs require behavioral assertions plus spans emitted by production code paths. |

The actor-model feature file covers the object/workflow core. The service runtime may keep its
existing `WorkflowDb` implementation while it satisfies the same public primitive semantics. Object
calls must not keep the old inbox/state/roster object topology once their public behavior has moved
to the owner-stream path.

## API Completion Bar

Do not treat the remaining work as horizontal layers. After the first object call slice lands, the
work should be driven by user-visible behavior:

```txt
given object({ handlers })
when client/sendClient/attach/poll/resolveSignal/run/sleep/state/deferred/awakeable are used
then the object behaves like the service primitive model,
but its durable facts live in the object owner stream.
```

API-complete means:

1. **Object calls route safely.** Object call ids have an unambiguous namespace or explicit submit
   kind, service ids cannot decode as object ids, and syntactically valid but unknown object ids
   return `Unknown`/`NotFound` rather than polling forever.
2. **Owner identity is schema-owned.** The owner stream is derived by one reversible owner codec.
   Object name and key cannot collide through delimiter composition.
3. **All object primitives work through the owner stream.** `state`, `run`, `sleep`, `signal`,
   `deferred`, `awakeable`, `attach`, and completion append or read durable owner-stream facts.
   Unsupported-object-primitive failures are removed.
4. **Recovery is product behavior, not a unit test.** Recovery is proven through Firelab over
   S2/S2Lite by restarting runtime scopes over the same streams and driving public APIs. Package
   vitest keeps pure/unit coverage only.
5. **Ingress is residency-independent.** `resolveSignal(callId, ...)` and awakeable/deferred
   resolution route from call id to owner stream and succeed whether or not a process currently
   hosts the call.
6. **The old object topology is gone.** No object call uses `ObjectInboxRow`, `ObjectStateDb`,
   object-path `WorkflowDb`, or object-path `RosterDb` for admission/state/completion/recovery.
7. **Firelab proves the feature without gaps for API semantics.** Remaining `--allow-missing`
   requirements, if any, are explicitly production-hardening requirements such as cross-process
   fencing or large framed checkpoints, not missing public API behavior.
8. **Recovery has an authoritative key source.** Object recovery either uses the append-only owner
   registry or has an explicit short-lived gate proving enumeration safety. The registry is the
   preferred path because it makes acknowledged admission recoverable by construction.

This is closer to a single integrated completion pass than a long sequence of layers. It may still
be reviewed in smaller PRs for risk, but each PR must land a public behavior and delete/replace the
old behavior it supersedes.

## Completion Plan

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

### Immediate Slice A Closeout

The first owner-stream object call path is the right vertical slice, but it is not mergeable as
complete until these correctness details are closed:

- call-id classification cannot be "decode any JSON string as an object id"; object ids need an
  explicit namespace/tag or `submit` needs an explicit object/service kind;
- `attach`/`poll` must distinguish unknown object call ids from pending calls;
- owner-stream path derivation must use one collision-resistant schema-owned owner codec, not
  delimiter-built `obj/${object}/${key}` strings;
- recovery coverage that touches S2 must move from package-local vitest into Firelab.

These are not new layers. They are the identity and observability bar for the public object call
path.

### Object API Completion Batch

Purpose: finish the public object semantics in one cohesive vertical pass.

Drive the batch from these product scenarios:

```txt
sendClient(counter, "acct").add(5)
attach(callId)

client(counter, "acct").methodThatUsesRunAndState(...)
client(counter, "acct").methodThatSleeps(...)
sendClient(counter, "acct").methodThatWaitsForSignal(...)
resolveSignal(callId, "approved", payload)
attach(callId)

restart runtime over same S2Lite basin
attach(callId) / resolveSignal(callId, ...) / next object call
```

Required behavior:

- `DurableExecutionRuntime.submit(... object key ...)` derives the owner stream through schemas.
- Admission CAS-appends `Accepted`; the S2 append ack is the commit point.
- The resident owner projection sees appended records and owns the per-key drainer.
- Existing handler machinery runs with the object-backed active invocation store.
- `state.get` / `state.set` / `state.delete` append/fold owner-stream facts.
- `run` records terminal success/failure facts in the owner stream and never re-runs recorded
  steps.
- `sleep` records timer intent/fired facts and survives restart.
- `signal`, `deferred`, and `awakeable` record durable pending/resolved facts in the owner stream;
  in-memory waiters are best-effort acceleration only.
- `attach`/`poll` are owner-projection views for object calls and roster views for service calls.
- First admission for a cold key records the owner key in an append-only registry before the
  acknowledged `Accepted`; orphan registry entries with empty streams are safe and compactable.
- Recovery folds the owner registry, opens each discovered owner stream from the latest available
  cursor, restarts the recovered pending head, and never re-runs a completed call.
- A handler observes its own state writes through a local overlay, while caller-visible results and
  other handlers observe only acked owner-stream facts.
- The store may batch same-turn facts, but the public operation is not considered durable until S2
  acknowledges the batch.
- Firelab proves these scenarios through public APIs and production spans.

This batch is the real API-completion milestone for `object(...)`.

### Projection Freshness And Performance

The API can be correct with bounded reads in early validation, but the production owner path should
not repeatedly read the whole log. If whole-log reads remain after the object API works, treat them
as a performance debt with a clear owner-loop follow-up:

- maintain `lastAppliedSeqNum` in resident owner projections;
- tail owner streams from a cursor with `readDecoded`/read sessions;
- use `check-tail` before strong projection reads when freshness matters;
- handle S2 command records separately from typed `ActorEvent`s;
- once a worker holds a valid owner lease/fence, serve owner-local strong reads from its projection
  without an extra `check-tail`; non-owner reads still need a freshness boundary.

Do not let this become a separate public runtime or validation-only facade. It is an internal
implementation improvement behind the same object scenarios.

### Workflow Completion

Purpose: expose workflow semantics as an object specialization, not a third runtime.

Required behavior:

- workflow `run` is an exclusive handler admitted at most once per workflow id;
- duplicate workflow start returns an already-started status, not a deduped second run;
- workflow signal/query handlers are shared handlers over the owner projection;
- workflow waits, timers, durable steps, and state reuse the same object primitive facts;
- Firelab includes a long-running workflow scenario with restart and signal ingress.

### Production Hardening

These are important, but they should not block declaring the public object API coherent:

- checkpoint + trim to bound replay, including a checkpoint fingerprint and covered `seq_num` that
  Firelab can verify by fold-reproduction before any trim;
- explicit idempotency/result horizon with `Expired`;
- S2 fencing/leases for multi-process per-key ownership; this is required before any multi-worker
  object execution is advertised, not an optional optimization;
- producer/reader version compatibility: older readers halt on newer unsupported ActorEvent
  versions, and deploy-window checkpointing is pinned to the oldest running compatible version;
- framed/chunked checkpoints beyond a single S2 batch;
- object lifecycle APIs such as `clearAll` / destroy;
- delayed send, cancellation, interruption, timeout/deadline, and richer retry policy;
- durable concurrency guardrails for advanced Effect concurrency inside handlers.

When implemented, checkpointing should still follow the S2-native model: `Checkpointed` as an
ActorEvent, `trim` as an S2 command record, and no trim before durable checkpoint coverage exists.
Checkpoint scheduling should be based on replay freshness, especially tail distance, write
throughput, and projected state size. Off-box snapshotting is not the default; consider it only for
large state classes whose checkpoint serialization would stall the single owner loop.

## Deferred work

These remain explicitly out of the current actor build:

- cross-process per-key leasing/fencing for multi-worker execution;
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
