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

- [`object-actor-model-sdd.md`](./object-actor-model-sdd.md) — the current execution target for
  keyed virtual objects: one schema-addressed actor stream per key, one ordered `ActorEvent` log,
  pure transition plus effectful interpreter.
- [`s2-resource-provisioning-sdd.md`](./s2-resource-provisioning-sdd.md) — the storage/resource
  layer beneath the runtime: `effect-s2`, `effect-s2-stream-db`, stream config, enumeration,
  ordered reads, checkpoints, and deferred provisioning policy.

Supporting executable validation:

- [`packages/firelab`](../../packages/firelab) validates feature requirements against real
  system behavior and OpenTelemetry evidence.
- `pnpm --filter firelab validate:proofs check effect-s2-durable/object-actor-model --allow-missing`
  reports which actor-model requirements still need proofs.
- `pnpm --filter firelab validate:run effect-s2-durable-object-actor-model --timeout-ms 120000`
  runs the current actor-model validation slice.

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
  object runtime: per-key ActorEvent log + projection + drainer
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
  append/read/list/checkTail/trim/fence/resource operations

S2
  append order, durability, stream lifecycle, trim, retention, seq_num
```

The most important boundary is this:

- object streams are **not** opened as `StreamDb` table instances;
- object streams are read/written as ordered `ActorEvent` logs via `effect-s2.readDecoded` and
  `S2Client.append`;
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

The object runtime folds an ordered `ActorEvent` log into an `ActorSnapshot`. A pure transition
plans `StartCall`, `WakeWaiter`, and `Checkpoint` actions; an effectful interpreter executes those
actions. Recovery replays history without executing historical actions, then starts the recovered
pending head if no resident fiber exists.

Details live in [`object-actor-model-sdd.md`](./object-actor-model-sdd.md).

### Workflows

A `workflow` is not a third runtime. It is an object specialization:

- `run` is an exclusive handler admitted at most once per workflow id;
- `signal` and `query` are shared handlers;
- durable promises, timers, child work, and state are scoped to the workflow's actor log.

This gives workflow semantics without adding another storage topology.

## Settled decisions

1. **One authoritative log per object key.** The object actor stream is one ordered `ActorEvent`
   log. Projection buckets such as pending calls, results, signals, and user state are derived
   views, not independent storage tables.
2. **S2 `seq_num` is the order.** Admission order and replay order come from S2. Do not invent an
   app-level sequence field to recover order from a latest-value projection.
3. **Done is derived.** A call is done iff a `Completed` event exists. There is no mutable inbox
   `status`, no dequeue row, and no atomic result/status transaction to get wrong.
4. **Ingress is an append.** `resolveSignal(callId, ...)` routes from the call id to the owner
   stream and appends a durable event. In-process waiter wakeups are best-effort only.
5. **Call ids self-route through schemas.** A call id carries a schema-decodable owner identity.
   The owner becomes an S2 path only through the owner key codec.
6. **Checkpointing is explicit.** Persistent object streams cannot rely on age retention because
   they hold permanent state. The drainer writes `Checkpointed` and trims only after the checkpoint
   is durable.
7. **Replay is fold-only.** Historical events rebuild the snapshot but do not execute actions.
   Only the recovered head and live-tail events are interpreted.
8. **StreamDb is not a generic event log.** `StreamDb.open` folds `ChangeMessage` rows. The actor
   log uses `readDecoded(ActorEvent)` and actor-specific checkpointing.

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

Each slice should land as a green stacked PR with unit tests and Firelab proof coverage for the
requirements it claims.

### Phase 1 — storage primitives

Status: implemented in the storage layer.

Purpose: give the runtime reliable lower affordances without smuggling policy into them.

- `StreamDb.open(key, { config })` for stream config at creation.
- `StreamDb.list()` for schema-decoded stream enumeration.
- `StreamDb.openExisting()` for non-creating reads.
- `checkpoint`/`trim` for latest-value table streams.
- `effect-s2.readDecoded` for metadata-preserving ordered reads.
- Centralized tracing in `S2Client` and `StreamDb`.

Relevant contract: `storage-primitives`.

### Phase 3a — pure actor core

Status: first actor-runtime slice.

Purpose: implement the deterministic core without touching S2 or the legacy runtime.

- `ActorEvent` schemas.
- reversible `CallId` / owner codecs.
- `ActorSnapshot`.
- pure `transition(snapshot, event) -> [snapshot, actions]`.
- `replay` fold that discards actions.
- `attach`/`poll` projection views.

This phase must not edit `Runtime.ts`, open S2 streams, or call `StreamDb.open`.

### Phase 3b — ActorLog and drainer

Purpose: add the effectful shell over the pure core.

- `ActorLog` over `effect-s2.readDecoded(ActorEvent)` and `S2Client.append`.
- admission as read-projection then CAS append.
- one per-key drainer for exclusive calls.
- durable primitive journal facts as `Journaled` events.
- completion append and derived advance.
- live-tail interpretation only after recovery has folded history.

Recommended internal split:

1. ActorLog read/append/tail/trim with tracing and S2-lite tests.
2. admission protocol with CAS loss and duplicate tests.
3. drainer loop over the pure transition.
4. ingress and waiter wake integration.

### Phase 3c — recovery, checkpointing, and idempotency horizon

Purpose: make the actor runtime restart-safe and replay-bounded.

- boot enumerate keys via `StreamDb.list()` for names only;
- fold each actor stream via `readDecoded(ActorEvent)`;
- start only keys with a pending head;
- write `Checkpointed` snapshots and trim after durability;
- retain completed-call metadata for a horizon;
- return `Expired` after the horizon without re-running;
- prove checkpoint fidelity.

### Phase 3d — shared handlers and workflow specialization

Purpose: finish the object/workflow semantic model.

- shared handlers run concurrently over a snapshot;
- shared handlers cannot mutate user state at the type level;
- shared handlers may append system ingress events;
- workflow `run` is admitted at most once per workflow id;
- workflow signal/query handlers are shared handlers.

### Phase 3e — cutover and deletion

Purpose: move `object(...)` onto the actor runtime and remove the old seams.

Delete or retire:

- `ObjectStateDb` inbox admission as the durable object model;
- separate per-object-call `wf/<executionId>` streams;
- roster dependency for object calls;
- window-2 idempotent guard;
- residency-retry signal tests.

Keep:

- `service(...)` on the ephemeral one-stream-per-call model;
- storage primitives used by service streams;
- public authoring ergonomics.

Definition of done: existing durable/recovery tests pass on the new runtime, Firelab checks the
object-actor feature without `--allow-missing`, and no object call depends on the legacy two-stream
coordination path.

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
