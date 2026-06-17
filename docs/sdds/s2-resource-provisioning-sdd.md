# S2 storage & resource primitives (`effect-s2` / `effect-s2-stream-db`)

Status: **proposal** · Scope: `effect-s2`, `effect-s2-stream-db` · Date: 2026-06-16
Normative contracts:
[`features/effect-s2-stream-db/storage-primitives.feature.yaml`](../../features/effect-s2-stream-db/storage-primitives.feature.yaml)
Deferred exploration:
[`docs/deferred-features/effect-s2/resource-spec.feature.yaml`](../deferred-features/effect-s2/resource-spec.feature.yaml)
Primary consumer: [`effect-s2-durable-consolidation-sdd.md`](./effect-s2-durable-consolidation-sdd.md) (the `effect-s2-durable` engine)
Top-level runtime SDD: [`effect-durable-execution-sdd.md`](./effect-durable-execution-sdd.md)

This is the **storage/resource layer** beneath the durable engine. It exists on its own because
these primitives are not engine-specific — services, the actor engine, and any other consumer of
`effect-s2-stream-db` use them. The engine consumes them through the `DurableStore` port; this doc
owns the primitives, the actor SDD owns how the engine uses them.

## Context

Scoping the engine's actor model + multi-process pass surfaced gaps in the layers beneath it, all
about **resource lifecycle, ordering, and config**:

1. **No per-stream config.** `StreamDb.open` already `createStream`s (catching `S2Conflict`, so
   it's effectively an ensure) but passes **no config**, so every stream silently inherits the
   basin default. No way to say "retain 1h" vs "infinite," or `deleteOnEmpty`.
2. **No enumeration.** Recovery wants "list the live instances"; S2 has `listStreams({ prefix })`
   natively but `effect-s2-stream-db` doesn't expose it in stream-db terms.
3. **No ordered-log read.** The fold is **latest-value** (a map per key); it does not expose
   records in `seq_num` order. An event-log consumer (the actor engine) needs ordered reads.
4. **No first-class trim/checkpoint.** `compact` (snapshot+trim) exists internally but isn't a
   surfaced, caller-driven primitive.
5. **Provisioning is a policy boundary.** Basins are created outside the hot path (tests set
   `create_stream_on_append` via an init file). An Effect-native reconciler may be useful later,
   but direct `effect-s2` control-plane calls or external S2 tooling are sufficient for this slice.

## What S2 actually gives us (confirmed against `@s2-dev/streamstore` 0.24.1 + docs)

- **`BasinConfig`**: `createStreamOnAppend`, `createStreamOnRead`, `defaultStreamConfig`,
  `streamCipher` (per-basin encryption).
- **`StreamConfig`**: `retentionPolicy` (`{ ageSecs }` | `{ infinite }`), `deleteOnEmpty`
  (`{ minAge }`), `storageClass`, `timestamping`.
- **Resource ops** (in `effect-s2` already): `createBasin`/`ensureBasin`/`reconfigureBasin`/
  `getBasinConfig`, `createStream`/`ensureStream`/`reconfigureStream`/`deleteStream`,
  `listStreams`/`listAllStreams` (both take `{ prefix }`), `checkTail`, and read/append carrying
  `seq_num`; trim is an explicit command record (or basin/stream age/size retention).
- **Declarative spec** (`s2 apply -f spec.json`): JSON, **basins + explicitly named streams**.
  **CLI-only**, partial-update reconcile (`+`/`~`/`=`). **No prefixes/templates, no tokens.**

Decisive constraint: the declarative spec covers **coarse, named** resources (basins, singleton
streams). It **cannot** express the engine's dynamic, unbounded streams (`obj/<key>`, `wf/<id>`);
those are created at runtime. So provisioning splits into a **control plane** (static, declarative)
and a **data plane** (dynamic, `open`-time).

## Layering decision

| Primitive | Layer | Why |
|---|---|---|
| Control-plane basin/stream provisioning | **effect-s2** or external S2 tooling | Policy/bootstrap concern; no new public reconciler is required in this slice. |
| `StreamDb.open({ config })` — per-stream `StreamConfig` | **effect-s2-stream-db** | `open` already creates the stream; it just needs to pass config. |
| `StreamDb.list()` — enumerate instances | **effect-s2-stream-db** | It owns the schema codec that can invert stream names back to typed keys. |
| Non-creating `openExisting` | **effect-s2-stream-db** | Non-creating opens must not materialize a stream as a side effect. |
| **Ordered event-log read** (records by `seq_num`) | **effect-s2** (`readDecoded`) → **effect-s2-durable** actor-log | Typed decode preserving `seq_num`/metadata; the actor-log is schema-owned. NOT a stream-db lens — stream-db owns the latest-value projection only. |
| **Checkpoint + trim** (snapshot live set, trim history) | **effect-s2-stream-db** | Bounded replay for persistent streams; surfaces existing `compact`. |
| Which basins, what retention, recovery policy | **effect-s2-durable** | Policy + the durable model; consumes the above via `DurableStore`. |

Principle: **mechanism low, policy high.**

## Proposal

### 1. `effect-s2` / external tooling — control-plane provisioning

Runtime/control-plane basins must be provisioned before recovery, especially when
`createStreamOnAppend` is false. For now that is satisfied by existing `effect-s2`
control-plane operations (`ensureBasin`, `reconfigureBasin`, `ensureStream`, etc.) or external S2
tooling such as `s2 apply`.

A public Effect-native `S2Spec`/`plan`/`apply` reconciler is deferred until there is a concrete
runtime/bootstrap caller. The deferred sketch lives outside active Firelab features so it does not
read as current implementation scope.

### 2. `effect-s2-stream-db` — config, enumerate, non-creating open, projection, trim (`storage-primitives`)

**(a) Per-instance stream config.** `open` gains an options arg whose `config` flows into the
`createStream` it already does (default: today's behaviour, basin defaults — backward compatible):

```ts
WorkflowDb.open(id, { config: { retentionPolicy: { ageSecs: 3600 }, deleteOnEmpty: { minAge: 0 } } })
```

Use it for **ephemeral** streams (short retention / `deleteOnEmpty`). **Do not** rely on age
retention for streams that hold *permanent* state mixed with transient records (e.g. an object
actor stream) — that would trim live state; those are GC'd by checkpoint+trim (§2e) instead.

**(b) Enumerate instances.** `open(key)` derives the name through the owning Effect Schema;
`list()` is the inverse — enumerate streams under the db base path, strip that base path, then
decode back to typed keys. Discovery of live instances, not name construction:

```ts
WorkflowDb.list()                              // → ReadonlyArray<ExecutionId>  (every live wf/<id>)
```

"Exists = live" holds **only for ephemeral streams dropped on completion**. A persistent stream's
existence means "it exists," not "there is work" — a `list()` caller must then check for pending
work (the actor engine checks the mailbox head). `includeDeleted` defaults false; callers tolerate
the brief delete-pending window.

**(c) Non-creating open.** A non-creating read must not create the stream:

```ts
ResultDb.openExisting(id)  // Effect<Option<Instance>>  — None if the stream doesn't exist
```

**(d) Ordered event-log read — NOT a stream-db primitive.** Ordered replay (records in `seq_num`
order) is owned by **`effect-s2.readDecoded`**, which decodes each record to a typed value while
**preserving its S2 metadata** (`seqNum`, `timestamp`, `headers`, `body`). The actor engine folds
that typed, ordered stream as a **schema-owned actor-log in `effect-s2-durable`**; the latest-value
table fold here stays the projection lens for materialized state, never the source of event order
(stateful-execution `LAYERING.6`). Stream-db deliberately does **not** expose a `readLog`/ordered-log
lens — that would conflate the table-projection layer with the event-log layer and would leak
checkpoint/snapshot records as if they were domain events.

```ts
import { readDecoded } from "effect-s2"
// the actor-log LogEntry { seqNum, event } is just a metadata-preserving typed read:
const entries = readDecoded(streamName, ActorEvent, {
  start: { from: { seqNum: from ?? 0 }, clamp: true },
  ignoreCommandRecords: true,
}) // → Stream<{ seqNum; timestamp; headers; body; value: ActorEvent }>
```

**(e) Checkpoint + trim.** Surface the snapshot-and-trim pattern (today's internal `compact`) as a
caller-driven primitive: append a snapshot of the live set at a cursor, then trim records before it.
Bounded by one S2 batch (`MAX_BATCH_RECORDS`); larger snapshots need framing (a follow-up).

```ts
const db = yield* SomeDb.open(key)
yield* db.checkpoint              // snapshot live rows + trim history before the cursor
yield* db.trim(cursor)            // explicit trim command (records < cursor)
```

## Consumers (pointer, not a redesign)

- **Services (ephemeral):** one stream per call, GC'd by `drop`/`deleteOnEmpty`/age-retention (§2a);
  results readable via `openExisting` (§2c); recovery via `list()` (§2b).
- **The actor engine (persistent objects):** one stream per key as an **ordered event log** —
  reads via §2d, GC via §2e (checkpoint+trim, *not* age-retention), enumeration via §2b. The full
  consumption model (admission, drainer, completion, recovery) is the **actor SDD**; this doc only
  guarantees the primitives it relies on.

Both reach these through the `DurableStore` port, so policy (basin, retention, namespace) is
injected, not hardcoded — and a non-S2 backend can answer the same contract.

## Boundaries (control plane vs data plane)

- **Control plane (static, rare):** basins + default config + singleton streams → provisioned via
  direct `effect-s2` operations or external S2 tooling. **Auto-create off** here, so the data-plane
  stream set is intentional and `list()` is trustworthy.
- **Data plane (dynamic, hot):** per-key / per-execution streams → created at runtime by
  `open({ config })`, GC'd by retention/`deleteOnEmpty` (ephemeral) or checkpoint+trim (persistent).

## Non-goals / open questions

- **`reconfigure` on open** — leaning: `open` only *creates* with config; reconfiguration is
  control-plane territory (no hot-path reconfigure). Decide before implementing §2a.
- **Multi-basin addressing.** `S2Client` is basin-scoped (one `S2_BASIN`); per-tenant/per-boundary
  basins imply multiple clients or a basin-parameterized client — the tenancy-pass lever, flagged
  not designed.
- **Framed/chunked snapshots** for checkpoints exceeding one batch (§2e) — a follow-up.
- **Encryption (`streamCipher`)** is a basin-level boundary; relevant when user-state basins split
  out (an engine decision).
