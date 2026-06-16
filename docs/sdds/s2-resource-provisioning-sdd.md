# S2 storage & resource primitives (`effect-s2` / `effect-s2-stream-db`)

Status: **proposal** · Scope: `effect-s2`, `effect-s2-stream-db` · Date: 2026-06-16
Normative contracts:
[`features/effect-s2/resource-spec.feature.yaml`](../../features/effect-s2/resource-spec.feature.yaml),
[`features/effect-s2-stream-db/storage-primitives.feature.yaml`](../../features/effect-s2-stream-db/storage-primitives.feature.yaml)
Primary consumer: [`object-actor-model-sdd.md`](./object-actor-model-sdd.md) (the `effect-s2-durable` engine)

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
5. **No provisioning story.** Basins are created ad hoc (tests set `create_stream_on_append` via an
   init file); no Effect-native reconcile, though S2 has the SDK ops and a declarative CLI.

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
| Declarative basin/stream **spec reconcile** (SDK analog of `s2 apply`) | **effect-s2** | Pure S2 resource management; useful with or without stream-db. |
| `StreamDb.open({ config })` — per-stream `StreamConfig` | **effect-s2-stream-db** | `open` already creates the stream; it just needs to pass config. |
| `StreamDb.list({ keyPrefix })` — enumerate instances | **effect-s2-stream-db** | It owns the `basePath`/key → stream-name mapping. |
| Non-creating `exists` / `openExisting` | **effect-s2-stream-db** | Probes must not materialize a stream as a side effect. |
| **Ordered-log read** (records by `seq_num`) | **effect-s2-stream-db** | Event-log consumers need order the latest-value fold doesn't give. |
| **Checkpoint + trim** (snapshot live set, trim history) | **effect-s2-stream-db** | Bounded replay for persistent streams; surfaces existing `compact`. |
| Which basins, what retention, recovery policy | **effect-s2-durable** | Policy + the durable model; consumes the above via `DurableStore`. |

Principle: **mechanism low, policy high.**

## Proposal

### 1. `effect-s2` — declarative resource reconcile (`resource-spec`)

A small `S2Spec` module: an Effect-native reconciler mirroring `s2 apply`, for bootstrap/runtime
use where the CLI isn't available (tests, server start, CI). A thin fold over existing client ops
— no new transport.

```ts
interface S2Spec {                                  // subset of the CLI JSON; basins + named streams
  readonly basins: ReadonlyArray<{
    readonly name: string
    readonly config?: BasinConfig                   // createStreamOnAppend, defaultStreamConfig, streamCipher
    readonly streams?: ReadonlyArray<{ readonly name: string; readonly config?: StreamConfig }>
  }>
}
const plan:  (spec: S2Spec) => Effect.Effect<S2Plan, S2ClientError, S2Client>   // +/~/= diff (dry-run)
const apply: (spec: S2Spec) => Effect.Effect<S2Plan, S2ClientError, S2Client>   // ensure + reconfigure
```

Non-goal: full CLI parity (no token/ACL). Handles basins + default config + singleton streams —
the "coarse, named" resources.

### 2. `effect-s2-stream-db` — config, enumerate, existence, ordered-read, trim (`storage-primitives`)

**(a) Per-instance stream config.** `open` gains an options arg whose `config` flows into the
`createStream` it already does (default: today's behaviour, basin defaults — backward compatible):

```ts
WorkflowDb.open(id, { config: { retentionPolicy: { ageSecs: 3600 }, deleteOnEmpty: { minAge: 0 } } })
```

Use it for **ephemeral** streams (short retention / `deleteOnEmpty`). **Do not** rely on age
retention for streams that hold *permanent* state mixed with transient records (e.g. an object
actor stream) — that would trim live state; those are GC'd by checkpoint+trim (§2e) instead.

**(b) Enumerate instances.** `open(key)` derives the name deterministically
(`${basePath}/${encode(key)}`); `list` is the inverse — `listStreams({ prefix: basePath })` → strip
`basePath/` → decode back to keys. Discovery of live instances, not name construction:

```ts
WorkflowDb.list()                              // → ReadonlyArray<ExecutionId>  (every live wf/<id>)
ObjectActorDb.list({ keyPrefix: "counter:" })  // structured keys only — all instances of one object
```

"Exists = live" holds **only for ephemeral streams dropped on completion**. A persistent stream's
existence means "it exists," not "there is work" — a `list()` caller must then check for pending
work (the actor engine checks the mailbox head). `includeDeleted` defaults false; callers tolerate
the brief delete-pending window.

**(c) Non-creating existence / read.** A probe must not create the stream:

```ts
ResultDb.exists(id)        // Effect<boolean>           — checkTail, never creates
ResultDb.openExisting(id)  // Effect<Option<Instance>>  — None if the stream doesn't exist
```

**(d) Ordered-log read.** Alongside the latest-value fold, expose records in **`seq_num` order**,
so an event-log consumer can fold a tagged event stream and use `seq_num` as the authoritative
order. (The fold remains the right lens for materialized state; this is the lens for ordered
events.)

```ts
SomeDb.readLog(key)                     // → Stream<{ seqNum: number; record: Encoded }>  (ascending)
SomeDb.readLog(key, { from: cursor })   // resume from a checkpoint cursor
```

**(e) Checkpoint + trim.** Surface the snapshot-and-trim pattern (today's internal `compact`) as a
caller-driven primitive: append a snapshot of the live set at a cursor, then trim records before it.
Bounded by one S2 batch (`MAX_BATCH_RECORDS`); larger snapshots need framing (a follow-up).

```ts
SomeDb.checkpoint(key)            // snapshot live rows + trim history before the cursor
SomeDb.trim(key, cursor)          // explicit trim command (records < cursor)
```

## Consumers (pointer, not a redesign)

- **Services (ephemeral):** one stream per call, GC'd by `drop`/`deleteOnEmpty`/age-retention (§2a);
  results readable via `exists`/`openExisting` (§2c); recovery via `list()` (§2b).
- **The actor engine (persistent objects):** one stream per key as an **ordered event log** —
  reads via §2d, GC via §2e (checkpoint+trim, *not* age-retention), enumeration via §2b. The full
  consumption model (admission, drainer, completion, recovery) is the **actor SDD**; this doc only
  guarantees the primitives it relies on.

Both reach these through the `DurableStore` port, so policy (basin, retention, namespace) is
injected, not hardcoded — and a non-S2 backend can answer the same contract.

## Boundaries (control plane vs data plane)

- **Control plane (static, rare):** basins + default config + singleton streams → declared once via
  `S2Spec.apply` (or `s2 apply`). **Auto-create off** here, so the data-plane stream set is
  intentional and `list()` is trustworthy.
- **Data plane (dynamic, hot):** per-key / per-execution streams → created at runtime by
  `open({ config })`, GC'd by retention/`deleteOnEmpty` (ephemeral) or checkpoint+trim (persistent).

## Non-goals / open questions

- **No token/ACL modeling** in `S2Spec` v1 (the CLI spec doesn't cover it either).
- **`reconfigure` on open** — leaning: `open` only *creates* with config; reconfiguration is
  `S2Spec`/CLI territory (no hot-path reconfigure). Decide before implementing §2a.
- **Multi-basin addressing.** `S2Client` is basin-scoped (one `S2_BASIN`); per-tenant/per-boundary
  basins imply multiple clients or a basin-parameterized client — the tenancy-pass lever, flagged
  not designed.
- **Framed/chunked snapshots** for checkpoints exceeding one batch (§2e) — a follow-up.
- **Encryption (`streamCipher`)** is a basin-level boundary; relevant when user-state basins split
  out (an engine decision).
