# Virtual objects as per-key actor streams

Status: **proposal** (contract for sign-off; no implementation yet) · Scope: `effect-s2-durable`
Date: 2026-06-16
Normative contract: [`features/effect-s2-durable/object-actor-model.feature.yaml`](../../features/effect-s2-durable/object-actor-model.feature.yaml)
Related: [`s2-resource-provisioning-sdd.md`](./s2-resource-provisioning-sdd.md)
Related design input: [`effect-encore-informed-actor-proposal.md`](./effect-encore-informed-actor-proposal.md)

This doc is the **narrative** (why + shape). The testable invariants live in the feature
file; sections below reference them as `object-actor-model.<GROUP>.<n>`.

## Layering: where the actor abstraction lives

The actor abstraction proposed here is an **`effect-s2-durable` runtime abstraction**. It is not a
new user-facing authoring API and it is not a replacement for `effect-s2-stream-db`.

```txt
user API
  service(...), object(...), client(...), sendClient(...), attach(...), poll(...)

effect-s2-durable runtime
  ActorDefinition, ActorOwnerKey schema, ActorCallId schema,
  actor log read model, actor projection, per-key drainer

effect-s2-stream-db
  StreamDb, Table, primaryKey, schema-owned key encoding,
  materialized latest-value table projection, compact/drop

effect-s2 / S2
  append, read, seq_num, trim, stream lifecycle
```

In this doc, an **actor** means:

> one schema-addressed durable object stream plus a deterministic interpreter over that stream.

Concretely:

- `object(...)` still defines the public virtual-object surface.
- `effect-s2-durable` compiles that definition into actor runtime metadata: owner-key schema,
  call-id schema, method schemas, handler mode (`exclusive`/`shared`), result schemas, and state
  table bindings.
- `effect-s2-durable` owns admission, call routing, ordered replay, the per-key drainer,
  completion, shared-handler snapshots, recovery, and signal ingress.
- `effect-s2-stream-db` owns the lower storage mechanics: schema-derived stream keys, table row
  schemas, primary keys, state folding, transactions, and compaction.
- S2 owns append order (`seq_num`), durability, reads, and trim.

So the proposal changes the **internal execution model for virtual objects**. It does not ask users
to write `Actor.fromObject(...)`, construct stream paths, or call a separate `objectStream(...)`
helper.

## Problem

A virtual `object` method is currently split across **two durable streams**:

- an `ObjectStateDb.open(objectKey)` stream — the object's persistent `state(Table)` rows
  **plus** a FIFO `inbox`.
- `wf/<executionId>` — that one call's execution journal (steps / deferreds / result).

Nothing transacts across the two, so each coordination point is a seam:

- **Completed-but-not-dequeued (window-2):** completion is on the `wf`/result side, dequeue on
  the `obj`/inbox side — not atomic, so a crash between them re-runs the head and double-applies.
  (Patched today with an idempotent guard — correct, but a patch over a structural gap.)
- **Residency-dependent ingress:** `resolveSignal(callId, …)` only works once the call is
  forked into the in-process `running` map; a queued call has no durable place to be resolved.
  (Our recovery test passes only because it *retries* the resolve until the call is resident.)

Both are consequences of the two-stream split, not of having a mailbox. We keep the
requirement (durable per-key state + restart-safe exclusivity) and remove the seam.

## Model: the object key schema **is** the durable execution boundary

One stream — opened through the object's `StreamDb` key schema, for example
`ObjectActorDb.open(ObjectActorKey.make(...))` — is the object's whole durable world and
**single system of record** (`SYSTEM_OF_RECORD.1`). The encoded S2 stream path is a codec output,
not a hand-built conceptual API. In-memory fibers/waiters/drainer are cache derived from it, never
required for durability or ingress (`SYSTEM_OF_RECORD.2`). It hosts:

| in-stream table | role |
|---|---|
| `accept-log` | admitted exclusive calls, append-only: `callId` (pk), `method`, `input` — **no status**; admission order is S2 `seq_num` |
| `steps` / `stateReads` / `deferreds` / `clockWakeups` | each call's journal, keyed by `callId` (`${callId}/run/N`, …) — identical to today's per-execution keying |
| `results` | a settled call's `Completed` event: `callId`, an `Exit` (success/failure/interrupt/defect) |
| user `state(Table)` rows | the object's persistent state |

There is **no** separate `wf/<executionId>` stream for object methods and no roster row. A
single **serial drainer** per key runs exclusive calls (`EXECUTION.1`).

> **Read model (`LAYERING.6`).** The table above is the *projection lens*, not the source of
> truth. Engine bookkeeping is **one ordered `ActorEvent` log** — `Accepted | Journaled |
> SignalResolved | TimerFired | Completed | StateChanged | Checkpointed` — consumed by S2
> `seq_num`. The latest-value table fold materializes user state and the result/pending views
> *from* that log; it is not where engine-event order comes from. This supersedes "several
> independent latest-value tables in one stream," and is the resolution of the earlier
> event-log-vs-table question. (That ordered log is read via `effect-s2.readDecoded` — a typed
> decode that preserves `seq_num`/metadata — folded as a schema-owned actor-log in this layer; the
> latest-value table fold, one layer down, is the projection lens, not the source of event order.)

At the `effect-s2-durable` layer, the internal shape is approximately:

```ts
type ActorDefinition<OwnerKey, Methods, StateTables> = {
  readonly name: string
  readonly ownerKey: Schema.Codec<OwnerKey, string>
  readonly methods: Methods
  readonly stateTables: StateTables
}

type ActorInstance<OwnerKey> = {
  readonly owner: OwnerKey
  readonly projection: ActorProjection
  readonly drainer: PerKeyDrainer
}
```

The `ActorInstance` is runtime structure, not storage structure. Its durable source of truth is the
`StreamDb` instance opened from `owner`; its in-memory projection and drainer can be discarded and
recreated from the stream.

### Exclusive vs shared handlers (`HANDLERS`)

Mirroring Restate (*"at most one handler with write access runs at a time per key"* + *"shared
handlers run concurrently, read-only"*), handlers come in two behaviours and only one is
serialized:

- **Exclusive** (default): read **and** write user state; **single-writer per key**. These go
  through the accept-log + serial drainer.
- **Shared**: **concurrent**, and **read-only with respect to user state** — it runs as an
  ephemeral execution over a *snapshot* of the object's materialized state and never enters the
  accept-log. "Read-only" means *no user-state writes*, **not** "no log writes": a shared
  signal/query handler **may append ingress rows** (signal/promise resolutions) to the object
  stream (`HANDLERS.5`). The user-state-write ban is a type-level guard (`HANDLERS.4`).

This is the principled fix for the wedged-key boundary: a parked exclusive handler stalls only
*other writes* — never reads, queries, or signals.

### Done is derived, not stored (`COMPLETION`)

The accept-log carries **no `status` field**. A call is **done iff its `Completed` event exists**;
`pending` = an accept-log entry with no `Completed`. The result is an `Exit`, and the
`attach`/`poll` view **normalizes** to `Pending | Success | Failure | Interrupted | Defect |
Expired` (`COMPLETION.5`) — `Expired` coming from the idempotency horizon (see Checkpointing). So:

- **completion is a single append** (the result row) — there is no dequeue step and nothing to
  make atomic;
- **"advance" is just re-deriving pending**, so a completed call can't be re-run after a crash
  — **window-2 is structurally impossible**, not patched.

> Design note — we deliberately diverge from the "keep a `status` field + atomic
> `transact(result, status=done)`" alternative. That version is self-consistent but
> *reintroduces* an atomicity obligation: a crash between the two writes leaves
> `result ∧ status=pending` and recovery double-applies. Deriving done from result-existence
> removes the obligation entirely. (Caveat → `LIFECYCLE.1`: GC must drop a call's accept-log
> entry + journal + result *together*, else a trimmed result could look pending again.)

### callId self-routes through schemas (`ROUTING`)

`attach`/`poll`/`resolveSignal` take only a `callId` and must find the owner stream **without a
roster or index**. So the callId carries a schema-decodable owner identity: an object call routes
to the object stream by decoding the owner key and opening the owner `StreamDb`; a service call's
callId resolves to its execution stream. Ingress/attach derive the stream from the id alone, but
they do not delimiter-parse a hand-built stream string.

### Ingress is an append (`INGRESS`)

`resolveSignal(callId, name, value)` derives the owner stream from the id and **appends a
`deferreds` row** — durable whether or not the call is resident. An in-process waiter is poked
best-effort; the row is the truth. This dissolves residency-dependent ingress (no
retry-until-resident).

## Consumer surface (unchanged ergonomics)

```ts
const id = yield* sendClient(counter, "k").add(5)            // exclusive: appends an accept-log row, returns callId
yield* attach(id, Schema.Number)                            // reads result row (waits while pending)
yield* resolveSignal(id, "approved", Schema.Boolean, true)  // appends a deferreds row — resident or not
yield* client(counter, "k").value()                         // shared/read-only: concurrent snapshot read,
                                                            // never queues behind a parked exclusive call
```

`client`/`sendClient`/`attach`/`resolveSignal` keep today's shapes; only what they write
changes (object-stream appends, no `wf` + roster). Handlers are exclusive by default; read-only
ones are marked shared at definition (`object.shared(...)`, mirroring Restate).

**Producer-only dispatch (`ADMISSION.6`).** Dispatch is *also* just an append: `sendClient` admits
a call by appending the `Accepted` event to the owner stream **without hosting the per-key
drainer** — the owning drainer (here, or elsewhere) picks it up. So both *dispatch* and *ingress*
are residency-independent appends; only *execution* needs the single owner. That split is what the
multi-process pass leans on (producers anywhere, one leased drainer per key).

## Before / after

```txt
before:  obj/counter:k   (state + inbox)        ← dequeue here
         wf/call-a        (journal + result)    ← completes here   ✗ two streams, non-atomic
         wf/call-b        (journal + result)
         + roster row + running-map residency for ingress

after:   ObjectActorDb.open(key) (state + accept-log + per-call journals + results)
         one serial drainer; done = result exists; ingress = append   ✓ one stream
```

Deletes from the engine: per-method `wf` streams for objects, the `obj`↔`wf` handshake, the
mailbox `status`/dequeue + window-2 guard, residency-gated ingress (and the retry-until-resident
in tests), and the object branch of roster/running coordination.

## Restate simplifying assumptions applied

1. A keyed object has **one authoritative ordered log**; everything for the key flows through
   it (Restate's partition log). No "accepted here, run there, reconciled after."
2. The **accept-log is the durable invocation event** for exclusive calls — ordered records in
   the object log, not a second subsystem.
3. Per-call journals / results / signals live in that **same** log, keyed by `callId`.
4. **In-memory execution is derivative**; the log is truth (so ingress never needs residency).
5. **`callId` routes** to its owner stream.
6. **Shared handlers bypass admission** and cannot mutate user state (but may append signals).

## Machine/statechart design input applied

Effect-smol PR #2351 (`effect/unstable/machine`) is not a durable runtime and not a public API
direction for us, but it contributes useful internal semantics:

1. **Plan before effects — a pure transition + an interpreter (`PLANNING.7–8`).** Ordering,
   parking, resume, completion, and checkpoint-eligibility live in a **pure** transition
   `(snapshot, event) -> (snapshot, action[])`; the drainer's interpreter executes the emitted
   actions only after durable facts exist. The transition decides validity; the interpreter never
   does. Because it's pure, the bulk of runtime behaviour is testable as
   `snapshot + event -> snapshot + actions` — no S2-lite, timers, fibers, or real handlers — which
   is decisive for a system whose hard bugs (window-2, residency) were all replay/ordering.
2. **Snapshot/projection is first-class.** The runtime should expose and checkpoint an
   `ActorSnapshot`/projection at an S2 cursor, not ad hoc internal maps. `attach`/`poll` are views
   over that projection.
3. **Path-aware identity.** Durable rows need a path (`calls/<callId>/run/<name>`,
   `state/<table>/<key>`, `signals/<callId>/<name>`, etc.), not only a row `_tag`. This prevents
   collisions once journals, state, signals, timers, results, and checkpoints share one stream.
4. **Schema boundary errors.** Decode failures should name the boundary (`accepted-input`,
   `journal`, `signal`, `timer`, `state`, `result`, `checkpoint`) and include the path/callId when known.
5. **Scoped invoked work.** Timers, signal waits, child workflows, and spawned/background work must
   have a durable owner scope so checkpoint, trim, recovery, and rerun can determine what is live
   without consulting an in-memory registry.
6. **Idempotent completion transitions.** Any continuation derived from completion (done handlers,
   signal fanout, child-result mapping) must be derived from durable facts and safe to re-plan
   without double-applying.

These are internal runtime constraints; they do not change the consumer surface below.

## Illustrative runtime shape (non-normative)

The feature file is the contract; this is just to make "pure transition + interpreter" concrete.
Sketches, not final signatures.

**The log.** Every append is one tagged event; S2 assigns its `seqNum` (the order — `ADMISSION.3`):

```ts
type CallId = string // a schema-encoded owner+method+nonce (ROUTING)

type ActorEvent =
  | { _tag: "Accepted";       callId: CallId; method: string; input: unknown }
  | { _tag: "Journaled";      callId: CallId; step: string; value: unknown }  // a run()/state read fact
  | { _tag: "SignalResolved"; callId: CallId; name: string; value: unknown }
  | { _tag: "Completed";      callId: CallId; exit: Exit.Exit<unknown, unknown> }
  | { _tag: "StateChanged";   table: string; key: string; value: unknown }
  | { _tag: "Checkpointed";   cursor: number; snapshot: ActorSnapshot }

interface LogEntry { readonly seqNum: number; readonly event: ActorEvent }
```

**The projection.** Derived state at a cursor — rebuildable purely by folding the log:

```ts
interface ActorSnapshot {
  readonly cursor: number                                   // last applied seqNum
  readonly pending: ReadonlyArray<CallId>                   // accepted ∧ ¬completed, in seqNum order
  readonly active: Option.Option<CallId>                    // the call currently being run
  readonly results: ReadonlyMap<CallId, Exit.Exit<unknown, unknown>>  // done = present here
  readonly signals: ReadonlyMap<string, unknown>            // resolved rows, key `${callId}/${name}`
  readonly state: MaterializedState                         // the user state(Table) fold
}
```

**What the pure core asks the shell to do:**

```ts
type ActorAction =
  | { _tag: "StartCall";  callId: CallId }                  // fork the handler body
  | { _tag: "WakeWaiter"; callId: CallId; name: string }    // poke an in-proc park (best-effort)
  | { _tag: "Checkpoint" }                                  // snapshot + trim (CHECKPOINTING)
```

**The pure transition** — `(snapshot, entry) -> (snapshot, actions)`, **no I/O**, the whole
decision layer. Note completion *derives* the advance (no dequeue write → window-2 impossible):

```ts
const transition = (s: ActorSnapshot, e: LogEntry): readonly [ActorSnapshot, ReadonlyArray<ActorAction>] => {
  const ev = e.event
  switch (ev._tag) {
    case "Accepted": {
      const pending = [...s.pending, ev.callId]                       // seqNum order = append order
      const base = { ...s, cursor: e.seqNum, pending }
      return Option.isNone(s.active)                                  // idle? the new head may start
        ? [{ ...base, active: Option.some(ev.callId) }, [{ _tag: "StartCall", callId: ev.callId }]]
        : [base, []]                                                  // busy? just enqueue
    }
    case "Completed": {
      const results = new Map(s.results).set(ev.callId, ev.exit)
      const pending = s.pending.filter((id) => id !== ev.callId)      // "advance" = re-derive pending
      const head = Option.fromNullable(pending[0])
      const base = { ...s, cursor: e.seqNum, results, pending, active: head }
      return Option.match(head, {
        onNone: () => [base, [{ _tag: "Checkpoint" }]],               // queue drained → safe boundary
        onSome: (callId) => [base, [{ _tag: "StartCall", callId }]],  // run the next head
      })
    }
    case "SignalResolved": {
      const signals = new Map(s.signals).set(`${ev.callId}/${ev.name}`, ev.value)
      return [{ ...s, cursor: e.seqNum, signals }, [{ _tag: "WakeWaiter", callId: ev.callId, name: ev.name }]]
    }
    case "Journaled":
    case "StateChanged":
      return [applyFold(s, e), []]                                    // pure fold into projection
    case "Checkpointed":
      return [{ ...s, cursor: e.seqNum }, []]
  }
}
```

**The effectful shell** — fold the log to rebuild the snapshot, running each emitted action. The
*same* fold is recovery (replay history) and steady-state (tail new entries): replay is just
transition over older `LogEntry`s, so boot and live use one code path.

```ts
const drain = (db: ActorDb) =>
  ActorLog.read(db).pipe(Effect.flatMap((entries) =>          // ordered by seqNum; history then live tail
    Effect.reduce(entries, ActorSnapshot.empty, (snap, entry) => {
      const [next, actions] = transition(snap, entry)         // PURE
      return Effect.forEach(actions, interpret(db, next), { discard: true }).pipe(Effect.as(next))
    })
  ))

// the interpreter — the ONLY place effects happen; it never decides validity
const interpret = (db: ActorDb, snap: ActorSnapshot) => (a: ActorAction) => {
  switch (a._tag) {
    case "StartCall":  return forkHandler(db, snap, a.callId)        // run body; on settle append Completed
    case "WakeWaiter": return poke(a.callId, a.name)                 // best-effort; the row is the truth
    case "Checkpoint": return writeCheckpoint(db, snap)             // CHECKPOINTING.2–4
  }
}

// StartCall closes the loop: the handler runs (journaling into db), then appends its Completed event,
// which the next transition turns into the advance.
const forkHandler = (db: ActorDb, snap: ActorSnapshot, callId: CallId) =>
  Effect.forkScoped(
    runMethod(db, snap, callId).pipe(
      Effect.exit,
      Effect.flatMap((exit) => ActorLog.append(db, { _tag: "Completed", callId, exit })),
    ),
  )
```

**Routing (`ROUTING`)** — the callId *is* a reversible codec, so `callId → owner DB` is a pure
decode; the owner becomes an S2 path **only** inside `StreamDb.open(owner)`'s key codec (no
delimiter parsing, no index):

```ts
const CallId: Schema.Codec<{ owner: OwnerKey; method: string; nonce: string }, string> = /* … */

const openFromCallId = (encoded: string) =>
  Schema.decodeUnknownEffect(CallId)(encoded).pipe(
    Effect.flatMap(({ owner }) => ObjectActorDb.open(owner)),   // the ONLY place owner → path
  )

// so the by-id entrypoints take a bare string and need no residency, roster, or parsing:
const resolveSignalById = (encoded: string, name: string, value: unknown) =>
  openFromCallId(encoded).pipe(Effect.flatMap((db) => resolveSignal(db, encoded, name, value)))
```

**Checkpoint (`CHECKPOINTING`)** — `interpret`'s `Checkpoint` action snapshots the live set,
records it, then trims history before the cursor. The transition only emits `Checkpoint` when no
call is `active` (queue drained), so trimming `< cursor` is safe:

```ts
const writeCheckpoint = (db: ActorDb, snap: ActorSnapshot) =>
  Effect.gen(function*() {
    // snap must fit one S2 batch (MAX_BATCH_RECORDS) — else framed/chunked (CHECKPOINTING.6)
    yield* ActorLog.append(db, { _tag: "Checkpointed", cursor: snap.cursor, snapshot: snap })  // durable first
    yield* db.trim(snap.cursor)                          // then discard < cursor, now covered by the snapshot
  })
// fidelity (CHECKPOINTING.7): folding the log forward from `cursor` must reconstruct an equal snapshot,
// so boot can resume from the latest Checkpointed event instead of from seq 0.
```

**Dispatch / ingress / attach are then trivial** — appends and a projection read, no residency:

```ts
const dispatch     = (db, callId, method, input) => ActorLog.append(db, { _tag: "Accepted", callId, method, input })       // ADMISSION.6
const resolveSignal = (db, callId, name, value)  => ActorLog.append(db, { _tag: "SignalResolved", callId, name, value })   // INGRESS.1
const attach = (snap: ActorSnapshot, callId: CallId): CallStatus =>                                                         // COMPLETION.5
  Option.match(Option.fromNullable(snap.results.get(callId)), { onSome: normalizeExit, onNone: () => ({ _tag: "Pending" }) })
```

**Worked trace** — `counter.add(5)` on an idle key:

```txt
append Accepted{add,5}      seq 1  → transition: pending=[c], active=c, actions=[StartCall c]
  interpret StartCall c     → forkHandler runs add: StateChanged appended, returns 5
append Completed{c, ok 5}   seq 2  → transition: results={c:5}, pending=[], active=None, actions=[Checkpoint]
  interpret Checkpoint      → writeCheckpoint(snapshot@2) + S2 trim < 2
attach(snap, c)             → results.has(c) → Success(5)
```

Everything above the `interpret` lines is pure and unit-testable as `snapshot + event -> snapshot
+ actions`; only the `interpret`/`fork`/`append` lines touch S2.

## Workflow = an object specialization (`WORKFLOW`, not a third engine)

A Restate Workflow is a Virtual Object where *"the `run` handler executes exactly once per
workflow ID"* plus shared handlers that *"run concurrently to signal, query, or wait."* Here
that is the object model + two constraints:

- **Run-once admission:** `run` is an exclusive handler admitted **at most once per key**; a
  second `run` is **rejected** ("already started"), not dedup-returned (stricter than a
  service's `idempotencyKey`).
- **Shared interactions:** `signal`/`query` are ordinary shared handlers — concurrent,
  read-only, ingress-by-append.

So `workflow` is a thin definition-time layer (run-once `run` + shared handlers + retention).
The taxonomy collapses to **service (ephemeral) + object (keyed actor)**, workflow as a usage
pattern.

## Checkpointing & idempotency horizon (`CHECKPOINTING`)

Because the object stream holds **permanent user state**, it cannot be GC'd by S2 age/size
retention (that would trim live state). So GC is **explicit checkpoint + trim**, owned by the
drainer — this is real runtime work, not a config, and was the gap an earlier review flagged.

- **Checkpoint = a planned action / durable event.** The drainer emits a `Checkpoint` action at a
  **safe boundary** (between exclusive calls, or at a cursor that preserves the active call) and
  writes a `Checkpointed` event `{cursor, snapshot}`. The snapshot covers user state, pending
  accepts, active per-call journals, unresolved signals/timers, retained completed-call
  results/idempotency metadata, and a **completed/expired watermark** (`CHECKPOINTING.2–3`).
- **Then trim.** Records before the checkpoint cursor may be S2-trimmed *only* once represented by
  the snapshot (`CHECKPOINTING.4`). This is how a completed call's accept/journal/result are
  reclaimed *together* (the `LIFECYCLE.1` caveat) — a trimmed result can never re-appear as pending.
- **Idempotency horizon (`CHECKPOINTING.5`).** Results are retained for an explicit horizon: within
  it, a duplicate `callId` returns the existing/pending result; after expiry, `attach` or a
  duplicate `callId` resolves as **`Expired`** and is never re-run.
- **v1 size limit (`CHECKPOINTING.6`).** A checkpoint must fit one S2 batch
  (`effect-s2-stream-db` `MAX_BATCH_RECORDS`); beyond that needs **framed/chunked snapshots** — a
  documented cap on per-object live footprint, and a stream-db follow-up.

## Trade-offs

- **GC = checkpoint/trim, not drop** — the price of co-locating durable per-key state with
  execution (detailed above), vs. the current model's free `db.drop` of an ephemeral `wf` stream.
- **Idle objects persist** (`LIFECYCLE.2`) — intrinsic to durable objects (as in Restate);
  motivates an object lifecycle (`clearAll`/destroy), a named follow-up.
- **Services unchanged** (`LIFECYCLE.3`) — stateless services keep ephemeral one-stream-per-call.

## Multi-process (forward note, not designed here)

The object stream has multiple appenders — the drainer (journal/results) and ingress (new-call
/ signal rows from arbitrary callers). In-process, the shared instance + per-key mutex + S2 CAS
serialize them (no regression). Across processes this is where a **per-key lease + fence**
lands — *one* stream to fence per key, vs coordinating `obj` + N `wf` streams today. Collapsing
first is what makes the leasing pass tractable.

## Non-goals / follow-ups

- `workflow(...)` definition sugar + run-once rejection (thin, once exclusive/shared land).
- Type-level shared-handler read-only guard (mechanism for `HANDLERS.4`).
- Object lifecycle (`clearAll`/destroy); framed/chunked snapshots for large object state
  (`CHECKPOINTING.6` ↔ `storage-primitives.CHECKPOINT.4`).
- Cross-process leasing/fencing (its own SDD; this model is the prerequisite).
- **Storage primitives** (one layer down, behind the `DurableStore` port — policy injected, not
  hardcoded): the engine consumes `storage-primitives` `ENUMERATE` (recovery), `EXISTENCE`
  (non-creating open), and `CHECKPOINT` (GC), reads the ordered ActorEvent log via
  `effect-s2.readDecoded` (typed decode preserving `seq_num`), and requires control-plane basin
  provisioning with `createStreamOnAppend` disabled via existing `effect-s2` operations or external
  S2 tooling. See
  [`s2-resource-provisioning-sdd.md`](./s2-resource-provisioning-sdd.md).
- **Not borrowed** (per the Encore note): no `Actor.fromObject(...)` public API, no Effect Cluster
  `MessageStorage`/`deleteEnvelope` as the durable mailbox, no mutable completion-status row, no
  delimiter-parsed routing, and no XState/Stately runtime dependency.
