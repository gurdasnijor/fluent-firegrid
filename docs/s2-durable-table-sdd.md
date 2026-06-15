# SDD — `S2DurableTable`: reworking the durable-execution substrate

**Status:** design · supersedes the bottom half of `log-interface-sketch.md`
(the `Log` / `Journal` / `Executor` / `Seed` / `Compactor` apparatus). Keeps the
`Ctx` surface and the durable-execution semantics; **changes the substrate beneath
them.**

> **The insight (why this exists).** The previous sketch rebuilt, by hand, a
> sequenced-log abstraction (`Log`), a fold/state-machine (`Journal`), a
> persist/replay engine (`Executor`), bounded replay (`Seed = Genesis | Snapshot`),
> and compaction (`Compactor`). The old Firegrid runtime already solved durable
> execution **without any of that** — by modeling all durable state as
> [`DurableTable`](https://github.com/gurdasnijor/firegrid/blob/main/packages/effect-durable-operators/src/DurableTable.ts)
> collections and letting the **Durable Streams State Protocol** be the fold. This
> SDD lifts that design onto S2: a new **`S2DurableTable`** primitive that adopts the
> State Protocol as its *materialization vocabulary* and S2 as its *storage
> substrate*. The engine becomes a near-drop-in of the existing
> [`DurableStreamsWorkflowEngine`](https://github.com/gurdasnijor/firegrid/tree/main/packages/runtime/src/engine).
> We do **not** adopt `@effect/workflow`; its `ClusterWorkflowEngine` is used only as
> a *shape reference* (memoization-by-key, suspend-as-value, durable-deferred,
> timer-as-signal), never a dependency.

---

## 0. Decisions already made — constraints, not open questions

These are the model. The design below depends on them; do not relitigate.

1. **Single-writer per execution stream.** One engine instance owns one execution's
   stream. This is *the model*, not an HA story. Under it, S2 positional CAS never
   contends, so **check-then-append is safe and per-write fencing is not required
   for correctness.** Ownership is enforced exactly as the existing
   `engine-runtime.ts` does it — an **in-process `running` map** plus **boot-time
   recovery sweeps** — *not* a per-append log fence. Where HA would break this is
   called out explicitly in §7.
2. **One S2 stream per execution**, not one shared stream per table namespace. Every
   engine read is already keyed by `executionId`, so per-execution streams are
   natural: no cross-execution CAS contention, the S2 **200-append-batch/s/client**
   limit becomes *per-execution* rather than aggregate, and compacting a finished
   execution is just **deleting its stream**. The cost — loss of the single
   cross-execution query view — is paid by the **roster** (§5).

The fence-on-every-persist invariant, the buffer-and-flush machinery, the
`Conflict("fence")` vs `Conflict("sequence")` asymmetry, the lease lifecycle — all
of that was load-bearing *only* under a multi-writer-per-stream assumption. Single
writer per stream dissolves it (§6 migration).

---

## 1. The two planes (keep them separate)

The whole simplification comes from pushing **all** durable state down into
`S2DurableTable`, so the engine above it holds only transient, in-process
coordination. The writeup keeps them apart on purpose:

```
                    ┌───────────────────────────── State plane ──────────────────────────────┐
   Ctx  ───▶  thin Engine  ───▶  S2DurableTable (collections)  ───▶  S2 (one stream / execution)
   (run/      (running map,      insert/insertOrGet/upsert/             append + match_seq_num CAS,
    sleep,     timers, sweeps)   delete/get/query/subscribe;            snapshot+trim, stream delete
    awakeable) │                 State-Protocol fold = materialize      │
               │                                                        └ S2 lives ONLY here
               └────────────── Coordination plane ──────────────┘        (no record/offset/seq leaks up)
                 in-process: running map · timer fibers · boot
                 recovery sweeps · roster maintenance
                 (no durable coordination substrate)
                                   │
                                   ▼
                              Roster (one durable index stream: executionId → status)
                              — cold-start enumeration source for the sweeps (§5)
```

- **State plane** = materialization + compaction. Owned entirely by `S2DurableTable`.
  This is what the State Protocol + S2 let us *stop hand-writing*.
- **Coordination plane** = "which execution runs now, and when to wake it." Stays
  in-process (running map + timer fibers + boot sweeps), exactly as the DS engine
  did — because none of it ever used the Durable Streams coordination plane, so
  moving the substrate to S2 doesn't touch it. Its one new dependency is the
  **roster**, which replaces the cross-execution query the sweeps used to run
  against a shared table.

---

## 2. `S2DurableTable` — the new bottom primitive

### 2.0 The directionality invariant (everything in this plane is a consequence)

The design **splits** the durable log (S2) from the readable materialized view
(`MaterializedState`). Restate has no such split — its RocksDB cache is fed *by* the
committed log, so the cache is structurally downstream and can never run ahead of the
log. Our `MaterializedState` is held in memory and updated directly by the engine, so
that same directionality is a **rule we keep, not a shape the architecture enforces**.
Adopt it as the invariant the whole State plane rests on:

> **Invariant (log→cache).** `MaterializedState` is *strictly a downstream
> materialization of the durably-committed S2 stream*. It is **never updated ahead of
> an S2 ack.**

Both correctness rules below (§2.2 ordering, §2.2a atomicity) are consequences of this
one invariant — they are the same principle applied to *when* and to *how much*.

### 2.1 Collection surface (drop-in with `DurableTable`)

`S2DurableTable` exposes the **same collection facade** the engine already consumes,
so engine code is a near-drop-in. Per the existing `DurableTable`:

```ts
export interface CollectionFacade<Row, Key> {
  readonly insert: (row: Row) => Effect.Effect<void, S2TableError>
  readonly insertOrGet: (row: Row) => Effect.Effect<InsertOrGetResult<Row>, S2TableError>
  readonly upsert: (row: Row) => Effect.Effect<void, S2TableError>
  readonly delete: (key: Key) => Effect.Effect<void, S2TableError>
  readonly get: (key: Key) => Effect.Effect<Option.Option<Row>, S2TableError>
  readonly query: <A>(build: (coll: Collection<Row>) => A) => Effect.Effect<A, S2TableError>
}

export type InsertOrGetResult<Row> =
  | { readonly _tag: "Inserted"; readonly offset: SeqNum } // receipt, not a token; see note
  | { readonly _tag: "Found"; readonly row: Row }
```

> **`rows`/`subscribe` (replay-then-tail) are cut from the engine-facing surface.** The
> `DurableTable` facade had them, but **the engine consumes none of them** — §3's
> mechanism table is all `get`/`query`/`upsert`/`insertOrGet`. Carrying them forward
> "for facade parity" is the same dead-weight hazard as a bare `offset`: they have no
> specified S2 semantics, and the *plausible* implementation — opening an S2 read
> session and folding records read back from the stream — is a **second
> materialization path**, exactly the "two folds that must agree" hazard §2.2
> eliminates. If a tail is ever needed, spec it as *"observes the owner's local
> `MaterializedState`, owner-process only"* (same single fold, emitting on each local
> apply) — **never** a second S2 read-session. Until there's a consumer, it stays cut.

> **`offset` is per-stream, not a cross-execution ordering key.** With one stream per
> execution (§0.2) it is a positional CAS tail *within that execution's stream* only.
> The engine does not use it for coordination. Either drop it from the row the engine
> sees, or keep this annotation — leaving a bare `SeqNum` invites someone to treat a
> per-stream position as a global ordering token across executions, which it is not.

A `Table` is a set of named, schema-typed collections sharing one execution's
stream; the engine declares exactly the schema the DS engine already declares —
`executions`, `activities`, `deferreds`, `clockWakeups`, and (see §7) `activityClaims`.

### 2.2 Materialization = State-Protocol fold

A write `insert`/`upsert`/`delete` on collection `C` appends one **State-Protocol
change message** to the execution's S2 stream:

```json
{ "type": "<collection>", "key": "<encoded-pk>", "value": <row>,
  "headers": { "operation": "insert" | "update" | "delete", "txid": "<id>" } }
```

Reading materializes by applying messages in stream order to a per-stream
`MaterializedState`: latest-value-per-`(type, key)`, `delete` removes. This **is**
the fold — there is no separate `Journal`/`OpRecord`/`byName`. `get`/`query`/`rows`
read the materialized view; `subscribe` tails it.

**Append-before-observe (the ordering rule).** A step's result becomes visible to the
next `ctx.*` **only after S2 acks its append.** The handler *blocks on that ack* — the
network wait is the **durability barrier**, not overhead to optimize away. DS needed
`awaitTxId` to wait for a server/CDN-materialized view; here the single owner *is* the
materializer, so the round-trip collapses to a local apply — but the apply happens
**after** the ack, never before. `txid` is retained only as an idempotency/audit tag.

> **Forbidden: apply-before-ack.** Applying to `MaterializedState` first "for latency"
> makes the in-memory view run ahead of the durable stream (violates §2.0). The failure
> mode is concrete: a crash between the local apply and the append landing leaves the
> handler having acted on state replay will find was never durable — e.g. it ships on a
> `charge` whose append never landed, and replay then re-runs the charge. **Apply only
> after the ack.**

**One fold for live and replay.** To honor the invariant without paying re-read
latency, the **apply-on-ack path and the cold-replay fold are the same function**.
Apply-on-ack hands the just-committed record to the *exact* fold replay uses — it does
**not** read the record back from S2. This collapses what would otherwise be two
materialization paths into one, so live and replay **cannot diverge**. Two separate
paths would be a correctness hazard (they must produce identical state); making them one
function removes it structurally — Restate's "one materialization, never ahead of the
log" guarantee, recovered on a split substrate.

### 2.2a Atomic step writes (buffer → one batch → ack → apply)

A logical step writes **multiple rows**: scheduling a sleep writes a `clockWakeups` row
*and* updates the `executions` row to `suspended`; a deferred resolution writes a
`deferreds` row *and* drives the resume's downstream writes. As separate appends, a
crash between them leaves a **torn step** — a pending timer with the execution not
marked suspended, or a deferred resolved with the resume half-applied.

> **This atomicity need is independent of fencing.** §6 deleted buffer-and-flush by
> attributing it entirely to the multi-writer fencing story. That conflated two things
> the buffer did: **fencing** (genuinely unnecessary under single-writer — correctly
> deleted) and **atomicity** (still necessary — wrongly deleted with it). Restore the
> atomicity half only.

S2 batches are **atomic up to 1 MiB on a per-execution stream**, so buffer a step's
writes and flush them as **one S2 batch**. Combined with §2.2, the rule is one
sentence:

> **A step buffers its mutations, flushes them as one atomic S2 batch, waits for the
> batch ack, then applies them to `MaterializedState` via the replay fold, then
> continues.**

The ack you block on is the *batch* ack; the local apply (via the shared fold) is
all-or-nothing.

> **A step's control-row writes must fit one batch — exceeding 1 MiB is a hard error,
> not a silent multi-batch.** Assert it: no legitimate single step writes >1 MiB of
> *control rows* (a step writes a handful — an `executions` update, a `clockWakeups` or
> `deferreds` row). The only oversized case is a **single large row *value***, which is
> a snapshot/replay concern handled by framing (§2.3), not a multi-row step. If a step's
> batch would exceed the cap, fail loudly — silently splitting it across batches
> reintroduces the torn-step this section exists to prevent.

### 2.3 Compaction — S2 native snapshot + trim (the whole point)

Two paths, in priority order:

**(a) Stream delete — the dominant path for workflows.** When an execution completes
and its final result is durable **and acknowledged consumed**, delete the entire S2
stream. Preload cost and storage both go to zero. For typical workflows this is the
*only* compaction that runs.

> **The completed result must outlive its stream.** "Consumed" cannot be left
> undefined: deleting the stream destroys the final-result row, which races a late
> `result(executionId)` caller — after deletion that caller gets *nothing*, whereas the
> pre-delete model kept the result queryable. Pick one:
> - **(a) the roster row carries the final result** (or a pointer to a results stream)
>   and survives until consumption is acked, or
> - **(b) deletion is gated on an explicit result-acknowledged signal** from the
>   consumer.
>
> This SDD takes **(a)**: the roster is the durable home for a completed execution's
> result. That puts the roster on the **result read path**, not just the recovery
> path — which §5's `executionId → status` shape does not yet account for. §5 extends
> the roster row accordingly.

> **Completion is a cross-stream sequence with no atomicity — order it like §2.2.**
> The completion path writes to **two different streams**: the `result` lands in the
> *roster* stream; the *execution* stream is then deleted. S2's atomic-batch guarantee
> is **per-stream** (§2.2a), so there is no single batch covering both — and §2.2's
> append-before-observe principle now applies to the *handoff between streams*, not just
> the intra-execution fold. The destructive op (stream delete) must be gated on the
> **roster `result`-write ack**, not on "the result is durable":
>
> ```
> 1. write result → roster        2. AWAIT roster ack        3. delete execution stream        4. (later) set resultAcked
> ```
>
> The trap is that "durable" is ambiguous: the result *was* durable the moment it was
> written to the execution stream — the stream you are about to **destroy**. Durability
> that counts here is durability *in the roster*. Interleavings:
> - crash between 1–2 or 2–3: result is in the roster, stream may still exist → boot
>   sees `completed` + `result`, deletes the stream idempotently. Safe.
> - crash between 3–4: stream gone, roster has `result`, `resultAcked: false` →
>   recoverable, result served from the roster. Safe (this is why we chose (a)).
> - **the hole, if 2 is skipped (pipelining 1 and 3):** delete lands but the roster
>   `result` write never did → **no stream and no result anywhere; the execution is
>   unrecoverably lost.** Hence: *nothing destructive may precede confirmation the
>   result has a durable home elsewhere.* Await the roster ack before the delete.

**(b) In-stream snapshot + trim — for long-running executions.** When a still-live
execution has accumulated history (many activities before completion), compact
in place. A snapshot is **one atomic conditional batch**, `match_seq_num = cursor`
(the current tail), containing:

```
[ control: { "control": "snapshot-start", "offset": <cursor> } ,
  change:  one insert per LIVE (type,key) in MaterializedState ,   // bounded by live-key count
  control: { "control": "snapshot-end",   "offset": <cursor> } ,
  command: Trim(upTo = cursor) ]                                    // S2 trim command record
```

On ack, the snapshot prefix becomes the new stream **head** (trim discards history
below the cursor). A fresh `preload()` reads from head, sees `snapshot-start`, loads
the snapshot records into a clean `MaterializedState`, sees `snapshot-end`, then
follows deltas. **Preload cost is bounded by live-key count, not total history** —
that is the property the hand-rolled `Seed`/`Compactor` was straining to provide.

- *No snapshot/delta race under single-writer.* The generic S2 "buffer concurrent
  deltas during snapshot" guidance assumes a **separate** snapshotting process racing
  the writer. Here the owner produces the snapshot **inline between steps** (§0.1), so
  there are **no concurrent deltas while the snapshot is produced** — nothing else is
  writing. The conditional batch (`match_seq_num = cursor`) either lands or fails a
  stale retry. Do **not** build a delta-buffering layer; it solves a race this model
  doesn't have.
- *Framed-fragment path — only for an oversized single row (> 1 MiB).* S2 caps
  record/batch size. The snapshot is already one record per live key, so the *only*
  > 1 MiB case is a **single row whose value exceeds the record cap**: split *that
  value* into ordered **framed fragments** — multiple change records between
  `snapshot-start`/`snapshot-end`, each carrying a `frame` header
  (`{ key, frameIndex, frameCount }`); the reader concatenates that key's frames before
  decoding. This reader-side fragment buffering is the *only* buffering the reader
  needs, and only for oversized rows. Normal multi-key snapshots need no framing.

### 2.4 S2 lives behind this primitive only

The old `Log` seam becomes an **internal** of `S2DurableTable`. The engine sees
collections; it never sees S2 records, offsets, `match_seq_num`, or `SeqNum`
(`InsertOrGet`'s `offset` is the one receipt that leaks, and it's read-only and
unused by the engine for coordination).

The S2 substrate is now the **`effect-s2`** package (`S2Client` — `createStream` /
`checkTail` / `append` / `appendSession` / `read` / `readBytes` / `producer` / `sink`,
plus `conditionalAppend`), with `effect-s2/testing`'s in-memory `TestS2` for
deterministic offline tests. `S2DurableTable` is built on `S2Client`; that package is
the sole importer of `@s2-dev/streamstore` (replacing the bespoke `s2Live.ts`). A
step's atomic batch (§2.2a) is one `S2Client.append`/`session.submit` with
`matchSeqNum`; the ack the handler blocks on (§2.2) is that submit resolving.

> **Compaction precondition — `effect-s2` does not yet expose `trim` or
> `deleteStream`.** Both §2.3 paths call S2 operations the current `S2Client` surface
> lacks, and `TestS2` does not model them. This is a **hard dependency, not a
> validation footnote**: §2.3 is not buildable — or testable offline — until
> `effect-s2` grows `trim` + `deleteStream` and `TestS2` models trim/snapshot
> atomicity, the **head-prefix replay** semantics (a fresh `preload` from the
> post-trim head must reconstruct identical state), and whether `deleteStream` is
> eventual (which decides whether a post-delete `result` read can race a
> not-yet-effective delete). **Sequencing:** add those two ops + their test doubles +
> the head-prefix replay test to `effect-s2` *first*, then build §2.3 on top. The
> data-plane writes/reads (§2.1/§2.2) need only today's `append`/`read`/`checkTail`.

---

## 3. The thin engine (near-unchanged from `DurableStreamsWorkflowEngine`)

Given `S2DurableTable` + a handler, the engine is the existing DS engine with the
substrate swapped. Each durable-execution mechanism maps to a collection operation —
the same calls `engine-runtime.ts` already makes:

| Mechanism | Implementation over `S2DurableTable` | DS engine parity |
|---|---|---|
| **Execution state** | `executions.upsert({ executionId, status, suspended, cause? })` / `get` | identical |
| **Activity memoization** | `activities.get(`execId/name/attempt`)` → if `result` present, return it; else run, `activities.upsert({ result })` | identical (minus the claim — §7) |
| **Suspend** | suspend-as-**value**: handler body returns `Result = Complete | Suspended`; on `Suspended`, `executions.upsert({ suspended: true, cause? })` | identical |
| **Durable deferred / awakeable** | `deferreds.get(name)` → `row.exit`; resolve via `deferreds.upsert({ exit })` then `resume(executionId)` | identical |
| **Durable timer / sleep** | `clockWakeups.upsert({ deadlineMs, status: "pending", deferredName })`; arm in-process `Effect.delay(deadline-now)` + `forkIn(engineScope)`; on fire set `status:"fired"` + `deferredDone` | identical |
| **Resume** | in-process `running: Map<execId, { fiber, instance }>`; `resume` checks `running`, else forks body into scope | identical |

`Ctx.run/sleep/awakeable` (§4 of the sketch) are unchanged in surface; underneath,
`run` is `activities` memoization, `sleep` is a `clockWakeups` row + in-process
delay, `awakeable` is a `deferreds` read parked until `deferredDone`. The
`run`-idempotency key stays `${executionId}:${name}` (now the activity row's key,
`execId/name/attempt`).

> **Shape reference, not dependency.** These four shapes — memoization-by-key,
> suspend-as-value, durable-deferred, timer-as-signal — are exactly what
> `ClusterWorkflowEngine` uses. We mirror the shapes with our own `Result`/`Ctx`
> types; we do not import `@effect/workflow`.

---

## 4. Coordination plane — what stays in-process (unchanged by the substrate)

These never used the Durable Streams coordination plane, so S2 changes nothing about
them. They are the existing DS engine code, essentially verbatim:

- **`running` map + resume.** `Map<executionId, { fiber, instance }>`. `resume`
  polls `running.get(id)?.fiber`, skips if already completed non-suspended, else
  forks the body into the engine scope. This **is** the ownership mechanism (with
  §0.1 single-writer): an execution is "owned" by being in this map on the one
  engine instance.
- **In-process timers.** `scheduleClockWakeup` = `Effect.delay(deadline − now)` +
  `forkIn(engineScope)` over a durable `clockWakeups` row; `fireClockWakeup` flips
  the row to `"fired"` and calls `deferredDone`.
- **Boot recovery sweeps.**
  - `recoverPendingClockWakeups` — for each `clockWakeups` row with
    `status==="pending"`, re-arm the lost `Effect.delay`+`forkIn`.
  - `recoverPendingDeferreds` — for suspended-not-interrupted-no-final-result
    executions parked on a deferred, re-drive the body (its in-process resume was
    lost to restart).

The **only** change to this plane: the sweeps used to enumerate by querying a
*shared* table (`clockWakeups.query(...)`, `deferreds.query(...)`) across all
executions. With one stream per execution there is no shared table to query — so the
enumeration source becomes the **roster** (§5). The per-execution recovery logic
itself is unchanged; it just runs scoped to each stream the roster names.

---

## 5. The one new thing — a roster

Per-execution streams trade away the single cross-execution query view the recovery
sweeps relied on ("all suspended executions", "all pending clock wakeups"). This is
the long-standing enumeration debt; name and design it rather than hand-waving where
boot's id-set comes from.

**Design.** A separate durable **roster** — itself an `S2DurableTable` over **one
shared index stream**. It carries more than `status`, because it is on both the
recovery path *and* (per §2.3a) the result read path, and (per the recovery invariant
below) must record *why* an execution is suspended:

```ts
// roster row
{
  executionId: string
  workflowName: string
  status: "running" | "suspended" | "completed"
  // why it suspended — REQUIRED for kind-aware recovery (see below)
  suspendKind?: "deferred-wait" | "pending-clock"
  // completed-result home — survives the execution's stream being deleted (§2.3a).
  // DEFAULT: store the result INLINE and cap its size. A `{ pointer }` to a separate
  // results stream adds a THIRD stream to the completion path (roster + results +
  // execution-delete), compounding the cross-stream ordering of §2.3a — every added
  // stream is another un-atomic edge to sequence. Prefer inline + a size cap; only
  // reach for a pointer if results genuinely exceed the cap, and then sequence
  // results-write → results-ack → roster-pointer-write → roster-ack → delete.
  result?: EncodedExit                           // present once status === "completed"
  resultAcked?: boolean                          // gate for stream + roster-row deletion
  updatedMs: number
}
```

- Maintained by the engine on every lifecycle transition: `submit` → `running`;
  suspend → `suspended` (with `suspendKind`); complete → `completed` (write `result`,
  then delete the execution's own stream §2.3a); the roster row is deleted/tombstoned
  only once `resultAcked`.
- **Cold-start enumeration:** at boot the engine `query`s the roster for
  `status ∈ {running, suspended}`, and for each opens that execution's stream,
  `preload`s its `S2DurableTable`, and runs the per-execution sweeps against its own
  `clockWakeups`/`deferreds` collections.
- **Result reads** for a completed execution whose stream is gone are served from the
  roster `result` field.

> **Recovery must stay kind-aware (preserve the tf-8f6y lesson).** The blanket
> "resume all suspended" sweep is **unsafe**: it cannot distinguish a *deferred-wait*
> from an *interrupt*. When enumeration moves from shared-table-query to roster-driven,
> the roster says `status: "suspended"` but not *why*. So either the roster carries
> `suspendKind` (as above) **or** the per-execution sweep re-derives the kind from the
> stream after opening it. The discrimination invariant the rewrite must not drop:
> **recovery re-drives only deferred-waits and pending-clocks — never interrupts, and
> never non-deferred suspensions.**

> Optionally the roster also carries the **nearest pending `deadlineMs`** per
> execution, so timer recovery can re-arm without opening every stream first. Start
> without it (open-and-sweep is simplest); add it if boot latency at scale demands.

**The roster is the first thing to break under HA — not a safe exception.** §5 must
not justify the roster's safety by its low write-rate; that is a red herring. The
roster is a **shared multi-writer stream — exactly what §0.2 banned for execution
streams**. It is safe *only* because there is still one engine instance (§0.1/§7), and
S2 has **no per-key producer fencing** to arbitrate concurrent roster writes. Two
engine instances both writing the roster contend on a single positional cursor with no
fence — so the roster breaks *before* the per-execution fences even matter. See §7.

**Memory: N resident materializers.** Per-execution streams mean every recovered
`running`/`suspended` execution holds a **live `MaterializedState` at boot** (one fold
per open stream). Fine for hundreds; a memory problem at hundreds of thousands. This is
the cost twin of the per-execution split (§8) and belongs with open question 3
(MaterializedState reuse / eviction of idle suspended executions).

---

## 6. Migration — what the previous sketch loses

Mapping `log-interface-sketch.md` sections to their fate:

| Section / artifact (old sketch) | Fate | Rationale |
|---|---|---|
| **§1 `Log`** (branded `SeqNum`/`FenceToken`, `Write` enum, `Conflict`, `append`/`read`/`tail`/`list`) | **Demoted** to an internal of `S2DurableTable` | The engine no longer needs a log seam; it needs collections. S2 stays behind the table. |
| **§2 `Journal`** (`fold`, `byName`, `OpRecord`, `replay`) | **Deleted** | State-Protocol materialization *is* the fold. `MaterializedState` replaces it. |
| **§2 `Seed = Genesis | Snapshot`** + the deferred **Compactor** | **Deleted** | Replaced by S2 snapshot+trim (§2.3b) and stream-delete (§2.3a). "Reseed on replay" = `snapshot-start`/`snapshot-end` becoming the head prefix. |
| **§3.1 `StepResult`** / **§3.2 `Executor.step`** / **Instance** | **Collapsed** | Per-row State-Protocol writes replace the persist machinery. Suspend stays a value (kept, §3). |
| **§3.2.1 buffer-and-flush** | **Split: fencing deleted, atomicity KEPT** | The old buffer did *two* things. **Fencing** under single-writer (§0.1) is unnecessary — deleted. **Atomicity** is still required — a step writes multiple rows and must not tear — so it's **restored** as buffer-one-step → one atomic S2 batch (§2.2a). Do not delete this half with the fence. |
| **§3.3 `Dispatcher`** (ready-set + `armTimer`/`armAwait` + durable swap) | **Mostly deleted** | No ready-set/await-dispatch needed: resume is the in-process `running` map; timers are `Effect.delay`+`forkIn` over a `clockWakeups` row; event delivery is `deferredDone`+`resume`. What remains is §4's coordination plane, which is smaller than the `Dispatcher` seam. |
| **§3.4 `Engine`** (`submit`/`signal`/`poll`/`result`/`run` pump) | **Kept, simplified** | Lifecycle verbs remain, but `run`-as-a-pump-over-`ready` disappears; the engine drives bodies directly via `running`/`resume`. `signal` = `deferreds.upsert` + `resume`. |
| **Fence-on-every-persist, `Conflict("fence")` asymmetry, lease lifecycle (Tier-1)** | **Deleted** | Load-bearing only under multi-writer-per-stream. Single-writer per stream ⇒ positional CAS never contends ⇒ no per-write fence. |
| **L1 (suspend-as-value), L7 (attempt-keyed memo)** | **Kept** | Now realized by `Result` + `activities` keyed `execId/name/attempt`. |
| **L3 (timer = awakeable), L5 (sweep backstop), L9 (uninterruptible flush)** | **Re-homed** | L3 = `clockWakeups` row → `deferredDone`. L5 = the boot sweeps (§4). L9 narrows to "snapshot batch is one atomic conditional append". |
| **L2/L4/L6/L8** | **Folded into §3/§4** | Instance state, distinct verbs, interrupt-as-deferred, encoded-core — all still present, now inside the thin engine over collections. |

**Engine code: drop-in vs. touch.**

- *Literal drop-in:* the `running` map + `resume`; `recoverPendingClockWakeups`;
  `recoverPendingDeferreds`; activity memoization read/return; deferred
  resolve+resume; timer arm/fire; suspend-as-value. These call only the collection
  facade, which `S2DurableTable` reproduces.
- *Needs a touch:*
  1. **Stream granularity** — DS used one shared stream per table namespace; we open
     **one stream per execution** (§0.2). The table-construction/preload path changes
     from "namespace stream" to "per-execution stream, lazily opened/closed."
  2. **Recovery enumeration** — swap the shared-table `query` sweeps for the
     **roster** (§5).
  3. **`insertOrGet`** — reimplement over single-writer check-then-append (§7), not
     DS producer-identity fencing.
  4. **Compaction** — add the snapshot+trim batch and stream-delete-on-complete
     (§2.3); DS leaned on the server's automatic compaction.
  5. **`awaitTxId`** — collapses to a local apply (§2.2); remove the round-trip.

---

## 7. What this does **not** do (and the precise failure modes)

State the assumptions as the things that break first when violated.

- **No HA / no multi-writer.** Exactly one engine instance runs. The breaks below are
  ordered by *what fails first* as you add a second instance:
  - *The roster breaks first — before any per-execution fence matters.* The roster
    (§5) is a **shared multi-writer stream** with no per-key producer fencing (S2 has
    none). Two engine instances writing lifecycle transitions contend on one positional
    cursor: one write wins, the other is rejected, and the roster — the enumeration and
    result-read index the whole recovery plane depends on — silently loses transitions.
    This fails *before* the per-execution concerns below, because both instances hit the
    one roster long before they happen to co-own a single execution. HA therefore starts
    at the roster: it needs an ownership/fencing story (or a single-writer roster shard
    per owner) before per-execution fencing is even relevant.
  - *Positional CAS contention (per execution).* If two instances co-own one execution:
    both append at the same `match_seq_num`; one wins,
    the loser's write is rejected — but the loser has *already run the side effect*,
    and there is no fence telling it to stop (we deleted per-write fencing). This is
    the exact scenario per-execution streams + single-writer were chosen to make
    impossible, not to survive.
  - *`insertOrGet` first-writer-wins violated.* S2 CAS is **positional, not
    key-conditional** — there is no native per-key first-writer-wins. We implement
    `insertOrGet` as **check-then-append under the single owner** (`get` miss →
    `append insert`). Two owners can both miss and both append ⇒ two "inserts" ⇒
    duplicate. **`activityClaims` is the collection whose entire reason for existing
    — cross-worker arbitration — is what breaks here.** Under single-writer the claim
    is *redundant* (one writer can't race itself), so this SDD treats `activityClaims`
    as **optional**: keep it only as the seam we'd re-arm for a future HA story;
    memoization itself needs only `activities` keyed `execId/name/attempt`.
  - HA, if ever wanted, is re-introduced by exactly one mechanism: a per-execution
    **fence** (the deleted Tier-1 apparatus) or an external ownership lease — layered
    *back* under the roster. The roster decides who *should* own; a fence would
    enforce who *gets to write*. They are different layers; do not conflate.
- **No `@effect/workflow`.** Shapes only (§3). If a future maintainer reaches for
  `WorkflowEngine`/`ClusterWorkflowEngine`, note it drags in cluster's
  mailbox/sharding model — at odds with the per-execution-stream design (see
  `s2-durable-approaches-comparison.md`).
- **No dispatcher / no scheduling plane** beyond in-process resume + roster-driven
  recovery. There is no ready-queue, no work-stealing, no fairness scheduler. Failure
  mode: a single engine instance is a throughput ceiling (its fibers + the
  200-batch/s **per-execution** cap). That cap is per-stream by construction (§0.2),
  so it bounds a single hot execution, not the fleet — but the *instance* is still
  one process. Scaling past one instance is the HA problem above.

---

## 8. The two S2-specific edges, head-on

1. **Why per-execution streams are mandatory on S2 (not a free choice).** DS
   tolerates one shared stream because it has **per-key producer fencing** — many
   writers, each fenced on its own key. S2 has only **positional CAS** (fence on the
   *next seq*, shared across all writers to a stream) plus a **200-append-batch/s
   per client per stream** cap. On a shared stream those make concurrent
   per-execution writes contend on a single cursor and share one rate budget. One
   stream per execution removes both: independent cursors, independent budgets,
   trivial compaction (delete the stream). The granularity choice is forced by S2's
   concurrency primitive, not arbitrary.
2. **`insertOrGet` has no native S2 analog.** Covered in §7: positional CAS ≠
   key-conditional, so first-writer-wins is a single-writer check-then-append
   property, and it is the first thing to break under HA. Documented at the call
   site (`activities`/optional `activityClaims`).

---

## 9. Deliverable checklist (per the rework brief)

- ✅ **`S2DurableTable` spec** — collection surface (§2.1), State-Protocol-over-S2
  materialization (§2.2), snapshot/trim compaction incl. conditional-batch shape and
  framed-snapshot path (§2.3).
- ✅ **Revised layer/seam diagram** — shrunk stack `S2DurableTable → engine → Ctx`
  plus roster (§1).
- ✅ **Migration note** — deleted / demoted / drop-in vs. touch (§6).
- ✅ **"What this does not do"** — no HA, no `@effect/workflow`, no dispatcher; precise
  failure modes (§7).
- ✅ **State plane vs coordination plane** kept cleanly separate throughout (§1, §4).

## Build sequencing & open questions for the next pass

- **PRECONDITION (build first): grow `effect-s2` + `TestS2` with `trim` and
  `deleteStream`.** §2.3 compaction (both paths) is not buildable or offline-testable
  until the client surfaces these and `TestS2` models trim/snapshot atomicity, the
  **head-prefix replay** (fresh `preload` from post-trim head reconstructs identical
  state), and `deleteStream` eventual-vs-immediate semantics (decides the post-delete
  `result`-read race). Land that + the head-prefix replay test, *then* build §2.3. The
  §2.1/§2.2 data plane needs only today's `append`/`read`/`checkTail`/`conditionalAppend`.
- **`S2DurableTable` over `s2 lite`** — validate against the real server (the AC-5
  analog): (a) snapshot+trim conditional batch and `snapshot-start`/`snapshot-end`
  head-prefix replay; (b) **one-step-one-batch atomicity** (§2.2a) — multi-row step lands
  all-or-nothing, crash mid-step replays cleanly; (c) **append-before-observe** (§2.2) —
  handler blocks on the batch ack before the next `ctx.*` sees the write; (d) **completion
  handoff ordering** (§2.3a) — crash between roster-`result`-ack and stream-delete leaves
  the result recoverable, and the delete never precedes the roster ack.
- **Completed-result home** (§2.3a) — roster-carries-result **inline** (chosen default,
  with a size cap); `{ pointer }` to a results stream deferred (it adds a third stream to
  the completion sequence). Settle the `resultAcked` protocol and the inline size cap.
- **Roster as the HA seam** — the roster is the first multi-writer break (§7). Decide
  now whether to design its ownership/fencing story or explicitly defer it with the
  single-writer assumption pinned. Also: roster write amplification (one write per
  lifecycle transition) and the status-keyed-shard threshold.
- **`MaterializedState` reuse + the N-resident-materializers cost** — can we use
  `@durable-streams/state`'s `MaterializedState` directly as the per-stream fold, or do
  we need an Effect-native re-impl (Schema-first, no `JSON.parse`, lint gates)? Whatever
  we pick is held **once per open execution stream** (§5), so this is also where
  eviction of idle suspended executions is decided (memory bound at 10^5+ executions).
- **`activityClaims` keep-or-cut** — cut now for minimality, or keep dormant as the HA
  seam? (§7 leans cut; flag for the user.)
