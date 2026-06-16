# SDD — `S2DurableTable` and the durable-execution engine

**Status:** design. Supersedes the `Log`/`Journal`/`Executor`/`Compactor` apparatus in
`log-interface-sketch.md`. Keeps the `Ctx` handler surface.

## Two layers, kept separate

This document specifies two things, and the whole point is that they are independent:

- **Part A — `S2DurableTable`** — a general-purpose durable, materialized, compactable
  **typed-collection store over a single S2 stream**. It knows nothing about workflows,
  executions, or results. It could back a chat room or a feature-flag store equally well.
- **Part B — the durable-execution engine** — a thin in-process coordinator that *uses*
  `S2DurableTable` (one per execution, plus one for a cross-execution index) to run
  durable workflows.

Part A is the substrate; Part B is one consumer of it. Nothing in Part A references a
concept from Part B. Read Part A as if Part B did not exist.

The underlying idea Part B exploits: a durable workflow is a few typed collections
(its activities, timers, deferreds, status) materialized from an append-only log. The
[Durable Streams State Protocol] already defines that materialization, and S2 is the
log — so the engine never hand-writes a journal, a replay fold, or a compactor. The
existing [`DurableStreamsWorkflowEngine`] is a *reference* that this collection-based
approach works over Durable Streams; Part B is built fresh on `S2DurableTable`, not
ported from it, and does not depend on `@effect/workflow`.

[Durable Streams State Protocol]: ./reference/durable-streams/packages/state/STATE-PROTOCOL.md
[`DurableStreamsWorkflowEngine`]: https://github.com/gurdasnijor/firegrid/tree/main/packages/runtime/src/engine

---

# Part A — `S2DurableTable` (substrate; workflow-agnostic)

A `S2DurableTable` is a set of named, schema-typed **collections** materialized from
**one S2 stream**, with a **single writer**. It provides typed CRUD, atomic
multi-collection transactions, and compaction. That is the entire abstraction.

## A1. Single-writer model

One owner writes one table's stream. Under that assumption S2's positional
compare-and-swap never contends, so check-then-append is correct and **per-write fencing
is unnecessary**. The table does *not* enforce single-writer; it assumes it. The
substrate facts that make the assumption load-bearing: S2 offers positional CAS (a fence
on the next sequence number, shared across all writers to a stream) and **no per-key
producer fencing**, plus a 200-append-batch/s/client cap per stream. Two concurrent
writers would contend on the one cursor with no per-key arbitration — out of scope here;
the consumer guarantees single-writer (Part B does this in-process).

## A2. Collection surface

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
  | { readonly _tag: "Inserted" }
  | { readonly _tag: "Found"; readonly row: Row }
```

`insertOrGet` is **check-then-append under the single owner**: `get` misses → append the
insert. First-writer-wins holds only because there is exactly one writer (S2 CAS is
positional, not key-conditional, so it cannot arbitrate two writers racing on the same
key — see A1).

The facade omits any replay-then-tail (`rows`/`subscribe`): the only plausible
implementation is a second S2 read-session folding records back, which is a second
materialization path that can drift from A4's. If a live tail is ever needed, it observes
the owner's in-memory materialization (the same fold), never a second read-session.

S2 lives behind this facade only — a caller sees collections, never S2 records, offsets,
or `match_seq_num`. The table is built on the `effect-s2` `S2Client` (append, conditional
append, read, trim, delete-stream) and is the sole importer of the S2 SDK.

## A3. Materialization is the State-Protocol fold

A write on collection `C` appends one State-Protocol change message:

```json
{ "type": "<collection>", "key": "<encoded-pk>", "value": <row>,
  "headers": { "operation": "insert" | "update" | "delete" } }
```

A read materializes by applying messages in stream order to an in-memory
`MaterializedState`: latest-value-per-`(type, key)`, `delete` removes. That fold *is* the
durable state — there is no separate journal. `get`/`query` read it.

## A4. The directionality invariant

The readable view must never run ahead of the durable log:

> **Invariant.** `MaterializedState` is strictly a downstream materialization of the
> durably-committed S2 stream. It is **never updated ahead of an S2 ack.**

Two consequences:

- **Read-after-ack.** A write becomes visible to a subsequent `get` only after S2 acks
  its append; the caller blocks on the ack. Applying locally first "for latency" is
  forbidden — a crash between the local apply and the append landing would expose state
  that replay finds was never durable.
- **One fold for live and replay.** The apply-on-ack path and the cold-replay fold are
  the *same function*: on ack, the just-committed record is handed to the exact fold a
  fresh preload uses — it is not read back from S2. One path, so live and replay cannot
  diverge.

## A5. Atomic transactions

Multiple writes — across one or several collections — commit as **one atomic S2 batch**:

> A transaction **buffers its writes, flushes them as one atomic S2 batch, waits for the
> batch ack, then applies them to `MaterializedState` via the fold.**

S2 batches are atomic up to 1 MiB on one stream. A transaction exceeding 1 MiB is a hard
error, not a silent multi-batch (which would partially apply on a crash). The only
legitimately large payload is a single oversized row *value*, handled by framing during
compaction (A6) — not by splitting a transaction.

## A6. Compaction

Two mechanisms; *when* to invoke them is the consumer's policy, not the table's.

- **`compact()` — snapshot + trim.** One atomic conditional batch
  (`match_seq_num = cursor`): a `snapshot-start` control record, one `insert` per live
  `(type, key)` in `MaterializedState`, a `snapshot-end` control record, then
  `trim(upTo = cursor)`. On ack the snapshot prefix becomes the stream head; a fresh
  preload reads from head, loads the records between the markers into a clean
  `MaterializedState`, then follows deltas. **Preload cost is bounded by live-key count,
  not total history.** Single-writer means there are no concurrent deltas during snapshot
  production, so no delta-buffering is needed. A single row value over the record cap is
  split into ordered **framed fragments** between the markers
  (`{ key, frameIndex, frameCount }`); the reader concatenates before decoding.
- **`delete()` — drop the stream.** Discards the table entirely. Preload cost and storage
  go to zero.

---

# Part B — the durable-execution engine (a consumer of `S2DurableTable`)

The engine runs durable workflows by composing `S2DurableTable` instances with a thin
in-process coordination layer. It contributes no new persistence machinery — only schema,
deployment, and coordination.

```
   Ctx ─▶ engine ─┬─▶ per-execution S2DurableTable ─▶ S2 stream  (executions/activities/…)
  (run/   (in-proc │
   sleep, coord)   └─▶ roster S2DurableTable ─────────▶ S2 stream  (executionId → status/result)
   awaitable)
                   in-process only: running map · timer fibers · boot recovery sweeps
```

## B1. Deployment — one table per execution, plus a roster

The engine instantiates **one `S2DurableTable` per execution** (so one S2 stream per
execution) and **one roster table** shared across executions.

Per-execution streams are the right granularity here precisely because of A1's substrate
facts: every engine read is already keyed by `executionId`; per-execution streams give
independent CAS cursors and independent 200-batch/s budgets, and let a finished execution
be compacted by `delete()`. A single shared stream would force every execution's writes
onto one cursor and one rate budget with no per-key fencing to separate them.

## B2. Schema

Per-execution table collections: `executions`, `activities`, `deferreds`,
`clockWakeups`, and (optional, see B7) `activityClaims`.

## B3. Mechanisms

Each durable-execution mechanism is a collection operation:

| Mechanism | Over the execution's `S2DurableTable` |
|---|---|
| **Execution state** | `executions.upsert({ executionId, status, suspended, cause? })` / `get` |
| **Activity memoization** | `activities.get(execId/name/attempt)` → return `result` if present; else run, `activities.upsert({ result })` |
| **Suspend** | suspend-as-value: the body returns `Result = Complete \| Suspended`; on `Suspended`, `executions.upsert({ suspended: true })` |
| **Durable deferred / awaitable** | `deferreds.get(name)` → `exit`; resolve via `deferreds.upsert({ exit })` then `resume(executionId)` |
| **Durable timer / sleep** | `clockWakeups.upsert({ deadlineMs, status: "pending" })`; arm `Effect.delay` + `forkIn(engineScope)`; on fire set `status: "fired"` + resolve the deferred |
| **Resume** | in-process `running: Map<execId, { fiber, instance }>`; `resume` checks `running`, else forks the body into scope |

`Ctx.run/sleep/awaitable` keep their surface: `run` is `activities` memoization
(idempotency key `execId/name/attempt`), `sleep` is a `clockWakeups` row plus an
in-process delay, `awaitable` is a `deferreds` read parked until resolution. These four
shapes — memoization-by-key, suspend-as-value, durable-deferred, timer-as-signal —
mirror `ClusterWorkflowEngine` as a *design reference*, with the engine's own
`Result`/`Ctx` types.

**A workflow step is one A5 transaction.** A step often writes several rows — a sleep
writes a `clockWakeups` row *and* sets `executions` to suspended; a deferred resolution
writes a `deferreds` row *and* drives the resume. Committing them as one transaction
inherits A5's atomicity, so a crash can't tear a step. The engine relies on A5 here; it
does not re-implement it.

## B4. Coordination plane (in-process)

This is the engine's only non-durable state, and it is what enforces single-writer
ownership (A1):

- **`running` map + resume.** `Map<executionId, { fiber, instance }>`. An execution is
  "owned" by being in this map on the one live engine instance. `resume` checks the live
  fiber, skips if completed, else forks the body into the engine scope.
- **Timers.** `Effect.delay(deadline − now)` + `forkIn(engineScope)` over a
  `clockWakeups` row; on fire, flip the row and resolve the deferred.
- **Boot recovery sweeps.** Re-arm pending clock wakeups; re-drive executions parked on a
  deferred whose in-process resume was lost to restart.

The sweeps need to enumerate executions across the fleet, which per-execution tables
don't provide — that is the roster's job.

## B5. The roster

A cross-execution index, itself an `S2DurableTable`, on one shared stream:

```ts
// roster row
{
  executionId: string
  workflowName: string
  status: "running" | "suspended" | "completed"
  suspendKind?: "deferred-wait" | "pending-clock"  // why suspended — for kind-aware recovery
  result?: EncodedExit                             // present once completed; outlives the stream
  resultAcked?: boolean                            // gates stream + roster-row deletion
  updatedMs: number
}
```

- The engine writes it on each lifecycle transition: `submit` → `running`; suspend →
  `suspended` (with `suspendKind`); complete → `completed` (write `result`, then delete
  the execution's stream per B6); the row is removed once `resultAcked`.
- **Cold-start enumeration:** at boot, query the roster for `status ∈ {running,
  suspended}`, open each named execution's table, preload it, and run the sweeps against
  its own `clockWakeups`/`deferreds`.
- **Result reads** for a completed execution whose stream is gone are served from the
  roster `result`.

**Kind-aware recovery.** A blanket "resume all suspended" sweep is unsafe — it cannot
tell a deferred-wait from an interrupt. The roster records `suspendKind` (or the sweep
re-derives it after opening the stream); recovery re-drives **only deferred-waits and
pending-clocks — never interrupts, never non-deferred suspensions.**

**Result home.** Store the result inline in the roster row, with a size cap. A pointer to
a separate results stream would add a third stream to the completion sequence (B6); reach
for it only if results exceed the cap.

**Memory note.** Every recovered running/suspended execution holds a live
`MaterializedState` at boot (one per open table). Fine for hundreds; at 10⁵⁺, idle
suspended executions need eviction.

## B6. Completion and compaction policy

Compaction *mechanism* is A6; *policy* is here.

- A long-running execution that accumulates history is compacted in place with
  `compact()`.
- A completed execution is compacted by `delete()` once its result is durably recorded
  **and acknowledged consumed** — the dominant path.

Because `delete()` destroys the final-result row, the result must outlive the stream, and
the completion path spans two streams (roster + execution) with no cross-stream
atomicity. Order it so the result has a durable home before the destructive delete:

```
1. write result → roster   2. await roster ack   3. delete execution stream   4. set resultAcked
```

Step 2 is load-bearing: if the roster write and the delete were pipelined and the process
crashed after the delete but before the roster write, the execution would have no stream
and no result — unrecoverable. With the ordering, every crash is safe: the result is in
the roster, and boot either deletes an orphaned stream idempotently or serves the result
from the roster.

## B7. What the engine does not do, and where it breaks

**No HA / no multi-writer.** One engine instance runs. Ordered by what fails first as you
add a second:

1. **The roster breaks first.** It is a shared multi-writer stream, and S2 has no per-key
   fencing (A1). Two instances writing lifecycle transitions contend on one cursor; the
   loser's write is dropped and the roster silently loses transitions. HA therefore starts
   at the roster — it needs an ownership/fencing story (or a per-owner shard) before
   per-execution concerns even matter.
2. **Per-execution CAS contention.** If two instances co-own an execution, both append at
   the same `match_seq_num`; the loser is rejected after already running its side effect,
   with no fence to stop it.
3. **`insertOrGet` first-writer-wins.** Single-writer-safe only (A2). Two owners can both
   miss and both insert. `activityClaims` — whose sole purpose is cross-worker
   arbitration — is what this breaks; under single-writer it is redundant, so it is
   **optional**, kept only as the seam a future HA story would re-arm.

Re-introducing HA is one mechanism layered back: a per-execution fence (or external lease)
enforcing *who may write*, with the roster deciding *who should own* — different layers.

**No `@effect/workflow`.** Reaching for `WorkflowEngine`/`ClusterWorkflowEngine` drags in
the cluster mailbox/sharding model, at odds with per-execution streams (see
`s2-durable-approaches-comparison.md`).

**No scheduler/dispatcher** beyond in-process resume + roster-driven recovery. A single
instance is the throughput ceiling; scaling past it is the HA problem above.

---

## Open questions

**Part A:**
- Validate against `s2 lite`: snapshot+trim head-prefix replay (a fresh preload from the
  post-trim head reconstructs identical state); transaction atomicity (a multi-write batch
  lands all-or-nothing, crash mid-batch replays cleanly); read-after-ack visibility.
- `MaterializedState`: reuse `@durable-streams/state`'s implementation as the fold, or an
  Effect-native re-impl (Schema-first, no `JSON.parse`)?

**Part B:**
- Confirm whether `deleteStream` is eventual — it decides whether a post-delete result
  read can race a not-yet-effective delete (B6).
- Roster as the HA seam: design its ownership/fencing now, or defer with single-writer
  pinned. Also roster write amplification and a status-keyed shard threshold.
- Result home: settle the inline size cap and the `resultAcked` protocol.
- `activityClaims`: cut now, or keep dormant as the HA seam?
- Eviction of idle suspended executions' materializers at 10⁵⁺ (B5 memory note).
