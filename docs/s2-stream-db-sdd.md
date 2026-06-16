# SDD — `S2StreamDb` and the durable-execution engine

**Status:** design. Part A (`S2StreamDb`) is **built and tested** against a real
`s2 lite` server — see `packages/effect-s2-stream-db/`. Part B (the engine) is **not
started**. This document supersedes the `Log`/`Journal`/`Executor`/`Compactor`
apparatus in `log-interface-sketch.md`; it keeps the `Ctx` handler surface.

## Two planes, kept separate

This document specifies two things, and the whole point is that they are independent:

- **Part A — `S2StreamDb`** — a general-purpose durable, materialized, compactable
  **typed state DB over a single S2 stream**. It knows nothing about workflows,
  executions, or results. It could back a chat room or a feature-flag store equally
  well. **This is now a dependency, not something to design — it ships as
  `effect-s2-stream-db`.**
- **Part B — the durable-execution engine** — a thin in-process coordinator that
  *uses* `S2StreamDb` (one db per execution, plus one shared roster db) to run durable
  workflows.

Part A is the **state plane** (the substrate); Part B is the **coordination plane**
(one consumer of it). Nothing in Part A references a concept from Part B. Read Part A
as if Part B did not exist.

The underlying idea Part B exploits: a durable workflow is a few typed tables (its
activities, timers, deferreds, status) materialized from an append-only log. The
[Durable Streams State Protocol] already defines that materialization, and S2 is the
log — so the engine never hand-writes a journal, a replay fold, or a compactor. The
existing [`DurableStreamsWorkflowEngine`] is a *reference* that this table-based
approach works over Durable Streams; Part B is built fresh on `S2StreamDb`, not ported
from it, and does not depend on `@effect/workflow`.

[Durable Streams State Protocol]: ./reference/durable-streams/packages/state/STATE-PROTOCOL.md
[`DurableStreamsWorkflowEngine`]: https://github.com/gurdasnijor/firegrid/tree/main/packages/runtime/src/engine

---

# Part A — `S2StreamDb` (substrate; workflow-agnostic)

A `S2StreamDb` is a **database** = **one S2 stream**, aggregating a set of named,
schema-typed **tables** materialized from that stream, with a **single writer**. It
provides typed CRUD per table, atomic multi-table transactions, and compaction. That
is the entire abstraction.

The vocabulary is load-bearing and final:

- **`S2StreamDb`** = a **db** = **one S2 stream**. (The thing over a stream is a *db*,
  not a table — the earlier "S2DurableTable" / "DurableTable" name was wrong.)
- **Table** = one **typed row collection** inside the db (one row type, one primary
  key). Tables are **`type`-partitions within the one stream** (the State-Protocol
  `type` discriminator), *not* separate streams. This is what gives **atomic
  multi-table writes** (one S2 batch covers all tables) and keeps **one stream per db**.

## A1. The definition surface (Schema-Class-style — curried + self-typed)

A table is fully described by its `Schema.Struct`: row fields, its relative path (the
State-Protocol `type`), and its primary key (the `primaryKey`-annotated field). A db
aggregates named tables over one stream, and the stream **path is derived from the
schema** — `open(key)` validates the key and derives `${basePath}/${encode(key)}`. You
never hand-build a stream path.

```ts
import { Effect, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"

// A Table is fully described by its Schema: row fields, relative path (the `type`,
// here "activities"), and primary key (the annotated field).
class Activity extends Table<Activity>("activities")({
  activityKey: Schema.String.pipe(primaryKey),
  result: Schema.Unknown,
}) {}

// optional schema-typed instance key (defaults to Schema.String)
const ExecutionId = Schema.String.pipe(Schema.brand("ExecutionId"))

// An S2StreamDb aggregates tables over one stream; path derives from the schema.
class WorkflowDb extends StreamDb<WorkflowDb>("wf")({ activities: Activity }, ExecutionId) {}

const program = Effect.gen(function*() {
  const db = yield* WorkflowDb.open(ExecutionId.make("exec-1"))   // → stream "wf/exec-1"
  yield* db.activities.insert({ activityKey: "charge", result: { ok: true } })
  const a = yield* db.activities.get("charge")                    // Option<row>, typed
  yield* db.transact((tx) => {                                    // atomic across tables
    tx.upsert("activities", { activityKey: "fulfill", result: null })
  })
  yield* db.compact                                               // snapshot + trim
  yield* db.drop                                                  // delete the stream
})
```

## A2. Single-writer model

One owner writes one db's stream. Under that assumption S2's positional
compare-and-swap never contends, so check-then-append is correct and **per-write
fencing is unnecessary**. The db does *not* enforce single-writer; it assumes it. The
substrate facts that make the assumption load-bearing: S2 offers positional CAS (a
fence on the next sequence number, shared across all writers to a stream) and **no
per-key producer fencing**, plus a 200-append-batch/s/client cap per stream. Two
concurrent writers would contend on the one cursor with no per-key arbitration — out
of scope here; the consumer guarantees single-writer (Part B does this in-process).

## A3. Table surface

Each table exposes a typed facade (`CollectionFacade<Row, Key>` in the code); the db
adds db-wide `transact` / `compact` / `drop`.

```ts
export interface CollectionFacade<Row, Key extends string = string> {
  readonly insert: (row: Row) => Effect.Effect<void, S2StreamDbError>
  readonly insertOrGet: (row: Row) => Effect.Effect<InsertOrGetResult<Row>, S2StreamDbError>
  readonly upsert: (row: Row) => Effect.Effect<void, S2StreamDbError>
  readonly delete: (key: Key) => Effect.Effect<void, S2StreamDbError>
  readonly get: (key: Key) => Effect.Effect<Option.Option<Row>, S2StreamDbError>
  readonly query: <A>(build: (rows: ReadonlyArray<Row>) => A) => Effect.Effect<A, S2StreamDbError>
}

export type InsertOrGetResult<Row> =
  | { readonly _tag: "Inserted" }
  | { readonly _tag: "Found"; readonly row: Row }
```

`insertOrGet` is **check-then-append under the single owner**: `get` misses → append
the insert. First-writer-wins holds only because there is exactly one writer (S2 CAS is
positional, not key-conditional, so it cannot arbitrate two writers racing on the same
key — see A2).

The facade omits any replay-then-tail (`rows`/`subscribe`): the only plausible
implementation is a second S2 read-session folding records back, which is a second
materialization path that can drift from A4's. `query` reads the *single* in-memory
materialization the owner already holds. If a live tail is ever needed, it observes
that same fold, never a second read-session.

S2 lives behind this facade only — a caller sees tables, never S2 records, offsets, or
`matchSeqNum`. The db is built on the `effect-s2` `S2Client` (append, conditional
append, read, trim, delete-stream) and is the sole importer of the S2 SDK. Every
operation's error channel is the single tagged `S2StreamDbError` (it carries the
failing `operation`, a `message`, and the `cause`), so callers `catchTag` one type.

## A4. Materialization is the State-Protocol fold

A write on table `T` appends one State-Protocol change message:

```json
{ "type": "<table>", "key": "<encoded-pk>", "value": <row>,
  "headers": { "operation": "insert" | "update" | "delete" } }
```

A read materializes by applying messages in stream order to an in-memory
`MaterializedState`: latest-value-per-`(type, key)`, `delete` removes. That fold *is*
the durable state — there is no separate journal. `get`/`query` read it. Command
records (trim/fence) carry an empty-name header and are skipped by the fold;
State-Protocol control messages (`snapshot-start`/`snapshot-end`) are *data* records
and pass through.

## A5. The directionality invariant

The readable view must never run ahead of the durable log:

> **Invariant.** `MaterializedState` is strictly a downstream materialization of the
> durably-committed S2 stream. It is **never updated ahead of an S2 ack.**

Two consequences:

- **Read-after-ack.** A write becomes visible to a subsequent `get` only after S2 acks
  its append; the caller blocks on the ack. Applying locally first "for latency" is
  forbidden — a crash between the local apply and the append landing would expose state
  that replay finds was never durable.
- **One fold for live and replay.** The apply-on-ack path and the cold-replay fold are
  the *same function*: on ack, the just-committed message is handed to the exact fold a
  fresh preload uses — it is not read back from S2. One path, so live and replay cannot
  diverge.

## A6. Atomic transactions

Multiple writes — across one or several tables — commit as **one atomic S2 batch**:

> A transaction **buffers its writes, flushes them as one atomic conditional S2 batch
> (`matchSeqNum = cursor`), waits for the batch ack, then applies them to
> `MaterializedState` via the fold.**

S2 batches are atomic up to 1000 records / 1 MiB on one stream. A transaction
exceeding that is a hard error, not a silent multi-batch (which would partially apply
on a crash). The only legitimately large payload is a single oversized row *value*,
handled by framing during compaction (A7) — not by splitting a transaction.

The single-table facade writes (`insert`/`upsert`/`delete`) are the one-message case of
the same commit path.

## A7. Compaction

Two mechanisms; *when* to invoke them is the consumer's policy, not the db's.

- **`compact` — snapshot + trim.** One atomic conditional batch (`matchSeqNum =
  cursor`): a `snapshot-start` control record, one `insert` per live `(type, key)` in
  `MaterializedState`, a `snapshot-end` control record, then `trim(upTo = cursor)`. On
  ack the snapshot prefix becomes the stream head; a fresh preload reads from head,
  loads the records between the markers into a clean `MaterializedState`, then follows
  deltas. **Preload cost is bounded by live-key count, not total history.**
  Single-writer means there are no concurrent deltas during snapshot production, so no
  delta-buffering is needed. The snapshot must fit one atomic batch (live keys + 3
  control/trim records ≤ 1000); a single row value over the record cap would be split
  into ordered **framed fragments** between the markers (`{ key, frameIndex,
  frameCount }`) — a reader concatenates before decoding (not yet implemented; the
  current `compact` rejects an over-cap snapshot rather than splitting).
- **`drop` — delete the stream.** Discards the db entirely. Preload cost and storage
  go to zero.

---

# Part B — the durable-execution engine (a consumer of `S2StreamDb`)

The engine runs durable workflows by composing `S2StreamDb` instances with a thin
in-process coordination layer. It contributes no new persistence machinery — only
schema, deployment, and coordination. **The state plane is now a dependency
(`effect-s2-stream-db`); the only thing to build is the coordination plane.**

```
   Ctx ─▶ engine ─┬─▶ per-execution WorkflowDb ─▶ S2 stream  (executions/activities/…)
  (run/   (in-proc │
   sleep, coord)   └─▶ roster S2StreamDb ───────▶ S2 stream  (executionId → status/result)
   awaitable)
                   in-process only: running map · timer fibers · boot recovery sweeps
```

## B1. Deployment — one db per execution, plus a roster

The engine instantiates **one `WorkflowDb` per execution** (so one S2 stream per
execution) and **one roster db** shared across executions.

Per-execution streams are the right granularity here precisely because of A2's
substrate facts: every engine read is already keyed by `executionId`; per-execution
streams give independent CAS cursors and independent 200-batch/s budgets, and let a
finished execution be compacted by `drop`. A single shared stream would force every
execution's writes onto one cursor and one rate budget with no per-key fencing to
separate them.

## B2. Schema

The per-execution db is a `StreamDb` with tables `executions`, `activities`,
`deferreds`, `clockWakeups`, and (optional, see B7) `activityClaims`:

```ts
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import { Schema } from "effect"

const ExecutionId = Schema.String.pipe(Schema.brand("ExecutionId"))

class ExecutionRow extends Table<ExecutionRow>("executions")({
  executionId: Schema.String.pipe(primaryKey),
  status: Schema.Literals(["running", "suspended", "completed"]),
  suspended: Schema.Boolean,
  cause: Schema.optional(Schema.Unknown),
}) {}

class ActivityRow extends Table<ActivityRow>("activities")({
  activityKey: Schema.String.pipe(primaryKey),   // execId/name/attempt
  result: Schema.Unknown,
}) {}

class DeferredRow extends Table<DeferredRow>("deferreds")({
  name: Schema.String.pipe(primaryKey),
  exit: Schema.optional(Schema.Unknown),         // present once resolved
}) {}

class ClockWakeupRow extends Table<ClockWakeupRow>("clockWakeups")({
  name: Schema.String.pipe(primaryKey),
  deadlineMs: Schema.Number,
  status: Schema.Literals(["pending", "fired"]),
}) {}

class WorkflowDb extends StreamDb<WorkflowDb>("wf")({
  executions: ExecutionRow,
  activities: ActivityRow,
  deferreds: DeferredRow,
  clockWakeups: ClockWakeupRow,
}, ExecutionId) {}
```

## B3. Mechanisms

Each durable-execution mechanism is a table operation on the execution's `WorkflowDb`:

| Mechanism | Over the execution's `WorkflowDb` |
|---|---|
| **Execution state** | `executions.upsert({ executionId, status, suspended, cause? })` / `get` |
| **Activity memoization** | `activities.get(execId/name/attempt)` → return `result` if present; else run, `activities.upsert({ result })` |
| **Suspend** | suspend-as-value: the body returns `Result = Complete \| Suspended`; on `Suspended`, `executions.upsert({ suspended: true })` |
| **Durable deferred / awaitable** | `deferreds.get(name)` → `exit`; resolve via `deferreds.upsert({ exit })` then `resume(executionId)` |
| **Durable timer / sleep** | `clockWakeups.upsert({ deadlineMs, status: "pending" })`; arm `Effect.delay` + `forkIn(engineScope)` (not `forkScoped` — the timer must outlive the step that armed it); on fire set `status: "fired"` + resolve the deferred |
| **Resume** | in-process `running: Map<execId, { fiber, instance }>`; `resume` checks `running`, else forks the body into scope |

`Ctx.run/sleep/awaitable` keep their surface: `run` is `activities` memoization
(idempotency key `execId/name/attempt`), `sleep` is a `clockWakeups` row plus an
in-process delay, `awaitable` is a `deferreds` read parked until resolution. These four
shapes — memoization-by-key, suspend-as-value, durable-deferred, timer-as-signal —
mirror `ClusterWorkflowEngine` as a *design reference*, with the engine's own
`Result`/`Ctx` types — **no `@effect/workflow` dependency.**

**A workflow step is one A6 transaction.** A step often writes several rows — a sleep
writes a `clockWakeups` row *and* sets `executions` to suspended; a deferred resolution
writes a `deferreds` row *and* drives the resume. Committing them as one `db.transact`
inherits A6's atomicity, so a crash can't tear a step. The engine relies on A6 here; it
does not re-implement it.

## B4. Coordination plane (in-process)

This is the engine's only non-durable state, and it is what enforces single-writer
ownership (A2):

- **`running` map + resume.** `Map<executionId, { fiber, instance }>`. An execution is
  "owned" by being in this map on the one live engine instance. `resume` checks the live
  fiber, skips if completed, else forks the body into the engine scope.
- **Timers.** `Effect.delay(deadline − now)` + `forkIn(engineScope)` over a
  `clockWakeups` row; on fire, flip the row and resolve the deferred. It must fork into
  the engine's long-lived scope, **not** `forkScoped` — the step that arms the timer
  finishes long before the timer fires, so a step-scoped fiber would be torn down with
  the step and the wakeup would never arrive.
- **Boot recovery sweeps.** Re-arm pending clock wakeups; re-drive executions parked on a
  deferred whose in-process resume was lost to restart.

The sweeps need to enumerate executions across the fleet, which per-execution dbs don't
provide — that is the roster's job.

## B5. The roster

A cross-execution index, itself an `S2StreamDb`, on one shared stream:

```ts
// table name "roster" — distinct from the per-execution `executions` table (B2);
// this is the cross-execution status index, on its own shared stream.
class RosterRow extends Table<RosterRow>("roster")({
  executionId: Schema.String.pipe(primaryKey),
  workflowName: Schema.String,
  status: Schema.Literals(["running", "suspended", "completed"]),
  suspendKind: Schema.optional(                  // why suspended — for kind-aware recovery
    Schema.Literals(["deferred-wait", "pending-clock"]),
  ),
  result: Schema.optional(Schema.Unknown),       // present once completed; outlives the stream
  resultAcked: Schema.optional(Schema.Boolean),  // gates stream + roster-row deletion
  updatedMs: Schema.Number,
}) {}

class RosterDb extends StreamDb<RosterDb>("roster")({ roster: RosterRow }) {}
// one shared stream: RosterDb.open("global")
```

- The engine writes it on each lifecycle transition: `submit` → `running`; suspend →
  `suspended` (with `suspendKind`); complete → `completed` (write `result`, then drop
  the execution's stream per B6); the row is removed once `resultAcked`.
- **Cold-start enumeration:** at boot, query the roster for `status ∈ {running,
  suspended}`, open each named execution's `WorkflowDb`, preload it, and run the sweeps
  against its own `clockWakeups`/`deferreds`.
- **Result reads** for a completed execution whose stream is gone are served from the
  roster `result`.

**Kind-aware recovery.** A blanket "resume all suspended" sweep is unsafe — it cannot
tell a deferred-wait from an interrupt. The roster records `suspendKind` (or the sweep
re-derives it after opening the stream); recovery re-drives **only deferred-waits and
pending-clocks — never interrupts, never non-deferred suspensions.**

**Result home.** Store the result inline in the roster row, with a size cap. A pointer
to a separate results stream would add a third stream to the completion sequence (B6);
reach for it only if results exceed the cap.

**Memory note.** Every recovered running/suspended execution holds a live
`MaterializedState` at boot (one per open db). Fine for hundreds; at 10⁵⁺, idle
suspended executions need eviction. The **roster is itself one `MaterializedState`**
holding every active + not-yet-`resultAcked` row, and it is the single
write-amplification target (every lifecycle transition of every execution writes it);
at 10⁵⁺ live executions the roster materializer is its own size/throughput concern,
distinct from the per-execution ones — see Open questions.

## B6. Completion and compaction policy

Compaction *mechanism* is A7; *policy* is here.

- A long-running execution that accumulates history is compacted in place with
  `db.compact`.
- A completed execution is compacted by `db.drop` once its result is durably recorded
  **and acknowledged consumed** — the dominant path.

Because `drop` destroys the final-result row, the result must outlive the stream, and
the completion path spans two streams (roster + execution) with no cross-stream
atomicity. Order it so the result has a durable home before the destructive drop:

```
1. write result → roster   2. await roster ack   3. drop execution stream   4. set resultAcked
```

Step 2 is load-bearing: if the roster write and the drop were pipelined and the process
crashed after the drop but before the roster write, the execution would have no stream
and no result — unrecoverable. With the ordering, every crash is safe: the result is in
the roster, and boot either drops an orphaned stream idempotently or serves the result
from the roster.

## B7. What the engine does not do, and where it breaks

**No HA / no multi-writer.** One engine instance runs. Ordered by what fails first as
you add a second:

1. **The roster breaks first.** It is a shared multi-writer stream, and S2 has no
   per-key fencing (A2). Two instances writing lifecycle transitions contend on one
   cursor; the loser's write is dropped and the roster silently loses transitions. HA
   therefore starts at the roster — it needs an ownership/fencing story (or a per-owner
   shard) before per-execution concerns even matter.
2. **Per-execution CAS contention.** If two instances co-own an execution, both append
   at the same `matchSeqNum`; the loser is rejected after already running its side
   effect, with no fence to stop it.
3. **`insertOrGet` first-writer-wins.** Single-writer-safe only (A3). Two owners can
   both miss and both insert. `activityClaims` — whose sole purpose is cross-worker
   arbitration — is what this breaks; under single-writer it is redundant, so it is
   **optional**, kept only as the seam a future HA story would re-arm.

Re-introducing HA is one mechanism layered back: a per-execution fence (or external
lease) enforcing *who may write*, with the roster deciding *who should own* — different
layers.

**No `@effect/workflow`.** Reaching for `WorkflowEngine`/`ClusterWorkflowEngine` drags
in the cluster mailbox/sharding model, at odds with per-execution streams (see
`s2-durable-approaches-comparison.md`).

**No scheduler/dispatcher** beyond in-process resume + roster-driven recovery. A single
instance is the throughput ceiling; scaling past it is the HA problem above.

---

## Open questions

**Part A** (the validation work is largely answered — the package is tested against
`s2 lite`; what remains is the framing edge case):
- ✅ Validated against `s2 lite`: snapshot+trim head-prefix replay, transaction
  atomicity, read-after-ack visibility (8 baseline tests green).
- Oversized single row value: `compact` currently *rejects* an over-cap snapshot rather
  than splitting it into framed fragments (A7). Implement framing only if a real row
  value approaches the record/byte cap.

**Part B:**
- Confirm whether `deleteStream` (`db.drop`) is eventual — it decides whether a
  post-drop result read can race a not-yet-effective delete (B6).
- Roster as the HA seam: design its ownership/fencing now, or defer with single-writer
  pinned. Also roster write amplification and a status-keyed shard threshold.
- Result home: settle the inline size cap and the `resultAcked` protocol.
- `activityClaims`: cut now, or keep dormant as the HA seam?
- Eviction of idle suspended executions' materializers at 10⁵⁺, and the roster's own
  materializer size / write-amplification ceiling (B5 memory note).
