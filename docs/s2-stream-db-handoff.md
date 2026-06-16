# Handoff — S2StreamDb + the durable-execution engine

For the next agent. Read this first, then skim `packages/effect-s2-stream-db/`,
then the design docs. **Branch:** `main`.

---

## TL;DR

We built **`effect-s2-stream-db`** — a durable, materialized, compactable **state DB
over a single S2 stream** (the S2/Effect analog of StreamDB), on top of the
`effect-s2` client. It is **done, and tested against a real `s2 lite` server** (8
baseline tests green, typecheck + effect-language-service diagnostics clean). This is
**Part A** (the substrate). **Part B — the durable-execution engine — is not started.**
The next job is to build the engine on top of this DB, and to **collapse the old
`docs/log-interface-sketch.md` design into a much thinner model** that leans entirely
on the DB (most of that sketch's apparatus no longer needs to exist).

Everything runs on **mainline `effect@4.0.0-beta.78`** (via `effect-s2`), **not**
effect-smol. No `@effect/workflow`. No fakes — tests spawn a real `s2 lite`.

---

## The mental model (settled — do NOT relitigate)

This was hard-won over many iterations. The vocabulary and structure are final:

- **`S2StreamDb`** = a **database** = **one S2 stream**. (The old "DurableTable" name
  was wrong; the thing over a stream is a *db*, not a table.)
- **Table** = one **typed row collection** inside the db (one row type, one primary
  key). Tables are **`type`-partitions within the one stream** (the State-Protocol
  `type` discriminator), *not* separate streams. This is what gives **atomic
  multi-table writes** (one S2 batch covers all tables) and keeps **one stream per
  db**.
- **One db per stream.** For the engine that means **one db per execution** (one S2
  stream per execution), forced by S2's constraints (positional CAS contention + the
  200-batch/s/client cap on a shared stream) — see `s2-stream-db-sdd.md` §8.
- **Single-writer per stream** is the model. Positional CAS never contends, so **no
  per-write fencing**. Ownership is enforced *above* the db (in-process), not by a log
  fence.
- **Path derives from the schema.** `Db.open(key)` → stream `${basePath}/${encode(key)}`.
  You never hand-build a stream path; the db's `basePath` + the (schema-typed) key give
  it.
- **Materialization = the Durable Streams State-Protocol fold** (insert/update/delete →
  latest-value-per-`(type,key)`). The **same fold runs for live apply-on-ack and cold
  replay**, so the in-memory view can't diverge from the durable log (the
  *directionality invariant*: the view is never updated ahead of an S2 ack).
- **Compaction is S2-native**: `compact` = in-stream snapshot + trim (bounded by
  live-key count); `drop` = delete the stream.

The design rationale lives in `docs/s2-stream-db-sdd.md` (Part A = the db, Part B = the
engine). Caveat: that SDD's *prose still says "S2DurableTable" / "collection"* in
places and its code snippets predate the final API — treat it as the **conceptual**
source of truth (single-writer, one-stream-per-execution, the fold, compaction, the
roster, the correctness invariants), not the literal API.

---

## What was built — `packages/effect-s2-stream-db`

### Public API (final, Schema-Class-style — curried + self-typed)

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

Facade per table: `insert` / `insertOrGet` (first-writer-wins, single-writer only) /
`upsert` / `delete` / `get` / `query`. Db-wide: `transact` / `compact` / `drop`.
Errors: one `S2StreamDbError`.

### Files

```
packages/effect-s2-stream-db/
  src/
    StreamDb.ts        — Table/StreamDb factories (curried, self-typed) + the runtime
                         (open→preload-fold, facade writes = CAS atomic batch, transact,
                         compact = snapshot+trim, drop). The only non-trivial file.
    ChangeMessage.ts   — State-Protocol message vocabulary (Schema-first) + JSON codec.
    MaterializedState.ts — the fold: latest-value-per-(type,key). Pure. The ONE fold
                           used by both live-apply-on-ack and cold replay.
    errors.ts          — S2StreamDbError (tagged).
    index.ts           — barrel.
  test/
    s2lite.ts          — boots a REAL `s2 lite` server as a Scope-managed Layer<S2Client>
    stream-db.test.ts  — 8 baseline tests (all green)
    usage.ts           — typecheck-only proof that per-table row inference works
  README.md            — current API + model
```

### Test harness — how `s2 lite` is run (IMPORTANT: no fakes, no bare `node:`)

`test/s2lite.ts` does everything through the **Effect Node platform** — the user was
emphatic about this:
- spawn `s2 lite --port <port>` via **`ChildProcess.make`** (`effect/unstable/process`
  + `@effect/platform-node/NodeChildProcessSpawner`) — killed on scope close.
- ephemeral free port via **`NodeSocketServer.make({ port: 0 })`** → `address.port`.
- init file (pre-creates the basin `streamdbtest`, ≥8 bytes, with
  `create_stream_on_append`) via **`FileSystem` + `Path`** services.
- `effect-s2` is pointed at the server by **env vars** it reads:
  `S2_ACCESS_TOKEN` / `S2_ACCOUNT_ENDPOINT` / `S2_BASIN_ENDPOINT` / `S2_BASIN`
  (all set to `http://127.0.0.1:<port>` / a dummy token / the basin).
- readiness: retry `S2Client.checkTail("readyprobe")` until it answers (a 404 = up).
- exposed as `S2LiteLive: Layer<S2Client>`; tests use
  `layer(S2LiteLive, { excludeTestServices: true })` — **`excludeTestServices` is
  mandatory** or `@effect/vitest`'s injected `TestClock` freezes `Effect.sleep` and the
  readiness retry hangs.

Run: `pnpm --filter effect-s2-stream-db test` (needs `s2` CLI on PATH —
`brew install s2`, currently v0.36.6). Also `… typecheck` and `… diagnostics`.

### Git state

The package is committed on `main` (`5e4475d`). **Uncommitted in the working tree:**
the test harness (`test/s2lite.ts`, `test/stream-db.test.ts`), the `StreamDb.ts`
`preload` fix, and the `package.json` platform-node devDeps. **Commit these first.**

---

## effect@4-beta API landmines (these cost real time — keep this list)

The published beta differs from both effect v3 and the effect-smol `.d.ts` in spots.
Verified-correct forms:
- `Schema.Literals([...])` — **not** `Schema.Literal(a, b, c)`.
- `Schema.Union([...])` — takes an **array**.
- `Option.fromNullishOr(x)` — **not** `fromNullable`.
- `Effect.catch(handler)` — the catch-all; **not** `catchAll`.
- `Layer.unwrap(eff)` — **not** `unwrapScoped` (it handles the Scope).
- `Semaphore.make(1)` from the `Semaphore` module; `lock.withPermits(1)(effect)`.
- Schema annotations are **string-keyed** (`Annotations` is `{ [x: string]: unknown }`);
  attach via `schema.annotate({ [key]: v })`, read via `SchemaAST.resolveAt<T>(key)(ast)`.
  (This is how `primaryKey` is encoded in the schema.)
- The `effect-s2` `S2*` errors all share `_tag: "S2Error"` → discriminate with
  `instanceof S2NotFound` / `S2Conflict` / `S2RangeNotSatisfiable`, **not** `catchTag`.
- **S2 read of an empty stream (tail 0) returns 416, not 404.** `preload` must
  `checkTail` first and skip the fold when tail is 0 (this was a real bug the tests
  caught). Never-appended = 404 (`S2NotFound`).
- Multi-record atomic batch + CAS: `AppendInput.create(records, { matchSeqNum })`
  (cap: 1000 records / 1 MiB). Trim is a command record: `AppendRecord.trim(seqNum)`,
  rides in the same batch. The cursor for snapshot CAS comes from `checkTail().tail.seqNum`
  (and `ack.tail.seqNum` after append). Command records (trim/fence) carry an
  empty-name header — the fold filters them by that; State-Protocol control messages
  (`snapshot-start`/`-end`) are *data* records and pass through.
- Platform spawn deps: `ChildProcess` from `effect/unstable/process`;
  `NodeChildProcessSpawner` / `NodeFileSystem` / `NodePath` / `NodeSocketServer` from
  `@effect/platform-node` (subpath imports). `NodeChildProcessSpawner.layer` needs
  `FileSystem | Path`.

---

## Next: the engine (Part B), and simplifying `log-interface-sketch.md`

### The single most important insight to carry forward

**`log-interface-sketch.md` is mostly obsolete.** It designed, by hand, a sequenced
log (`Log`), a fold/state-machine (`Journal`), a persist/replay `Executor`, a
`Dispatcher`, bounded replay (`Seed`), and a `Compactor`. **All of that is now provided
by `S2StreamDb`:** the State-Protocol fold *is* the journal, `compact`/`drop` *is* the
compactor, the db facade *is* the persistence + replay, and `transact` *is* the atomic
persist. The engine no longer owns any of it.

So the engine that remains is genuinely thin — it is just **coordination over DBs**:

1. **State** lives entirely in two kinds of `S2StreamDb`:
   - one **`WorkflowDb` per execution** (`open(executionId)` → stream `wf/<execId>`),
     with tables `executions`, `activities`, `deferreds`, `clockWakeups` (and optional
     `activityClaims` — see SDD §7; likely cut under single-writer).
   - one **roster db** (a single shared `StreamDb`) indexing `executionId → status`
     (+ `suspendKind`, the completed `result`, `resultAcked`). This is the cold-start
     enumeration source and the home for a completed execution's result after its
     stream is dropped (SDD §B5/§B6).
2. **Coordination** is in-process only (it never used a durable coordination plane):
   a `running: Map<execId, fiber>` for resume/ownership, timers as
   `Effect.delay(...) + forkScoped` over `clockWakeups` rows, and **boot recovery
   sweeps** (re-arm pending clock wakeups; re-drive deferred-waiters) driven by the
   roster. Keep recovery **kind-aware** (only deferred-waits + pending-clocks; never
   interrupts) — SDD §B5.
3. **`Ctx`** (`run` / `sleep` / `awaitable`) is the handler surface, built on the db:
   `run` = activity memoization (`activities` keyed `execId/name/attempt`, idempotency
   key the same), `sleep` = a `clockWakeups` row + in-process delay, `awaitable` = a
   `deferreds` read parked until resolution. Mirror the four `ClusterWorkflowEngine`
   shapes (memoization-by-key, suspend-as-value, durable-deferred, timer-as-signal) as
   a *design reference* with our own types — **no `@effect/workflow` dependency**.
4. **A workflow step is one `db.transact`** — a step that writes a `clockWakeups` row
   *and* flips `executions` to suspended commits both atomically (one S2 batch), so a
   crash can't tear it. The engine relies on the db's atomicity; it re-implements none
   of it.

### Concrete guidance for evolving `log-interface-sketch.md`

Rewrite it (or replace it) as a short **"engine over S2StreamDb"** sketch. Specifically:

- **Delete** the `Log`, `Journal`, `Executor`, `Scheduler`/`Dispatcher`, `StepResult`,
  `Seed`/`Compactor`, fence/buffer-flush, and `lifts L1–L9` sections. They solved
  problems the db now owns. Keep a one-paragraph "what the db replaced" table for
  posterity if useful, then move on.
- **Keep & repurpose** only: `Ctx` (the handler surface) and the *coordination* concerns
  (running map, timers, boot sweeps, roster). These are the actual engine.
- **Frame the new doc around the two planes** (already in `s2-stream-db-sdd.md` §B):
  *state plane* = the DBs (nothing to design — it's `effect-s2-stream-db`), *coordination
  plane* = the thin in-process engine. The whole point of the rework is that the state
  plane is now a dependency, not something to build.
- **Show the engine as a consumer of the final db API**: define `WorkflowDb` and the
  roster as `StreamDb` classes with their tables, then the lifecycle (`submit` → open db
  + roster row; `step`/`run` the handler with `Ctx`; `suspend` = write rows + park;
  `resume` = re-open/re-fold + continue; `complete` = write result to roster, then
  `db.drop`, ordered so the result outlives the stream — SDD §B6).
- **Carry the open questions** from `s2-stream-db-sdd.md` "Open questions" (deleteStream
  eventual-vs-immediate; roster as the first HA break; `activityClaims` keep-or-cut;
  result inline size cap; materializer eviction at 10⁵⁺ open executions).

The target: someone reading the new sketch should see "the engine is a `running` map +
timers + boot sweeps + `Ctx`, over per-execution `S2StreamDb`s and a roster db" — and
nothing about hand-rolled logs or folds.

---

## Doc inventory

- `docs/s2-stream-db-sdd.md` — the SDD (Part A db / Part B engine). Conceptually
  authoritative; prose/API names partially stale (says "S2DurableTable"/"collection").
- `docs/log-interface-sketch.md` — **superseded**; the thing to collapse (above). Has a
  dangling link to the old SDD filename (`s2-durable-table-sdd.md` → now
  `s2-stream-db-sdd.md`).
- `docs/s2-durable-approaches-comparison.md` — why we build our own runtime vs adopting
  `@effect/workflow`'s `ClusterWorkflowEngine` (cluster mailbox/sharding model is at odds
  with per-execution S2 streams). Still valid context.
- `docs/reference/durable-streams/` — the Durable Streams + State Protocol reference
  (STATE-PROTOCOL.md, stream-db.md, durable-state.md). The protocol our fold implements.
- The old from-scratch spike lives on branch `worktree-s2-durable` (package
  `packages/fluent-s2-durable`) — historical; superseded by `effect-s2-stream-db`.

## How to run

```sh
brew install s2                                    # the s2 CLI (s2 lite server)
pnpm install
pnpm --filter effect-s2-stream-db typecheck
pnpm --filter effect-s2-stream-db diagnostics      # effect-language-service, must be 0 errors
pnpm --filter effect-s2-stream-db test             # spawns real s2 lite; 8 tests
# S2LITE_DEBUG=1 surfaces the server's stdout/stderr during tests
```
