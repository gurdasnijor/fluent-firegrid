# Sketch: the durable-execution engine over `S2StreamDb`

**Status:** design sketch · the engine half of [`s2-stream-db-sdd.md`](./s2-stream-db-sdd.md)
(Part B), written as a consumer of the shipped `effect-s2-stream-db` package.

> **History.** This file used to design, by hand, a sequenced byte log (`Log`), a
> fold/state-machine (`Journal`), a persist/replay `Executor`, a `Dispatcher`, bounded
> replay (`Seed`), a `Compactor`, and a fence/buffer-flush persist model (with nine
> "lifts" L1–L9 from Effect's `WorkflowEngine`). **All of that is gone** — `S2StreamDb`
> now owns it (see "What the db replaced" below). What survives is the actual engine:
> the `Ctx` handler surface and a thin in-process coordination layer.

---

## The shape: two planes

The engine is split along the same line as the SDD:

- **State plane = the DBs.** Nothing to design — it is `effect-s2-stream-db`. One
  **`WorkflowDb` per execution** (one S2 stream `wf/<execId>`) holding `executions` /
  `activities` / `deferreds` / `clockWakeups`, plus one **shared roster db** indexing
  `executionId → status` (+ `suspendKind`, the completed `result`, `resultAcked`).
- **Coordination plane = the thin in-process engine.** A `running` map for
  ownership/resume, timer fibers over `clockWakeups`, and boot-recovery sweeps driven by
  the roster. This is the only thing this document designs.

```
   Ctx ─▶ engine ─┬─▶ per-execution WorkflowDb ─▶ S2 stream  (executions/activities/…)
  (run/   (in-proc │
   sleep, coord)   └─▶ roster S2StreamDb ───────▶ S2 stream  (executionId → status/result)
   awaitable)
                   in-process only: running map · timer fibers · boot recovery sweeps
```

The whole point of the rework: the state plane is a **dependency**, not something to
build. The engine is just *coordination over DBs* — a `running` map + timers + boot
sweeps + `Ctx`.

---

## What the db replaced

Everything the old bottom-half seams solved is now a property of `S2StreamDb`. For
posterity:

| Old hand-rolled seam | Now provided by `S2StreamDb` |
|---|---|
| `Log` (sequenced byte log, fence, trim) | the db's S2 stream + conditional-append commit path |
| `Journal` / `fold` / `replay` | the State-Protocol fold (`MaterializedState`); `open` preloads it |
| `Executor` persist + replay | the table facades + `open`-time fold (one fold for live & replay) |
| `Seed` / `Compactor` (`Write.Trim` + snapshot) | `db.compact` (snapshot + trim) / `db.drop` |
| buffer-the-wake / flush-once / fence-on-every-persist (L1, L9, Tier-1) | `db.transact` — one atomic conditional batch per step |
| per-write fencing / `Conflict` asymmetry | single-writer per stream (SDD A2): positional CAS never contends |

The fence/buffer-flush machinery in particular evaporated because we moved from a
shared multi-writer log to **one stream per execution with a single in-process writer**
— so there is no lease to present and no `Conflict("fence")` to handle.

---

## `Ctx` — the handler-facing primitives (survives unchanged)

The top seam the user-facing combinator API (restate-fluent) builds on. Each primitive
is a plain `Effect`, now backed by the per-execution `WorkflowDb` rather than a raw log.

```ts
export interface Ctx {
  /** Activity memoization. Idempotency key `${execId}/${name}/${attempt}` — on replay,
   *  short-circuits from the folded `activities` row; else runs and upserts the result. */
  readonly run: <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | WfError, R>
  /** Durable timer: write a `clockWakeups` row + arm an in-process delay; on fire,
   *  flip the row and resolve the waiter. */
  readonly sleep: (name: string, duration: Duration.Duration) => Effect.Effect<void, WfError>
  /** Durable deferred: read the `deferreds` row; park until an external `signal`
   *  upserts its `exit` and resumes the execution. */
  readonly awaitable: <A>(name: string) => Effect.Effect<A, WfError>
  // seams: state / call / send
}
```

These four shapes — **memoization-by-key, suspend-as-value, durable-deferred,
timer-as-signal** — mirror Effect's `ClusterWorkflowEngine` as a *design reference*
only; the engine uses its own `Result`/`Ctx` types and takes **no `@effect/workflow`
dependency** (the cluster mailbox/sharding model is at odds with per-execution S2
streams — see `s2-durable-approaches-comparison.md`).

> **Clock/random seeding is genesis-once** — a handler reading wall-time reads the
> *start* time for the life of the execution (deterministic, the intended default).
> Flag it: a handler branching on *elapsed* wall-time reads genesis time forever. For
> per-call wall-clock semantics, record each read as its own `activities`-style row, not
> the genesis seed.

---

## The coordination plane

### Running map + resume

`running: Map<executionId, { fiber, instance }>`. An execution is "owned" by being in
this map on the one live engine instance — this is what enforces single-writer (SDD
A2). `resume` checks the live fiber, skips if completed, else forks the body into the
engine scope and re-drives it. Resume is **re-open + re-fold + continue**: open the
execution's `WorkflowDb` (which preloads the fold), then run the handler — `ctx.run`
short-circuits from the folded `activities`, `ctx.sleep` re-arms its timer, `ctx.awaitable`
re-parks or reads its now-resolved `deferreds` row.

### Timers

`Effect.delay(deadline − now)` + `forkIn(engineScope)` over a `clockWakeups` row —
**not** `forkScoped`: the timer is armed inside a step (`ctx.sleep`) that finishes long
before the fiber fires, so a step-scoped fiber would be torn down with the step and the
wakeup would be lost; it must fork into the engine's long-lived scope. On fire, two
distinct things happen: (1) a durable `db.transact` flips the `clockWakeups` row to
`fired` and writes the deferred's `exit`; (2) **separately**, the in-process resume
re-drives the parked fiber — the resume is *not* part of the batch. No separate timer
store — the `clockWakeups` table *is* the durable timer; the fiber is just the live arm.

### Boot recovery sweeps (roster-driven)

At boot the per-execution dbs give no fleet-wide enumeration, so recovery is driven by
the roster:

1. Query the roster for `status ∈ {running, suspended}`.
2. For each, open its `WorkflowDb` (preloads the fold), and run **kind-aware** recovery:
   re-arm `pending` `clockWakeups`, re-drive deferred-waiters. **Only deferred-waits and
   pending-clocks — never interrupts, never non-deferred suspensions** (a blanket
   "resume all suspended" cannot tell a wait from an interrupt; SDD B5).

Recovery needs no separate timer-rehydration pass: re-driving the handler re-steps it to
its `ctx.sleep`, which re-arms the timer for free — so boot is *enumerate + resume*.

---

## Lifecycle (the public surface, over the final db API)

```ts
class WorkflowDb extends StreamDb<WorkflowDb>("wf")({
  executions, activities, deferreds, clockWakeups,
}, ExecutionId) {}                                   // → stream "wf/<execId>"  (SDD B2)

// RosterRow's table name is "roster" (not "executions") — it's the cross-execution
// status index, distinct from WorkflowDb's per-execution `executions` table (SDD B5).
class RosterDb extends StreamDb<RosterDb>("roster")({ roster: RosterRow }) {}
const roster = yield* RosterDb.open("global")        // one shared stream "roster/global"
```

| Verb | What it does |
|---|---|
| **`submit(execId, input)`** | `RosterDb` row → `running`; `WorkflowDb.open(execId)`; fork the handler into the engine scope and add to `running`. |
| **`step` / handler run** | run the handler with `Ctx`; each `ctx.*` reads/writes the execution's `WorkflowDb`. **One step = one `db.transact`** (e.g. a sleep writes a `clockWakeups` row *and* flips `executions` to suspended atomically — a crash can't tear it). |
| **`suspend`** | write the rows (clock wakeup / deferred park) + flip `executions`/roster to `suspended` with `suspendKind`; park the fiber. |
| **`resume(execId)`** | re-open + re-fold + continue (see coordination plane); skip if already in `running` and live. |
| **`signal(execId, name, value)`** | `deferreds.upsert({ name, exit })` then `resume(execId)`. |
| **`complete`** | write `result` → roster; **await roster ack**; then `WorkflowDb.drop`; then set `resultAcked`. The ordering is load-bearing — the result must outlive the stream (SDD B6). |
| **`poll` / `result`** | read the execution's `executions`/result; once the stream is dropped, serve the result from the roster row. |

A completed execution is reclaimed by `db.drop` (after its result is durably in the
roster); a long-running one is compacted in place by `db.compact` (SDD B6/A7). The
engine re-implements none of this — it relies on the db.

---

## Open questions (carried from `s2-stream-db-sdd.md` Part B)

- Confirm whether `deleteStream` (`db.drop`) is eventual — it decides whether a
  post-drop result read can race a not-yet-effective delete (B6).
- Roster as the first HA break: design its ownership/fencing now, or defer with
  single-writer pinned. Also roster write amplification and a status-keyed shard
  threshold.
- Result home: settle the inline size cap and the `resultAcked` protocol.
- `activityClaims`: cut now, or keep dormant as the HA seam (cross-worker arbitration,
  redundant under single-writer — SDD B7)?
- Eviction of idle suspended executions' materializers at 10⁵⁺ open executions (B5
  memory note).
