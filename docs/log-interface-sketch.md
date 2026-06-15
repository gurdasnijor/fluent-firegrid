# Sketch: a type-aligned `Log` port (and the layers above it)

> **⚠️ Superseded (mostly) by `s2-durable-table-sdd.md`.** That rework pushes all
> durable state down into an `S2DurableTable` (State-Protocol fold over S2), which
> **deletes the `Log`/`Journal`/`Executor`/`Seed`/`Compactor` apparatus below** and
> **collapses the fence/buffer-flush machinery** (single-writer per execution stream
> removes per-write fencing). What survives from this doc: the `Ctx` surface (§4),
> suspend-as-value (L1), attempt-keyed memoization (L7), and the patterns lifted from
> Effect's engine as *shape references*. Read this only for that context and the
> `ClusterWorkflowEngine` analysis; the bottom-half seams here are no longer the plan.
> See `s2-durable-table-sdd.md` §6 for the section-by-section migration.

**Status:** ⚠️ partially superseded · design sketch · Goal: replace the blurry
`runtime.ts` surface with crisp, composable seams — bottom (durable log) to handler API.

The current `runtime.ts` mixes a grab-bag of responsibilities behind one `Worker`
(`start`/`tick`/`resolveEvent`/`awaitResult`/`boot`/`snapshot`/`runLoop`), and the
S2 boundary leaks SDK types upward. This sketches the seams that make the whole
thing legible, each a small `Context.Service` — with the "Engine" itself factored
into `Executor` + `Dispatcher` + `Engine` (§3):

```
Log         — durable, sequenced, fenceable byte log         (S2 lives here, and only here)
  ▲
Journal     — fold Log entries → invocation state            (the StateMachine; pure)
  ▲
Executor    — advance ONE execution: lease→fold→run→persist  (emits StepResult)   ┐
Dispatcher   — which execution is ready, and when (in-memory)                      ├ "Engine"
Engine      — composition + lifecycle: submit/signal/result/run (the pump)        ┘
  ▲
Ctx         — the handler-facing primitives (run/sleep/awakeable)
```

Only `Log` is substrate-specific; everything above is pure Effect over `Log`. The
"Engine" is really three small services (`Executor` + `Dispatcher` + `Engine`) — see §3.

---

## 1. `Log` — the bottom seam (what S2 provides)

Modeled in effect-smol's idiom: branded ids via `Schema.brand`, `Schema.Class`
records, `Data.TaggedError`, a `Context.Service`, `Effect`/`Stream`. No SDK type
leaks past this file; `s2Live.ts` is the single adapter.

```ts
import { Brand, Context, Data, Effect, Schema, Stream } from "effect"

// ── Branded scalars ─────────────────────────────────────────────────────────

/** A log id — one log per workflow execution (`wf/{execId}`). */
export type LogId = string & Brand.Brand<"LogId">
export const LogId = Schema.String.pipe(Schema.brand("LogId"))

/** S2's LSN: a position in a single log. Total order within a `LogId`.
 *  `number` is safe for per-execution journals (tiny, short-lived). The inbox
 *  under heavy signal volume and any future long-lived state stream are u64 in S2
 *  and would overrun 2^53 — brand those `bigint` (a separate `SeqNum64`), or at
 *  minimum treat this `number` width as a per-journal decision, not a default. */
export type SeqNum = number & Brand.Brand<"SeqNum">
export const SeqNum = Schema.Number.pipe(Schema.int(), Schema.brand("SeqNum"))

/** A lease token. S2 caps these at 36 UTF-8 bytes. */
export type FenceToken = string & Brand.Brand<"FenceToken">
export const FenceToken = Schema.String.pipe(Schema.maxLength(36), Schema.brand("FenceToken"))

// ── Records & writes ────────────────────────────────────────────────────────

/** A record read back from the log. */
export class LogRecord extends Schema.Class<LogRecord>("LogRecord")({
  seq: SeqNum,
  body: Schema.Uint8Array,
}) {}

/** One entry in an append batch — a data record or a command. */
export type Write = Data.TaggedEnum<{
  Append: { readonly body: Uint8Array }
  Fence: { readonly token: FenceToken }   // claim/transfer the lease
  Trim: { readonly upTo: SeqNum }          // discard history below `upTo`
}>
export const Write = Data.taggedEnum<Write>()

/** Append preconditions — S2's two concurrency primitives, named.
 *
 *  `fence` is structurally optional here (acquisition writes the *first* fence
 *  with no token to present), but **S2 fencing is cooperative**: an append that
 *  omits the token still commits, so a superseded owner that simply leaves it off
 *  writes successfully — a zombie hole. The invariant that closes it lives one
 *  layer up: `Executor.step` must present the held token on *every* journal
 *  persist (see §3.2 / Tier-1 invariant), not only on acquisition. The `Log` port
 *  permits the unfenced append (the inbox is written unfenced by design); the
 *  *Executor* is what guarantees journal writes are always fenced. */
export interface Precondition {
  /** present the current lease; a stale token is rejected (`Conflict("fence")`). */
  readonly fence?: FenceToken
  /** commit only if the next assignable seq equals this (`Conflict("sequence")`).
   *  Always sourced from `Log.tail`, **never** from a folded record count — seqs
   *  are non-contiguous (see `Conflict` and §2). */
  readonly matchSeq?: SeqNum
}

// ── Errors ──────────────────────────────────────────────────────────────────

/** A conditional-append `412`. The two reasons are **not symmetric** — handling
 *  them the same way is a double-execution bug:
 *
 *  - `reason: "sequence"` → a benign position race *under your own held lease*
 *    (e.g. a crashed worker's in-flight append landed). Re-fold to the new tail
 *    and retry the persist. Side effects already journaled are not re-run.
 *  - `reason: "fence"` → **a newer owner exists.** Abort the step *silently*: no
 *    retry, no re-`submit`, and crucially **no re-running of side effects.** This
 *    is a fencing event (lost lease), not a transient error. The current owner
 *    will make progress; this worker must stop touching the journal. */
export class Conflict extends Data.TaggedError("Log/Conflict")<{
  readonly logId: LogId
  readonly reason: "fence" | "sequence"
  /** the tail S2 would assign next (for `reason: "sequence"`). */
  readonly tail: SeqNum
  /** the lease S2 currently holds (for `reason: "fence"`). */
  readonly currentFence?: FenceToken
}> {}

export class LogError extends Data.TaggedError("Log/Error")<{
  readonly op: "append" | "read" | "tail"
  readonly logId: LogId
  readonly cause: unknown
}> {}

// ── The service ─────────────────────────────────────────────────────────────

export class Log extends Context.Service<Log, {
  /**
   * Atomically append a batch (data + fence/trim commands) under optional
   * preconditions. Returns the new tail. The exactly-once + fencing guarantees.
   */
  readonly append: (
    log: LogId,
    writes: ReadonlyArray<Write>,
    precondition?: Precondition,
  ) => Effect.Effect<SeqNum, Conflict | LogError>

  /**
   * Read from `from` (a SeqNum). Bounded reads page to the tail internally;
   * `follow` keeps the stream open at the tail. Command records are not surfaced.
   */
  readonly read: (
    log: LogId,
    from: SeqNum,
    options?: { readonly follow?: boolean },
  ) => Stream.Stream<LogRecord, LogError>

  /** The next assignable SeqNum (the physical tail). Cheap (single-digit ms). */
  readonly tail: (log: LogId) => Effect.Effect<SeqNum, LogError>

  /**
   * Enumerate logs under a prefix — the source of the incomplete-execution set on
   * restart (`boot`). Without it, `boot` has no way to find what to recover.
   * Backed by S2 `ListStreams` at the basin; a durable `Dispatcher` may instead
   * own this set (its subscription *is* the live roster), in which case this is
   * the cold-start seed.
   */
  readonly list: (prefix: string) => Stream.Stream<LogId, LogError>
}>()("@firegrid/fluent-s2-durable/Log") {}
```

That is the S2-shaped surface — four methods, two error cases. It maps 1:1 to S2:
`append` → conditional `AppendInput` (with `Write.Fence`/`Write.Trim` as
`AppendRecord.fence`/`.trim`), `read` → `read`/`readSession`, `tail` → `checkTail`,
`list` → `ListStreams`. `s2Live.ts` is the only place that imports
`@s2-dev/streamstore`.

**What this fixes vs today's `s2.ts`:** positions are a branded `SeqNum` (not bare
`number` that leaks from the SDK), the lease is a branded `FenceToken` (with the
36-byte cap encoded in the type), and the batch is a named `Write` union instead
of SDK `AppendRecord`. The boundary is legible and substrate-agnostic in *type*,
while still being exactly S2 in *behavior*.

---

## 2. `Journal` — fold entries into invocation state (pure, the StateMachine)

No S2, no Effect-runtime concerns — just `LogRecord` bodies → state. This already
exists as `journal.ts`; the sketch just names its seam.

```ts
import { Data, Effect, HashMap, Option } from "effect"
import { Log, LogId, SeqNum, type LogRecord } from "./Log.ts"

/** The fold's starting point. The moment a `Write.Trim({ upTo })` lands,
 *  fold-from-zero is dead below `upTo` — so replay must discover where to start.
 *  This is the real content behind the deferred Compactor, and it changes this
 *  type, so it's decided here rather than later. */
export type Seed = Data.TaggedEnum<{
  Genesis: { readonly at: SeqNum }                       // clock/random seed + input, at seq 0
  Snapshot: { readonly at: SeqNum; readonly state: unknown } // a reseed point left by trim
}>
export const Seed = Data.taggedEnum<Seed>()

export interface Journal {
  readonly byName: HashMap.HashMap<string, OpRecord> // run/sleep/awakeable entries
  readonly seed: Option.Option<Seed>
  readonly input: unknown
  readonly completed: Option.Option<StepOutcome>
}

/** Pure: decode + fold a sequence of records (Restate's StateMachine, per-invocation).
 *  Must **not** assume contiguous seqs: `Write.Fence`/`Write.Trim` consume sequence
 *  numbers and are hidden by `Log.read`, so folded `LogRecord.seq` values have gaps.
 *  Never derive `matchSeq` from `records.length` — it comes from `Log.tail`. */
export const fold: (records: Iterable<LogRecord>) => Effect.Effect<Journal, CodecError>

/** Read a log to its tail and fold it. The only place Journal touches `Log`.
 *  Discovers the latest `Seed` (Genesis or Snapshot) and starts the bounded read
 *  at its seq, so replay cost is bounded by the last snapshot, not the whole log. */
export const replay = (log: LogId): Effect.Effect<Journal, CodecError | LogError, Log>
```

---

## 3. The Engine — the executor / control plane (replaces the `Worker` grab-bag)

### 3.0 What "Engine" is

Given a `Log` and a `Handler`, the Engine is the component that makes executions
*progress*. It's the same role every durable-execution platform has — Inngest's
**Executor**, Effect's **`WorkflowEngine`**, Restate's **PartitionProcessor**.

| Layer here | Restate | Inngest | Temporal | Effect |
|---|---|---|---|---|
| `Log` | Bifrost partition log | event/step store | Server History | `WorkflowEngine` *backend* |
| `Journal` | StateMachine (per-invocation) | step memoization | replay state | inside `WorkflowInstance` |
| **Engine** | PartitionProcessor + Invoker | the Executor | Worker + task dispatch | `WorkflowEngine` |
| `Ctx` | SDK ctx | `step.run`/`sleep`/`waitForEvent` | activity/sleep | `Activity`/`DurableClock`/`DurableDeferred` |

Its job is nine responsibilities. The grouping is load-bearing: it's the line
along which Restate/Temporal/Inngest **split the engine into two deployables** — a
stateful **control plane** (server: journal, timers, ownership, decisions) and a
stateless **data plane** (worker: runs the handler). Our single-process engine
fuses both; the split seam is exactly between #3 (Decide) and #4 (Run).

| # | Responsibility | Plane | Owned by (below) |
|---|---|---|---|
| 1 | **Own** — claim the lease (fence) | control | `Executor.step` |
| 2 | **Replay** — fold the log to state | control | `Executor.step` (via `Journal`) |
| 3 | **Decide** — what's next | control | `Executor.step` |
| 4 | **Run** — execute handler code | **data** | `Executor.step` *(the split point)* |
| 5 | **Persist** — append results | control | `Executor.step` |
| 6 | **Suspend/resume** — release + re-arm | control | `Executor.step` → `StepResult` |
| 7 | **Ingest** — external events → awakeables | control | `Engine.signal` + inbox watch |
| 8 | **Schedule** — which execution, when | control | `Dispatcher` |
| 9 | **Lifecycle** — submit / result | control | `Engine` |

> In our fused model #3 (Decide) and #4 (Run) are *interleaved* — the handler runs
> and its `ctx.*` calls **are** the decisions (each replays from the journal or
> suspends). Restate separates them (server-side StateMachine decides; worker
> runs); they only become separable when you split the planes.

So the Engine is really **three** services, not one — which is what makes it
legible:

- **`Executor`** — *advance one execution by one step* (1–6). Pure control over
  `Log`; knows nothing about scheduling. Its `step` returns a **`StepResult`** —
  a description of what happened and what to wait on. (Restate calls these the
  StateMachine's "effects".)
- **`Dispatcher`** — *which execution is ready, and when* (8). In-memory only
  (today's `Dispatch` + `TimerHeap`, unified). No `Log`.
- **`Engine`** — the *composition + lifecycle* (7, 9): wires Executor + Dispatcher
  into the pump, ingests events, exposes `submit`/`signal`/`result`/`run`.

### 3.1 `StepResult` — the decoupling contract

```ts
import { Data } from "effect"

/** What one execution step did, and what it now waits on. The Executor emits it;
 *  the Engine/Dispatcher react. (≈ Restate StateMachine effects.) */
export type StepResult = Data.TaggedEnum<{
  Idle: {}        // not started / no seed yet
  Completed: {}   // execution finished
  Suspended: {
    readonly timers: ReadonlyArray<{ readonly name: string; readonly fireAt: number }>
    readonly awaiting: ReadonlyArray<string> // awakeable names to wake on
  }
}>
export const StepResult = Data.taggedEnum<StepResult>()
```

> **Lift L1 — suspend is a *value*, not a defect** (from Effect's `Workflow.intoResult`).
> Effect runs the handler through one combinator that turns its `Exit` into
> `Complete | Suspended` — suspension flows *out* as a value the engine matches on,
> never as a thrown defect caught at the host. The spike today raises `Effect.die(Suspend)`
> and recovers it via `Cause.findDefect`. In the refactor, `Executor.step` runs the
> handler through a single `intoStepResult` combinator that centralizes "completed
> vs suspended vs failed". `Effect.die(Suspend)` may stay as an *internal* `Ctx`
> control mechanism, but **the Executor boundary speaks `StepResult` values.** This
> is what makes `StepResult` the real decoupling contract rather than a post-hoc
> description. See the [Lifts index](#patterns-lifted-from-effects-workflowengine).
>
> **`intoStepResult` owns the flush.** It is what commits the buffered wake (§3.2.1)
> — on `Completed` *and* `Suspended`. A retryable *failure* mid-wake flushes
> **nothing**: the succeeded `ctx.run`s re-run on the next attempt, masked by their
> `${logId}:${stepName}` idempotency keys. (This is the one place L1 and the
> buffering model interact — stated here so it isn't found as a "why did my effect
> run twice" bug.)

### 3.2 `Executor` — advance one execution (deps: `Log`)

```ts
export class Executor extends Context.Service<Executor, {
  /** Genesis: write the seed (clock/random + input) once. */
  readonly start: <I>(id: LogId, input: I) => Effect.Effect<void, WfError>

  /** One lease → fold → reconcile → run → flush cycle. Pure of scheduling.
   *  Persist is a *single* fenced `Log.append` of the buffered wake (§3.2.1).
   *  `Conflict("fence")` aborts the step silently (lost lease); `Conflict("sequence")`
   *  re-folds and retries. The lease persists across wakes (claimed once per
   *  ownership episode). */
  readonly step: (id: LogId) => Effect.Effect<StepResult, WfError>
}>()("@firegrid/fluent-s2-durable/Executor") {}

/** Built over Log; the handler is closed over at layer-construction time. */
export const Executor: { layer: <I, O>(handler: Handler<I, O>) => Layer.Layer<Executor, never, Log> }
```

> **Lift L2 — a per-run `Instance` service holds transient control state** (from
> Effect's `WorkflowInstance`). Effect builds a fresh mutable record per attempt —
> `{ suspended, interrupted, cause, activityState }` — and `provideService`s it into
> the handler run. It is *distinct from the durable journal*: the journal is what's
> persisted; the instance is the control flags for this one `step`. Today's
> `runtime.ts` fuses this into a closure, which is why "am I suspended" lives in
> defect plumbing. Give `Executor.step` an explicit `Instance` service for the
> duration of one step, so the Executor is reentrant and the control flags have a
> legible home:
>
> ```ts
> export class Instance extends Context.Service<Instance, {
>   readonly id: LogId
>   readonly token: FenceToken              // the held lease — presented on every persist (Tier-1)
>   suspended: boolean
>   interrupted: boolean
>   cause: Cause.Cause<never> | undefined
>   readonly buffer: Array<Write>           // intended writes, flushed once per wake (§3.2.1)
> }>()("@firegrid/fluent-s2-durable/Instance") {}
> ```

#### 3.2.1 The persist model — buffer the wake, flush once under the fence

The naive model (each `ctx.*` appends directly) is wrong on four counts at once.
Instead `ctx.run`/`ctx.sleep`/`ctx.awakeable` **buffer** their intended `Write`s
into `Instance.buffer`, and `intoStepResult` (L1) flushes the whole buffer as
**one** atomic `Log.append`, carrying `Instance.token`, on *both* `Completed` and
`Suspended`. One change buys four properties:

- **Atomicity** — a wake's effects land all-or-nothing; the batch is the atomic
  unit, so there is no torn step.
- **Rate budget** — an S2 client is capped at **200 append batches/sec**, and a
  batch targets one stream (no cross-execution coalescing). One batch per wake
  keeps a wake to one unit instead of N (one per `ctx` call).
- **Trivial fencing** — exactly one append per wake ⇒ the fence is presented
  exactly once (the Tier-1 invariant becomes a single call site).
- **The plane split for free** — "handler produces buffered intended-effects, the
  executor commits them" *is* the #3/#4 (Decide/Run) seam. Relocating Run later is
  a local change, not a rewrite.

**The trade, and why it's sound.** Buffering moves more potential re-execution onto
a mid-wake crash: the *whole* buffer re-runs on retry, not just the un-persisted
tail. That is acceptable **only because `ctx.run` already carries a deterministic
idempotency key** — `${logId}:${stepName}` — for exactly-once external effects, so
mid-wake re-execution is masked by external dedup. Make that key part of the
`ctx.run` contract now; it is the same key, not extra machinery.

> **Residual to document:** a genuinely *non-idempotent* effect gets at-least-once
> across a mid-wake crash. The escape hatch is to give it its **own wake** (its own
> flush boundary) so it is durable before the next step proceeds.

The single flush is the place — and the *only* place — to mark `uninterruptible`
(this is where **L9** actually lands, not on every `Log.append`): a half-cancelled
flush must not tear the batch.

**Lease lifecycle (Tier 4).** The fence is claimed **once per ownership episode**
(one `Write.Fence`), then `Instance.token` is presented on every wake's flush —
steady state is *one claim + one batch per wake*, not a re-fence each step. The
`Executor` contract therefore reads: *the lease persists across wakes.* A future
multi-worker `Dispatcher` must keep dispatch **sticky-by-execution while suspended**,
or it re-fences every wake and doubles the batch budget.

### 3.3 `Dispatcher` — which execution, when (the wait strategy lives here)

> **Name (do not call this `Scheduler`).** Effect already exports `Scheduler` — the
> *fiber* task dispatcher (`Context.Reference<Scheduler>` keyed `"effect/Scheduler"`,
> the `MixedScheduler`, op-budget yielding). Our service is "which *execution* is
> ready, and when", a different layer entirely; naming it `Scheduler` shadows a core
> runtime service. The spike already had a non-colliding name (`Dispatch`, the
> ready-set); `Dispatcher` is its generalization (ready-set + timers + awaits). Note
> `Schedule` (singular — the retry/repeat combinators) is a *different* module we
> *reuse* (L5), not shadow.

The Engine loop must **not** know *how* an execution waits — only that it became
ready. So both arm-verbs live on the Dispatcher, and the inbox-watch (today inlined
in `Engine.run`) moves in beside them. This is what makes the durable swap a *layer
change* rather than a rewrite — the loop stays strategy-agnostic.

```ts
export class Dispatcher extends Context.Service<Dispatcher, {
  /** Mark an execution ready to be stepped. */
  readonly submit: (id: LogId) => Effect.Effect<void>
  /** Take the next ready execution (blocks). */
  readonly ready: Effect.Effect<LogId>
  /** Arm a durable-timer wakeup; on fire → `submit(id)`. */
  readonly armTimer: (id: LogId, fireAt: number) => Effect.Effect<void>
  /** Arm an await on inbox names; on resolution → `submit(id)`. Owns the watch
   *  strategy (today's inlined `watchInbox`). */
  readonly armAwait: (id: LogId, names: ReadonlyArray<string>) => Effect.Effect<void>
  /** A `signal` landed for `id`. In-memory: `submit`. Durable: no-op (the
   *  subscription auto-wakes). Lets `Engine.signal` be just `append inbox; notify`. */
  readonly notify: (id: LogId) => Effect.Effect<void>
}>()("@firegrid/fluent-s2-durable/Dispatcher") {}

/** v1: single-process, in-memory. Built from Effect primitives, not hand-rolled:
 *   - `ready` set      → `Queue.unbounded<LogId>` (today's `dispatch.ts` already).
 *   - `armTimer`/`armAwait` → ONE `FiberMap<string>` keyed by `id` (timer) /
 *     `${id}:await` (watch): `FiberMap.run(key, effect, { onlyIfMissing: true })`
 *     gives keyed dedupe + cancel-on-resolve for free — exactly how
 *     `WorkflowEngine.layerMemory` arms its durable clocks. (Today's `timerHeap.ts`
 *     bare-`forkChild`s with no handle, so a re-arm leaks a fiber and can't cancel.)
 *  Named `inMemoryLive` explicitly because the inline `Log.read(inbox, follow)` is
 *  the scale ceiling — one open follow-stream per suspended execution does NOT
 *  survive 100k sleepers. */
export const Dispatcher: { inMemoryLive: Layer.Layer<Dispatcher> }
```

> **Lift L3 — a durable timer is just an awakeable resolved at a `fireAt`** (from
> Effect's `DurableClock`, which is implemented *as* a `DurableDeferred`: on fire,
> the clock entity calls `deferredDone(clock.deferred, Exit.void)`). `armTimer` and
> `armAwait` are the two arm-verbs, both funnelling to `submit` — L3 realized
> *structurally*: `ctx.sleep(name, d)` is an internal awakeable the Dispatcher
> resolves at `fireAt`, identical in shape to an external `signal` resolving an
> `awakeable`. This unifies today's `TimerHeap` and inbox-watcher into one dispatch
> — concretely, **one `FiberMap`** holds both (timer fibers and watch fibers, keyed),
> the same primitive `WorkflowEngine.layerMemory` uses to arm durable clocks.
>
> **Durable impls (the swap target, Tier 2):** a durable `Dispatcher` makes pokes
> reliable so the L5 sweep is pure insurance. `armAwait` becomes *one* subscription
> globbing `inbox/*` (pending = `tail > acked`, generation-fenced, free
> redelivery — no per-execution streams); a shared `ready` stream that `signal`
> also appends to. Both keep the same `submit`/`ready`/`armTimer`/`armAwait`/`notify`
> surface, so the Engine loop is unchanged.

### 3.4 `Engine` — composition + public surface (deps: `Log`, `Executor`, `Dispatcher`)

```ts
export class Engine extends Context.Service<Engine, {
  /** Genesis + schedule. */
  readonly submit: <I>(id: LogId, input: I) => Effect.Effect<void, WfError>
  /** Ingest an external event (resolves a `ctx.awakeable`): append inbox + wake. */
  readonly signal: (id: LogId, name: string, value: unknown) => Effect.Effect<void, WfError>
  /** Non-blocking read of completed state (fold → `Option<Result>`). */
  readonly poll: <O>(id: LogId) => Effect.Effect<Option.Option<O>, WfError>
  /** Follow the log until `Completed` and return the result. */
  readonly result: <O>(id: LogId) => Effect.Effect<O, WfError>
  /** The pump: forever { ready → step → react }. Survives step errors; stops on interrupt. */
  readonly run: Effect.Effect<never, WfError>
}>()("@firegrid/fluent-s2-durable/Engine") {}

export const Engine: { layer: Layer.Layer<Engine, never, Log | Executor | Dispatcher> }
```

> **Lift L4 — keep `poll` / `result` / `run` as distinct verbs** (Effect's engine
> separates `poll` (read current result without running), `execute` (drive), and
> `resume` (re-attempt)). Today's `Worker` conflates them; the split above keeps a
> non-blocking `poll` alongside the blocking `result`.
>
> **Lift L5 — a *rate-limited* `suspendedRetrySchedule` backstop** (from Effect's
> `execute`, which re-attempts a `Suspended` run on `exponential(200,1.5)` either
> `spaced(30s)`). Our pump is poke-driven, and FINDINGS notes "a dropped poke is a
> lost wakeup", so the periodic re-`submit` self-heals a missed poke. **But it is
> O(N) re-fold** — re-stepping every suspended execution re-folds its journal — so
> at scale it is the dominant cost. Keep it, but bound it to a **slow sweep**, not
> `spaced(30s)` over the whole set; the real fix is the durable `Dispatcher` (whose
> subscription redelivery makes pokes reliable), with L5 as insurance only. Drive it
> with `Effect.repeat(sweep, Schedule.spaced(slow))` — reuse the `Schedule` module,
> don't hand-roll the loop.
>
> A re-`submit` from the sweep can **race the real owner** — so it must run the
> same fenced step, and `Conflict("fence")`-and-abort (Tier-1). Otherwise the
> backstop becomes a double-execution source.

The whole control loop is now ~12 readable lines — and it's the *only* place the
three services meet. Note it no longer knows the wait strategy: `armTimer` /
`armAwait` both live on the `Dispatcher`, and `signal` is `append; notify`:

```ts
const run = Effect.forever(
  Effect.gen(function* () {
    const id = yield* dispatcher.ready
    const result = yield* executor.step(id).pipe(survivingStepErrors) // fence-abort is a *clean* exit, not an error
    yield* StepResult.$match(result, {
      Idle: () => Effect.void,
      Completed: () => Effect.void,
      Suspended: ({ timers, awaiting }) =>
        Effect.gen(function* () {
          yield* Effect.forEach(timers, (t) => dispatcher.armTimer(id, t.fireAt), { discard: true })
          if (awaiting.length > 0) yield* dispatcher.armAwait(id, awaiting) // strategy lives in the Dispatcher now
        }),
    })
  }),
)

const signal = (id, name, value) =>
  log.append(inboxOf(id), [Write.Append({ body: encodeEvent(name, value) })]).pipe( // inbox is unfenced by design
    Effect.zipRight(dispatcher.notify(id)),
  )
```

`tick`/`boot`/`snapshot` from today's `Worker` disappear from the public surface:
`tick` → `Executor.step`; `snapshot` → an `Executor` maintenance method or a
separate `Compactor` (the `Write.Trim` + `Seed.Snapshot` pair from §2). **`boot`**
→ enumerate the incomplete set via `Log.list(prefix)` (or the durable Dispatcher's
roster) and `submit` each. Recovery needs *no* separate timer-rehydration pass:
re-`submit` re-steps the handler to its `ctx.sleep`, which re-arms the timer for
free — so boot is genuinely just *enumerate + submit*.

---

## 4. `Ctx` — the handler-facing primitives (already clean)

Unchanged in spirit; this is the top seam the user-facing combinator API
(restate-fluent) is built on. Each primitive is a plain `Effect`. The one
structural change vs. the spike: **`ctx.*` do not append — they buffer `Write`s
into `Instance.buffer`** (§3.2.1); `intoStepResult` flushes once per wake.

```ts
export interface Ctx {
  /** Journaled, exactly-once-journaled / at-least-once-executed. The deterministic
   *  idempotency key `${logId}:${name}` is part of the contract — it's what makes
   *  the buffer-and-flush trade sound (mid-wake re-execution is masked by external
   *  dedup, §3.2.1). On replay, short-circuits from the folded entry (keyed by name,
   *  + `attempt` for retries — L7). */
  readonly run: <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | WfError, R>
  readonly sleep: (name: string, duration: Duration.Duration) => Effect.Effect<void, WfError>
  readonly awakeable: <A>(name: string) => Effect.Effect<A, WfError>
  // seams: state / call / send
}
```

> **Clock/random seeding is genesis-once — so `ctx.now()` is frozen at start time**
> (Tier 3). Seeding the deterministic Clock/Random once at genesis means a handler
> reading wall-time reads the *start* time for the life of the execution. That is
> deterministic and is the intended default — but flag it: a handler that branches
> on *elapsed* wall-time will read genesis time forever. If per-call wall-clock
> semantics are wanted (what Restate/Effect do — record each read at its call site),
> that's a per-call recorded `Write`, not the genesis seed.

---

## Patterns lifted from Effect's `WorkflowEngine`

We do **not** adopt `effect/unstable/workflow` (the SDD forbids it, and the only
durable impl — `ClusterWorkflowEngine` — is welded to `cluster`'s mailbox/sharding
model, which is at odds with an ordered S2 log; see
`s2-durable-approaches-comparison.md`). But the engine's *internal shapes* are worth
lifting into our own runtime. Each lift is anchored at the seam it touches above.

| # | Lift | Source in Effect | Lands in |
|---|---|---|---|
| **L1** | **Suspend is a returned value, not a defect** — one `intoStepResult` combinator turns the handler `Exit` into `StepResult`; `Effect.die(Suspend)` is at most an internal `Ctx` detail. | `Workflow.intoResult` → `Complete \| Suspended` | `StepResult` / `Executor.step` (§3.1–3.2) |
| **L2** | **A per-run `Instance` service** holds transient control flags (`suspended`/`interrupted`/`cause`), provided fresh per `step`, distinct from the durable journal. | `WorkflowInstance` (mutable, `provideService`'d per attempt) | `Executor` (§3.2) |
| **L3** | **A durable timer is an awakeable resolved at a `fireAt`** — `sleep` and event-wait share one resume path; `armTimer` and `signal` funnel into one "resolve → submit" dispatch. | `DurableClock` *is* a `DurableDeferred` (`deferredDone` on fire) | `Dispatcher` (§3.3) |
| **L4** | **Distinct verbs** `poll` (non-blocking read) / `result` (blocking) / `run` (drive) instead of the `Worker` grab-bag. | `poll` vs `execute` vs `resume` | `Engine` (§3.4) |
| **L5** | **A *rate-limited* `suspendedRetrySchedule` backstop** — a slow sweep re-`submit`s still-suspended executions (O(N) re-fold, so bound it); the durable Dispatcher is the real fix, this is insurance. A sweep re-`submit` must run the fenced step and abort on `Conflict("fence")`. | `execute`'s `exponential(200,1.5) ∪ spaced(30s)` re-attempt | `Engine.run` (§3.4) |
| **L6** | **Control signals ride the data-signal path** — interrupt is a *reserved awakeable name* resolved like any event, checked at handler `onExit`; no separate channel. | `InterruptSignal` = a reserved `DurableDeferred` | `Ctx`/`Engine.signal` (#7 ingest) |
| **L7** | **Activity identity carries an `attempt`** (`name/attempt`), so a retried side-effect is a distinct journal entry rather than a replay short-circuit. | `activityPrimaryKey = name/attempt` + `resetActivityAttempt` | `record.ts` / `ctx.run` codec |
| **L8** | **Encoded core / typed facade** — `Executor`/`Engine` operate on encoded `LogRecord` bodies; Schema decode/encode lives only at the `Ctx`/`Journal` edges. | `makeUnsafe(options: Encoded)` wraps an encoded core with schema + spans | `Executor` ↔ `Journal`/`Ctx` boundary |
| **L9** | **The single wake flush is `uninterruptible`** — *not* every `Log.append`. With buffer-and-flush (§3.2.1) there is exactly one journal append per wake; that one is the critical section. | `Rpc.wrap({ uninterruptible: true })`, `Uninterruptible` annotations | `intoStepResult` flush (§3.2.1) |

> **Two Tier-1 correctness invariants the lifts depend on** (architect review):
>
> 1. **Fence on every persist; handle `Conflict` asymmetrically.** S2 fencing is
>    cooperative — an unfenced append still commits — so `Executor.step` must present
>    `Instance.token` on *every* journal write, not just acquisition. `Conflict("sequence")`
>    → re-fold + retry (benign race under your own lease); `Conflict("fence")` → abort
>    **silently**, no retry, no re-`submit`, **no re-running side effects** (a newer
>    owner exists). The L5 sweep depends on this or it becomes a double-execution source.
> 2. **Buffer the wake; flush once (§3.2.1).** `ctx.*` buffer `Write`s; `intoStepResult`
>    flushes one fenced batch per wake. Buys atomicity, the 200-batch/s rate budget,
>    exactly-one fence presentation, and the future plane split — sound because
>    `ctx.run` carries the `${logId}:${stepName}` idempotency key.

**Confirmations** (our design already matches Effect's, independent evidence it's right):
name-keyed memoization = Effect's activity `primaryKey`; resolve-then-resume
(`deferredDone` writes then resumes) = our `signal` (append-inbox + poke).

**Deliberate divergence** (do *not* lift): **ownership.** Effect resolves every op
through `sharding.getShardId` and resumes by redelivering stored messages
(`sharding.reset` + `pollStorage`); ownership is ShardManager assignment over a
*mailbox*. Ours is the **fence on the log** (conditional-append-is-the-lock) and
resume is **fold-to-tail**. The fence stays; the sharding/mailbox machinery is
exactly the part that doesn't fit S2.

---

## How it composes (the legibility win)

A well-factored engine module is just the three services stacked with
`Layer.provide`, over `Log` over S2:

```ts
// the engine module: Executor + Dispatcher → Engine, all over Log
const engineLayer = <I, O>(handler: Handler<I, O>): Layer.Layer<Engine, never, Log> =>
  Engine.layer.pipe(
    Layer.provide(Layer.mergeAll(Executor.layer(handler), Dispatcher.inMemoryLive)),
  )

// wire once, at the app entry:
const LogLive    = S2Live.layer({ endpoint, basin })          // Log over S2
const EngineLive = engineLayer(orderHandler).pipe(Layer.provide(LogLive))

// run it:
Effect.gen(function* () {
  const engine = yield* Engine
  yield* Effect.forkScoped(engine.run)                        // start the pump
  yield* engine.submit(LogId("ord-1"), { orderId: "ord-1", amount: 100 })
  yield* engine.signal(LogId("ord-1"), "approval", true)
  const receipt = yield* engine.result<Receipt>(LogId("ord-1"))
}).pipe(Effect.provide(EngineLive), Effect.scoped)
```

The dependency graph reads top-down and is fully explicit:

```
Engine ── needs ──▶ Executor ──▶ Log ──▶ S2
   └──── needs ──▶ Dispatcher  (in-memory)
```

Today's `runtime.ts` collapses `Executor` + `Dispatcher` + `Journal` + `Ctx`-wiring
+ host-loop into one closure — which is exactly why the "public interface" is hard
to see. Splitting into `Log` / `Journal` / `Executor` / `Dispatcher` / `Engine` / `Ctx`
makes each seam a named `Context.Service` with a handful of methods, makes the S2
coupling sit behind exactly one of them, and makes the future control-plane /
data-plane split a *local* change to `Executor.step` (#4, Run) rather than a rewrite.

---

## Notes / open questions for the sketch

- **`Write.Fence` carries the lease.** Acquisition is `append([Write.Fence(token)], { matchSeq: tail })`
  — the conditional-append-is-the-lock pattern, surfacing `Conflict("sequence")`
  on a race (retry) vs `Conflict("fence")` on a genuinely lost lease.
- **`read` returning `Stream` for both bounded and follow** keeps one method; an
  alternative closer to `EventJournal` is `entries(log): Effect<Array>` +
  `changes(log, from): Effect<Stream, _, Scope>`. Either is fine; the `Stream`
  form composes more directly with `Journal.fold`.
- **This re-introduces a thin re-model** (`Write`, `LogRecord`, branded `SeqNum`)
  that the earlier "collapse onto SDK types" removed — deliberately. The collapse
  optimized for *less code*; this optimizes for a *legible, substrate-agnostic
  seam*. They're the two ends of the same trade, and this sketch is the principled
  version of the abstraction (idiomatic brands/Schema/tagged errors), not the
  half-baked `S2Write`/`S2Record` that prompted the collapse.
```
