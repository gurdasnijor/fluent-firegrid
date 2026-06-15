# Sketch: a type-aligned `Log` port (and the layers above it)

**Status:** design sketch · Goal: replace the blurry `runtime.ts` surface with crisp,
composable seams — starting from the bottom (the durable log) up to the handler API.

The current `runtime.ts` mixes a grab-bag of responsibilities behind one `Worker`
(`start`/`tick`/`resolveEvent`/`awaitResult`/`boot`/`snapshot`/`runLoop`), and the
S2 boundary leaks SDK types upward. This sketches the seams that make the whole
thing legible, each a small `Context.Service` — with the "Engine" itself factored
into `Executor` + `Scheduler` + `Engine` (§3):

```
Log         — durable, sequenced, fenceable byte log         (S2 lives here, and only here)
  ▲
Journal     — fold Log entries → invocation state            (the StateMachine; pure)
  ▲
Executor    — advance ONE execution: lease→fold→run→persist  (emits StepResult)   ┐
Scheduler   — which execution is ready, and when (in-memory)                      ├ "Engine"
Engine      — composition + lifecycle: submit/signal/result/run (the pump)        ┘
  ▲
Ctx         — the handler-facing primitives (run/sleep/awakeable)
```

Only `Log` is substrate-specific; everything above is pure Effect over `Log`. The
"Engine" is really three small services (`Executor` + `Scheduler` + `Engine`) — see §3.

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

/** S2's LSN: a position in a single log. Total order within a `LogId`. */
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

/** Append preconditions — S2's two concurrency primitives, named. */
export interface Precondition {
  /** present the current lease; a stale token is rejected (`Conflict("fence")`). */
  readonly fence?: FenceToken
  /** commit only if the next assignable seq equals this (`Conflict("sequence")`). */
  readonly matchSeq?: SeqNum
}

// ── Errors ──────────────────────────────────────────────────────────────────

/** A conditional-append `412`. Distinguishes lost-lease from position-taken. */
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
}>()("@firegrid/fluent-s2-durable/Log") {}
```

That is the entire S2-shaped surface — three methods, two error cases. It maps
1:1 to S2: `append` → conditional `AppendInput` (with `Write.Fence`/`Write.Trim`
as `AppendRecord.fence`/`.trim`), `read` → `read`/`readSession`, `tail` →
`checkTail`. `s2Live.ts` is the only place that imports `@s2-dev/streamstore`.

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
import { Effect, HashMap, Option } from "effect"
import { Log, LogId, type LogRecord } from "./Log.ts"

export interface Journal {
  readonly byName: HashMap.HashMap<string, OpRecord> // run/sleep/awakeable entries
  readonly seed: Option.Option<Seed>
  readonly input: unknown
  readonly completed: Option.Option<StepOutcome>
}

/** Pure: decode + fold a sequence of records (Restate's StateMachine, per-invocation). */
export const fold: (records: Iterable<LogRecord>) => Effect.Effect<Journal, CodecError>

/** Read a log to its tail and fold it. The only place Journal touches `Log`. */
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
| 8 | **Schedule** — which execution, when | control | `Scheduler` |
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
- **`Scheduler`** — *which execution is ready, and when* (8). In-memory only
  (today's `Dispatch` + `TimerHeap`, unified). No `Log`.
- **`Engine`** — the *composition + lifecycle* (7, 9): wires Executor + Scheduler
  into the pump, ingests events, exposes `submit`/`signal`/`result`/`run`.

### 3.1 `StepResult` — the decoupling contract

```ts
import { Data } from "effect"

/** What one execution step did, and what it now waits on. The Executor emits it;
 *  the Engine/Scheduler react. (≈ Restate StateMachine effects.) */
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

### 3.2 `Executor` — advance one execution (deps: `Log`)

```ts
export class Executor extends Context.Service<Executor, {
  /** Genesis: write the seed (clock/random + input) once. */
  readonly start: <I>(id: LogId, input: I) => Effect.Effect<void, WfError>

  /** One lease → fold → reconcile → run → persist cycle. Pure of scheduling. */
  readonly step: (id: LogId) => Effect.Effect<StepResult, WfError>
}>()("@firegrid/fluent-s2-durable/Executor") {}

/** Built over Log; the handler is closed over at layer-construction time. */
export const Executor: { layer: <I, O>(handler: Handler<I, O>) => Layer.Layer<Executor, never, Log> }
```

### 3.3 `Scheduler` — which execution, when (in-memory; no `Log`)

```ts
export class Scheduler extends Context.Service<Scheduler, {
  /** Mark an execution ready to be stepped. */
  readonly submit: (id: LogId) => Effect.Effect<void>
  /** Take the next ready execution (blocks). */
  readonly ready: Effect.Effect<LogId>
  /** Arm a durable-timer wakeup; on fire it calls `submit(id)`. */
  readonly armTimer: (id: LogId, fireAt: number) => Effect.Effect<void>
}>()("@firegrid/fluent-s2-durable/Scheduler") {}

export const Scheduler: { layer: Layer.Layer<Scheduler> } // = today's Dispatch + TimerHeap
```

### 3.4 `Engine` — composition + public surface (deps: `Log`, `Executor`, `Scheduler`)

```ts
export class Engine extends Context.Service<Engine, {
  /** Genesis + schedule. */
  readonly submit: <I>(id: LogId, input: I) => Effect.Effect<void, WfError>
  /** Ingest an external event (resolves a `ctx.awakeable`): append inbox + wake. */
  readonly signal: (id: LogId, name: string, value: unknown) => Effect.Effect<void, WfError>
  /** Follow the log until `Completed` and return the result. */
  readonly result: <O>(id: LogId) => Effect.Effect<O, WfError>
  /** The pump: forever { ready → step → react }. Survives step errors; stops on interrupt. */
  readonly run: Effect.Effect<never, WfError>
}>()("@firegrid/fluent-s2-durable/Engine") {}

export const Engine: { layer: Layer.Layer<Engine, never, Log | Executor | Scheduler> }
```

The whole control loop is now ~12 readable lines — and it's the *only* place the
three services meet:

```ts
const run = Effect.forever(
  Effect.gen(function* () {
    const id = yield* scheduler.ready
    const result = yield* executor.step(id).pipe(survivingStepErrors)
    yield* StepResult.$match(result, {
      Idle: () => Effect.void,
      Completed: () => Effect.void,
      Suspended: ({ timers, awaiting }) =>
        Effect.gen(function* () {
          yield* Effect.forEach(timers, (t) => scheduler.armTimer(id, t.fireAt), { discard: true })
          if (awaiting.length > 0) yield* watchInbox(id, awaiting) // forks Log.read(inbox, follow) → scheduler.submit(id)
        }),
    })
  }),
)

const signal = (id, name, value) =>
  log.append(inboxOf(id), [Write.Append({ body: encodeEvent(name, value) })]).pipe(
    Effect.zipRight(scheduler.submit(id)),
  )
```

`tick`/`boot`/`snapshot` from today's `Worker` disappear from the public surface:
`tick` → `Executor.step`; `boot` → `Effect.forEach(ids, scheduler.submit)`;
`snapshot` → an `Executor` maintenance method or a separate `Compactor`.

---

## 4. `Ctx` — the handler-facing primitives (already clean)

Unchanged in spirit; this is the top seam the user-facing combinator API
(restate-fluent) is built on. Each primitive is a plain `Effect`.

```ts
export interface Ctx {
  readonly run: <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | WfError, R>
  readonly sleep: (name: string, duration: Duration.Duration) => Effect.Effect<void, WfError>
  readonly awakeable: <A>(name: string) => Effect.Effect<A, WfError>
  // seams: state / call / send
}
```

---

## How it composes (the legibility win)

A well-factored engine module is just the three services stacked with
`Layer.provide`, over `Log` over S2:

```ts
// the engine module: Executor + Scheduler → Engine, all over Log
const engineLayer = <I, O>(handler: Handler<I, O>): Layer.Layer<Engine, never, Log> =>
  Engine.layer.pipe(
    Layer.provide(Layer.mergeAll(Executor.layer(handler), Scheduler.layer)),
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
   └──── needs ──▶ Scheduler  (in-memory)
```

Today's `runtime.ts` collapses `Executor` + `Scheduler` + `Journal` + `Ctx`-wiring
+ host-loop into one closure — which is exactly why the "public interface" is hard
to see. Splitting into `Log` / `Journal` / `Executor` / `Scheduler` / `Engine` / `Ctx`
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
