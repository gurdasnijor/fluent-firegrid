# effect-s2-durable

An Effect-native durable execution runtime over S2.

The canonical design lives in
[`docs/sdds/effect-durable-execution-sdd.md`](../../docs/sdds/effect-durable-execution-sdd.md).
The S2 owner-stream object rewrite is specified in
[`docs/sdds/effect-s2-durable-consolidation-sdd.md`](../../docs/sdds/effect-s2-durable-consolidation-sdd.md) and
[`features/effect-s2-durable/object-actor-model.feature.yaml`](../../features/effect-s2-durable/object-actor-model.feature.yaml).

Implementation is currently transitional:

- `service(...)` uses the existing ephemeral one-stream-per-call runtime.
- `object(...)` is being moved from the legacy two-stream/roster path to the per-key
  S2 owner-stream model.
- `workflow(...)` is planned as an object specialization, not a third runtime.

## Authoring surface

The ergonomic surface is **restate-sdk-gen-shaped**: group handlers in a `service` as
**generator methods** (`*greet(input) { … }`) — the input is the argument (no
`handlerRequest`, no `Effect.gen` wrapper) — and call through a typed `client` that
hides the execution id and the submit/attach dance. Inside, `yield* run(...)` etc. stay
typed (an Effect is `yield*`-able); the **free primitives**
(`run`/`sleep`/`state`/`signal`/`awakeable`/`deferred`) read an internal
active-invocation slot and delegate to `DurableExecutionRuntime` — no `ctx` object.

```ts
import { Duration, Effect, Layer, Schema } from "effect"
import { S2Client } from "effect-s2"
import { client, run, service, serviceLayer } from "effect-s2-durable"

const greeter = service({
  name: "greeter",
  handlers: {
    *greet(req: { name: string }) {
      // `run(action, options?)` — the action is any Effect (typically an external
      // call); it runs once, its result is journaled and replayed. The name is
      // optional (defaults to the step's journal position).
      const greeting = yield* run(Effect.sync(() => `Hello, ${req.name}!`), {
        retry: { maxAttempts: 3, initialInterval: Duration.millis(100) },
      })
      return { greeting }
    },
  },
})

const program = Effect.gen(function*() {
  // submit + attach + execution-id are hidden; returns the typed result
  return yield* client(greeter).greet({ name: "ada" }) // → { greeting: "..." }
})

program.pipe(
  // `serviceLayer(greeter)` seeds the recovery registry; `DurableExecutionRuntime.layer()`
  // is the bare engine (no recovery) when you don't need cross-restart durability.
  Effect.provide(serviceLayer(greeter).pipe(Layer.provide(S2Client.layerConfig))),
  Effect.scoped,
)
```

`sendClient(greeter).greet(input)` is the fire-and-forget form (returns the execution
id); pair it with `attach(id, schema)` (block for the result) or `poll(id, schema)`
(non-blocking). Pass `{ idempotencyKey }` to pin the id (idempotent invocation).
Optional per-handler `schemas: { greet: { input, output } }` set the durable
encode/decode boundary (default: opaque JSON).

### Low-level primitives

`service`/`client` are a thin layer over the engine primitives, which are also public:
`handler(name, { input, output })(program)` (the definition primitive; `program` uses
`handlerRequest(Schema)` to read the input), `DurableExecutionRuntime.submit(handler,
id, input)`, and the by-id `attach(id, schema)` / `poll(id, schema)`. Reach for these
only when you want to manage execution ids yourself.

## Semantics

- **`run(action, { name? })`** is the durable step / replay boundary. The step's
  `steps` row (keyed by `name`, else by journal position) is a terminal fact: on replay
  it returns the recorded value (or replays a recorded typed failure) and never re-runs.
  No row → the action runs (retry is pre-terminal); a crash before the row is written
  re-runs it (at-least-once).
- **Positional step keys require deterministic control flow.** An unnamed `run`
  is keyed by its journal position, so on replay (recovery, or a retry-after-failure)
  the handler **must issue the same sequence of `run`s** — control flow may branch only
  on the input and on already-journaled results, never on wall-clock/random/un-journaled
  reads. If a handler can branch non-deterministically, **name the steps** (`run("reserve",
  …)`) so a key tracks identity, not position. (Pinning an id with `idempotencyKey` does
  **not** weaken this: a second call to a pinned id is *deduplicated* — it returns the
  first execution's result and never re-runs — so two divergent calls can't alias one
  stream's positional keys.)
- **Effect Schema is the only serialization boundary.** `output`/`error` are discharged
  schemas (`Schema<A, I, never>`); storage holds encoded values.
- **Completion** writes the result to the roster, awaits its ack, drops the execution
  stream, then marks `resultAcked` — so the result outlives the stream (SDD §B6).
- **Single-writer:** one in-process owner per execution (the `running` map).

## User-defined durable state

`state(Table)` gives a handler a mutable durable record store scoped to its own
execution — any `effect-s2-stream-db` `Table`, whose rows live alongside the
engine's own tables in the one stream. It returns the binding **synchronously**
(it just names a table); only the operations are Effects. v1 surface is
`get`/`set`/`delete` (`set` is upsert; the primary key is a row field).

```ts
import { service, state } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"

class Cart extends Table<Cart>("cart")({
  cartId: Schema.String.pipe(primaryKey),
  items: Schema.Array(Schema.String),
}) {}

export const checkout = service({
  name: "checkout",
  handlers: {
    *go(_req: { user: string }) {
      const cart = state(Cart)                    // synchronous; reusable as a value
      yield* cart.set({ cartId: "c1", items: ["apple"] })
      const current = yield* cart.get("c1")       // read-after-ack sees the write
      // ...
    },
  },
})
```

A `run` action **cannot** use durable primitives (`run`/`sleep`/`state`/`signal`):
its type forbids `DurableExecutionRuntime` in `R`, so `run(state(Cart).set(…))`
is a *compile* error at the `run` call — the Effect analog of Restate's ctx-less
run closure. Perform durable work in the handler body, not inside a `run` action.

## Virtual objects (keyed, stateful)

`object({ name, handlers })` defines a keyed virtual object: a stateful entity whose public
authoring shape matches `service`, but whose durable boundary is an object key.

The target model is one schema-addressed S2 stream per object key, read and written as an ordered
`ActorEvent` log. Exclusive calls append `Accepted` events and a serial drainer runs them by S2
`seq_num`; completion is derived from a `Completed` event; signals append ingress events and do not
depend on the call being resident in memory. Persistent user state is a projection of `StateChanged`
events. See the actor SDD for the exact invariants.

```ts
import { client, object, state } from "effect-s2-durable"

class CounterState extends Table<CounterState>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

const counter = object({
  name: "counter",
  handlers: {
    *add(amount: number) {
      const st = state(CounterState)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      yield* st.set({ id: "v", value: cur + amount })
      return cur + amount
    },
    *value() { // a no-arg method is called as `.value()`
      const st = state(CounterState)
      return Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
    },
  },
})

// the call surface carries the key: `client(object, key)` / `sendClient(object, key)`
yield* client(counter, "user-1").add(5)   // → 5
yield* client(counter, "user-1").value()  // → 5  (state persisted across calls)
yield* client(counter, "user-2").value()  // → 0  (a different key is isolated)
```

The ordering is durable because it comes from the key's actor stream. The in-process drainer fiber
is cache and can be recreated by folding the stream. Cross-process key leasing is deferred; the
current actor build targets the single-process owner first.

## Park-and-resume primitives

`signal` / `awakeable` / `deferred` are one mechanism: a durable `deferreds` row is
the resolution (the truth), a transient in-process `Deferred` is the wake (best-effort,
rebuilt from rows on recovery). Resolve writes the row, **awaits its ack, then pokes**
(ack-before-poke); park checks the row first (so a resolve-before-await is never lost).

```ts
const approval = yield* signal("approval", Approval)        // receiver parks
yield* resolveSignal(executionId, "approval", Approval, v)  // ingress door resolves

const done = deferred("done", Result)                       // handler-resolved promise
yield* done.resolve(r); const r2 = yield* done.get()

const awk = yield* awakeable(Approval)                      // { id, promise }
// awk.id is replay-stable (executionId + ordinal); hand it to an ingress client
```

For the actor runtime, ingress targets the durable owner stream derived from the call id and is
therefore residency-independent. The existing service runtime still uses the resident execution
map for some park-and-resume paths until those semantics are moved through the actor/drainer model.

## Status

Built + tested against `s2 lite` for the existing service/runtime surface:
- `handler` / `handlerRequest`; `run` (memoize / retry / typed-failure facts);
  `submit` / `attach` / `poll`; completion ordering.
- `sleep` (durable timer row, `pending`→`fired`).
- `state(Table)` — user durable records; `get` is **journaled** (a `${execId}/read/N`
  record replays its original value, so read-modify-write is replay-sound); a type-level
  guard forbids durable ops inside a `run` action.
- `signal` / `awakeable` / `deferred` — park-and-resume over durable `deferreds` rows.
- **Boot recovery** — on layer build the current service runtime sweeps the roster for `running`/
  `suspended` executions and re-drives each (looked up by `handlerName` in the registry
  seeded via `serviceLayer(...services)`): it re-opens the `WorkflowDb`, reads the genesis
  `input`, and re-runs the handler from the top. Replay-from-top is what makes this work —
  `run` short-circuits from its `steps` fact, journaled `state.get` replays, `sleep`
  recomputes its remaining delay, and a `signal`/`awakeable` reads its resolved row or
  re-parks. A recovered execution is resident again, so `attach` / ingress resolution work
  across a restart (proven end-to-end over s2 lite by the Firelab validation
  `effect-s2-durable-service-recovery`, feature `service-recovery`). An execution whose
  `handlerName` isn't in the registry is skipped (so a partial registry never crashes boot).

Use **`serviceLayer(...services)`** (not the bare `DurableExecutionRuntime.layer()`)
whenever an execution can outlive the process, so its handlers are registered for recovery.

The object actor runtime is tracked by the top-level SDD and feature spec linked above.

**Not yet:** `resultAcked` is written but not yet consumed (no post-completion reclaim
sweep). Durable timers (`sleep`) recover their *remaining* delay on boot, but there is no
separate timer-wheel fiber re-armed from `clockWakeups` independent of the handler re-run.
