# effect-s2-durable

A thin **durable-execution engine over [`effect-s2-stream-db`](../effect-s2-stream-db)** —
the S2 substrate's implementation of the authoring surface specified in
[`docs/sdds/effect-durable-execution-sdd.md`](../../docs/sdds/effect-durable-execution-sdd.md)
(the S2 analog of the durable-streams-backed `effect-durable-execution`).

State lives in `effect-s2-stream-db`: one `WorkflowDb` (one S2 stream) per execution,
plus one shared roster db. The engine adds only coordination — an in-process running
map, the completion ordering, and (in later slices) timers + recovery.

## Authoring surface

`handler(...)` is the only definition primitive. A durable program is an ordinary
`Effect` that uses the **free primitives** (`run`, `handlerRequest`, …) — there is no
`ctx` object; the primitives read an internal active-invocation slot and delegate to
the ambient `DurableExecutionRuntime`.

```ts
import { Duration, Effect, Schema } from "effect"
import { handler, handlerRequest, run } from "effect-s2-durable"

const Request = Schema.Struct({ name: Schema.String })
const Result = Schema.Struct({ greeting: Schema.String })

export const greet = handler("greet", { input: Request, output: Result })(
  Effect.gen(function*() {
    const req = yield* handlerRequest(Request)
    const greeting = yield* run("compose", composeGreeting(req.name), {
      output: Schema.String,
      retry: { maxAttempts: 3, initialInterval: Duration.millis(100) },
    })
    return { greeting }
  }),
)
```

Run it over a real S2 backend:

```ts
import { Effect, Layer } from "effect"
import { S2Client } from "effect-s2"
import { DurableExecutionRuntime } from "effect-s2-durable"

const program = Effect.gen(function*() {
  const rt = yield* DurableExecutionRuntime
  yield* rt.submit(greet, "greet-1", { name: "ada" })
  return yield* rt.attach(greet, "greet-1") // → { greeting: "..." }
})

program.pipe(
  Effect.provide(DurableExecutionRuntime.layer.pipe(Layer.provide(S2Client.layerConfig))),
  Effect.scoped,
)
```

## Semantics

- **`run(key, action, options?)`** is the durable step / replay boundary. A `steps` row
  for `key` is a terminal fact: on replay it returns the recorded value (or replays a
  recorded typed failure) and never re-runs. No row → the action runs (retry is
  pre-terminal); a crash before the row is written re-runs it (at-least-once, which is
  why side effects should carry an `idempotencyKey`).
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
import { handler, handlerRequest, state } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"

class Cart extends Table<Cart>("cart")({
  cartId: Schema.String.pipe(primaryKey),
  items: Schema.Array(Schema.String),
}) {}

export const checkout = handler("checkout", { input: Request, output: Result })(
  Effect.gen(function*() {
    const cart = state(Cart)                    // synchronous; reusable as a value
    yield* cart.set({ cartId: "c1", items: ["apple"] })
    const current = yield* cart.get("c1")       // read-after-ack sees the write
    // ...
  }),
)
```

A `run` action **cannot** use durable primitives (`run`/`sleep`/`state`/`signal`):
its type forbids `DurableExecutionRuntime` in `R`, so `run("x", state(Cart).set(…))`
is a *compile* error at the `run` call — the Effect analog of Restate's ctx-less
run closure. Perform durable work in the handler body, not inside a `run` action.

## Status

Built + tested against `s2 lite`:
- Slice 1 — `handler`, `handlerRequest`, `run` (memoization / retry / typed-failure
  facts), `submit` / `attach` / `poll`, completion ordering.
- Slice 2 — `sleep` (durable timer: a `clockWakeups` row, `pending`→`fired`; replay
  short-circuits a fired wakeup and recomputes the remaining delay of a pending one).
- Slice 3 (part) — `state(Table)` user-defined durable records (`get`/`set`/`delete`)
  over `db.table`, with a type-level guard against durable ops inside a `run` action.

Next: `signal` / `awakeable` / `deferred`, then roster-driven boot recovery (which
re-arms pending wakeups into the engine scope).
