# effect-s2-durable

A thin **durable-execution engine over [`effect-s2-stream-db`](../effect-s2-stream-db)** —
the S2 substrate's implementation of the authoring surface specified in
[`docs/sdds/effect-durable-execution-sdd.md`](../../docs/sdds/effect-durable-execution-sdd.md)
(the S2 analog of the durable-streams-backed `effect-durable-execution`).

State lives in `effect-s2-stream-db`: one `WorkflowDb` (one S2 stream) per execution,
plus one shared roster db. The engine adds only coordination — an in-process running
map, the completion ordering, and (in later slices) timers + recovery.

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
import { client, DurableExecutionRuntime, run, service } from "effect-s2-durable"

const greeter = service({
  name: "greeter",
  handlers: {
    *greet(req: { name: string }) {
      // run(action, options?) — name optional (defaults to journal position)
      const greeting = yield* run(composeGreeting(req.name), {
        output: Schema.String,
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
  Effect.provide(DurableExecutionRuntime.layer.pipe(Layer.provide(S2Client.layerConfig))),
  Effect.scoped,
)
```

`sendClient(greeter).greet(input)` is the fire-and-forget form (returns the execution
id). Pass `{ idempotencyKey }` to pin the id (idempotent invocation). Optional
per-handler `schemas: { greet: { input, output } }` set the durable encode/decode
boundary (default: opaque JSON).

### Low-level primitives

`service`/`client` are a thin layer over the engine primitives, which are also public:
`handler(name, { input, output })(program)` (the definition primitive; `program` uses
`handlerRequest(Schema)` to read the input) and `DurableExecutionRuntime.submit` /
`attach` / `poll`. Reach for these only when you need to manage execution ids yourself.

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
its type forbids `DurableExecutionRuntime` in `R`, so `run(state(Cart).set(…))`
is a *compile* error at the `run` call — the Effect analog of Restate's ctx-less
run closure. Perform durable work in the handler body, not inside a `run` action.

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

## Status

Built + tested against `s2 lite` (the full Restate-SDK authoring surface):
- `handler` / `handlerRequest`; `run` (memoize / retry / typed-failure facts);
  `submit` / `attach` / `poll`; completion ordering.
- `sleep` (durable timer row, `pending`→`fired`).
- `state(Table)` — user durable records; `get` is **journaled** (a `${execId}/read/N`
  record replays its original value, so read-modify-write is replay-sound); a type-level
  guard forbids durable ops inside a `run` action.
- `signal` / `awakeable` / `deferred` — park-and-resume over durable `deferreds` rows.

**Not yet (recovery slice):** `sleep`/signal waits don't survive a process restart —
the durable rows + `suspended`/`suspendKind` roster markers are written, but no boot
sweep re-opens suspended executions and replays them yet. `resultAcked` is written but
not yet consumed (no post-completion reclaim). `attach` across a restart fails.
