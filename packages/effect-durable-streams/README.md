# `effect-durable-streams`

Low-level Effect adapter for the
[Durable Streams Protocol](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md).

Most Firegrid state should use
[`effect-durable-operators` `DurableTable`](../effect-durable-operators/README.md)
instead. Use this package only when you intentionally need raw retained stream
semantics, such as an append-only fact stream or a generic producer-fenced
append path.

## Public API

```ts
import { DurableStream } from "effect-durable-streams"
```

`DurableStream.define(...)` returns a schema-bound stream facade with
Effect-native operations:

- `create`
- `append`
- `appendWithProducer`
- `collect`
- `read`
- `producer`
- `snapshotThenFollow`

Reads are `Stream`s. Writes are `Effect`s or scoped producers. Schema
validation happens at the wire boundary.

Prefer a service-shaped, URL-keyed surface with optional schema? See
`DurableStreamClient` below — it delegates to this same core.

`appendWithProducer` is a one-shot producer-fenced append for callers that
need to distinguish a newly accepted append from an idempotent duplicate:

```ts
const result = yield* DurableStream.appendWithProducer({
  endpoint,
  schema: Message,
  event: { user: "alice", text: "hello" },
  producerId: "message-123",
  producerEpoch: 0,
  producerSeq: 0,
})
```

## Example

```ts
import { DurableStream } from "effect-durable-streams"
import { Effect, Schema, Stream } from "effect"

const Message = Schema.Struct({
  user: Schema.String,
  text: Schema.String,
})

const messages = DurableStream.define({
  endpoint: { url: "https://streams.example.com/v1/stream/chat.room-1" },
  schema: Message,
})

const write = messages.append({ user: "alice", text: "hello" })

const readLive = messages.read({ live: "sse" }).pipe(
  Stream.tap((message) => Effect.log(`${message.user}: ${message.text}`)),
  Stream.runDrain,
)
```

## URL-keyed client facade (optional schema)

For an ergonomic, service-shaped surface — provide one batteries-included
layer, then call methods by URL — use `DurableStreamClient`. Schema is
*optional*: raw ops skip it for quick use; `withSchema(schema)` gives the fully
typed `Stream<A>` / `Sink<A>` surface over the same core (same transport, same
typed errors, same real follow loop).

```ts
import {
  DurableStreamClient,
  DurableStreamClientLayerFetch, // bundles FetchHttpClient; no R left to provide
} from "effect-durable-streams"
import { Effect, Schema, Stream } from "effect"

const program = Effect.gen(function* () {
  const client = yield* DurableStreamClient
  const url = "https://streams.example.com/v1/stream/chat.room-1"

  // Raw (no schema): body sent verbatim; session yields `unknown`.
  yield* client.create(url, { contentType: "application/json" })
  yield* client.append(url, JSON.stringify({ user: "alice", text: "hi" }))
  const raw = yield* client.stream(url).json // ReadonlyArray<unknown>

  // Typed: bind a Schema once, get decoded values + a Sink producer.
  const Message = Schema.Struct({ user: Schema.String, text: Schema.String })
  const chat = client.withSchema(Message)
  yield* chat.append(url, { user: "bob", text: "yo" })
  yield* chat.read(url, { live: "sse" }).pipe(
    Stream.tap((m) => Effect.log(`${m.user}: ${m.text}`)),
    Stream.runDrain,
  )
})

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.provide(DurableStreamClientLayerFetch)),
)
```

`DurableStreamClientLayer` is the bring-your-own-`HttpClient` variant.
`jsonBatches()` exposes per-response `{ items, offset, upToDate, cursor }`
metadata; producers add `close` / `pendingCount` / `epoch` / `nextSeq`.

### Which surface?

The two are **intentional siblings**, both thin delegations over the same
`Reader`/`Writer`/`Producer` core (identical performance) — not redundant:

- **`DurableStream.define(...)` — the primary low-level / library-runtime
  idiom.** Schema mandatory; pre-binds a **full `Endpoint`** so `headers`,
  `params`, `onError` (auth-refresh / signed-URL renewal), `onErrorMaxRetries`,
  and `retrySchedule` flow through every operation; `HttpClient` threads
  through `R` (provide once at the edge). Use this inside reusable
  services/runtime layers — it's what `fluent-runtime` and `fluent-firegrid`
  use, because they accept a caller-supplied `Endpoint` with its policy.
- **`DurableStreamClient` — the optional app / edge facade.** URL-keyed,
  optional schema, batteries-included layer, `HttpClient` captured in the
  layer. Best for scripts, examples, simple/untyped clients, and raw-stream
  tooling. It takes a `url` + per-call headers, **not** a full `Endpoint`, so
  for endpoints that need `onError`/`retrySchedule` policy, prefer `define`.

## When Not To Use This Package

Do not build table state, checkpoints, projections, or app query surfaces on
raw streams. Model those as owner-local `DurableTable` declarations and use
the generated `insert`, `upsert`, `delete`, `get`, `query`, `subscribe`, and
read-only TanStack collection views.

This package remains as the narrow raw-stream escape hatch.
