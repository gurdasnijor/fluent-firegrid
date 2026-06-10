# `effect-durable-client`

Low-level Effect adapter for the
[Durable Streams Protocol](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md).

Use this package when you intentionally need raw retained stream semantics, such
as an append-only fact stream or a generic producer-fenced append path. Higher
level table, projection, or query state should use a purpose-built abstraction
instead of raw streams.

## Public API

```ts
import { DurableStream } from "effect-durable-client"
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

Prefer a service-shaped, path-bound surface? See `DurableStreamClient` below —
it delegates to this same core.

`appendWithProducer` is a one-shot producer-fenced append for callers that
need to distinguish a newly accepted append from an idempotent duplicate:

```ts
const result =
  yield *
  DurableStream.appendWithProducer({
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
import { DurableStream } from "effect-durable-client"
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
  Stream.runDrain
)
```

## Path-bound client handles

For an ergonomic, service-shaped surface — provide one batteries-included
layer, then call methods by URL or stream path — use `DurableStreamClient`.
Schema is optional for raw tooling, but typed application code binds schema and
path together with `client.stream(path, schema)`.

```ts
import {
  DurableStreamClient,
  DurableStreamClientLayerFetch, // bundles FetchHttpClient; no R left to provide
} from "effect-durable-client"
import { Effect, Schema, Stream } from "effect"

const program = Effect.gen(function* () {
  const client = yield* DurableStreamClient
  const url = "https://streams.example.com/v1/stream/chat.room-1"

  // Raw (no schema): body sent verbatim; session yields `unknown`.
  yield* client.create(url, { contentType: "application/json" })
  yield* client.append(url, JSON.stringify({ user: "alice", text: "hi" }))
  const raw = yield* client.stream(url).json // ReadonlyArray<unknown>

  // Typed: bind path + Schema together, get decoded values + a producer.
  const Message = Schema.Struct({ user: Schema.String, text: Schema.String })
  const chat = client.stream(url, Message)
  yield* chat.append({ user: "bob", text: "yo" })
  yield* chat.read({ until: "close" }).pipe(
    Stream.tap((m) => Effect.log(`${m.user}: ${m.text}`)),
    Stream.runDrain
  )
})

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.provide(DurableStreamClientLayerFetch))
)
```

`DurableStreamClientLayer` is the bring-your-own-`HttpClient` variant.
`jsonBatches()` exposes per-response `{ items, offset, upToDate, cursor }`
metadata; producers add `close` / `pendingCount` / `epoch` / `nextSeq`.

For fixed singleton streams, wrap that projection with ordinary
`Effect.Service` application code. Keep the `DurableStreamClient` layer at the
app root; the stream service depends on the bare tag and supplies runtime
address data such as base URL and headers when it creates the handle.

```ts
import {
  DurableStreamClient,
  DurableStreamClientLayerFetch,
  ReadFrom,
} from "effect-durable-client"
import { Config, Effect, Redacted, Schema, Stream } from "effect"

const Message = Schema.Struct({
  user: Schema.String,
  text: Schema.String,
})

class ChatMessages extends Effect.Service<ChatMessages>()("ChatMessages", {
  effect: Effect.gen(function* () {
    const client = yield* DurableStreamClient
    const streamRoot = yield* Config.string("DURABLE_STREAMS_URL")
    const token = yield* Config.redacted("DURABLE_STREAMS_TOKEN")
    return client.stream(
      new URL("chat.room-1", streamRoot).toString(),
      Message,
      { headers: { authorization: () => `Bearer ${Redacted.value(token)}` } }
    )
  }),
}) {}

const program = Effect.gen(function* () {
  const chat = yield* ChatMessages
  yield* chat.create({ contentType: "application/json" })
  yield* chat.append({ user: "alice", text: "hello" })
  return yield* chat
    .read({ from: ReadFrom.beginning, until: "tail" })
    .pipe(Stream.runCollect)
}).pipe(
  Effect.provide(ChatMessages.Default),
  Effect.provide(DurableStreamClientLayerFetch)
)
```

`client.withSchema(schema)` remains as a legacy URL-keyed compatibility helper
for older tests and benchmarks. It is not the canonical typed API because it
creates a pathless schema mini-client.

### Which surface?

The two are **intentional siblings**, both thin delegations over the same
`Reader`/`Writer`/`Producer` core (identical performance) — not redundant:

- **`DurableStream.define(...)` — the primary low-level / library-runtime
  idiom.** Schema mandatory; pre-binds a **full `Endpoint`** so `headers`,
  `params`, `onError` (auth-refresh / signed-URL renewal), `onErrorMaxRetries`,
  and `retrySchedule` flow through every operation; `HttpClient` threads
  through `R` (provide once at the edge). Use this inside reusable
  services/runtime layers, because they can accept a caller-supplied `Endpoint`
  with its policy.
- **`DurableStreamClient.stream(path, schema)` — the low-ceremony typed app
  idiom.** Path-bound, schema-bound, batteries-included layer, `HttpClient`
  captured in the layer. Best for scripts, examples, simple typed clients, and
  raw-stream tooling. It takes a `url` or stream path + per-call headers,
  **not** a full `Endpoint`, so for endpoints that need
  `onError`/`retrySchedule` policy, prefer `define`.

## Reserved coordination endpoints

The current public API covers L0 streams and producer-fenced append. Reserved
coordination bindings must preserve the protocol semantics in `PROTOCOL.md`:
subscription claims are scoped leases, producer identities are scoped fenced
resources, schedules are delayed producer-fenced appends, and webhook handlers
verify JWKS-backed signatures before exposing wake payloads. The boundary is
constrained by
[docs/client-curr.md](docs/client-curr.md).

## When Not To Use This Package

Do not build table state, checkpoints, projections, or app query surfaces on
raw streams. Model those through a purpose-built state/query abstraction and
reserve this package for retained stream protocol access.

This package remains as the narrow raw-stream escape hatch.
