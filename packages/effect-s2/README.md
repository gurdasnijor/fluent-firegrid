# effect-s2

Effect-native facade for the S2 TypeScript SDK.

`effect-s2` wraps `@s2-dev/streamstore` in a behavioral Effect service. The
public API is service-owned (`S2Client.*`), scoped resources are closed by
Effect, SDK errors are mapped into typed errors, and SDK input/output types are
preserved.

## Install

```sh
pnpm add effect @s2-dev/streamstore effect-s2
```

This package expects the same runtime environment as the S2 SDK, plus a basin
name for the Effect layer:

```sh
export S2_ACCESS_TOKEN="..."
export S2_BASIN="..."
```

Optional SDK endpoint variables such as `S2_ACCOUNT_ENDPOINT` and
`S2_BASIN_ENDPOINT` are still read by the underlying SDK.

## Service Model

SDK operations are exposed through the `S2Client` service:

```ts
S2Client.listBasins(args)
S2Client.createBasin(args)
S2Client.ensureBasin(args)
S2Client.deleteBasin(name)
S2Client.listStreams(args)
S2Client.createStream(name)
S2Client.deleteStream(name)
S2Client.checkTail(name)
S2Client.readBatch(name, options)
S2Client.append(name, records, options)
S2Client.appendSession(name, config)
S2Client.read(name, options)
S2Client.readBytes(name, options)
S2Client.producer(name, config)
S2Client.sink(producer)
```

Account-scoped SDK surfaces are covered as Effect methods: basins, access
tokens, locations, and metrics. Basin/stream-scoped surfaces are covered with
the configured `S2_BASIN` as the default basin; pass `{ basinName }` in the
operation options to target another basin.

Use `S2Client.layerConfig` when reading configuration from the environment:

```ts
import { NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { AppendRecord, S2Client } from "effect-s2"

const program = Effect.gen(function*() {
  yield* S2Client.createStream("events")
  yield* S2Client.append("events", [
    AppendRecord.string({ body: "hello" }),
  ])
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
```

## Unary Appends And Reads

Unary appends are direct SDK append calls with Effect errors:

```ts
import { Effect, Stream } from "effect"
import { AppendRecord, S2Client } from "effect-s2"

const program = Effect.gen(function*() {
  yield* S2Client.createStream("events")
  yield* S2Client.append("events", [
    AppendRecord.string({ body: "a" }),
    AppendRecord.string({ body: "b" }),
  ])

  const records = yield* S2Client.read("events", {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 2 } },
  }).pipe(Stream.runCollect)

  return records.map((record) => record.body)
})
```

`S2Client.read` opens an SDK read session under the Stream scope and cancels it
when the stream scope ends.

## Append Sessions

Use append sessions for high-throughput ordered batch production. The session is
scoped, and `submit` resolves only after the submitted batch is durable:

```ts
import { Effect } from "effect"
import { AppendRecord, S2Client } from "effect-s2"

const program = Effect.scoped(
  Effect.gen(function*() {
    yield* S2Client.createStream("session-events")
    const session = yield* S2Client.appendSession("session-events", {
      maxInflightBytes: 1024 * 1024,
      maxInflightBatches: 4,
    })

    const ack = yield* session.submit([
      AppendRecord.string({ body: "a" }),
      AppendRecord.string({ body: "b" }),
    ])

    return [ack.start.seqNum, ack.end.seqNum]
  }),
)
```

Append sessions also accept conditional append options:

```ts
yield* session.submit(
  [AppendRecord.string({ body: "exactly-once" })],
  { matchSeqNum: expectedSeqNum },
)
```

## Producer And Sink

`S2Client.producer` wraps the SDK `Producer` and `BatchTransform`. It gives
per-record submit semantics on top of append-session batching:

```ts
import { Effect, Stream } from "effect"
import { AppendRecord, S2Client } from "effect-s2"

const program = Effect.scoped(
  Effect.gen(function*() {
    yield* S2Client.createStream("producer-events")
    const producer = yield* S2Client.producer("producer-events", {
      lingerDurationMillis: 5,
      maxBatchRecords: 100,
      maxBatchBytes: 1024 * 1024,
      maxInflightBytes: 3 * 1024 * 1024,
    })

    yield* Stream.fromIterable([
      AppendRecord.string({ body: "one" }),
      AppendRecord.string({ body: "two" }),
    ]).pipe(Stream.run(S2Client.sink(producer)))
  }),
)
```

The producer is closed when the surrounding `Effect.scoped` exits.

## Bytes

Use `AppendRecord.bytes` and `S2Client.readBytes` for binary payloads and binary
headers. String records are encoded as UTF-8 when read through `readBytes`.

```ts
import { Effect, Stream } from "effect"
import { AppendRecord, S2Client } from "effect-s2"

const program = Effect.gen(function*() {
  yield* S2Client.createStream("bytes")
  yield* S2Client.append("bytes", [
    AppendRecord.bytes({
      body: new Uint8Array([0, 1, 2, 255]),
      headers: [[new Uint8Array([1]), new Uint8Array([2])]],
    }),
  ])

  const records = yield* S2Client.readBytes("bytes", {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 1 } },
  }).pipe(Stream.runCollect)

  return records
})
```

## Typed Channels

The channel helpers encode values with Effect `Schema` as JSON string records.
They are intentionally small and do not add a separate framing protocol.

```ts
import { Effect, Schema, Stream } from "effect"
import { S2Client, publish, readDecoded } from "effect-s2"

class Order extends Schema.Class<Order>("Order")({
  id: Schema.String,
  total: Schema.Number,
}) {}

const program = Effect.gen(function*() {
  yield* S2Client.createStream("orders")
  yield* publish("orders", Order, Order.make({ id: "o-1", total: 42 }))

  return yield* readDecoded("orders", Order, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 1 } },
  }).pipe(Stream.runCollect)
})
```

`conditionalAppend` combines schema encoding with `matchSeqNum`:

```ts
import { Schema } from "effect"
import { conditionalAppend } from "effect-s2"

yield* conditionalAppend("orders", Schema.String, "created", expectedSeqNum)
```

## Examples

The repository includes runnable examples:

- `examples/01-quickstart.ts`: create, append, read
- `examples/02-config-and-tail.ts`: environment config and tail checks
- `examples/03-producer-sink.ts`: scoped producer with `S2Client.sink`
- `examples/04-tail-while-write.ts`: live tailing while writing
- `examples/05-typed-channel.ts`: schema-backed JSON channel helpers
- `examples/06-append-session.ts`: scoped append-session batch submits
- `examples/07-read-bytes.ts`: binary records and byte reads
- `examples/08-session-conditional-append.ts`: session CAS with `matchSeqNum`
- `examples/09-control-plane.ts`: stream list/config/delete wrappers
- `examples/10-command-records.ts`: `AppendRecord.fence`, `AppendRecord.trim`, and command filtering

Run an example from the package directory with `tsx`, after setting the S2
environment variables for live examples.
