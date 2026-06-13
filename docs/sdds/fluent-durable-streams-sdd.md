# Fluent Durable Streams SDD

Status: draft

Primary package: `packages/fluent-durable-streams`

References:

- S2 TypeScript SDK: `https://github.com/s2-streamstore/s2-sdk-typescript`
- S2 REST protocol: `https://s2.dev/docs/api/protocol`
- S2 Lite: `https://s2.dev/docs/s2-lite`
- Effect HTTPAPI: `https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/HTTPAPI.md`

## Purpose

`fluent-durable-streams` is an S2-native HTTP API package. It uses the official
`@s2-dev/streamstore` SDK directly and describes the public HTTP boundary with
Effect `HttpApi`.

This package is not:

- a Durable Streams compatibility gateway;
- a custom transport layer;
- an in-memory protocol surface;
- a `DurableStreamLog` facade over CRUD methods;
- a local reimplementation of S2 SDK types and sessions.

## Boundary

The public boundary is:

```text
Effect HttpApi
  -> HttpApiBuilder group handlers
    -> S2Profile service
      -> official S2 SDK basin/stream handles
```

The API speaks in S2-native concepts:

- basin is configured by the `S2Profile` layer;
- route params use S2 stream names;
- appends use S2-style batches of records;
- expected-tail uses `matchSeqNum`;
- cooperative fencing uses `fencingToken`;
- reads use S2 sequence coordinates and tail offsets;
- responses expose S2 `AppendAck`, `ReadBatch`, and tail positions.

There is intentionally no `StreamPath -> S2StreamName` projection in this
package. If a product wants path routing, it should define that routing at its
own application boundary before calling this API.

## Package Shape

```text
packages/fluent-durable-streams/
  src/
    api.ts      # Effect HttpApi schemas and endpoints
    s2.ts       # S2Profile config/layer with SDK handles
    server.ts   # HttpApiBuilder handlers over the SDK
    errors.ts   # SDK promise/error normalization
    index.ts
```

Do not add `names.ts`, `records.ts`, `headers.ts`, profile modules, or synthetic
protocol transports unless a concrete feature requires them and the SDD is
updated first.

## HTTPAPI Definition

The API is defined once with Effect HTTPAPI:

- `PUT /streams/:stream` ensures an S2 stream.
- `GET /streams/:stream/tail` checks the stream tail.
- `POST /streams/:stream/records` appends an S2-style JSON batch.
- `POST /streams/:stream/records/raw` appends one raw byte record using
  `HttpApiSchema.asUint8Array`.
- `GET /streams/:stream/records` reads bytes and returns JSON records with
  base64 bodies.

Schemas are plain `Schema.Struct` values so generated clients pass normal
objects, not class instances.

## S2 Layer

`S2Profile` owns configuration and SDK handles only:

```ts
interface S2ProfileService {
  readonly s2: S2
  readonly basinName: string
  readonly basin: ReturnType<S2["basin"]>
}
```

Handlers call SDK methods directly:

- `profile.basin.streams.ensure`
- `profile.basin.stream(stream).append`
- `profile.basin.stream(stream).read`
- `profile.basin.stream(stream).checkTail`

The package should not mirror every SDK verb on its own service interface.

## Error Model

SDK promises are lifted with `tryS2`, preserving known S2 SDK errors:

- `S2Error`
- `SeqNumMismatchError`
- `FencingTokenMismatchError`
- `RangeNotSatisfiableError`

HTTPAPI handlers map those into the declared `ApiError` schema. This keeps S2
failure details at the HTTP boundary without inventing Durable Streams outcome
variants.

## Local Validation

Use S2 Lite for integration tests that need a real backend:

```bash
s2 lite --port 18080 --init-file path/to/init.json
```

For normal unit tests, prefer HTTPAPI schema/client tests that do not start a
server. Do not introduce an in-memory protocol package to fake end-to-end
behavior.

## Deferred Product Services

Scheduling, forks, subscriptions, and wake delivery are product coordination
features on top of S2. They should be introduced as explicit HTTPAPI groups when
needed, with their own SDD sections and storage model.
