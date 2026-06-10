# Effect Durable Streams Server SDD v2

Status: draft
Version: 2
Owner: Firegrid / Durable Streams
Primary package: `packages/effect-durable-streams`
Protocol source: `PROTOCOL.md`
HTTP contract source: `typespec/`

## Purpose

This SDD defines the Effect Durable Streams server design from actual code
context, not just the HTTP protocol inventory.

The design input is the `CodeForBreakfast/eventsourcing` architecture and
source code at commit `12e788d7b7820a7e70fc8781dccb955fca84c271`, especially:

- `docs/ARCHITECTURE.md`
- `packages/eventsourcing-store/src/lib/services.ts`
- `packages/eventsourcing-store/src/lib/streamTypes.ts`
- `packages/eventsourcing-store/src/lib/eventstore.ts`
- `packages/eventsourcing-store-inmemory/src/lib/InMemoryStore.ts`
- `packages/eventsourcing-store-inmemory/src/lib/inMemoryEventStore.ts`
- `packages/eventsourcing-store-postgres/src/sqlEventStore.ts`
- `packages/eventsourcing-aggregates/src/lib/aggregateRootEventStream.ts`
- `packages/eventsourcing-protocol/src/lib/protocol.ts`
- `packages/eventsourcing-protocol/src/lib/server-protocol.ts`
- `packages/eventsourcing-transport/src/lib/{shared,client,server}.ts`
- `docs/plans/2025-10-25-eventsourcing-server-components-design.md`

The point to lift is not event sourcing itself. The point is the shape of the
server: a small semantic store algebra, streams and sinks as the primary Effect
surface, explicit boundary transformations, and protocol/transport separation.

## What The Eventsourcing Code Actually Does

The reference system is organized around four layers:

```text
Layer 1 Domain
  domain events, aggregates, projections

Layer 2 Wire API
  public serialized commands/events with unknown payloads

Layer 3 Protocol
  internal message envelopes, correlation, subscriptions

Layer 4 Transport
  protocol-agnostic message delivery
```

The strongest design move is that the store interface is tiny:

```ts
interface EventStore<TEvent> {
  append(to: EventStreamPosition): Sink.Sink<EventStreamPosition, TEvent, TEvent, ...>
  read(from: EventStreamPosition): Effect.Effect<Stream.Stream<TEvent, ...>, ...>
  subscribe(from: EventStreamPosition): Effect.Effect<Stream.Stream<TEvent, ...>, ...>
  subscribeAll(): Effect.Effect<Stream.Stream<StreamEvent<TEvent>, ...>, ...>
}
```

Important concrete patterns:

- `append` is a `Sink`, so writing is a stream operation, not a method that
  accepts a pre-collected array.
- `read` returns historical events only.
- `subscribe` returns historical events followed by live events.
- `subscribeAll` is live-only and cross-stream.
- `EventStreamPosition` is the optimistic concurrency token:
  `{ streamId, eventNumber }`.
- `encodedEventStore(schema)` wraps a store and moves schema
  encode/decode to the boundary.
- In-memory storage uses per-stream `PubSub` plus an all-events `PubSub`.
- Postgres storage uses database rows for history and LISTEN/NOTIFY bridged
  into in-process subscription managers.
- Protocol and transport are deliberately separate: transport moves string
  payload messages; protocol parses envelopes and manages correlation.
- The planned server components are bridges over the store:
  `EventBus`, `CommandDispatcher`, `StoreSubscriptionManager`,
  `ProtocolBridge`.

That design is better than an endpoint-first server because domain operations
compose directly in Effect. HTTP/WebSocket/etc are adapters, not the semantic
center.

## What We Should Lift

Lift these ideas:

- A small stream-native store interface.
- `Sink` for append paths, so streaming request bodies can flow into storage.
- `Stream` for read, long-poll, SSE, and subscription surfaces.
- Schema encoding/decoding wrappers at storage/wire boundaries.
- Domain-specific service tags where higher layers need typed facts.
- Live cross-stream tail notification as a first-class primitive.
- Separate public wire DTOs from internal protocol decisions.
- Bridge components that wire services together without becoming semantic
  owners.
- Store contract tests shared across memory and durable backends.

Do not lift these directly:

- The aggregate command layer as part of Durable Streams. That is above us.
- `WireCommand` as the server API model. Durable Streams is a stream substrate,
  not an application command bus.
- WebSocket protocol as a required transport. HTTP is already the Durable
  Streams binding; WebSocket can be an adapter later.
- Numeric event numbers as public offsets. Durable Streams offsets are opaque,
  lexicographically sortable protocol tokens.
- Best-effort `subscribeAll` for required webhook/pull-wake delivery. We need a
  durable wake substrate for coordination, not only an in-memory event bus.

## Current Local Code Context

Current local package shape:

```text
packages/effect-durable-streams/src/
  Api.ts
  ApiLive.ts
  Config.ts
  DurableStreamsServer.ts
  MemoryStore.ts
  Protocol.ts
  ProtocolError.ts
  Server.ts
  Store.ts
  Telemetry.ts
  index.ts
```

Current good parts:

- `Protocol.ts` already names protocol concepts and decisions.
- `Store.ts` is transport-neutral and wrapped by `Telemetry.traced`.
- `MemoryStore.ts` owns append precedence and producer rules, rather than
  scattering all decisions through the HTTP router.
- `DurableStreamsServer.ts` is correctly described as an optional RPC adapter.

Current design issues:

- `Store.ts` is still endpoint-shaped:
  `createStream`, `append`, `read`, `head`, `deleteStream`.
- `read` returns one `ReadChunk` instead of an Effect `Stream`, making long-poll
  and SSE adapters harder than they need to be.
- `append` accepts a fully materialized `Uint8Array`; this loses the natural
  streaming shape of HTTP request bodies.
- `Api.ts` defines the public stream wildcard under `/v1/stream`, and
  `ApiLive.ts` contains a reserved-path check for `__ds`. That is route design
  leaking into the data-plane adapter.
- Control-plane APIs are not modeled in the server package yet.
- There is no first-class all-stream tail stream equivalent to
  `EventStore.subscribeAll()`.

## Desired Architecture

Durable Streams should have four layers, analogous to eventsourcing but with
Durable Streams concepts:

```text
Layer 1 Semantic Consumers
  Firegrid runtime, event-sourced apps, projections, durable execution, clients

Layer 2 Durable Streams API
  typed public HTTP DTOs from TypeSpec/OpenAPI and client/server adapters

Layer 3 Durable Streams Protocol
  internal stream decisions, producer fencing, offsets, wake generation

Layer 4 Store/Transport
  byte log storage, tail notifications, HTTP/SSE/webhook delivery mechanics
```

The server package owns Layers 2-4 for Durable Streams. It does not own Layer 1
application event semantics.

## Core Service Shape

The central service should become stream-native, closer to
`EventStore<TEvent>` than to a catalog of HTTP endpoints.

```ts
export interface DurableStreamLog {
  readonly create: (
    request: CreateStreamRequest
  ) => Effect.Effect<CreateStreamResult, ProtocolError>

  readonly append: (
    request: AppendEnvelope
  ) => Sink.Sink<AppendResult, Uint8Array, Uint8Array, ProtocolError>

  readonly read: (
    from: StreamPosition
  ) => Effect.Effect<Stream.Stream<StreamRecord, ProtocolError>, ProtocolError>

  readonly subscribe: (
    from: StreamPosition
  ) => Effect.Effect<Stream.Stream<StreamRecord, ProtocolError>, ProtocolError>

  readonly subscribeAll: () => Effect.Effect<
    Stream.Stream<TailAdvanced, ProtocolError>,
    ProtocolError
  >

  readonly head: (
    path: StreamPath
  ) => Effect.Effect<StreamMetadata, ProtocolError>

  readonly delete: (
    path: StreamPath
  ) => Effect.Effect<DeleteStreamResult, ProtocolError>
}
```

`append` as a `Sink` is the biggest change. It lets the HTTP adapter pipe a raw
request stream into the protocol/store layer without buffering the whole body at
the route boundary. The sink can still accumulate internally when a backend
requires whole-record writes, but the public server seam becomes stream-native.

`read` and `subscribe` split the current `read` operation:

- `read` is historical catch-up only.
- `subscribe` is catch-up followed by live updates.
- HTTP long-poll is an adapter over `subscribe` with `Stream.take(1)` and a
  timeout.
- HTTP SSE is an adapter over `subscribe` with SSE framing.

`subscribeAll` is not the webhook guarantee mechanism by itself. It is the live
tail signal from which wake evaluation and in-process observers can be driven.
Durable wake snapshots remain persisted state.

## Position Model

The eventsourcing code uses:

```ts
{ streamId, eventNumber }
```

Durable Streams should use:

```ts
interface StreamPosition {
  readonly path: StreamPath
  readonly offset: Offset
  readonly subOffset?: number
}
```

Differences:

- `offset` remains opaque externally.
- `subOffset` exists only for fork/addressing boundaries where the protocol
  needs it.
- Internally a backend may map offsets to byte ranges, sequence numbers, WAL
  positions, or content-framed records.
- Optimistic write checks are not only expected position; they include content
  type, closed state, `Stream-Seq`, and idempotent producer tuple.

## Boundary Encoding

Lift the `encodedEventStore(schema)` idea.

Durable Streams has a generic byte store at the substrate boundary, but higher
layers need typed records. We should provide adapters rather than pushing typed
event concerns into the server:

```ts
export const encodedStreamLog =
  <A, I>(schema: Schema.Schema<A, I>, encoding: ContentEncoding) =>
  (log: DurableStreamLog): TypedStreamLog<A> => ({
    append: (request) =>
      Sink.mapInputEffect(log.append(request), encodeRecord(schema, encoding)),
    read: (from) =>
      log.read(from).pipe(Effect.map(Stream.mapEffect(decodeRecord(schema, encoding)))),
    subscribe: (from) =>
      log.subscribe(from).pipe(Effect.map(Stream.mapEffect(decodeRecord(schema, encoding)))),
  })
```

This keeps:

- bytes and protocol headers in `effect-durable-streams`;
- typed domain events in Firegrid/application packages;
- schema validation at explicit boundaries.

## Naming Rules

Adopt the reference naming discipline.

Use:

- `Http*` or `Api*` for TypeSpec/OpenAPI DTOs and HTTP binding models.
- `Protocol*` for internal protocol decisions and state-machine inputs.
- `Store*` for physical persistence rows/records.
- `Stream*` for semantic Durable Streams concepts.
- `Wire*` only if we add a non-HTTP serialized public message API.

Avoid:

- leaking `Protocol*` types into public TypeSpec DTOs;
- using storage row shapes as HTTP response schemas;
- using generic `unknown` payloads except at explicit wire/storage boundaries.

## Server Components To Build

### StreamLog

Equivalent role to `EventStore<TEvent>`.

Responsibilities:

- create stream
- append bytes through a sink
- read historical records
- subscribe historical plus live records
- expose live all-stream tail advances
- head/delete stream

This is the semantic center of the server.

### DataPlaneHttp

HTTP adapter over `StreamLog`.

Responsibilities:

- decode TypeSpec/OpenAPI route shapes
- preserve raw request body streaming
- translate `GET` modes to `read`, long-poll, and SSE streams
- lower protocol errors to HTTP responses

It should not own producer logic or wake logic.

### TailAdvanceBus

Equivalent to the live part of `subscribeAll`.

Responsibilities:

- expose committed tail advances as an Effect `Stream`
- support in-process observers
- back wake evaluation
- tolerate multiple server instances through backend-specific notification
  mechanisms

Memory backend implementation should look like the reference in-memory store:
per-stream pubsub plus all-stream pubsub. Durable backend implementation should
look closer to the Postgres bridge: persisted rows plus notification listener.

### WakeEvaluator

Equivalent in spirit to the planned `EventBus` plus server-side process-manager
bridge, but durable.

Responsibilities:

- consume `TailAdvanceBus`
- match explicit and pattern subscriptions
- evaluate filters
- advance internal evaluated cursors
- persist generation-fenced wake snapshots

It must not directly call webhooks as the source of truth. It persists wake
state first.

### SubscriptionDeliveryManager

Equivalent to `StoreSubscriptionManager`, but for Durable Streams
subscription/wake delivery.

Responsibilities:

- expose pull-wake claim/ack/release
- dispatch webhook attempts from persisted wake snapshots
- validate callback and lease tokens
- handle stale generations safely

### ScheduleRunner

Server-owned background component.

Responsibilities:

- claim due schedules
- append through `StreamLog`
- record completion/failure
- retry with producer idempotency

### HttpApiBridge

Equivalent to `ProtocolBridge`: wiring, not a semantic service.

Responsibilities:

- connect TypeSpec/Effect `HttpApi` handlers to protocol services
- translate between public DTOs and internal inputs
- own no state-machine decisions

## Store Backends

### Memory

Reference mapping:

- eventsourcing in-memory store uses `SynchronizedRef<HashMap<streamId,
  EventStream>>`;
- each `EventStream` stores historical events and a `PubSub`;
- an all-events stream stores tagged stream events.

Our memory store should use the same conceptual shape, with STM or
`SynchronizedRef` depending on transaction complexity:

```text
streams: path -> metadata + records + pubsub
allTailAdvances: pubsub
subscriptions: id -> config/state
wakeSnapshots: subscription/generation -> wake
schedules: id -> schedule
```

Append updates stream metadata, records, producer state, and all-tail pubsub as
one atomic operation.

### SQL/Postgres

Reference mapping:

- `sqlEventStore.ts` stores historical rows in `events`;
- read path selects rows ordered by event number;
- subscribe path concatenates historical rows with live notification stream;
- LISTEN/NOTIFY is bridged into subscription managers.

Our SQL store should follow the same shape but with richer protocol tables:

```text
stream_metadata
stream_records
producer_state
stream_sequence_state
tail_advances
subscriptions
subscription_streams
wake_snapshots
schedules
```

Durable correctness comes from transactions, not notifications. Notifications
only wake local fibers after durable rows exist.

### Ordered KV

An ordered KV backend is still viable, but it should implement the same
`StreamLog` and store-contract tests. It should not drive public API shape.

## Transport And Protocol Separation

The reference transport package only knows:

```ts
TransportMessage {
  id: MessageId
  type: string
  payload: string
  metadata: Record<string, unknown>
}
```

The protocol package parses those payloads into commands, results, and events.

For Durable Streams:

- HTTP is a concrete transport binding.
- TypeSpec/OpenAPI is the public HTTP contract.
- `Protocol.ts` should contain internal decisions, not public route DTOs.
- `Api.ts` should translate HTTP DTOs to protocol inputs.
- A future WebSocket or RPC transport should reuse protocol services, not HTTP
  route handlers.

## Control Plane Design

The previous SDD route split still matters, but as a consequence of the code
architecture, not as the architecture itself.

Canonical public HTTP binding:

```text
Data plane:
  /v1/streams/{+path}

Control plane:
  /v1/subscriptions/{id}
  /v1/subscriptions/{id}/streams/{+path}
  /v1/subscription-delivery/{id}/callback
  /v1/subscription-delivery/{id}/claim
  /v1/subscription-delivery/{id}/ack
  /v1/subscription-delivery/{id}/release
  /v1/schedules/{id}
  /v1/jwks.json
```

The reason is not aesthetics. It keeps HTTP adapter routes from leaking into
`StreamLog` semantics and prevents generated routers from encoding precedence
rules.

If `/v1/stream/__ds/...` remains, it is a compatibility HTTP adapter that calls
the same control-plane services. It is not the conceptual model.

## Effect Package Plan

Target package shape:

```text
src/
  StreamLog.ts                 core semantic store service
  StreamPosition.ts            branded path/offset/sub-offset types
  StreamRecord.ts              internal record envelope
  EncodedStreamLog.ts          schema boundary adapter
  DataPlaneHttp.ts             HTTP stream adapter
  ControlPlaneHttp.ts          HTTP control adapter
  Protocol.ts                  decisions and internal protocol inputs
  ProtocolError.ts             typed errors
  TailAdvanceBus.ts            live all-stream notification service
  WakeEvaluator.ts             durable wake creation
  SubscriptionService.ts       subscription config/membership
  SubscriptionDelivery.ts      claim/ack/release/webhook callback
  ScheduleRunner.ts            due schedule processing
  WebhookSigner.ts             signing and JWKS
  Store.ts                     physical backend algebra, if separate from StreamLog
  MemoryStore.ts
  SqlStore.ts
  Telemetry.ts
  Server.ts
```

Some of these can start as modules over the existing `Store` tag. The important
change is the direction: `StreamLog` becomes the semantic service and HTTP
routes become adapters.

## API Migration From Current Code

Current:

```ts
Store.createStream(request): Effect<CreateDecision>
Store.append(request): Effect<AppendResult>
Store.read(path, offset): Effect<ReadChunk>
```

Target:

```ts
StreamLog.create(request): Effect<CreateStreamResult>
StreamLog.append(envelope): Sink<AppendResult, Uint8Array, Uint8Array, ProtocolError>
StreamLog.read(from): Effect<Stream<StreamRecord, ProtocolError>>
StreamLog.subscribe(from): Effect<Stream<StreamRecord, ProtocolError>>
StreamLog.subscribeAll(): Effect<Stream<TailAdvanced, ProtocolError>>
```

Bridge plan:

1. Introduce `StreamLog` alongside current `Store`.
2. Implement `StreamLog` adapter over existing `Store` for the current memory
   slice.
3. Move HTTP read/append adapters to consume `StreamLog`.
4. Move memory implementation to implement `StreamLog` directly.
5. Keep `Store` only if needed as a lower physical persistence algebra.

## TypeSpec Role

TypeSpec remains essential, but it is not the server architecture.

Use TypeSpec to define:

- HTTP paths
- headers
- query parameters
- request/response schemas
- OpenAPI output
- mock/scaffold tooling

Do not use TypeSpec as:

- the internal store model;
- the protocol decision model;
- the shape of Effect service interfaces;
- the source for application event schemas.

## Error Model

Reference code uses tagged errors with operation context and recovery hints.
Lift that discipline.

Public HTTP errors should remain protocol-specific, but internal errors should
carry:

- module
- operation
- stream path or resource id
- expected/actual version where applicable
- cause
- recovery hint for logs/tests

Example:

```ts
class StreamConcurrencyError extends Data.TaggedError("StreamConcurrencyError")<{
  readonly path: StreamPath
  readonly expectedOffset: Offset
  readonly actualOffset: Offset
  readonly operation: "append" | "fork" | "schedule"
}> {}
```

Route handlers lower these into protocol response variants.

## Telemetry

The reference code adds spans around protocol send/result paths. Our telemetry
should wrap semantic services, not just HTTP handlers.

Required spans:

- `streamlog.create`
- `streamlog.append`
- `streamlog.read`
- `streamlog.subscribe`
- `streamlog.subscribe_all`
- `tail_advance.publish`
- `wake.evaluate`
- `subscription.claim`
- `subscription.ack`
- `webhook.deliver`
- `schedule.run`

This makes HTTP, RPC, and in-process use visible in the same way.

## Tests To Steal Conceptually

The reference repo has shared event-store test contracts. We need the same for
Durable Streams.

Store contract tests should run against memory and every durable backend:

- append returns the next position
- append rejects stale expected/concurrency state
- read is historical only
- subscribe is historical plus live
- subscribeAll is live all-stream tail advances
- schema adapter round-trips typed records
- concurrent writers preserve producer fencing
- durable backend notifications are not the source of correctness

HTTP tests are separate:

- raw body preservation
- header/status lowering
- long-poll adapter over `subscribe`
- SSE adapter over `subscribe`
- control-plane handlers over subscription services

## Implementation Slices

### Slice 1: Add Stream-Native Core

- Add `StreamPosition.ts`.
- Add `StreamRecord.ts`.
- Add `StreamLog.ts`.
- Add an adapter from current `Store` to `StreamLog`.
- Add tests for `read` versus `subscribe`.

### Slice 2: Move HTTP Data Plane To StreamLog

- Make `ApiLive.ts` pipe request bodies into `StreamLog.append`.
- Implement catch-up `GET` using `StreamLog.read`.
- Implement long-poll and SSE using `StreamLog.subscribe`.
- Keep current endpoints passing while the internal seam changes.

### Slice 3: Add TailAdvanceBus

- Expose `StreamLog.subscribeAll`.
- Memory backend publishes committed tail advances.
- Add tests equivalent to the reference `subscribeAll` contract.

### Slice 4: Rework Control Plane As Bridges

- Add `SubscriptionService`.
- Add `WakeEvaluator`.
- Add `SubscriptionDelivery`.
- HTTP handlers become bridge code only.

### Slice 5: Revisit Routes In TypeSpec

- Move canonical routes to non-overlapping data/control prefixes.
- Keep compatibility aliases only if needed.
- Regenerate OpenAPI.

### Slice 6: Durable Backend

- Implement SQL or ordered-KV backend against `StreamLog` contracts.
- Add notification bridge like the reference Postgres implementation.
- Prove crash/restart correctness through contract tests.

## Acceptance Criteria

The server design is corrected when:

- `StreamLog` is the semantic center of the package.
- Append/read/subscribe use Effect `Sink`/`Stream` surfaces.
- HTTP route handlers contain no producer/wake/schedule state-machine logic.
- TypeSpec describes HTTP only.
- Memory and durable backends run the same stream-log contract tests.
- Wake evaluation is driven by committed tail advances, not by HTTP route code.
- Application typed event adapters live above the byte stream substrate.
