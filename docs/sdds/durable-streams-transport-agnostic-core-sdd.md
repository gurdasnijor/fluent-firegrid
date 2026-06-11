# Durable Streams Transport-Agnostic Core SDD

Status: draft
Owner: Firegrid / Durable Streams
Primary protocol source: `PROTOCOL.md`
Current core-slice implementation targets:

- `packages/fluent-stream-log`
- `packages/fluent-stream-log-inmemory`
- `packages/fluent-transport`
- `packages/fluent-transport-inmemory`
- `packages/fluent-protocol`
- `packages/fluent-client`

Deferred Phase 6 implementation target:

- `packages/fluent-transport-http`

Reference design inputs:

- `repos/eventsourcing/packages/eventsourcing-store`
- `repos/eventsourcing/packages/eventsourcing-store-inmemory`
- `repos/eventsourcing/packages/eventsourcing-aggregates`
- `repos/eventsourcing/packages/eventsourcing-commands`
- `repos/eventsourcing/packages/eventsourcing-transport`
- `repos/eventsourcing/packages/eventsourcing-transport-inmemory`
- `repos/eventsourcing/packages/eventsourcing-protocol`
- `repos/eventsourcing/packages/eventsourcing-server`
- `repos/eventsourcing/packages/eventsourcing-testing-contracts`
- `repos/effect/packages/experimental/src/EventLogRemote.ts`
- `repos/effect/packages/experimental/src/EventLogServer.ts`

## Purpose

`PROTOCOL.md` is an HTTP protocol binding. It is not the implementation
architecture.

The core Durable Streams system must be built behind the wire first:

```text
semantic stream log
  -> protocol commands and responses
  -> protocol-agnostic transport
  -> in-memory transport for local client/server development
  -> HTTP transport implementation for PROTOCOL.md
```

Most implementation work should happen before HTTP exists. HTTP headers,
routes, SSE framing, ETags, cache cursors, and reserved path precedence are
HTTP transport concerns. Producer fencing, offsets, stream closure, forks,
subscriptions, leases, schedules, and JSON message boundaries are
stream-log/protocol concerns.

This SDD defines that split and gives the agents a concrete build target. The
build is intentionally serial until the in-memory stream log can provide a usable
protocol client. Parallel work before that point creates fake surfaces.

The first mergeable slice must stop at a production-shaped in-memory path:
stream log, in-memory stream log, transport, in-memory transport, protocol, and
client. HTTP helpers that do not implement `ClientTransport` or
`ServerTransport` should not ship under `fluent-transport-http`; Phase 6 starts
only when HTTP can plug into the transport contracts.

## Reference Shape To Lift

The eventsourcing repository has the right package geometry:

```text
eventsourcing-store
  tiny store algebra, Stream/Sink surface, reusable store contract tests

eventsourcing-store-inmemory
  clean in-memory implementation using SynchronizedRef, per-stream PubSub,
  all-events live stream, and contract-test wiring

eventsourcing-testing-contracts
  behavior suites every store and transport implementation must pass

eventsourcing-transport
  protocol-agnostic client/server message transport contracts

eventsourcing-transport-inmemory
  direct in-memory connector/acceptor for local client/server tests

eventsourcing-protocol
  envelopes, correlation, subscriptions, protocol state over transport

eventsourcing-server
  server-side components that bridge protocol messages to store semantics

eventsourcing-commands / eventsourcing-aggregates
  user/domain layer above the log
```

The important code shape is not event sourcing itself. The important code shape
is this interface from `eventsourcing-store`:

```ts
interface EventStore<TEvent> {
  append(to: EventStreamPosition): Sink.Sink<EventStreamPosition, TEvent, TEvent, ...>
  read(from: EventStreamPosition): Effect.Effect<Stream.Stream<TEvent, ...>, ...>
  subscribe(from: EventStreamPosition): Effect.Effect<Stream.Stream<TEvent, ...>, ...>
  subscribeAll(): Effect.Effect<Stream.Stream<StreamEvent<TEvent>, ...>, ...>
}
```

For Durable Streams, the analogous contract is a byte/log substrate plus a
protocol service over it. We should clone the approach, not the domain names.

The Effect experimental `EventLogRemote` / `EventLogServer` files are the
reference shape for the client/server protocol machinery over a transport:
single inbound pump, request id to `Deferred` correlation, scoped streaming
subscription resources, terminal mailboxes for long-lived remote changes,
tagged schema protocol messages, binary framing, chunking, ping/pong liveness,
and retry around the transport loop.

Do not copy `EventJournal` / `EventLog` as the Durable Streams log model. That
domain is local-first, multi-writer, encrypted sync with client-minted ids and
remote sequence cursors. Durable Streams needs an authoritative byte log with
server-minted opaque offsets, content-type validation, producer fencing,
closure, forks, subscriptions, leases, and schedules.

The first implementation source should be
`repos/eventsourcing/packages/eventsourcing-store-inmemory`, specifically:

- `src/lib/InMemoryStore.ts`
- `src/lib/inMemoryEventStore.ts`
- `src/lib/inMemory.spec.ts`
- `src/lib/subscriptionManager.ts`

Do not use the current `packages/effect-durable-streams/src/Store.ts` or
`MemoryStore.ts` as the design basis for the new core. They are useful only as
legacy behavior references while we delete endpoint-shaped assumptions.

## Non-Goals

- Do not make HTTP routes the first implementation surface.
- Do not make TypeSpec/OpenAPI the internal model.
- Do not make `streamPath` or `/v1/stream/{+path}` drive the core API shape.
- Do not start with durable execution. Execution consumes this substrate later.
- Do not expose event-sourcing aggregate or command APIs as Durable Streams
  primitives.
- Do not treat ordered KV, HTTP, WebSocket, or Durable Objects as the public
  semantic model. They are implementations or transports.

## Layers

The implementation should have these layers, even if the first code slice keeps
them inside existing packages instead of creating new workspace packages.

```text
Layer 5 Application Consumers
  Firegrid runtime, execution, projections, user clients

Layer 4 Client API
  Effect-native client resources over protocol transport

Layer 3 Durable Streams Protocol
  typed semantic commands, responses, correlation, producer and lease fencing

Layer 2 Transport
  protocol-agnostic request/response and subscription message delivery

Layer 1 Stream Log
  append-only byte log, stream metadata, offsets, tail notifications

Layer 0 Backend
  memory now, durable backend later
```

HTTP is a transport implementation of `PROTOCOL.md`. `fluent-transport-http`
is the network server/client boundary for `PROTOCOL.md`; HTTP is not Layer 1,
2, or 3.

## Package Layout And Dependency Rules

Directory structure is part of the design. To avoid another round of invented
boundaries, lock the package boundaries now and mirror
`repos/eventsourcing/packages/*` as closely as possible.

Package names are intentionally standalone. Shared transport and protocol
modules are not owned by the client and are not hidden inside the HTTP server
package.

File naming rule:

- source modules use camelCase: `streamTypes.ts`, `operations.ts`,
  `serverProtocol.ts`;
- test files use kebab-case with `.test.ts`;
- do not mix kebab and camel inside source module names.

### Package Map

```text
packages/fluent-stream-log
  mirrors repos/eventsourcing/packages/eventsourcing-store

packages/fluent-stream-log-inmemory
  mirrors repos/eventsourcing/packages/eventsourcing-store-inmemory

packages/fluent-transport
  mirrors repos/eventsourcing/packages/eventsourcing-transport

packages/fluent-transport-inmemory
  mirrors repos/eventsourcing/packages/eventsourcing-transport-inmemory

packages/fluent-protocol
  mirrors repos/eventsourcing/packages/eventsourcing-protocol

packages/fluent-transport-http
  HTTP transport implementation for PROTOCOL.md, analogous to
  repos/eventsourcing/packages/eventsourcing-transport-websocket

packages/fluent-transport-rpc
  RPC transport implementation, analogous to other transport packages

packages/fluent-client
  public Effect-native client over the protocol/transport packages
```

### `fluent-stream-log`

Owns the semantic stream log contract. It has no transport, protocol, HTTP, or
server dependencies.

```text
packages/fluent-stream-log/src/
  domainTypes.ts
  streamTypes.ts
  errors.ts
  durableStreamLog.ts
  operations.ts
  testing/
    durable-stream-log-test-suite.ts
  index.ts
```

Allowed dependencies:

- `effect`

Forbidden dependencies:

- `fluent-protocol`
- `fluent-transport`
- `effect-durable-streams`
- `fluent-client`
- `@effect/platform`

### `fluent-stream-log-inmemory`

Owns the clean in-memory stream-log implementation, following
`eventsourcing-store-inmemory`.

```text
packages/fluent-stream-log-inmemory/src/
  inMemoryDurableStreamLog.ts
  layer.ts
  index.ts
```

Allowed dependencies:

- `effect`
- `fluent-stream-log`

Forbidden dependencies:

- `fluent-protocol`
- `fluent-transport`
- `effect-durable-streams`
- current `packages/effect-durable-streams/src/Store.ts`
- current `packages/effect-durable-streams/src/MemoryStore.ts`

### `fluent-transport`

Owns the protocol-agnostic transport contracts. It does not know Durable Streams
commands, offsets, streams, producers, subscriptions, or HTTP.

```text
packages/fluent-transport/src/
  shared.ts
  client.ts
  server.ts
  index.ts
```

Allowed dependencies:

- `effect`

Forbidden dependencies:

- `fluent-stream-log`
- `fluent-protocol`
- `fluent-client`
- `@effect/platform`

### `fluent-transport-inmemory`

Owns the in-memory transport connector/acceptor, following
`eventsourcing-transport-inmemory`.

```text
packages/fluent-transport-inmemory/src/
  inMemoryTransport.ts
  layer.ts
  index.ts
```

Allowed dependencies:

- `effect`
- `fluent-transport`

Forbidden dependencies:

- `fluent-protocol`
- `fluent-stream-log`
- `fluent-client`

### `fluent-protocol`

Owns the Durable Streams request/response algebra, typed protocol outcomes,
server-side request handling over `DurableStreamLog`, and the `DurableTransport`
contract. The local transport does not serialize; codec logic over
`TransportMessage` belongs to future socket/RPC transport implementations.

```text
packages/fluent-protocol/src/
  request.ts
  response.ts
  transport.ts
  handler.ts
  localTransport.ts
  index.ts
```

Allowed dependencies:

- `effect`
- `fluent-stream-log`
- `fluent-transport`

Forbidden dependencies:

- `fluent-stream-log-inmemory`
- `fluent-transport-inmemory`
- `effect-durable-streams`
- `fluent-client`
- `@effect/platform`

The current `packages/effect-durable-streams/src/Protocol.ts` is not source
material for this package. Build `fluent-protocol` from the transport/protocol
reference shape and `PROTOCOL.md` semantics instead.

### `fluent-transport-http`

Owns the HTTP transport implementation for `PROTOCOL.md`. It is a transport
package, not a facade over the stream log. It is the HTTP server/client
transport analogous to eventsourcing WebSocket transport.

The reference shape is
`repos/eventsourcing/packages/eventsourcing-transport-websocket/src/lib/websocket-server.ts`,
adapted to HTTP routes, request bodies, long-poll, SSE, and reserved control
paths from `PROTOCOL.md`.

```text
packages/fluent-transport-http/src/
  httpServer.ts
  httpClient.ts
  routes.ts
  sse.ts
  headers.ts
  cache.ts
  typeSpec.ts
  layer.ts
  index.ts
```

Allowed dependencies:

- `fluent-transport`
- `@effect/platform`
- `@effect/platform-node`
- `effect`

Forbidden dependencies:

- `fluent-client`
- `effect-durable-execution`

### `fluent-transport-rpc`

Owns an RPC transport implementation, if the system needs one. RPC is a
transport, not a server semantic layer and not an exception to the transport
boundary.

```text
packages/fluent-transport-rpc/src/
  rpcServer.ts
  rpcClient.ts
  layer.ts
  index.ts
```

Allowed dependencies:

- `fluent-transport`
- RPC framework dependencies
- `effect`

Forbidden dependencies:

- `fluent-client`
- `effect-durable-execution`

### Deprecated Current Package

`packages/effect-durable-streams` is not part of the target architecture. Do
not port its HTTP API, route model, store shape, protocol schemas, or server
layer forward. New work must happen in the `fluent-*` packages above.

Current file disposition:

| Current file | Fate |
| --- | --- |
| `src/Store.ts` | do not port; endpoint-shaped legacy store |
| `src/MemoryStore.ts` | do not port; replace with `fluent-stream-log-inmemory` |
| `src/Protocol.ts` | do not port; replace with `fluent-protocol` |
| `src/Api.ts` | do not port; incompatible with target HTTP transport architecture |
| `src/ApiLive.ts` | do not port; incompatible with target HTTP transport architecture |
| `src/Server.ts` | do not port; replace with `fluent-transport-http` user-pluggable transport |
| `src/DurableStreamsServer.ts` | do not port; replace with `fluent-transport-rpc` as the RPC transport |

### `fluent-client`

Owns the public Effect-native client. It imports shared protocol and transport
interfaces; it does not define them.

```text
packages/fluent-client/src/
  client/
    DurableStreamsClient.ts
    StreamHandle.ts
    Producer.ts
    Reader.ts
    Errors.ts
    SchemaCodec.ts
  http/
    HttpClientTransport.ts
    Sse.ts
    Headers.ts
  subscriptions/
    SubscriptionClient.ts
    LeaseClient.ts
  state/
    StateBinding.ts
  index.ts
```

Allowed dependencies:

- `effect`
- `@effect/platform` in `src/http/*` only
- `fluent-stream-log`
- `fluent-transport`
- `fluent-protocol`

Forbidden dependencies:

- `fluent-stream-log-inmemory`
- `fluent-transport-inmemory`
- `fluent-transport-http` internals
- `effect-durable-streams`
- `effect-durable-execution`

Client tests may depend on in-memory packages as dev-only test dependencies.
Runtime client code may not.

### Tests

Tests live with the package that owns the contract:

```text
packages/fluent-stream-log/test/
  durable-stream-log.contract.test.ts
  offsets.test.ts
  json-boundaries.test.ts
  producer-fencing.test.ts

packages/fluent-stream-log-inmemory/test/
  in-memory-stream-log.test.ts

packages/fluent-transport/test/
  transport.contract.test.ts

packages/fluent-transport-inmemory/test/
  in-memory-transport.test.ts

packages/fluent-protocol/test/
  in-memory-transport.test.ts

packages/fluent-transport-http/test/
  http-transport.conformance.test.ts

packages/fluent-client/test/
  client/
  http/
  conformance/
```

Contract tests belong at the lowest package that can express the behavior. Do
not test producer fencing first through HTTP. Test it through stream-log/protocol
first, then add HTTP conformance coverage later.

### Composition Direction

The semantic import graph mirrors eventsourcing:

```text
stream-log
  ↑
stream-log-inmemory

transport
  ↑
transport-inmemory

transport + stream-log
  ↑
protocol
```

Transport is separate from protocol:

```text
transport        protocol
    ↑              ↑
transport-inmemory │
                   │
        integration layer
          imports both sides
```

HTTP and public client surfaces sit above the protocol and transport surfaces:

```text
protocol handler / transport
  ↑
client public API
  ↑
http transport
```

No lower layer may import a higher layer to avoid circular architecture. If a
lower layer needs a concept from above, the concept belongs lower or should be
passed as a callback/interface.

## Behind-The-Wire Concepts

These are the Durable Streams concepts that must exist without HTTP.

| `PROTOCOL.md` concept | Stream-log/protocol owner | Transport projection |
| --- | --- | --- |
| stream path | branded `StreamPath` | URL path extraction/encoding |
| content type | stream metadata | `Content-Type` header |
| create/ensure stream | protocol command + stream metadata | HTTP `PUT` |
| append bytes | `Sink` into stream log | HTTP `POST` body streaming |
| close stream | monotonic stream state transition | `Stream-Closed` header |
| read catch-up | historical `Stream` from offset | HTTP `GET` response body |
| live long-poll | subscribe/take/timeout | `live=long-poll`, 200/204 mapping |
| SSE | subscribe stream + control records | SSE event framing/base64 |
| head metadata | stream metadata query | HTTP `HEAD` headers |
| delete | delete/soft-delete state machine | HTTP `DELETE` |
| offsets | offset service/codec | query/header serialization |
| forks | stream graph + read stitching | fork headers |
| JSON boundaries | content-type-aware record codec | JSON response array |
| producer fencing | protocol/stream-log atomic decision | producer headers |
| subscriptions | subscription state machine | `__ds` control endpoints |
| wake delivery | delivery manager | webhook HTTP, pull-wake stream |
| leases/generation | subscription state machine | callback/ack/release endpoints |
| schedules | durable timer + append command | `__ds/schedules` endpoints |
| caching/cursors/ETags | HTTP transport metadata from offsets/state | HTTP cache headers |
| route precedence | HTTP transport only | router ordering |

## Stream Log Contract

The first implementation target is a stream-native byte-log contract modeled after
`eventsourcing-store`, adapted for Durable Streams bytes and offsets.

```ts
export interface DurableStreamLog {
  readonly create: (
    request: CreateStream
  ) => Effect.Effect<CreateStreamResult, StreamLogError>

  readonly append: (
    request: AppendStream
  ) => Sink.Sink<AppendResult, Uint8Array, Uint8Array, StreamLogError>

  readonly read: (
    from: ReadPosition
  ) => Effect.Effect<Stream.Stream<StreamRecord, StreamLogError>, StreamLogError>

  readonly subscribe: (
    from: ReadPosition
  ) => Effect.Effect<Stream.Stream<StreamRecord, StreamLogError>, StreamLogError>

  readonly subscribeAll: () => Effect.Effect<
    Stream.Stream<TailAdvanced, StreamLogError>,
    StreamLogError
  >

  readonly head: (
    path: StreamPath
  ) => Effect.Effect<StreamMetadata, StreamLogError>

  readonly delete: (
    path: StreamPath
  ) => Effect.Effect<DeleteStreamResult, StreamLogError>
}
```

Rules:

- `append` is a `Sink`, not a method that requires pre-collected bytes.
- `read` returns historical records only.
- `subscribe` returns historical records followed by live records.
- `subscribeAll` is live-only and emits tail advancement across streams.
- stream closure is part of metadata and must be observable through `read`,
  `subscribe`, and `head`.
- non-existent stream reads return empty historical streams only if the semantic
  operation being tested is raw event-store behavior. Protocol-level reads of
  missing Durable Streams still map to `NotFound`.

The first version should be a fresh in-memory implementation adapted from the
reference `InMemoryStore.ts`. The contract tests must target `DurableStreamLog`,
not HTTP routes and not the current endpoint-shaped `Store.ts`.

## Offset Model

Offsets are semantic data, not HTTP strings.

```ts
type Offset = string & Brand.Brand<"Offset">
type StreamPath = string & Brand.Brand<"StreamPath">

interface ReadPosition {
  readonly path: StreamPath
  readonly offset: Offset | Beginning | Now
  readonly subOffset?: number
}
```

Requirements:

- clients must not interpret offsets;
- server-generated offsets are lexicographically sortable per stream;
- generated offsets must never be `-1` or `now`;
- generated offsets must not contain `,`, `&`, `=`, `?`, or `/`;
- `-1` means beginning;
- `now` means current tail;
- fork sub-offset is a separate non-negative addressing dimension.

The initial in-memory backend can mint simple padded sequence offsets as long as
they satisfy the public offset invariants.

## Content Record Model

The store stores stream records, not HTTP response chunks.

```ts
interface StreamRecord {
  readonly path: StreamPath
  readonly fromOffset: Offset
  readonly nextOffset: Offset
  readonly bytes: Uint8Array
  readonly contentType: string
  readonly closed?: boolean
}
```

`application/json` needs a codec above raw byte storage:

- validate appended JSON;
- reject empty arrays;
- flatten one array level into logical messages;
- preserve per-message boundaries;
- read responses can reassemble JSON arrays at the protocol or HTTP transport
  edge.

For non-JSON content, records can be byte ranges.

## Protocol Command Algebra

The protocol layer expresses `PROTOCOL.md` operations without HTTP.

```ts
type DurableStreamsCommand =
  | { readonly _tag: "CreateStream"; readonly request: CreateStream }
  | { readonly _tag: "AppendToStream"; readonly request: AppendStream }
  | { readonly _tag: "CloseStream"; readonly request: CloseStream }
  | { readonly _tag: "ReadStream"; readonly request: ReadStream }
  | { readonly _tag: "SubscribeStream"; readonly request: SubscribeStream }
  | { readonly _tag: "HeadStream"; readonly request: HeadStream }
  | { readonly _tag: "DeleteStream"; readonly request: DeleteStream }
  | { readonly _tag: "CreateSubscription"; readonly request: CreateSubscription }
  | { readonly _tag: "GetSubscription"; readonly request: GetSubscription }
  | { readonly _tag: "DeleteSubscription"; readonly request: DeleteSubscription }
  | { readonly _tag: "AddSubscriptionStreams"; readonly request: AddSubscriptionStreams }
  | { readonly _tag: "RemoveSubscriptionStream"; readonly request: RemoveSubscriptionStream }
  | { readonly _tag: "ClaimSubscription"; readonly request: ClaimSubscription }
  | { readonly _tag: "AckSubscription"; readonly request: AckSubscription }
  | { readonly _tag: "ReleaseSubscription"; readonly request: ReleaseSubscription }
  | { readonly _tag: "PutSchedule"; readonly request: PutSchedule }
  | { readonly _tag: "GetSchedule"; readonly request: GetSchedule }
  | { readonly _tag: "DeleteSchedule"; readonly request: DeleteSchedule }
```

Responses are tagged semantic responses. HTTP status codes are added later by
`fluent-transport-http`.

```ts
type AppendResponse =
  | { readonly _tag: "Appended"; readonly nextOffset: Offset; readonly closed: boolean }
  | { readonly _tag: "AppendDuplicate"; readonly nextOffset: Offset; readonly closed: boolean }
  | { readonly _tag: "EpochFenced"; readonly currentEpoch: number }
  | { readonly _tag: "SequenceGap"; readonly expectedSeq: number; readonly receivedSeq: number }
  | { readonly _tag: "WriteToClosed"; readonly finalOffset: Offset }
  | { readonly _tag: "ContentMismatch"; readonly code: "content-mismatch"; readonly expected: string; readonly actual: string }
  | { readonly _tag: "OffsetConflict"; readonly code: "offset-conflict"; readonly expectedTailOffset: Offset; readonly actualTailOffset: Offset }
  | { readonly _tag: "StreamNotFound" }
  | { readonly _tag: "StreamGone" }

type CloseResponse =
  | { readonly _tag: "Appended"; readonly nextOffset: Offset; readonly closed: true }
  | { readonly _tag: "AppendDuplicate"; readonly nextOffset: Offset; readonly closed: true }
  | { readonly _tag: "Closed"; readonly finalOffset: Offset }
  | Exclude<AppendResponse, { readonly _tag: "WriteToClosed" | "Appended" | "AppendDuplicate" }>

type CreateResponse =
  | { readonly _tag: "Created"; readonly tailOffset: Offset; readonly closed: boolean; readonly contentType: string }
  | { readonly _tag: "AlreadyExists"; readonly tailOffset: Offset; readonly closed: boolean; readonly contentType: string }
  | { readonly _tag: "CreateConflict"; readonly code: "create-conflict"; readonly reason: "config-mismatch" | "closure-mismatch" }
  | { readonly _tag: "StreamGone" }
```

The protocol layer owns validation and decision precedence. For append:

1. closed stream;
2. content type mismatch;
3. expected-tail offset conflict;
4. producer epoch/sequence validation;
5. store append.

The HTTP transport maps those tagged outcomes to `204`, `200`, `400`, `403`,
`409`, `410`, etc. Variants that share a status code must self-discriminate on
the wire: 409 variants carry a literal `code` value, and `204` append responses
decode as `Appended` unless the transport has an explicit duplicate marker.
Repeated plain `close()` is idempotent and returns `Closed`, not `WriteToClosed`.

The handler must map store typed errors with `Effect.catchTags` over every
current `DurableStreamLogError` member. Meaningful domain errors become typed
responses; impossible failures become defects. It must not use `catchAll` to
turn unexpected failures into `StreamGone`.

The existing `packages/effect-durable-streams/src/Protocol.ts` should not be
ported. It contains HTTP-shaped request and decision schemas. `fluent-protocol`
must instead be aligned with `repos/eventsourcing/packages/eventsourcing-protocol`:

- schema-validated protocol envelopes that travel over any transport;
- client-side protocol service with pending request correlation;
- server-side protocol service that validates incoming messages and routes them
  to semantic commands;
- subscription messages that carry stream records over transport;
- no URL, header, status-code, or TypeSpec concepts in the protocol package.

The split should resemble:

```text
request.ts
  create/append/close/read/head/delete request schemas

response.ts
  typed protocol outcome schemas; no lossy Failure variant

transport.ts
  DurableTransport.call(request) and DurableTransport.stream(readLive)

handler.ts
  handle(log, request) -> typed response

localTransport.ts
  direct dispatch plus scoped Mailbox live reads
```

The exact filenames can change, but the boundary cannot: protocol messages are
transport payloads, not HTTP request DTOs.

## Producer Fencing

Producer state is part of the core protocol/stream-log transaction, not an HTTP
header concern.

```ts
interface ProducerFence {
  readonly producerId: string
  readonly epoch: number
  readonly seq: number
}
```

Requirements:

- all producer fields appear together or the command is invalid;
- `producerId` is non-empty;
- epoch and seq are safe non-negative integers;
- epoch regression fences stale producers;
- new epoch must start at seq `0`;
- duplicate seq returns idempotent success;
- seq gap returns expected/received sequence;
- validation and append serialize per `(stream, producerId)`;
- durable backends should commit producer state and appended bytes atomically;
- forks do not inherit producer state.

## Forks And Soft Delete

Forks are a stream graph concern.

```ts
interface ForkMetadata {
  readonly source: StreamPath
  readonly offset: Offset
  readonly subOffset: number
}
```

Requirements:

- fork content type must match source or inherit it;
- fork reads stitch source data before the fork boundary and fork-local data
  after the boundary;
- source appends after fork creation do not appear in the fork;
- fork is open even if source is closed;
- fork does not inherit producer state;
- deleting a source with active forks soft-deletes the source;
- soft-deleted streams return gone for direct operations but remain readable
  through forks;
- deleting the last fork can cascade cleanup.

## Subscription State Machine

Subscriptions are store/server semantics. Webhook HTTP and pull-wake HTTP
endpoints are transports over a subscription service.

```ts
interface SubscriptionService {
  readonly put: (request: CreateSubscription) =>
    Effect.Effect<PutSubscriptionResult, SubscriptionError>

  readonly get: (id: SubscriptionId) =>
    Effect.Effect<SubscriptionView, SubscriptionError>

  readonly delete: (id: SubscriptionId) =>
    Effect.Effect<void, SubscriptionError>

  readonly addStreams: (request: AddSubscriptionStreams) =>
    Effect.Effect<void, SubscriptionError>

  readonly removeStream: (request: RemoveSubscriptionStream) =>
    Effect.Effect<void, SubscriptionError>

  readonly onTailAdvanced: (event: TailAdvanced) =>
    Effect.Effect<void, SubscriptionError>

  readonly claim: (request: ClaimSubscription) =>
    Effect.Effect<ClaimResult, SubscriptionError>

  readonly ack: (request: AckSubscription) =>
    Effect.Effect<AckResult, SubscriptionError>

  readonly release: (request: ReleaseSubscription) =>
    Effect.Effect<ReleaseResult, SubscriptionError>
}
```

Requirements:

- normalized subscription config hash drives idempotent reconfirmation;
- at least one of pattern or streams is required;
- lease TTL is `1000..600000` ms;
- existing matching streams are eagerly linked at tail on creation;
- future matching appends are linked before wake evaluation;
- one cursor per linked stream with inclusive `acked_offset`;
- explicit links override glob links in serialized views;
- wake generation increments monotonically;
- every wake has unique `wake_id`;
- callbacks, ack, and release are fenced by token, generation, and `wake_id`;
- lease expiry clears holder and schedules another wake if work remains;
- pull-wake writes wake events to an ordinary durable stream;
- webhook delivery signs exact raw body bytes and uses key discovery;
- filtered subscriptions maintain public ack cursor and internal evaluated
  cursor separately.

The first implementation can defer webhook transport and schedules, but it must not design subscriptions as HTTP routes. HTTP should project these semantics through `fluent-transport-http` once the protocol service exists.

## Transport Contracts

Lift the `eventsourcing-transport` shape, but adapt payloads to Durable Streams
protocol messages.

```ts
interface TransportMessage {
  readonly id: MessageId
  readonly type: string
  readonly payload: string
  readonly metadata: Record<string, unknown>
}

interface ClientTransport {
  readonly connectionState: Stream.Stream<ConnectionState>
  readonly publish: (message: TransportMessage) => Effect.Effect<void, TransportError>
  readonly subscribe: (
    filter?: (message: TransportMessage) => boolean
  ) => Effect.Effect<Stream.Stream<TransportMessage>, TransportError>
}

interface ServerTransport {
  readonly connections: Stream.Stream<ClientConnection>
  readonly broadcast: (message: TransportMessage) => Effect.Effect<void, TransportError>
}
```

The local protocol transport is the first client/server integration target:

```text
fluent-client
  -> fluent-protocol DurableTransport
  -> fluent-protocol handler
  -> fluent-stream-log-inmemory
```

This gives client work a real local target before HTTP server work exists. The
raw `fluent-transport` byte-pipe is exercised later by HTTP/socket transports,
not by the ergonomic client.

## HTTP Transport

HTTP comes after store, protocol, transport, and in-memory integration.

Responsibilities:

- map routes and reserved `__ds` precedence;
- parse headers/query/body into protocol commands;
- pipe request body `Stream<Uint8Array>` into append sink;
- map tagged protocol responses to status codes and headers;
- frame read streams as catch-up bodies, long-poll, or SSE;
- base64 encode non-text SSE data events;
- generate HTTP cursors, ETags, cache headers, browser safety headers;
- expose TypeSpec/OpenAPI as documentation and generated HTTP transport
  contract.

TypeSpec remains valuable, but only for this HTTP transport surface.

## Testing Strategy

The build is driven by contract suites, not endpoint snapshots.

### Stream Log Contract

Clone/customize `repos/eventsourcing/packages/eventsourcing-store/src/lib/testing/eventstore-test-suite.ts`.

Required cases:

- append to empty stream at beginning;
- reject append at wrong expected position;
- append at returned end position;
- read all historical records from beginning;
- read historical records from mid-stream;
- read empty/non-existent raw stream-log stream as empty where applicable;
- read immediately after write;
- subscribe returns historical then live records;
- multiple subscribers receive the same live records;
- `subscribeAll` is live-only and includes stream positions;
- subscription interruption cleans up.

Durable Streams additions:

- offsets are opaque and lexicographically increasing;
- `now` and `-1` sentinel reads;
- stream closure EOF through read/subscribe/head;
- append to closed stream conflict precedence;
- content type mismatch conflict;
- JSON flattening/preservation;
- fork read stitching;
- soft-delete direct gone but fork reads still work.

### Producer Contract

Required cases:

- producer fields must be all-or-none;
- first `(epoch=0, seq=0)` accepted;
- duplicate sequence dedupes;
- sequence gap conflicts with expected/received values;
- epoch regression fences;
- epoch bump with seq `0` accepted;
- epoch bump with non-zero seq rejected;
- parallel appends for one `(stream, producerId)` serialize.

### Subscription Contract

Required cases:

- create/reconfirm same config is idempotent;
- changed config conflicts;
- invalid lease TTL rejected;
- pattern/stream linking;
- explicit membership add/remove;
- tail advancement creates wake only when idle;
- claim creates lease and fences other workers;
- ack heartbeats extend lease;
- done ack advances cursors and releases;
- stale generation/wake requests conflict;
- release without ack rewakes if pending remains.

### Transport Contract

Clone/customize `eventsourcing-testing-contracts` transport tests:

- client/server connection establishes under `Scope`;
- multiple clients connect to one server;
- client-to-server publish works;
- server broadcast works;
- client filtering works;
- cleanup on scope close works.

### Protocol Contract

Required cases:

- client command correlates to one response;
- server validates incoming protocol payloads;
- protocol errors do not corrupt pending state;
- subscribe command receives records after append;
- in-memory client/server pair can create, append, read, and subscribe without
  HTTP.

## Serial Implementation Phases

Work must proceed in this order. Do not dispatch client, protocol, or HTTP work
against imagined surfaces. Each phase creates the concrete dependency for the
next one.

### Phase 0: Stabilize Docs And Ownership

Deliverables:

- this SDD;
- update server/client SDDs to point at this document as the core architecture;
- stop assigning implementation work directly against HTTP/TypeSpec until
  Phase 6.

### Phase 1: Stream Log

Owner: Agent3.

Deliverables:

- `DurableStreamLog` interface;
- fresh in-memory implementation adapted from
  `repos/eventsourcing/packages/eventsourcing-store-inmemory/src/lib/InMemoryStore.ts`;
- subscription/live stream support adapted from
  `repos/eventsourcing/packages/eventsourcing-store-inmemory/src/lib/subscriptionManager.ts`
  where useful;
- customized stream-log contract suite;
- tests for historical read and live subscribe.
- a minimal in-process read/write client over `DurableStreamLog` so
  consumers can prove create/append/read/subscribe without protocol transport.

Non-deliverables:

- no HTTP route rewrite;
- no TypeSpec changes;
- no execution API.
- no dependency on `packages/effect-durable-streams/src/Store.ts`.
- no implementation inside `packages/effect-durable-streams`.
- no HTTP transport implementation in this phase.

### Phase 2: Protocol

Owner: Agent3 with architecture review. Starts only after Phase 1 passes.

Deliverables:

- `packages/fluent-protocol`;
- replacement for the current HTTP-shaped
  `packages/effect-durable-streams/src/Protocol.ts`; do not port that file;
- transport-message command/response/subscribe/record envelope schemas aligned
  with `repos/eventsourcing/packages/eventsourcing-protocol`;
- client protocol service with pending request correlation;
- server protocol service with validated incoming command stream and outgoing
  result/record publishers;
- producer fencing decisions;
- offset/sentinel codec;
- content-type-aware record codec;
- protocol service over `DurableStreamLog`;
- protocol contract tests.

### Phase 3: Transport And In-Memory Pair

Owner: Agent3 unless explicitly reassigned. Starts only after Phase 2 passes.

Deliverables:

- `packages/fluent-transport`;
- `packages/fluent-transport-inmemory`;
- protocol-agnostic transport contracts;
- in-memory connector/acceptor;
- transport contract tests;
- protocol client/server running over in-memory transport.
- the same basic client from Phase 1 backed by in-memory protocol
  transport instead of direct stream-log calls.

### Phase 4: Effect Client Over Protocol Transport

Owner: Agent2. Starts only after Phase 3 provides a real in-memory protocol
server/client pair.

Deliverables:

- `DurableStreamClient` service over protocol transport;
- stream handle: create/head/read/subscribe/delete;
- producer resource owning producer id/epoch/seq;
- typed schema wrapper using `Schema` at the client boundary;
- tests against in-memory protocol server.
- no runtime dependency on `fluent-stream-log-inmemory`,
  or `fluent-transport-inmemory`.

Non-deliverables:

- no durable execution;
- no HTTP dependency as the only client backend.

### Phase 5: Subscriptions

Owner: Agent3.

Deliverables:

- subscription service state machine;
- tail advancement integration;
- pull-wake claim/ack/release core;
- subscription contract tests.

Webhook outbound delivery and schedules can follow after pull-wake works.

### Phase 6: HTTP Transport

Owner: Agent1 review, implementation after Phases 1-5.

Deliverables:

- map TypeSpec/OpenAPI operations to protocol commands;
- first define pure `wireEncode`, `wireDecode`, and `parseSse` helpers in
  `fluent-protocol` against canned `(status, headers, body)` fixtures;
- make `(status, code, headers) -> Response` inverse mapping total before
  adding network handlers;
- preserve `__ds` route precedence in `fluent-transport-http`;
- catch-up, long-poll, and SSE HTTP transport handlers over `read`/`subscribe`;
- webhook callback/ack/release HTTP endpoints over subscription service;
- conformance tests through HTTP.

### Phase 7: Durable Backend

Deliverables:

- persistent backend implementing `DurableStreamLog`;
- same stream-log/producer/subscription contract suites;
- documented atomicity for producer state + appends.

### Phase 8: Execution

Only after the log/protocol/client substrate exists:

- state schema bindings;
- state machine/event lowering;
- MCP tool wait/sleep/spawn/schedule bindings;
- execution SDD implementation.

## Agent Assignment Guidance

Agent1 is coordinator/architect:

- owns SDD coherence;
- reviews boundaries;
- prevents HTTP-first drift;
- does not take the first implementation slice unless explicitly asked.

Agent3 should build server/core:

- Phase 1 Store;
- Phase 2 Protocol;
- Phase 3 Transport And In-Memory Pair;
- Phase 5 Subscriptions.

Agent2 should build client against in-memory protocol:

- Phase 4 Effect client;
- no execution state API until substrate is real.

## Open Questions

1. Should stream-log/protocol/transport be separate workspace packages immediately?

   Decision: yes. These boundaries are now fixed by the Package Map. Do not
   stage package-neutral store, protocol, or transport code inside
   `packages/effect-durable-streams`.

2. Should raw byte append records be chunked by request, by backend chunk, or by
   content message?

   Recommendation: persist stream-log records with byte ranges; JSON mode stores
   message boundaries, binary/text can store backend chunks as long as offsets
   stay stable.

3. Should `subscribeAll` be durable?

   Recommendation: no. `subscribeAll` is live notification. Durable subscription
   state is separate and persisted by `SubscriptionService`.

4. Should HTTP route precedence concerns affect store path naming?

   Recommendation: no. Store/protocol uses `StreamPath`; HTTP transport reserves
   `__ds`.

## Acceptance Criteria

The architecture is on track when:

- an in-memory server and client can create, append, read, and subscribe without
  opening an HTTP port;
- the in-memory stream log passes an eventstore-style contract suite;
- producer fencing is tested without HTTP headers;
- subscription claim/ack/release is tested without HTTP endpoints;
- HTTP becomes a transport implementation over tagged protocol responses;
- TypeSpec/OpenAPI documents the HTTP transport, not stream-log/protocol.
