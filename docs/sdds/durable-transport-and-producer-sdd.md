# Durable Transport and Producer SDD

## Status

Draft for the next implementation slice. This SDD narrows the client/protocol
boundary after the stream-log ADT work. It is inspired by
`/Users/gnijor/Downloads/durable-transport-and-producer-handoff (7).md`, but
is scoped to what this repository should build now.

## Problem

Durable Streams needs a client-facing protocol boundary that is not the raw
message transport. The raw transport shape from `fluent-transport` is a dumb
connected pipe:

```ts
publish(TransportMessage): Effect<void, TransportError>
subscribe(filter?): Effect<Stream<TransportMessage>, TransportError, Scope>
```

That is the right shape for WebSocket-like transports and for
`fluent-transport-inmemory`, but it is not the user-facing Durable Streams API.
Durable Streams operations are request/response plus one server-streaming read
mode. If the client talks directly to raw transport, it must own correlation,
envelope encoding, response demuxing, and protocol failure mapping. That is
architecture debt.

## Decision

`fluent-protocol` owns a two-verb Durable Streams protocol contract:

```ts
call(Request): Effect<Response, TransportError>
stream(ReadLive): Effect<ReadEvent session, TransportError, Scope>
```

This contract is named `DurableTransport` for now because it represents the
transport-neutral Durable Streams protocol session. It is not equivalent to
`fluent-transport.ClientTransport`.

`fluent-client` depends on this two-verb contract only. It must not import
`fluent-transport` or concrete transport packages in production code.

## Package Boundaries

`fluent-stream-log`
: Owns the durable byte log ADT: stream identity/path, offsets, create, append,
read windows, live changes, close, fork, trim, delete, producer fencing state,
and typed log errors.

`fluent-protocol`
: Owns `Request`, `Response`, `ReadEvent`, `DurableTransport`, `handle(log,
request)`, protocol-level `TransportError`, and the local reference
implementation `makeLocalTransport(log)`. It does not import the raw
`fluent-transport` pipe.

`fluent-client`
: Owns ergonomic argument shaping and the `Producer` resource. It calls
`DurableTransport.call` and `DurableTransport.stream`.

`fluent-transport`
: Owns the raw message pipe only. It has no Durable Streams semantics.

`fluent-transport-inmemory`
: Owns an in-memory implementation of the raw message pipe. It is tested with
transport contract tests, not used as the first client E2E path.

`fluent-transport-http`
: Later phase. Implements `DurableTransport` over HTTP/SSE using
`fluent-protocol`, Effect platform HTTP/SSE primitives, and the RFC HTTP
mapping. It does not implement the raw `fluent-transport.ClientTransport`
pipe.

## Non-Goals

- Do not create `fluent-protocol-inmemory`.
- Do not create a protocol-over-raw-transport pump for this slice.
- Do not put request correlation in `fluent-client`.
- Do not serialize `Uint8Array` as JSON `number[]`.
- Do not collapse protocol outcomes into a stringly `Failure`.
- Do not use raw `fluent-transport-inmemory` to make client E2E look real in
this slice.

## Request Algebra

`fluent-protocol/src/request.ts` owns:

- `Create`
- `Append`
- `Close`
- `Read`
- `Head`
- `Delete`
- `ReadLive`
- `ProducerFence`

Producer fencing is modeled as a single optional object:

```ts
producer?: {
  producerId: string
  epoch: number
  seq: number
}
```

This makes the RFC all-or-none producer tuple unrepresentable as three
independently optional fields.

## Response Algebra

`fluent-protocol/src/response.ts` owns typed response variants. Protocol
outcomes are values, not the Effect error channel:

- `Appended`
- `AppendDuplicate`
- `EpochFenced`
- `SequenceGap`
- `WriteToClosed`
- `Closed`
- `ContentMismatch`
- `OffsetConflict`
- `StreamNotFound`
- `StreamGone`
- `ReadResult`
- `InvalidOffset`
- `Created`
- `AlreadyExists`
- `CreateConflict`
- `HeadResult`
- `Deleted`

The Effect error channel on `DurableTransport` is reserved for transport or
decode failure.

## Handler

`fluent-protocol/src/handler.ts` maps:

```ts
DurableStreamLog + Request -> Response
```

Rules:

- Preserve structured store error data in response variants.
- Do not map defects or unknown errors to fake protocol successes.
- Add new store errors to the mapping explicitly.
- Keep append/close idempotency visible in the response algebra.

## Local Reference Transport

`fluent-protocol/src/localTransport.ts` is the first implementation:

```ts
makeLocalTransport(log): Effect<DurableTransportService>
```

It has no envelope, serialization, raw transport, or correlation:

- `call` dispatches directly to `handle(log, request)`.
- `stream` adapts `log.changes(readPosition)` into a scoped live-read session.

The desired session primitive is `Mailbox<ReadEvent, TransportError>` because it
models done/fail terminal semantics. The current Effect v4 beta in this repo
does not expose `effect/Mailbox`, so the implementation uses
`Queue.Dequeue<ReadEvent, TransportError | Done>` as the local stand-in. When
the dependency exposes Mailbox, this should be swapped without changing the
public semantics.

## Client

`fluent-client` is argument shaping:

- `create` constructs `Create` and calls `transport.call`.
- `append` constructs `Append` and calls `transport.call`.
- `read` constructs `Read` and calls `transport.call`.
- `tail` constructs `ReadLive` and calls `transport.stream`.
- `head`, `delete`, `close` construct the corresponding requests.
- `producer` builds the `Producer` resource over the same contract.

No production client file may import `fluent-transport`.

## Producer

The `Producer` resource owns `(producerId, epoch, seq)`:

- `seq` advances only on `Appended` or `AppendDuplicate`.
- `TransportError` retries use the same `(epoch, seq)` tuple.
- `EpochFenced` is surfaced unless `autoClaim` is enabled.
- `autoClaim` retries once with `currentEpoch + 1` and `seq = 0`.
- `SequenceGap` is surfaced as a producer error.
- Appends are serialized per producer until a later muxed transport supports
safe pipelining.

## Testing

First E2E path:

```text
DurableStreamsClient
  -> DurableTransport from makeLocalTransport(log)
  -> handle(log, request)
  -> fluent-stream-log-inmemory
```

Separate transport path:

```text
fluent-transport-inmemory
  -> fluent-transport contract tests
```

Do not combine those paths until a real wire DurableTransport implementation is
being built.

## Later Phases

HTTP/SSE:
: Implement `DurableTransport` over HTTP/SSE. Prefer Effect HTTP/API
derivation for unary endpoints and raw `Sse` parsing for live reads.

Muxed socket/RPC:
: Implement the request-id pump and stream fan-out only here. This is the first
place where `Map<id, Deferred>` correlation is required.

Content types:
: Centralize `application/json` framing/validation/flattening separately from
SSE transfer encoding. `application/ndjson` remains opaque byte/text content,
not JSON mode.

Promise/Web Streams adapter:
: Build later as a thin edge over the Effect-native client surface.
