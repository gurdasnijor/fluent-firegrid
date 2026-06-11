# Durable Stream Log Design

Status: draft
Owner: Firegrid / Durable Streams
Component: `packages/fluent-stream-log`
Primary protocol source: `docs/reference/durable-streams/PROTOCOL.md`

## Purpose

`DurableStreamLog` is the authoritative substrate for Durable Streams. It is
not an HTTP API, not a client API, and not a transport. It defines the abstract
data type that all backends must implement before protocol, client, HTTP, SSE,
subscriptions, or durable execution can be trusted.

The log owns durable stream semantics:

- stream identity and metadata;
- append-only ordered data;
- opaque server-minted offsets;
- content type equality;
- stream closure as a monotonic terminal state;
- producer fencing and idempotent duplicate detection;
- catch-up reads and live changes;
- forks and soft-delete lifecycle;
- trimming and retention boundaries.

The protocol layer above it maps these semantics to command/response unions.
The HTTP transport later maps those unions to `PROTOCOL.md` routes, headers,
status codes, SSE frames, and cache behavior.

## Non-Goals

- Do not expose HTTP methods, URLs, headers, status codes, SSE fields, or
  TypeSpec DTOs.
- Do not use `Machine` or cluster primitives as the log model. Those can help
  runtime processes around the log, not the log ADT.
- Do not make byte offsets part of the public contract. A backend may use byte
  positions internally, but offsets remain opaque tokens.
- Do not treat `application/ndjson` or `application/*+json` as JSON mode.
  `application/json` is the only content type with special framing semantics.
- Do not encode stream closure as a fake empty byte record.

## Conceptual ADT

The log is a family of named streams. Each stream has:

- `streamId`: stable identity within the log;
- `contentType`: immutable content type chosen at creation;
- `tail`: the offset immediately after the last committed item;
- `state`: open, closed, soft-deleted, or deleted;
- optional fork metadata;
- producer state scoped by `(streamId, producerId)`;
- retention metadata such as earliest available offset.

Operations are linearizable per stream. If a backend cannot provide global
linearizability, it must still serialize all operations that mutate the same
stream and all producer state associated with that stream.

## Target API Shape

This is the target shape, not a promise that the current code already matches
it exactly.

```ts
export interface DurableStreamLog {
  readonly create: (request: CreateStream) =>
    Effect.Effect<CreateOutcome, CreateError | LogBackendError>

  readonly append: (request: AppendStream) =>
    Effect.Effect<AppendOutcome, AppendError | LogBackendError>

  readonly read: (request: ReadWindowRequest) =>
    Effect.Effect<ReadWindow, ReadError | LogBackendError>

  readonly changes: (request: ChangesRequest) =>
    Stream.Stream<ChangeEvent, ReadError | LogBackendError>

  readonly head: (streamId: StreamId) =>
    Effect.Effect<StreamHead, StreamNotFound | StreamGone | LogBackendError>

  readonly fork: (request: ForkStream) =>
    Effect.Effect<CreateOutcome, ForkError | LogBackendError>

  readonly trim: (request: TrimStream) =>
    Effect.Effect<void, TrimError | LogBackendError>

  readonly delete: (streamId: StreamId) =>
    Effect.Effect<DeleteOutcome, DeleteError | LogBackendError>
}
```

`append` is an effect over one framed append request, not a transport-shaped
sink. Protocol or HTTP may stream request bodies into framed messages before
calling the log. The log receives the flattened message batch and commits the
whole batch atomically.

## Core Types

```ts
export type Offset = string & Brand.Brand<"Offset">
export type StreamId = string & Brand.Brand<"StreamId">
export type StreamPath = string & Brand.Brand<"StreamPath">
export type ProducerId = string & Brand.Brand<"ProducerId">

export interface ProducerFence {
  readonly producerId: ProducerId
  readonly epoch: number
  readonly seq: number
}

export interface StreamHead {
  readonly streamId: StreamId
  readonly contentType: string
  readonly tail: Offset
  readonly closed: boolean
  readonly gone: boolean
}

export interface StreamChunk {
  readonly fromOffset: Offset
  readonly nextOffset: Offset
  readonly bytes: Uint8Array
}

export type ChangeEvent =
  | { readonly _tag: "Chunk"; readonly chunk: StreamChunk }
  | { readonly _tag: "CaughtUp"; readonly offset: Offset }
  | { readonly _tag: "Closed"; readonly finalOffset: Offset }
```

`StreamId` and `StreamPath` are separate concepts. `StreamId` is the stable
internal identity used by fork references and retained data. `StreamPath` is the
namespace name clients address. Soft-delete blocks path recreation while the
original stream id remains alive for fork stitching.

`CaughtUp` is a log-level event, not an SSE detail. It marks the seam between
historical replay and live tailing. Protocol can map it to
`Control{upToDate:true, closed:false}` and HTTP/SSE can map that to the
required control frame.

## Offsets

Offsets are opaque, server-minted tokens.

Required properties:

- clients and upper layers must not parse offset structure;
- generated offsets must never be `-1` or `now`;
- generated offsets must not contain `,`, `&`, `=`, `?`, or `/`;
- for one stream, lexicographic order reflects stream order;
- offsets are strictly increasing for appended data;
- the tail offset identifies the position immediately after the last committed
  item;
- `-1` and `now` are protocol sentinels and should be resolved before or at the
  log boundary, not generated by the log.

A backend may internally use byte positions, sequence numbers, file cursors, or
compound keys. That choice must not leak into the ADT.

Reads from an offset use range semantics: return the first item whose position
is at or after the requested position. Backends may reject malformed, trimmed,
or invalid offsets with typed errors.

## Append Semantics

Append is an atomic decision:

1. locate stream;
2. resolve producer state and short-circuit `Fenced`, `Deduplicated`, or
   `SequenceGap`;
3. reject soft-deleted stream;
4. handle closed stream;
5. validate content type equality when messages are appended;
6. validate expected tail if supplied;
7. commit data, closure, and producer state together;
8. publish live change events in commit order.

Producer preflight must happen before closed/content-type/expected-tail
validation. A retried producer append can carry stale expected-tail or even a
different body; if the `(producerId, epoch, seq)` tuple has already committed,
the log returns the stored deduplicated decision without re-running validations.

Append outcomes are success-channel protocol facts, not transport failures:

```ts
export type AppendOutcome =
  | { readonly _tag: "Appended"; readonly nextOffset: Offset; readonly closed: boolean; readonly highestSeq?: number }
  | { readonly _tag: "Deduplicated"; readonly nextOffset: Offset; readonly closed: boolean; readonly highestSeq: number }
  | { readonly _tag: "AlreadyClosed"; readonly finalOffset: Offset }
  | { readonly _tag: "Fenced"; readonly currentEpoch: number }
  | { readonly _tag: "SequenceGap"; readonly expectedSeq: number; readonly receivedSeq: number }
```

Domain errors include:

- `StreamNotFound`;
- `StreamGone`;
- `StreamClosed` with `finalOffset`;
- `ContentTypeMismatch` with `expected` and `actual`;
- `OffsetConflict` with `expectedTailOffset` and `actualTailOffset`;
- `OffsetTrimmed`;
- `InvalidOffset`.

Unexpected backend faults become `LogBackendError`. Defects should not be
converted into domain outcomes.

## Producer Fencing

Producer state is scoped by `(streamId, producerId)`.

Rules:

- all producer fields must appear together;
- `producerId` is non-empty;
- `epoch` and `seq` are non-negative safe integers;
- unknown producer state accepts only `seq = 0`;
- newer epoch accepts only `seq = 0` and resets sequence state;
- older epoch returns `Fenced`;
- same epoch and `seq <= lastSeq` returns `Deduplicated`;
- same epoch and `seq === lastSeq + 1` appends;
- same epoch and any larger sequence returns `SequenceGap`;
- forks do not inherit producer state.

The append bytes, closure state, and producer checkpoint must commit atomically.
If commit fails, none of them are visible.

## Closure

Closure is a first-class stream state transition.

Rules:

- close is durable and monotonic;
- once closed, a stream cannot reopen;
- close may occur with a final append;
- close-only appends do not create fake empty data records;
- `changes` emits `Closed{finalOffset}` and then completes;
- `read` at the final tail can report `closed = true` without returning bytes;
- plain close-only on an already-closed stream is idempotent;
- append-with-body to an already-closed stream fails unless it is a producer
  duplicate of the original closing append.

Protocol may expose a distinct `Closed{finalOffset}` response for idempotent
plain close. The log should preserve enough information for protocol to make
that distinction.

## Read And Changes

`read` is finite catch-up. It returns a `ReadWindow`:

```ts
export interface ReadWindow {
  readonly chunks: ReadonlyArray<StreamChunk>
  readonly nextOffset: Offset
  readonly upToDate: boolean
  readonly closed: boolean
}
```

`changes` is catch-up followed by live events.

Correct `changes` implementation order:

1. acquire the live subscription;
2. snapshot stream metadata and tail;
3. read historical backlog from requested offset up to the snapshot tail;
4. emit backlog;
5. emit `CaughtUp{offset:snapshotTail}`;
6. admit live events at or after the snapshot tail;
7. emit `Closed` and complete when closure is observed.

For an already-closed stream, the deterministic sequence is:

```text
backlog -> CaughtUp -> Closed -> end
```

Backlog uses `fromOffset < snapshotTail`; live admits `fromOffset >=
snapshotTail`. This avoids both the historical-to-live race and duplicate
delivery at the seam.

Publishing must preserve commit order. The in-memory reference may publish to
an unbounded `PubSub` while holding its synchronized mutation section. Durable
backends should not publish from inside an uncommitted database transaction;
they should use a committed, ordered handoff to one publisher.

## Content Types

The log owns content type equality: appends with data must match the stream's
configured content type.

There are three separate concerns:

- content type equality: log concern;
- JSON framing: protocol append preparation concern;
- SSE transfer encoding: HTTP transport concern.

`application/json` is the only special framing mode. It requires:

- validate appended JSON;
- reject append body `[]`;
- flatten exactly one array level into logical messages;
- preserve message boundaries;
- return catch-up reads as one JSON array at the protocol/HTTP boundary.

The log does not parse JSON. Framing happens above the log, but the framed
result reaches the log as `ReadonlyArray<Uint8Array>` and is committed as one
atomic append. Producer sequence counts append batches, not flattened messages.
A five-element JSON array is one producer sequence, five committed message
records, and one deduplication decision.

All other content types are opaque byte/message data, including
`application/ndjson`, `application/x-protobuf`, `text/plain`, and custom MIME
types.

SSE encoding is a different partition: `text/*` and `application/json` can be
sent as UTF-8 data events; every other content type is base64 encoded by the
HTTP/SSE transport.

## Forks

Forking is a semantic log operation, not an HTTP helper.

Rules:

- fork inherits source content up to the fork boundary;
- fork content type is inherited unless explicitly provided;
- explicit fork content type must match source content type;
- fork is open even if the source is closed;
- source appends after fork creation do not appear in the fork;
- reads on the fork stitch inherited data and fork-local data;
- fork uses the same public offset space before the divergence point;
- fork does not inherit producer state.

The in-memory reference should implement pointer stitching, not copy-on-fork.
Copy-on-fork makes soft-delete, reference counts, and trim-vs-dependent-fork
contracts dead code. Durable backends may choose a different storage strategy
as long as the observable ADT behavior and retention constraints match.

## Delete And Retention

Delete has two possible effects:

- hard delete when no forks depend on the stream;
- soft delete when active forks still reference the stream.

A soft-deleted stream:

- returns gone for direct operations;
- blocks path recreation;
- remains internally readable for fork stitching;
- can be garbage collected when no forks depend on it.

`trim` advances the earliest readable offset for retention. Reads before the
trim point fail with `OffsetTrimmed{earliest}`. Trimming must reject when the
requested retention floor would pass any dependent fork's divergence offset; it
must not silently clamp or break fork reads.

## Backend Requirements

Every backend must satisfy the same contract suite.

Required properties:

- per-stream create/append/close/delete serialization;
- producer state and append data committed atomically;
- no live publish before commit;
- no publish while holding a mutation lock;
- historical-to-live changes seam cannot drop records;
- multiple subscribers see the same committed events;
- close is observable through `head`, `read`, and `changes`;
- content type equality is enforced;
- offsets are opaque and monotonic;
- malformed, trimmed, and invalid offsets are typed failures;
- forks and soft-delete preserve source data as required.

The in-memory backend should be the reference conformance implementation. The
durable SQL backend should later pass the exact same suite.

## Relationship To Effect Primitives

Effect `Schema` should define tagged request, response, error, and stored-data
boundaries. Avoid naked `JSON.parse` at public or persisted boundaries.

Effect `Mailbox`, `Machine`, and cluster primitives are useful around the log:

- `Mailbox` for scoped live-read and remote subscription sessions at the
  protocol boundary;
- `Machine` for producer resources, SSE sessions, wake workers, or retrying
  runtime processes;
- cluster sharding and runner storage for future distributed ownership.

They do not replace `DurableStreamLog`.

## Migration From Current Code

Current code still exposes several legacy/event-store-shaped choices:

- `append` as `Sink`;
- `subscribe` returning only records;
- `subscribeAll` as a tail bus;
- close represented by a record with `closed`;
- producer errors in the error channel;
- `StreamPath` as the only identity where the log needs internal `StreamId`
  plus path resolution;
- no `fork`, `trim`, `soft-delete`, or `CaughtUp` event.

Recommended migration order:

1. Introduce target domain types beside current types.
2. Add `ChangeEvent = Chunk | CaughtUp | Closed`.
3. Replace fake close records with first-class closure state.
4. Refactor in-memory implementation to commit and publish in synchronized
   commit order.
5. Change producer fencing to success-channel `AppendOutcome`.
6. Add contract tests for close EOF, historical-to-live seam, duplicate
   producer append, sequence gap, and epoch regression.
7. Add fork and trim contracts before durable SQL backend work.
8. Let `fluent-protocol` map log outcomes to protocol responses with
   `Effect.catchTags` and exhaustive matching.
