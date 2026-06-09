/**
 * Shared Durable Streams protocol schemas, named with PROTOCOL.md terminology.
 *
 * effect-server.PROTOCOL.1 effect-server.PROTOCOL.2 effect-server.PROTOCOL.3
 */
import { Schema } from "effect"

/** Opaque position token used to resume reads and identify the stream tail. */
export const Offset = Schema.String
export type Offset = typeof Offset.Type

/** Path component relative to the server's `/v1/stream/` root. */
export const StreamPath = Schema.String
export type StreamPath = typeof StreamPath.Type

/** Strict decimal non-negative integer header value. */
export const UintFromString = Schema.compose(
  Schema.String.pipe(Schema.pattern(/^\d+$/)),
  Schema.NumberFromString,
).pipe(
  Schema.int(),
  Schema.nonNegative(),
  Schema.lessThanOrEqualTo(9_007_199_254_740_991),
)

/** Entity body bytes from PROTOCOL.md §5 HTTP operations. */
export const EntityBody = Schema.Uint8ArrayFromSelf
export type EntityBody = typeof EntityBody.Type

/** Idempotent producer tuple from PROTOCOL.md §5.2.1. */
export const IdempotentProducer = Schema.Struct({
  id: Schema.String,
  epoch: Schema.Number.pipe(Schema.int(), Schema.nonNegative()),
  seq: Schema.Number.pipe(Schema.int(), Schema.nonNegative()),
})
export type IdempotentProducer = typeof IdempotentProducer.Type

/** @deprecated Prefer `IdempotentProducer`, matching PROTOCOL.md §5.2.1. */
export const Producer = IdempotentProducer
/** @deprecated Prefer `IdempotentProducer`, matching PROTOCOL.md §5.2.1. */
export type Producer = IdempotentProducer

/** PUT create-stream request. */
export const CreateRequest = Schema.Struct({
  path: StreamPath,
  contentType: Schema.String,
  entityBody: EntityBody,
  close: Schema.Boolean,
})
export type CreateRequest = typeof CreateRequest.Type

/** POST append-to-stream request. */
export const AppendRequest = Schema.Struct({
  path: StreamPath,
  contentType: Schema.String,
  entityBody: EntityBody,
  close: Schema.Boolean,
  streamSeq: Schema.OptionFromSelf(Schema.String),
  idempotentProducer: Schema.OptionFromSelf(IdempotentProducer),
})
export type AppendRequest = typeof AppendRequest.Type

/** GET catch-up/live read request. */
export const ReadRequest = Schema.Struct({
  path: StreamPath,
  offset: Offset,
})
export type ReadRequest = typeof ReadRequest.Type

export const HeadRequest = Schema.Struct({
  path: StreamPath,
})
export type HeadRequest = typeof HeadRequest.Type

export const DeleteRequest = Schema.Struct({
  path: StreamPath,
})
export type DeleteRequest = typeof DeleteRequest.Type

/** Internal tail-advance fact emitted when a stream appends or closes. */
export const TailAdvanced = Schema.Struct({
  path: StreamPath,
  tailOffset: Offset,
  closed: Schema.Boolean,
})
export type TailAdvanced = typeof TailAdvanced.Type

/** HEAD stream metadata result. */
export const StreamTail = Schema.Struct({
  path: StreamPath,
  tailOffset: Offset,
  closed: Schema.Boolean,
  contentType: Schema.String,
})
export type StreamTail = typeof StreamTail.Type

/** Read response body and metadata for bytes from requested offset to tail. */
export const ReadChunk = Schema.Struct({
  path: StreamPath,
  contentType: Schema.String,
  entityBody: EntityBody,
  nextOffset: Offset,
  upToDate: Schema.Boolean,
  closed: Schema.Boolean,
})
export type ReadChunk = typeof ReadChunk.Type

/** Stream-listing projection used by glob/list backfill. */
export const StreamSnapshot = Schema.Struct({
  path: StreamPath,
  tailOffset: Offset,
  closed: Schema.Boolean,
  contentType: Schema.String,
})
export type StreamSnapshot = typeof StreamSnapshot.Type

/** PUT create-stream protocol decision. */
export const CreateDecision = Schema.Union(
  Schema.Struct({
    _tag: Schema.Literal("Created"),
    tailOffset: Offset,
    closed: Schema.Boolean,
  }),
  Schema.Struct({
    _tag: Schema.Literal("AlreadyExists"),
    tailOffset: Offset,
    closed: Schema.Boolean,
  }),
)
export type CreateDecision = typeof CreateDecision.Type

/** POST append-to-stream protocol decision, including conflict precedence. */
export const AppendDecision = Schema.Union(
  Schema.Struct({
    _tag: Schema.Literal("PlainAccepted"),
    nextOffset: Offset,
    closed: Schema.Boolean,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ProducerAccepted"),
    nextOffset: Offset,
    closed: Schema.Boolean,
    producerEpoch: Schema.Number,
    highestAcceptedSeq: Schema.Number,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ProducerDuplicate"),
    nextOffset: Offset,
    closed: Schema.Boolean,
    producerEpoch: Schema.Number,
    highestAcceptedSeq: Schema.Number,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ProducerFenced"),
    currentEpoch: Schema.Number,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ProducerGap"),
    expectedSeq: Schema.Number,
    receivedSeq: Schema.Number,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ClosedConflict"),
    finalOffset: Offset,
  }),
  Schema.Struct({ _tag: Schema.Literal("ContentTypeMismatch") }),
  Schema.Struct({ _tag: Schema.Literal("StreamSeqRegression") }),
)
export type AppendDecision = typeof AppendDecision.Type

/** Append result plus the tail-advance fact consumed by wake evaluation. */
export const AppendResult = Schema.Struct({
  append: AppendDecision,
  tailAdvanced: Schema.OptionFromSelf(TailAdvanced),
})
export type AppendResult = typeof AppendResult.Type
