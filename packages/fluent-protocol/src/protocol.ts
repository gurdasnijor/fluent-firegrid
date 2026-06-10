import { Data, Schema } from "effect"

export const ByteArray = Schema.Array(Schema.Number.pipe(Schema.int(), Schema.between(0, 255)))
export type ByteArray = typeof ByteArray.Type

export const WireStreamRecord = Schema.Struct({
  path: Schema.String,
  fromOffset: Schema.String,
  nextOffset: Schema.String,
  bytes: ByteArray,
  contentType: Schema.String,
  closed: Schema.Boolean,
})
export type WireStreamRecord = typeof WireStreamRecord.Type

export const DurableStreamsCommand = Schema.Union(
  Schema.Struct({
    _tag: Schema.Literal("CreateStream"),
    path: Schema.String,
    contentType: Schema.String,
    closed: Schema.optional(Schema.Boolean),
  }),
  Schema.Struct({
    _tag: Schema.Literal("AppendToStream"),
    path: Schema.String,
    contentType: Schema.String,
    bytes: ByteArray,
    expectedTailOffset: Schema.optional(Schema.String),
    close: Schema.optional(Schema.Boolean),
  }),
  Schema.Struct({
    _tag: Schema.Literal("ReadStream"),
    path: Schema.String,
    offset: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("HeadStream"),
    path: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("DeleteStream"),
    path: Schema.String,
  }),
)
export type DurableStreamsCommand = typeof DurableStreamsCommand.Type

export const DurableStreamsResponse = Schema.Union(
  Schema.Struct({
    _tag: Schema.Literal("Created"),
    tailOffset: Schema.String,
    closed: Schema.Boolean,
    contentType: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("AlreadyExists"),
    tailOffset: Schema.String,
    closed: Schema.Boolean,
    contentType: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Appended"),
    tailOffset: Schema.String,
    closed: Schema.Boolean,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Noop"),
    tailOffset: Schema.String,
    closed: Schema.Boolean,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ReadResult"),
    records: Schema.Array(WireStreamRecord),
  }),
  Schema.Struct({
    _tag: Schema.Literal("HeadResult"),
    tailOffset: Schema.String,
    closed: Schema.Boolean,
    contentType: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Deleted"),
  }),
  Schema.Struct({
    _tag: Schema.Literal("NotFound"),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Failure"),
    reason: Schema.String,
    message: Schema.String,
  }),
)
export type DurableStreamsResponse = typeof DurableStreamsResponse.Type

export const ProtocolEnvelope = Schema.Union(
  Schema.Struct({
    kind: Schema.Literal("command"),
    id: Schema.String,
    command: DurableStreamsCommand,
  }),
  Schema.Struct({
    kind: Schema.Literal("response"),
    id: Schema.String,
    response: DurableStreamsResponse,
  }),
)
export type ProtocolEnvelope = typeof ProtocolEnvelope.Type

export class ProtocolValidationError extends Data.TaggedError("ProtocolValidationError")<
  Readonly<{
    readonly message: string
    readonly rawData: unknown
    readonly cause?: unknown
  }>
> {}

export class ProtocolCodecError extends Data.TaggedError("ProtocolCodecError")<
  Readonly<{
    readonly message: string
    readonly cause?: unknown
  }>
> {}
