import { Schema } from "effect"
import { BeginningOffset, NowOffset, Offset, StreamPath } from "@firegrid/fluent-store"

const SequenceNumber = Schema.Number.pipe(
  Schema.int(),
  Schema.between(0, Number.MAX_SAFE_INTEGER),
)

export class ProducerFence extends Schema.Class<ProducerFence>("ProducerFence")({
  producerId: Schema.NonEmptyString,
  epoch: SequenceNumber,
  seq: SequenceNumber,
}) {}

export const ReadOffsetSchema = Schema.Union(Offset, Schema.Literal(BeginningOffset, NowOffset))
export type ReadOffsetSchema = typeof ReadOffsetSchema.Type

export class Append extends Schema.TaggedClass<Append>("Append")("Append", {
  path: StreamPath,
  contentType: Schema.String,
  bytes: Schema.Uint8ArrayFromSelf,
  close: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  producer: Schema.optional(ProducerFence),
  expectedTailOffset: Schema.optional(Offset),
}) {}

export class Create extends Schema.TaggedClass<Create>("Create")("Create", {
  path: StreamPath,
  contentType: Schema.String,
  closed: Schema.optionalWith(Schema.Boolean, { default: () => false }),
}) {}

export class Read extends Schema.TaggedClass<Read>("Read")("Read", {
  path: StreamPath,
  offset: ReadOffsetSchema,
}) {}

export class Close extends Schema.TaggedClass<Close>("Close")("Close", {
  path: StreamPath,
  producer: Schema.optional(ProducerFence),
}) {}

export class Head extends Schema.TaggedClass<Head>("Head")("Head", {
  path: StreamPath,
}) {}

export class Delete extends Schema.TaggedClass<Delete>("Delete")("Delete", {
  path: StreamPath,
}) {}

export const Request = Schema.Union(Append, Create, Read, Close, Head, Delete)
export type Request = typeof Request.Type

export class ReadLive extends Schema.TaggedClass<ReadLive>("ReadLive")("ReadLive", {
  path: StreamPath,
  offset: ReadOffsetSchema,
}) {}
