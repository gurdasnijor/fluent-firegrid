import { Schema } from "effect"
import { Offset, StreamPath } from "@firegrid/fluent-stream-log"
import { SequenceNumber } from "./schemaShared.ts"

export class WireRecord extends Schema.Class<WireRecord>("WireRecord")({
  path: StreamPath,
  fromOffset: Offset,
  nextOffset: Offset,
  bytes: Schema.Uint8Array,
  contentType: Schema.String,
  closed: Schema.Boolean,
}) {}

export class Appended extends Schema.TaggedClass<Appended>("Appended")("Appended", {
  nextOffset: Offset,
  closed: Schema.Boolean,
}) {}

export class AppendDuplicate extends Schema.TaggedClass<AppendDuplicate>("AppendDuplicate")("AppendDuplicate", {
  nextOffset: Offset,
  closed: Schema.Boolean,
}) {}

export class EpochFenced extends Schema.TaggedClass<EpochFenced>("EpochFenced")("EpochFenced", {
  currentEpoch: SequenceNumber,
}) {}

export class SequenceGap extends Schema.TaggedClass<SequenceGap>("SequenceGap")("SequenceGap", {
  expectedSeq: SequenceNumber,
  receivedSeq: SequenceNumber,
}) {}

export class WriteToClosed extends Schema.TaggedClass<WriteToClosed>("WriteToClosed")("WriteToClosed", {
  finalOffset: Offset,
}) {}

export class Closed extends Schema.TaggedClass<Closed>("Closed")("Closed", {
  finalOffset: Offset,
}) {}

export class ContentMismatch extends Schema.TaggedClass<ContentMismatch>("ContentMismatch")("ContentMismatch", {
  code: Schema.Literal("content-mismatch"),
  expected: Schema.String,
  actual: Schema.String,
}) {}

export class StreamNotFound extends Schema.TaggedClass<StreamNotFound>("StreamNotFound")("StreamNotFound", {}) {}

export class StreamGone extends Schema.TaggedClass<StreamGone>("StreamGone")("StreamGone", {}) {}

export class Created extends Schema.TaggedClass<Created>("Created")("Created", {
  tailOffset: Offset,
  closed: Schema.Boolean,
  contentType: Schema.String,
}) {}

export class AlreadyExists extends Schema.TaggedClass<AlreadyExists>("AlreadyExists")("AlreadyExists", {
  tailOffset: Offset,
  closed: Schema.Boolean,
  contentType: Schema.String,
}) {}

export class CreateConflict extends Schema.TaggedClass<CreateConflict>("CreateConflict")("CreateConflict", {
  code: Schema.Literal("create-conflict"),
  reason: Schema.Literals(["config-mismatch", "closure-mismatch"]),
}) {}

export const CreateResponse = Schema.Union([Created, AlreadyExists, CreateConflict, StreamGone])
export type CreateResponse = typeof CreateResponse.Type

export class OffsetConflict extends Schema.TaggedClass<OffsetConflict>("OffsetConflict")("OffsetConflict", {
  code: Schema.Literal("offset-conflict"),
  expectedTailOffset: Offset,
  actualTailOffset: Offset,
}) {}

export const AppendResponse = Schema.Union([
  Appended,
  AppendDuplicate,
  EpochFenced,
  SequenceGap,
  WriteToClosed,
  ContentMismatch,
  OffsetConflict,
  StreamNotFound,
  StreamGone,
])
export type AppendResponse = typeof AppendResponse.Type

export const CloseResponse = Schema.Union([
  Appended,
  AppendDuplicate,
  Closed,
  EpochFenced,
  SequenceGap,
  ContentMismatch,
  OffsetConflict,
  StreamNotFound,
  StreamGone,
])
export type CloseResponse = typeof CloseResponse.Type

export class ReadResult extends Schema.TaggedClass<ReadResult>("ReadResult")("ReadResult", {
  records: Schema.Array(WireRecord),
  nextOffset: Offset,
  upToDate: Schema.Boolean,
  closed: Schema.Boolean,
}) {}

export class InvalidOffset extends Schema.TaggedClass<InvalidOffset>("InvalidOffset")("InvalidOffset", {
  offset: Schema.String,
}) {}

export const ReadResponse = Schema.Union([ReadResult, InvalidOffset, StreamNotFound, StreamGone])
export type ReadResponse = typeof ReadResponse.Type

export class HeadResult extends Schema.TaggedClass<HeadResult>("HeadResult")("HeadResult", {
  tailOffset: Offset,
  closed: Schema.Boolean,
  contentType: Schema.String,
}) {}
export const HeadResponse = Schema.Union([HeadResult, StreamNotFound, StreamGone])
export type HeadResponse = typeof HeadResponse.Type

export class Deleted extends Schema.TaggedClass<Deleted>("Deleted")("Deleted", {}) {}

export const DeleteResponse = Schema.Union([Deleted, StreamNotFound, StreamGone])
export type DeleteResponse = typeof DeleteResponse.Type

export const Response = Schema.Union([
  Appended,
  AppendDuplicate,
  Closed,
  EpochFenced,
  SequenceGap,
  WriteToClosed,
  ContentMismatch,
  StreamNotFound,
  StreamGone,
  Created,
  AlreadyExists,
  CreateConflict,
  OffsetConflict,
  ReadResult,
  InvalidOffset,
  HeadResult,
  Deleted,
])
export type Response = typeof Response.Type
