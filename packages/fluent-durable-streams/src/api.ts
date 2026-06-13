import { Schema } from "effect"
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "effect/unstable/httpapi"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const StreamPosition = Schema.Struct({
  seqNum: NonNegativeInt,
  timestamp: Schema.String,
})
export type StreamPosition = typeof StreamPosition.Type

export const TailResponse = Schema.Struct({
  tail: StreamPosition,
})
export type TailResponse = typeof TailResponse.Type

export const AppendAck = Schema.Struct({
  start: StreamPosition,
  end: StreamPosition,
  tail: StreamPosition,
})
export type AppendAck = typeof AppendAck.Type

export const RecordKind = Schema.Literals(["data", "state", "close", "meta"])
export type RecordKind = typeof RecordKind.Type

export const HeaderPair = Schema.Tuple([Schema.String, Schema.String])
export type HeaderPair = typeof HeaderPair.Type

export const AppendRecordInput = Schema.Struct({
  body: Schema.String,
  headers: Schema.optional(Schema.Array(HeaderPair)),
})
export type AppendRecordInput = typeof AppendRecordInput.Type

export const AppendConditions = Schema.Struct({
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
})
export type AppendConditions = typeof AppendConditions.Type

export const AppendPayload = Schema.Struct({
  records: Schema.Array(AppendRecordInput).check(Schema.isMinLength(1)),
  ...AppendConditions.fields,
})
export type AppendPayload = typeof AppendPayload.Type

export const AppendConditionsQuery = AppendConditions
export type AppendConditionsQuery = typeof AppendConditionsQuery.Type

export const ReadQuery = Schema.Struct({
  seqNum: Schema.optional(NonNegativeInt),
  tailOffset: Schema.optional(NonNegativeInt),
  count: Schema.optional(PositiveInt),
  waitSecs: Schema.optional(NonNegativeInt),
  ignoreCommandRecords: Schema.optional(Schema.Boolean),
})
export type ReadQuery = typeof ReadQuery.Type

export const ReadRecord = Schema.Struct({
  seqNum: NonNegativeInt,
  body: Schema.String,
  headers: Schema.Array(HeaderPair),
  kind: RecordKind,
  timestamp: Schema.String,
})
export type ReadRecord = typeof ReadRecord.Type

export const ReadBatch = Schema.Struct({
  records: Schema.Array(ReadRecord),
  tail: Schema.optional(StreamPosition),
  closed: Schema.optional(Schema.Boolean),
})
export type ReadBatch = typeof ReadBatch.Type

export const EnsureStreamResponse = Schema.Struct({
  result: Schema.Literals(["created", "updated", "noop"]),
  stream: Schema.String,
})
export type EnsureStreamResponse = typeof EnsureStreamResponse.Type

export const ReadSseStream = Schema.String.pipe(
  HttpApiSchema.asText({ contentType: "text/event-stream" }),
)

export const RawByteStream = Schema.Uint8Array.pipe(
  HttpApiSchema.asUint8Array({ contentType: "application/octet-stream" }),
)

export const StateOperation = Schema.Literals(["insert", "update", "delete"])
export type StateOperation = typeof StateOperation.Type

export const StateControlKind = Schema.Literals(["snapshot-start", "snapshot-end", "reset"])
export type StateControlKind = typeof StateControlKind.Type

export const StateChangeHeaders = Schema.Struct({
  operation: StateOperation,
  txid: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.String),
  eventId: Schema.optional(Schema.String),
  schema: Schema.optional(Schema.String),
})
export type StateChangeHeaders = typeof StateChangeHeaders.Type

export const StateControlHeaders = Schema.Struct({
  control: StateControlKind,
  txid: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.String),
  seqNum: Schema.optional(Schema.String),
  schema: Schema.optional(Schema.String),
})
export type StateControlHeaders = typeof StateControlHeaders.Type

export const StateChange = Schema.Struct({
  type: Schema.NonEmptyString,
  key: Schema.NonEmptyString,
  value: Schema.optional(Schema.Json),
  old_value: Schema.optional(Schema.Json),
  headers: StateChangeHeaders,
})
export type StateChange = typeof StateChange.Type

export const StateControl = Schema.Struct({
  headers: StateControlHeaders,
})
export type StateControl = typeof StateControl.Type

export const StateMessage = Schema.Union([StateChange, StateControl])
export type StateMessage = typeof StateMessage.Type

export const StateAppendPayload = Schema.Struct({
  records: Schema.Array(StateMessage).check(Schema.isMinLength(1)),
  ...AppendConditions.fields,
})
export type StateAppendPayload = typeof StateAppendPayload.Type

export const StateRecord = Schema.Struct({
  seqNum: NonNegativeInt,
  timestamp: Schema.String,
  message: StateMessage,
})
export type StateRecord = typeof StateRecord.Type

export const StateReadBatch = Schema.Struct({
  records: Schema.Array(StateRecord),
  tail: Schema.optional(StreamPosition),
  closed: Schema.optional(Schema.Boolean),
})
export type StateReadBatch = typeof StateReadBatch.Type

const ApiErrorFields = {
  message: Schema.String,
  code: Schema.optional(Schema.String),
}

export class BadRequestError extends Schema.TaggedErrorClass<BadRequestError>()(
  "BadRequestError",
  ApiErrorFields,
) {}

export class ForbiddenError extends Schema.TaggedErrorClass<ForbiddenError>()(
  "ForbiddenError",
  ApiErrorFields,
) {}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()(
  "NotFoundError",
  ApiErrorFields,
) {}

export class TimeoutError extends Schema.TaggedErrorClass<TimeoutError>()(
  "TimeoutError",
  ApiErrorFields,
) {}

export class ConflictError extends Schema.TaggedErrorClass<ConflictError>()(
  "ConflictError",
  ApiErrorFields,
) {}

export class AppendConditionFailed extends Schema.TaggedErrorClass<AppendConditionFailed>()(
  "AppendConditionFailed",
  {
    ...ApiErrorFields,
    reason: Schema.optional(Schema.Literals(["seq_num_mismatch", "fencing_token_mismatch"])),
  },
) {}

export class RangeNotSatisfiableError extends Schema.TaggedErrorClass<RangeNotSatisfiableError>()(
  "RangeNotSatisfiableError",
  ApiErrorFields,
) {}

export class StateRecordError extends Schema.TaggedErrorClass<StateRecordError>()(
  "StateRecordError",
  ApiErrorFields,
) {}

export class UpstreamError extends Schema.TaggedErrorClass<UpstreamError>()(
  "UpstreamError",
  ApiErrorFields,
) {}

export type ApiError =
  | BadRequestError
  | ForbiddenError
  | NotFoundError
  | TimeoutError
  | ConflictError
  | AppendConditionFailed
  | RangeNotSatisfiableError
  | StateRecordError
  | UpstreamError

const ApiErrorResponse = [
  BadRequestError.pipe(HttpApiSchema.status(400)),
  ForbiddenError.pipe(HttpApiSchema.status(403)),
  NotFoundError.pipe(HttpApiSchema.status(404)),
  TimeoutError.pipe(HttpApiSchema.status(408)),
  ConflictError.pipe(HttpApiSchema.status(409)),
  AppendConditionFailed.pipe(HttpApiSchema.status(412)),
  RangeNotSatisfiableError.pipe(HttpApiSchema.status(416)),
  StateRecordError.pipe(HttpApiSchema.status(422)),
  UpstreamError.pipe(HttpApiSchema.status(502)),
]

export const StreamsGroup = HttpApiGroup.make("Streams")
  .add(
    HttpApiEndpoint.put("ensureStream", "/streams/:stream", {
      params: { stream: Schema.String },
      success: EnsureStreamResponse,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("checkTail", "/streams/:stream/records/tail", {
      params: { stream: Schema.String },
      success: TailResponse,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.post("append", "/streams/:stream/records", {
      params: { stream: Schema.String },
      payload: AppendPayload,
      success: AppendAck,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.post("appendRaw", "/streams/:stream/records/raw", {
      params: { stream: Schema.String },
      query: AppendConditionsQuery,
      payload: RawByteStream,
      success: AppendAck,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.post("close", "/streams/:stream/close", {
      params: { stream: Schema.String },
      query: AppendConditionsQuery,
      success: AppendAck,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("read", "/streams/:stream/records", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: ReadBatch,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("readRaw", "/streams/:stream/records/raw", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: RawByteStream,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("readLive", "/streams/:stream/records/live", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: ReadSseStream,
      error: ApiErrorResponse,
    }),
  )

export const StateGroup = HttpApiGroup.make("State")
  .add(
    HttpApiEndpoint.post("appendState", "/state/:stream/records", {
      params: { stream: Schema.String },
      payload: StateAppendPayload,
      success: AppendAck,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("readState", "/state/:stream/records", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: StateReadBatch,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("readStateLive", "/state/:stream/records/live", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: ReadSseStream,
      error: ApiErrorResponse,
    }),
  )

export const DurableStreamsApi = HttpApi.make("DurableStreams")
  .add(StreamsGroup)
  .add(StateGroup)
