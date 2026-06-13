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

export const AppendAck = Schema.Struct({
  start: StreamPosition,
  end: StreamPosition,
  tail: StreamPosition,
})
export type AppendAck = typeof AppendAck.Type

export const HeaderPair = Schema.Tuple([Schema.String, Schema.String])
export type HeaderPair = typeof HeaderPair.Type

export const AppendRecordInput = Schema.Struct({
  bodyBase64: Schema.String,
  headers: Schema.optional(Schema.Array(HeaderPair)),
})
export type AppendRecordInput = typeof AppendRecordInput.Type

export const AppendPayload = Schema.Struct({
  records: Schema.Array(AppendRecordInput).check(Schema.isMinLength(1)),
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
})
export type AppendPayload = typeof AppendPayload.Type

export const AppendRawQuery = Schema.Struct({
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
})
export type AppendRawQuery = typeof AppendRawQuery.Type

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
  bodyBase64: Schema.String,
  headers: Schema.Array(HeaderPair),
  timestamp: Schema.String,
})
export type ReadRecord = typeof ReadRecord.Type

export const ReadBatch = Schema.Struct({
  records: Schema.Array(ReadRecord),
  tail: Schema.optional(StreamPosition),
})
export type ReadBatch = typeof ReadBatch.Type

export const EnsureStreamResponse = Schema.Struct({
  result: Schema.Literals(["created", "updated", "noop"]),
  stream: Schema.String,
})
export type EnsureStreamResponse = typeof EnsureStreamResponse.Type

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
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
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
})
export type StateReadBatch = typeof StateReadBatch.Type

export class ApiError extends Schema.TaggedErrorClass<ApiError>()("ApiError", {
  status: Schema.Int,
  message: Schema.String,
  code: Schema.optional(Schema.String),
}) {}

const ApiErrorResponse = ApiError.pipe(HttpApiSchema.status(502))

export const StreamsGroup = HttpApiGroup.make("Streams")
  .add(
    HttpApiEndpoint.put("ensureStream", "/streams/:stream", {
      params: { stream: Schema.String },
      success: EnsureStreamResponse,
      error: ApiErrorResponse,
    }),
  )
  .add(
    HttpApiEndpoint.get("checkTail", "/streams/:stream/tail", {
      params: { stream: Schema.String },
      success: StreamPosition,
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
      query: AppendRawQuery,
      payload: Schema.Uint8Array.pipe(HttpApiSchema.asUint8Array()),
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

export const DurableStreamsApi = HttpApi.make("DurableStreams")
  .add(StreamsGroup)
  .add(StateGroup)
