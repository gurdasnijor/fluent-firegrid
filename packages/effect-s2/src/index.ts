export { conditionalAppend, type DecodedRecord, guardedAppend, publish, readDecoded } from "./Channel.ts"
export type { S2Record, S2RecordBytes } from "./internal/record.ts"
export {
  AppendInput,
  AppendRecord,
  BatchTransform,
  FencingTokenMismatchError,
  MAX_APPEND_BYTES,
  MAX_APPEND_RECORDS,
  meteredBytes,
  Producer,
  randomToken,
  RangeNotSatisfiableError,
  S2Environment,
  SdkS2Error,
  SeqNumMismatchError,
  utf8ByteLength
} from "./internal/sdk.ts"
export type * from "./internal/sdk.ts"
export {
  type AppendOptions,
  type AppendSessionConfig,
  layer,
  type ProducerConfig,
  type S2AppendSession,
  S2Client,
  type S2ClientApi,
  type S2OperationOptions,
  type S2Producer,
  type S2ProducerAck
} from "./S2Client.ts"
export {
  conflict,
  fromUnknown,
  type S2ClientError,
  S2Conflict,
  S2Error,
  S2NotFound,
  S2RangeNotSatisfiable,
  S2Throttled
} from "./S2Error.ts"
