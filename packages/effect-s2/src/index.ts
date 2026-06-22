export {
  S2Client,
  layer,
  type AppendSessionConfig,
  type AppendOptions,
  type ProducerConfig,
  type S2AppendSession,
  type S2ClientApi,
  type S2OperationOptions,
  type S2Producer,
  type S2ProducerAck,
} from "./S2Client.ts"
export { conditionalAppend, type DecodedRecord, guardedAppend, publish, readDecoded } from "./Channel.ts"
export {
  S2Conflict,
  S2Error,
  S2NotFound,
  S2RangeNotSatisfiable,
  S2Throttled,
  conflict,
  fromUnknown,
  type S2ClientError,
} from "./S2Error.ts"
export {
  AppendInput,
  AppendRecord,
  BatchTransform,
  FencingTokenMismatchError,
  MAX_APPEND_BYTES,
  MAX_APPEND_RECORDS,
  meteredBytes,
  Producer,
  RangeNotSatisfiableError,
  randomToken,
  S2Environment,
  SdkS2Error,
  SeqNumMismatchError,
  utf8ByteLength,
} from "./internal/sdk.ts"
export type * from "./internal/sdk.ts"
export type { S2Record, S2RecordBytes } from "./internal/record.ts"
