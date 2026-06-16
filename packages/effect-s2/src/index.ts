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
} from "./S2Client.ts"
export { conditionalAppend, publish, readDecoded, type DecodedRecord } from "./Channel.ts"
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
  Producer,
  S2Environment,
  SdkS2Error,
  SeqNumMismatchError,
  FencingTokenMismatchError,
  RangeNotSatisfiableError,
} from "./internal/sdk.ts"
export type * from "./internal/sdk.ts"
export type { S2Record, S2RecordBytes } from "./internal/record.ts"
