/**
 * effect-durable-client
 *
 * Effect-native client for the Durable Streams Protocol. Reads are Stream,
 * writes are Sink, schema sits at the wire boundary.
 *
 * Provide `FetchHttpClient.layer` (or any `HttpClient` layer) at the top of
 * your program — every operation requires `HttpClient.HttpClient` in `R`.
 */

export * as DurableStream from "./namespace.ts"
export * as CEL from "./CEL.ts"
export {
  DurableStreamClient,
  ReadFrom,
  layer as DurableStreamClientLayer,
  layerFetch as DurableStreamClientLayerFetch,
} from "./Client.ts"
export {
  filteredPullWakeConfig,
  makeSubscriptionClient,
} from "./Subscription.ts"
export type { CelExpression, CelPath } from "./CEL.ts"
export type {
  DurableStreamClientService,
  DurableStreamHandle,
  RawAppendOptions,
  RawBatch,
  RawStreamOptions,
  RawStreamSession,
  ReadEvent,
  ReadFrom as ReadFromInput,
  ReadonlyDurableStreamHandle,
  StreamHandleOptions,
  TypedClient,
} from "./Client.ts"
export type {
  FilteredPullWakeOptions,
  PullWakeSubscriptionConfig,
  SubscriptionClient,
  SubscriptionFilter,
} from "./Subscription.ts"
export type {
  Bound,
  CloseOptions,
  CreateOptions,
  Endpoint,
  ErrorHandler,
  HeadResult,
  HeadersRecord,
  HeaderValue,
  LiveMode,
  Offset,
  ParamsRecord,
  Producer,
  ProducerAppendOpts,
  ProducerAppendResult,
  ProducerFailure,
  ProducerOptions,
  ReadError,
  ReadOpts,
  RetryOpts,
  SnapshotResult,
  WriteError,
} from "./DurableStream.ts"
export {
  Conflict,
  AlreadyClaimed,
  ConfigConflict,
  DecodeError,
  Fenced,
  Gone,
  NotFound,
  SequenceGap,
  StaleEpoch,
  StreamClosed,
  TransportError,
} from "./errors.ts"
