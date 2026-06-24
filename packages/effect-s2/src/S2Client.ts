import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import type * as Scope from "effect/Scope"
import * as Sink from "effect/Sink"
import * as Stream from "effect/Stream"
import type { S2Record, S2RecordBytes } from "./internal/record.ts"
import { toS2Record, toS2RecordBytes } from "./internal/record.ts"
import {
  type AccessTokenInfo,
  type AccountMetricsInput,
  type AppendAck,
  type AppendSessionOptions,
  type BasinConfig,
  type BasinInfo,
  type BasinMetricsInput,
  BatchTransform,
  type BatchTransformOptions,
  type CreateBasinInput,
  type CreateBasinResponse,
  type CreateStreamInput,
  type CreateStreamResponse,
  type DeleteBasinInput,
  type DeleteStreamInput,
  type EnsureBasinInput,
  type EnsureBasinResponse,
  type EnsureStreamInput,
  type EnsureStreamResponse,
  type GetBasinConfigInput,
  type GetStreamConfigInput,
  type IssueAccessTokenInput,
  type IssueAccessTokenResponse,
  type ListAccessTokensInput,
  type ListAccessTokensResponse,
  type ListAllAccessTokensInput,
  type ListAllBasinsInput,
  type ListAllStreamsInput,
  type ListBasinsInput,
  type ListBasinsResponse,
  type ListLocationsResponse,
  type ListStreamsInput,
  type ListStreamsResponse,
  type LocationInfo,
  type MetricSetResponse,
  Producer,
  type ReadBatch,
  type ReadOptions,
  type ReconfigureBasinInput,
  type ReconfigureBasinResponse,
  type ReconfigureStreamInput,
  type ReconfigureStreamResponse,
  type RevokeAccessTokenInput,
  S2,
  type S2EndpointsInit,
  S2Environment,
  type S2RequestOptions,
  type S2RetryConfig,
  type SdkAppendInput,
  type SdkAppendRecord,
  type SetDefaultLocationInput,
  type SetDefaultLocationResponse,
  type StreamConfig,
  type StreamInfo,
  type StreamMetricsInput,
  type StreamOptions,
  type Tail
} from "./internal/sdk.ts"
import { fromUnknown, type S2ClientError } from "./S2Error.ts"

export interface AppendOptions {
  readonly matchSeqNum?: number
  readonly fencingToken?: string
}

export interface ProducerConfig {
  readonly lingerDurationMillis?: number
  readonly maxBatchRecords?: number
  readonly maxBatchBytes?: number
  readonly maxInflightBytes?: number
  readonly maxInflightBatches?: number
  /** Fencing token enforced on every batch (static across batches). */
  readonly fencingToken?: string
  /** Expected seq-num for the first batch; the producer auto-increments it per batch. */
  readonly matchSeqNum?: number
}

/** A producer's per-record acknowledgement: the record's own `seqNum` plus its enclosing batch ack. */
export interface S2ProducerAck {
  readonly seqNum: number
  readonly batch: AppendAck
}

export interface S2Producer {
  readonly submit: (record: SdkAppendRecord) => Effect.Effect<S2ProducerAck, S2ClientError>
}

export interface AppendSessionConfig {
  readonly maxInflightBytes?: number
  readonly maxInflightBatches?: number
}

export interface S2AppendSession {
  readonly submit: (input: SdkAppendInput) => Effect.Effect<AppendAck, S2ClientError>
}

export interface S2OperationOptions {
  readonly basinName?: string
  readonly request?: S2RequestOptions
  readonly stream?: StreamOptions
}

export interface S2ClientApi {
  readonly listBasins: (
    args?: ListBasinsInput,
    options?: S2RequestOptions
  ) => Effect.Effect<ListBasinsResponse, S2ClientError>
  readonly listAllBasins: (
    args?: ListAllBasinsInput,
    options?: S2RequestOptions
  ) => Stream.Stream<BasinInfo, S2ClientError>
  readonly createBasin: (
    args: CreateBasinInput,
    options?: S2RequestOptions
  ) => Effect.Effect<CreateBasinResponse, S2ClientError>
  readonly getBasinConfig: (
    args: GetBasinConfigInput,
    options?: S2RequestOptions
  ) => Effect.Effect<BasinConfig, S2ClientError>
  readonly deleteBasin: (
    args: DeleteBasinInput,
    options?: S2RequestOptions
  ) => Effect.Effect<void, S2ClientError>
  readonly ensureBasin: (
    args: EnsureBasinInput,
    options?: S2RequestOptions
  ) => Effect.Effect<EnsureBasinResponse, S2ClientError>
  readonly reconfigureBasin: (
    args: ReconfigureBasinInput,
    options?: S2RequestOptions
  ) => Effect.Effect<ReconfigureBasinResponse, S2ClientError>
  readonly listAccessTokens: (
    args?: ListAccessTokensInput,
    options?: S2RequestOptions
  ) => Effect.Effect<ListAccessTokensResponse, S2ClientError>
  readonly listAllAccessTokens: (
    args?: ListAllAccessTokensInput,
    options?: S2RequestOptions
  ) => Stream.Stream<AccessTokenInfo, S2ClientError>
  readonly issueAccessToken: (
    args: IssueAccessTokenInput,
    options?: S2RequestOptions
  ) => Effect.Effect<IssueAccessTokenResponse, S2ClientError>
  readonly revokeAccessToken: (
    args: RevokeAccessTokenInput,
    options?: S2RequestOptions
  ) => Effect.Effect<void, S2ClientError>
  readonly listLocations: (options?: S2RequestOptions) => Effect.Effect<ListLocationsResponse, S2ClientError>
  readonly getDefaultLocation: (options?: S2RequestOptions) => Effect.Effect<LocationInfo, S2ClientError>
  readonly setDefaultLocation: (
    args: SetDefaultLocationInput,
    options?: S2RequestOptions
  ) => Effect.Effect<SetDefaultLocationResponse, S2ClientError>
  readonly accountMetrics: (
    args: AccountMetricsInput,
    options?: S2RequestOptions
  ) => Effect.Effect<MetricSetResponse, S2ClientError>
  readonly basinMetrics: (
    args: BasinMetricsInput,
    options?: S2RequestOptions
  ) => Effect.Effect<MetricSetResponse, S2ClientError>
  readonly streamMetrics: (
    args: StreamMetricsInput,
    options?: S2RequestOptions
  ) => Effect.Effect<MetricSetResponse, S2ClientError>
  readonly listStreams: (
    args?: ListStreamsInput,
    options?: S2OperationOptions
  ) => Effect.Effect<ListStreamsResponse, S2ClientError>
  readonly listAllStreams: (
    args?: ListAllStreamsInput,
    options?: S2OperationOptions
  ) => Stream.Stream<StreamInfo, S2ClientError>
  readonly createStream: (
    args: CreateStreamInput,
    options?: S2OperationOptions
  ) => Effect.Effect<CreateStreamResponse, S2ClientError>
  readonly getStreamConfig: (
    args: GetStreamConfigInput,
    options?: S2OperationOptions
  ) => Effect.Effect<StreamConfig, S2ClientError>
  readonly deleteStream: (
    args: DeleteStreamInput,
    options?: S2OperationOptions
  ) => Effect.Effect<void, S2ClientError>
  readonly ensureStream: (
    args: EnsureStreamInput,
    options?: S2OperationOptions
  ) => Effect.Effect<EnsureStreamResponse, S2ClientError>
  readonly reconfigureStream: (
    args: ReconfigureStreamInput,
    options?: S2OperationOptions
  ) => Effect.Effect<ReconfigureStreamResponse, S2ClientError>
  readonly checkTail: (
    name: string,
    options?: S2OperationOptions
  ) => Effect.Effect<Tail, S2ClientError>
  readonly append: (
    name: string,
    input: SdkAppendInput,
    operationOptions?: S2OperationOptions
  ) => Effect.Effect<AppendAck, S2ClientError>
  readonly readBatch: (
    name: string,
    options?: ReadOptions,
    operationOptions?: S2OperationOptions
  ) => Effect.Effect<ReadBatch<"string">, S2ClientError>
  readonly readBatchBytes: (
    name: string,
    options?: ReadOptions,
    operationOptions?: S2OperationOptions
  ) => Effect.Effect<ReadBatch<"bytes">, S2ClientError>
  readonly read: (
    name: string,
    options: ReadOptions,
    operationOptions?: S2OperationOptions
  ) => Stream.Stream<S2Record, S2ClientError>
  readonly readBytes: (
    name: string,
    options: ReadOptions,
    operationOptions?: S2OperationOptions
  ) => Stream.Stream<S2RecordBytes, S2ClientError>
  readonly appendSession: (
    name: string,
    config?: AppendSessionConfig,
    options?: S2OperationOptions
  ) => Effect.Effect<S2AppendSession, S2ClientError, Scope.Scope>
  readonly producer: (
    name: string,
    config?: ProducerConfig,
    options?: S2OperationOptions
  ) => Effect.Effect<S2Producer, S2ClientError, Scope.Scope>
}

export class S2Client extends Context.Service<S2Client, S2ClientApi>()("effect-s2/S2Client") {
  static readonly listBasins = (
    args?: ListBasinsInput,
    options?: S2RequestOptions
  ): Effect.Effect<ListBasinsResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.listBasins(args, options))

  static readonly listAllBasins = (
    args?: ListAllBasinsInput,
    options?: S2RequestOptions
  ): Stream.Stream<BasinInfo, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.listAllBasins(args, options)))

  static readonly createBasin = (
    args: CreateBasinInput,
    options?: S2RequestOptions
  ): Effect.Effect<CreateBasinResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.createBasin(args, options))

  static readonly getBasinConfig = (
    args: GetBasinConfigInput,
    options?: S2RequestOptions
  ): Effect.Effect<BasinConfig, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.getBasinConfig(args, options))

  static readonly deleteBasin = (
    args: DeleteBasinInput,
    options?: S2RequestOptions
  ): Effect.Effect<void, S2ClientError, S2Client> => Effect.flatMap(this, (client) => client.deleteBasin(args, options))

  static readonly ensureBasin = (
    args: EnsureBasinInput,
    options?: S2RequestOptions
  ): Effect.Effect<EnsureBasinResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.ensureBasin(args, options))

  static readonly reconfigureBasin = (
    args: ReconfigureBasinInput,
    options?: S2RequestOptions
  ): Effect.Effect<ReconfigureBasinResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.reconfigureBasin(args, options))

  static readonly listAccessTokens = (
    args?: ListAccessTokensInput,
    options?: S2RequestOptions
  ): Effect.Effect<ListAccessTokensResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.listAccessTokens(args, options))

  static readonly listAllAccessTokens = (
    args?: ListAllAccessTokensInput,
    options?: S2RequestOptions
  ): Stream.Stream<AccessTokenInfo, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.listAllAccessTokens(args, options)))

  static readonly issueAccessToken = (
    args: IssueAccessTokenInput,
    options?: S2RequestOptions
  ): Effect.Effect<IssueAccessTokenResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.issueAccessToken(args, options))

  static readonly revokeAccessToken = (
    args: RevokeAccessTokenInput,
    options?: S2RequestOptions
  ): Effect.Effect<void, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.revokeAccessToken(args, options))

  static readonly listLocations = (
    options?: S2RequestOptions
  ): Effect.Effect<ListLocationsResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.listLocations(options))

  static readonly getDefaultLocation = (
    options?: S2RequestOptions
  ): Effect.Effect<LocationInfo, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.getDefaultLocation(options))

  static readonly setDefaultLocation = (
    args: SetDefaultLocationInput,
    options?: S2RequestOptions
  ): Effect.Effect<SetDefaultLocationResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.setDefaultLocation(args, options))

  static readonly accountMetrics = (
    args: AccountMetricsInput,
    options?: S2RequestOptions
  ): Effect.Effect<MetricSetResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.accountMetrics(args, options))

  static readonly basinMetrics = (
    args: BasinMetricsInput,
    options?: S2RequestOptions
  ): Effect.Effect<MetricSetResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.basinMetrics(args, options))

  static readonly streamMetrics = (
    args: StreamMetricsInput,
    options?: S2RequestOptions
  ): Effect.Effect<MetricSetResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.streamMetrics(args, options))

  static readonly listStreams = (
    args?: ListStreamsInput,
    options?: S2OperationOptions
  ): Effect.Effect<ListStreamsResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.listStreams(args, options))

  static readonly listAllStreams = (
    args?: ListAllStreamsInput,
    options?: S2OperationOptions
  ): Stream.Stream<StreamInfo, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.listAllStreams(args, options)))

  static readonly createStream = (
    args: CreateStreamInput,
    options?: S2OperationOptions
  ): Effect.Effect<CreateStreamResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.createStream(args, options))

  static readonly getStreamConfig = (
    args: GetStreamConfigInput,
    options?: S2OperationOptions
  ): Effect.Effect<StreamConfig, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.getStreamConfig(args, options))

  static readonly deleteStream = (
    args: DeleteStreamInput,
    options?: S2OperationOptions
  ): Effect.Effect<void, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.deleteStream(args, options))

  static readonly ensureStream = (
    args: EnsureStreamInput,
    options?: S2OperationOptions
  ): Effect.Effect<EnsureStreamResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.ensureStream(args, options))

  static readonly reconfigureStream = (
    args: ReconfigureStreamInput,
    options?: S2OperationOptions
  ): Effect.Effect<ReconfigureStreamResponse, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.reconfigureStream(args, options))

  static readonly checkTail = (
    name: string,
    options?: S2OperationOptions
  ): Effect.Effect<Tail, S2ClientError, S2Client> => Effect.flatMap(this, (client) => client.checkTail(name, options))

  static readonly append = (
    name: string,
    input: SdkAppendInput,
    operationOptions?: S2OperationOptions
  ): Effect.Effect<AppendAck, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.append(name, input, operationOptions))

  static readonly readBatch = (
    name: string,
    options: ReadOptions = {},
    operationOptions?: S2OperationOptions
  ): Effect.Effect<ReadBatch<"string">, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.readBatch(name, options, operationOptions))

  static readonly readBatchBytes = (
    name: string,
    options: ReadOptions = {},
    operationOptions?: S2OperationOptions
  ): Effect.Effect<ReadBatch<"bytes">, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.readBatchBytes(name, options, operationOptions))

  static readonly read = (
    name: string,
    options: ReadOptions,
    operationOptions?: S2OperationOptions
  ): Stream.Stream<S2Record, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.read(name, options, operationOptions)))

  static readonly readBytes = (
    name: string,
    options: ReadOptions,
    operationOptions?: S2OperationOptions
  ): Stream.Stream<S2RecordBytes, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.readBytes(name, options, operationOptions)))

  static readonly appendSession = (
    name: string,
    config: AppendSessionConfig = {},
    options?: S2OperationOptions
  ): Effect.Effect<S2AppendSession, S2ClientError, S2Client | Scope.Scope> =>
    Effect.flatMap(this, (client) => client.appendSession(name, config, options))

  static readonly producer = (
    name: string,
    config: ProducerConfig = {},
    options?: S2OperationOptions
  ): Effect.Effect<S2Producer, S2ClientError, S2Client | Scope.Scope> =>
    Effect.flatMap(this, (client) => client.producer(name, config, options))

  static readonly sink = (
    s2Producer: S2Producer
  ): Sink.Sink<void, SdkAppendRecord, never, S2ClientError> => Sink.forEach(s2Producer.submit)

  static get layerConfig(): Layer.Layer<S2Client, Config.ConfigError> {
    return layer({
      accessToken: Config.redacted("S2_ACCESS_TOKEN"),
      basinName: Config.string("S2_BASIN")
    })
  }

  static readonly layer = layer
}

const resolveOption = <A>(value: Config.Config<A> | A): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value)

const streamHandleOptions = (
  forceTransport: "fetch" | "s2s" | undefined,
  options?: StreamOptions
): StreamOptions => ({
  ...(forceTransport === undefined ? {} : { forceTransport }),
  ...(options === undefined ? {} : options)
})

const trySdk = <A>(
  operation: string,
  promise: () => Promise<A>
): Effect.Effect<A, S2ClientError> =>
  Effect.tryPromise({
    try: promise,
    catch: fromUnknown(operation)
  })

type TraceAttributeValue = string | number | boolean
type TraceAttributes = Record<string, TraceAttributeValue>

const encodeTraceArgs = (args: unknown): string | undefined => {
  if (args === undefined) return undefined
  try {
    return JSON.stringify(args)
  } catch {
    return typeof args === "string"
      ? args
      : typeof args === "number" || typeof args === "boolean" || typeof args === "bigint"
      ? args.toString()
      : Object.prototype.toString.call(args)
  }
}

const traceAttributes = (
  args?: unknown,
  extra?: TraceAttributes
): TraceAttributes => {
  const attributes: TraceAttributes = { ...extra }
  const encodedArgs = encodeTraceArgs(args)
  if (encodedArgs !== undefined) attributes.args = encodedArgs
  return attributes
}

const withSdkSpan = <A, E, R>(
  operation: string,
  attributes: TraceAttributes,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E, R> => effect.pipe(Effect.withSpan(`S2.${operation}`, { attributes }))

const tracedSdk = <A>(
  operation: string,
  args: unknown,
  promise: () => Promise<A>,
  extra?: TraceAttributes
): Effect.Effect<A, S2ClientError> => withSdkSpan(operation, traceAttributes(args, extra), trySdk(operation, promise))

const tracedSdkStream = <A>(
  operation: string,
  args: unknown,
  iterable: AsyncIterable<A>,
  extra?: TraceAttributes
): Stream.Stream<A, S2ClientError> =>
  Stream.fromAsyncIterable(iterable, fromUnknown(operation)).pipe(
    Stream.withSpan(`S2.${operation}`, { attributes: traceAttributes(args, extra) })
  )

const batchTransformOptions = (config: ProducerConfig): BatchTransformOptions => ({
  ...(config.lingerDurationMillis === undefined
    ? {}
    : { lingerDurationMillis: config.lingerDurationMillis }),
  ...(config.maxBatchRecords === undefined ? {} : { maxBatchRecords: config.maxBatchRecords }),
  ...(config.maxBatchBytes === undefined ? {} : { maxBatchBytes: config.maxBatchBytes }),
  ...(config.fencingToken === undefined ? {} : { fencingToken: config.fencingToken }),
  ...(config.matchSeqNum === undefined ? {} : { matchSeqNum: config.matchSeqNum })
})

const appendSessionOptions = (config: AppendSessionConfig): AppendSessionOptions => ({
  ...(config.maxInflightBytes === undefined ? {} : { maxInflightBytes: config.maxInflightBytes }),
  ...(config.maxInflightBatches === undefined
    ? {}
    : { maxInflightBatches: config.maxInflightBatches })
})

const liveOptions = (options: {
  readonly accessToken: Redacted.Redacted<string>
  readonly basinName: string
  readonly endpoints?: S2EndpointsInit
  readonly retry?: S2RetryConfig
  readonly forceTransport?: "fetch" | "s2s"
}) => ({
  accessToken: options.accessToken,
  basinName: options.basinName,
  ...optionalLiveOptions(options)
})

const optionalLiveOptions = (options: {
  readonly endpoints?: S2EndpointsInit
  readonly retry?: S2RetryConfig
  readonly forceTransport?: "fetch" | "s2s"
}) => ({
  ...(options.endpoints === undefined ? {} : { endpoints: options.endpoints }),
  ...(options.retry === undefined ? {} : { retry: options.retry }),
  ...(options.forceTransport === undefined ? {} : { forceTransport: options.forceTransport })
})

const makeLiveApi = (input: {
  readonly accessToken: Redacted.Redacted<string>
  readonly basinName: string
  readonly endpoints?: S2EndpointsInit
  readonly retry?: S2RetryConfig
  readonly forceTransport?: "fetch" | "s2s"
}): S2ClientApi => {
  const environment = S2Environment.parse()
  const retry: S2RetryConfig = {
    ...input.retry,
    appendRetryPolicy: input.retry?.appendRetryPolicy ?? "noSideEffects"
  }
  const client = new S2({
    ...environment,
    accessToken: Redacted.value(input.accessToken),
    ...(input.endpoints === undefined ? {} : { endpoints: input.endpoints }),
    retry
  })
  const basinHandle = (name?: string) => client.basin(name ?? input.basinName)
  const defaultBasinName = input.basinName
  const selectedStreamHandle = (name: string, options?: S2OperationOptions) =>
    basinHandle(options?.basinName).stream(
      name,
      streamHandleOptions(input.forceTransport, options?.stream)
    )
  type StreamReadRequestOptions = Parameters<ReturnType<typeof selectedStreamHandle>["read"]>[1]

  const listBasins = (args?: ListBasinsInput, options?: S2RequestOptions) =>
    tracedSdk("listBasins", args, () => client.basins.list(args, options))

  const listAllBasins = (
    args?: ListAllBasinsInput,
    options?: S2RequestOptions
  ): Stream.Stream<BasinInfo, S2ClientError> =>
    tracedSdkStream("listAllBasins", args, client.basins.listAll(args, options))

  const createBasin = (args: CreateBasinInput, options?: S2RequestOptions) =>
    tracedSdk("createBasin", args, () => client.basins.create(args, options))

  const getBasinConfig = (args: GetBasinConfigInput, options?: S2RequestOptions) =>
    tracedSdk("getBasinConfig", args, () => client.basins.getConfig(args, options))

  const deleteBasin = (args: DeleteBasinInput, options?: S2RequestOptions) =>
    tracedSdk("deleteBasin", args, () => client.basins.delete(args, options))

  const ensureBasin = (args: EnsureBasinInput, options?: S2RequestOptions) =>
    tracedSdk("ensureBasin", args, () => client.basins.ensure(args, options))

  const reconfigureBasin = (args: ReconfigureBasinInput, options?: S2RequestOptions) =>
    tracedSdk("reconfigureBasin", args, () => client.basins.reconfigure(args, options))

  const listAccessTokens = (args?: ListAccessTokensInput, options?: S2RequestOptions) =>
    tracedSdk("listAccessTokens", args, () => client.accessTokens.list(args, options))

  const listAllAccessTokens = (
    args?: ListAllAccessTokensInput,
    options?: S2RequestOptions
  ): Stream.Stream<AccessTokenInfo, S2ClientError> =>
    tracedSdkStream("listAllAccessTokens", args, client.accessTokens.listAll(args, options))

  const issueAccessToken = (args: IssueAccessTokenInput, options?: S2RequestOptions) =>
    tracedSdk("issueAccessToken", args, () => client.accessTokens.issue(args, options))

  const revokeAccessToken = (args: RevokeAccessTokenInput, options?: S2RequestOptions) =>
    tracedSdk("revokeAccessToken", args, () => client.accessTokens.revoke(args, options))

  const listLocations = (options?: S2RequestOptions) =>
    tracedSdk("listLocations", undefined, () => client.locations.list(options))

  const getDefaultLocation = (options?: S2RequestOptions) =>
    tracedSdk("getDefaultLocation", undefined, () => client.locations.getDefault(options))

  const setDefaultLocation = (args: SetDefaultLocationInput, options?: S2RequestOptions) =>
    tracedSdk("setDefaultLocation", args, () => client.locations.setDefault(args, options))

  const accountMetrics = (args: AccountMetricsInput, options?: S2RequestOptions) =>
    tracedSdk("accountMetrics", args, () => client.metrics.account(args, options))

  const basinMetrics = (args: BasinMetricsInput, options?: S2RequestOptions) =>
    tracedSdk("basinMetrics", args, () => client.metrics.basin(args, options))

  const streamMetrics = (args: StreamMetricsInput, options?: S2RequestOptions) =>
    tracedSdk("streamMetrics", args, () => client.metrics.stream(args, options))

  const listStreams = (args?: ListStreamsInput, options?: S2OperationOptions) =>
    tracedSdk("listStreams", args, () => basinHandle(options?.basinName).streams.list(args, options?.request), {
      basin: options?.basinName ?? defaultBasinName
    })

  const listAllStreams = (
    args?: ListAllStreamsInput,
    options?: S2OperationOptions
  ): Stream.Stream<StreamInfo, S2ClientError> =>
    tracedSdkStream(
      "listAllStreams",
      args,
      basinHandle(options?.basinName).streams.listAll(args, options?.request),
      { basin: options?.basinName ?? defaultBasinName, prefix: args?.prefix ?? "" }
    )

  const createStream = (
    args: CreateStreamInput,
    options?: S2OperationOptions
  ) =>
    tracedSdk("createStream", args, () => basinHandle(options?.basinName).streams.create(args, options?.request), {
      basin: options?.basinName ?? defaultBasinName,
      stream: args.stream
    })

  const getStreamConfig = (args: GetStreamConfigInput, options?: S2OperationOptions) =>
    tracedSdk(
      "getStreamConfig",
      args,
      () => basinHandle(options?.basinName).streams.getConfig(args, options?.request),
      { basin: options?.basinName ?? defaultBasinName, stream: args.stream }
    )

  const deleteStream = (
    args: DeleteStreamInput,
    options?: S2OperationOptions
  ) =>
    tracedSdk("deleteStream", args, () => basinHandle(options?.basinName).streams.delete(args, options?.request), {
      basin: options?.basinName ?? defaultBasinName,
      stream: args.stream
    })

  const ensureStream = (
    args: EnsureStreamInput,
    options?: S2OperationOptions
  ) =>
    tracedSdk("ensureStream", args, () => basinHandle(options?.basinName).streams.ensure(args, options?.request), {
      basin: options?.basinName ?? defaultBasinName,
      stream: args.stream
    })

  const reconfigureStream = (args: ReconfigureStreamInput, options?: S2OperationOptions) =>
    tracedSdk(
      "reconfigureStream",
      args,
      () => basinHandle(options?.basinName).streams.reconfigure(args, options?.request),
      { basin: options?.basinName ?? defaultBasinName, stream: args.stream }
    )

  const checkTail = (
    name: string,
    options?: S2OperationOptions
  ) =>
    tracedSdk("checkTail", { stream: name }, () => selectedStreamHandle(name, options).checkTail(options?.request), {
      basin: options?.basinName ?? defaultBasinName,
      stream: name
    })

  const append = (
    name: string,
    input: SdkAppendInput,
    operationOptions?: S2OperationOptions
  ) =>
    withSdkSpan(
      "append",
      traceAttributes(
        { stream: name, recordCount: input.records.length, matchSeqNum: input.matchSeqNum },
        { basin: operationOptions?.basinName ?? defaultBasinName, stream: name }
      ),
      Effect.gen(function*() {
        const ack = yield* trySdk("append", () =>
          selectedStreamHandle(name, operationOptions).append(input, operationOptions?.request))
        yield* Effect.annotateCurrentSpan({ seqNum: ack.start.seqNum })
        return ack
      })
    )

  const readBatchFrom = <Encoding extends "string" | "bytes">(
    operation: string,
    name: string,
    options: ReadOptions,
    operationOptions: S2OperationOptions | undefined,
    request: StreamReadRequestOptions
  ): Effect.Effect<ReadBatch<Encoding>, S2ClientError> =>
    withSdkSpan(
      operation,
      traceAttributes(options, {
        basin: operationOptions?.basinName ?? defaultBasinName,
        stream: name
      }),
      trySdk(operation, () => selectedStreamHandle(name, operationOptions).read(options, request)) as Effect.Effect<
        ReadBatch<Encoding>,
        S2ClientError
      >
    )

  const readBatch = (
    name: string,
    options: ReadOptions = {},
    operationOptions?: S2OperationOptions
  ) => readBatchFrom("readBatch", name, options, operationOptions, operationOptions?.request)

  const readBatchBytes = (
    name: string,
    options: ReadOptions = {},
    operationOptions?: S2OperationOptions
  ) =>
    readBatchFrom("readBatchBytes", name, options, operationOptions, {
      ...operationOptions?.request,
      as: "bytes"
    })

  const read = (
    name: string,
    options: ReadOptions,
    operationOptions?: S2OperationOptions
  ): Stream.Stream<S2Record, S2ClientError> =>
    readStream(
      name,
      "read",
      options,
      operationOptions,
      Effect.tryPromise({
        try: () => selectedStreamHandle(name, operationOptions).readSession(options, operationOptions?.request),
        catch: fromUnknown("readSession")
      }),
      toS2Record
    )

  const readStream = <Record, A>(
    name: string,
    operation: string,
    options: ReadOptions,
    operationOptions: S2OperationOptions | undefined,
    acquireSession: Effect.Effect<AsyncIterable<Record> & { cancel: () => Promise<void> }, S2ClientError>,
    mapRecord: (record: Record) => A
  ): Stream.Stream<A, S2ClientError> =>
    Stream.unwrap(
      Effect.gen(function*() {
        const session = yield* Effect.acquireRelease(
          acquireSession,
          (readSession) => Effect.promise(() => readSession.cancel())
        )
        return Stream.fromAsyncIterable(session, fromUnknown("read")).pipe(
          Stream.map(mapRecord)
        )
      })
    ).pipe(
      Stream.withSpan(`S2.${operation}`, {
        attributes: traceAttributes(options, {
          basin: operationOptions?.basinName ?? defaultBasinName,
          stream: name
        })
      })
    )

  const readBytes = (
    name: string,
    options: ReadOptions,
    operationOptions?: S2OperationOptions
  ): Stream.Stream<S2RecordBytes, S2ClientError> =>
    readStream(
      name,
      "readBytes",
      options,
      operationOptions,
      Effect.tryPromise({
        try: () =>
          selectedStreamHandle(name, operationOptions).readSession(
            options,
            { ...operationOptions?.request, as: "bytes" }
          ),
        catch: fromUnknown("readSession")
      }),
      toS2RecordBytes
    )

  const appendSession = (
    name: string,
    config: AppendSessionConfig = {},
    options?: S2OperationOptions
  ) =>
    withSdkSpan(
      "appendSession",
      traceAttributes(config, { basin: options?.basinName ?? defaultBasinName, stream: name }),
      Effect.gen(function*() {
        const handle = selectedStreamHandle(name, options)
        const sdkSession = yield* Effect.acquireRelease(
          trySdk("appendSession", () => handle.appendSession(appendSessionOptions(config), options?.request)),
          (session) => Effect.promise(() => session.close())
        )

        const submit = (input: SdkAppendInput) =>
          withSdkSpan(
            "appendSession.submit",
            traceAttributes(
              { stream: name, recordCount: input.records.length, matchSeqNum: input.matchSeqNum },
              { basin: options?.basinName ?? defaultBasinName, stream: name }
            ),
            Effect.gen(function*() {
              const ticket = yield* trySdk("appendSession.submit", () => sdkSession.submit(input))
              const ack = yield* trySdk("appendSession.ack", () => ticket.ack())
              yield* Effect.annotateCurrentSpan({ seqNum: ack.start.seqNum })
              return ack
            })
          )

        return { submit }
      })
    )

  const producer = (
    name: string,
    config: ProducerConfig = {},
    options?: S2OperationOptions
  ) =>
    withSdkSpan(
      "producer",
      traceAttributes(config, { basin: options?.basinName ?? defaultBasinName, stream: name }),
      Effect.gen(function*() {
        const handle = selectedStreamHandle(name, options)
        const sdkProducer = yield* Effect.acquireRelease(
          trySdk("producer", async () =>
            new Producer(
              new BatchTransform(batchTransformOptions(config)),
              await handle.appendSession(appendSessionOptions(config), options?.request),
              name
            )),
          (p) => Effect.promise(() => p.close())
        )

        const submit = (record: SdkAppendRecord) =>
          withSdkSpan(
            "producer.submit",
            traceAttributes(undefined, { basin: options?.basinName ?? defaultBasinName, stream: name }),
            Effect.gen(function*() {
              const ticket = yield* trySdk("producer.submit", () => sdkProducer.submit(record))
              const ack = yield* trySdk("producer.ack", () => ticket.ack())
              const seqNum = ack.seqNum()
              yield* Effect.annotateCurrentSpan({ seqNum })
              // preserve the upstream per-record coordinate alongside the batch ack —
              // a batched owner write still knows its own assigned seq-num.
              return { seqNum, batch: ack.batchAppendAck() }
            })
          )

        return { submit }
      })
    )

  return {
    listBasins,
    listAllBasins,
    createBasin,
    getBasinConfig,
    deleteBasin,
    ensureBasin,
    reconfigureBasin,
    listAccessTokens,
    listAllAccessTokens,
    issueAccessToken,
    revokeAccessToken,
    listLocations,
    getDefaultLocation,
    setDefaultLocation,
    accountMetrics,
    basinMetrics,
    streamMetrics,
    listStreams,
    listAllStreams,
    createStream,
    getStreamConfig,
    deleteStream,
    ensureStream,
    reconfigureStream,
    checkTail,
    append,
    readBatch,
    readBatchBytes,
    read,
    readBytes,
    appendSession,
    producer
  }
}

export function layer(options: {
  readonly accessToken: Config.Config<Redacted.Redacted<string>> | Redacted.Redacted<string>
  readonly basinName: Config.Config<string> | string
  readonly endpoints?: S2EndpointsInit
  readonly retry?: S2RetryConfig
  readonly forceTransport?: "fetch" | "s2s"
}): Layer.Layer<S2Client, Config.ConfigError> {
  return Layer.effect(
    S2Client,
    Effect.gen(function*() {
      const accessToken = yield* resolveOption(options.accessToken)
      const basinName = yield* resolveOption(options.basinName)
      return makeLiveApi(
        liveOptions({
          accessToken,
          basinName,
          ...optionalLiveOptions(options)
        })
      )
    })
  )
}
