import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

import { serialization as PatternSerialization } from "@s2-dev/streamstore-patterns"
import {
  AppendInput,
  AppendRecord,
  BatchTransform,
  FencingTokenMismatchError,
  IndexedAppendAck,
  MAX_APPEND_BYTES,
  MAX_APPEND_RECORDS,
  meteredBytes,
  Producer as SdkProducer,
  randomToken,
  RangeNotSatisfiableError,
  S2,
  S2Environment,
  S2Error,
  SeqNumMismatchError,
  utf8ByteLength
} from "@s2-dev/streamstore"
import type {
  AccessTokenInfo,
  AccountMetricsInput,
  AppendAck,
  AppendSession,
  AppendSessionOptions,
  BasinInfo,
  BasinMetricsInput,
  CreateBasinInput,
  CreateStreamInput,
  DeleteBasinInput,
  DeleteStreamInput,
  EncryptionKeyInput,
  EnsureBasinInput,
  EnsureStreamInput,
  GetBasinConfigInput,
  GetDefaultLocationResponse,
  GetStreamConfigInput,
  IssueAccessTokenInput,
  IssueAccessTokenResponse,
  ListAccessTokensInput,
  ListAccessTokensResponse,
  ListAllAccessTokensInput,
  ListAllBasinsInput,
  ListAllStreamsInput,
  ListBasinsInput,
  ListBasinsResponse,
  ListLocationsResponse,
  ListStreamsInput,
  ListStreamsResponse,
  MetricSetResponse,
  ReadBatch,
  ReadInput,
  ReadRecord,
  ReadSession,
  ReconfigureBasinInput,
  ReconfigureBasinResponse,
  ReconfigureStreamInput,
  ReconfigureStreamResponse,
  RevokeAccessTokenInput,
  S2ClientOptions,
  S2RequestOptions,
  SetDefaultLocationInput,
  SetDefaultLocationResponse,
  StreamInfo,
  StreamMetricsInput,
  StreamOptions,
  TailResponse
} from "@s2-dev/streamstore"

export {
  AppendInput,
  AppendRecord,
  BatchTransform,
  FencingTokenMismatchError,
  IndexedAppendAck,
  MAX_APPEND_BYTES,
  MAX_APPEND_RECORDS,
  meteredBytes,
  PatternSerialization as S2Patterns,
  randomToken,
  RangeNotSatisfiableError,
  S2,
  S2Environment,
  S2Error,
  SdkProducer as Producer,
  SeqNumMismatchError,
  utf8ByteLength
}

type SdkBasin = ReturnType<S2["basin"]>
type SdkStream = ReturnType<SdkBasin["stream"]>
type ReadFormat = "string" | "bytes"
type MessageRange = { readonly start: number; readonly end: number }

export interface AppendOptions {
  readonly matchSeqNum?: number
  readonly fencingToken?: string
}

const toS2Error = (error: unknown): S2Error =>
  error instanceof S2Error
    ? error
    : new S2Error({
      message: error instanceof Error ? error.message : String(error),
      data: error,
      origin: "sdk",
      status: 0
    })

const trySync = <A>(evaluate: () => A): Effect.Effect<A, S2Error> => Effect.try({ try: evaluate, catch: toS2Error })

const tryPromise = <A>(evaluate: () => PromiseLike<A>): Effect.Effect<A, S2Error> =>
  Effect.tryPromise({ try: evaluate, catch: toS2Error })

const wrapPromise =
  <Args extends Array<unknown>, A>(evaluate: (...args: Args) => PromiseLike<A>) =>
  (...args: Args): Effect.Effect<A, S2Error> => tryPromise(() => evaluate(...args))

const wrapAsyncIterable =
  <Args extends Array<unknown>, A>(evaluate: (...args: Args) => AsyncIterable<A>) =>
  (...args: Args): Stream.Stream<A, S2Error> => Stream.fromAsyncIterable(evaluate(...args), toS2Error)

const ignoreFinalizerError = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<void, never, R> =>
  Effect.ignore(effect)

const s2ErrorAttributes = (error: S2Error): Record<string, string> => ({
  "s2.error.code": error.code ?? "",
  "s2.error.expected_fencing_token": error instanceof FencingTokenMismatchError
    ? error.expectedFencingToken
    : "",
  "s2.error.expected_seq_num": error instanceof SeqNumMismatchError
    ? String(error.expectedSeqNum)
    : "",
  "s2.error.kind": error.name,
  "s2.error.status": String(error.status)
})

const withS2Span = <A, R>(
  name: string,
  attributes: Record<string, unknown>,
  effect: Effect.Effect<A, S2Error, R>
) =>
  effect.pipe(
    Effect.tap(() => Effect.annotateCurrentSpan("s2.operation.status", "ok")),
    Effect.tapError((error) =>
      Effect.annotateCurrentSpan({
        ...s2ErrorAttributes(error),
        "s2.operation.status": "error"
      })
    ),
    Effect.withSpan(name, { attributes })
  )

export interface S2ClientApi {
  readonly raw: S2
  readonly basins: BasinsApi
  readonly accessTokens: AccessTokensApi
  readonly locations: LocationsApi
  readonly metrics: MetricsApi
  readonly basin: (name: string) => Effect.Effect<BasinApi, S2Error>
  readonly stream: (basin: string, stream: string, options?: StreamOptions) => Effect.Effect<StreamApi, S2Error>
}

export interface BasinApi {
  readonly raw: SdkBasin
  readonly name: string
  readonly streams: StreamsApi
  readonly stream: (name: string, options?: StreamOptions) => Effect.Effect<StreamApi, S2Error>
}

export interface BasinsApi {
  readonly list: (args?: ListBasinsInput, options?: S2RequestOptions) => Effect.Effect<ListBasinsResponse, S2Error>
  readonly listAll: (args?: ListAllBasinsInput, options?: S2RequestOptions) => Stream.Stream<BasinInfo, S2Error>
  readonly create: (args: CreateBasinInput, options?: S2RequestOptions) => Effect.Effect<BasinInfo, S2Error>
  readonly getConfig: (
    args: GetBasinConfigInput,
    options?: S2RequestOptions
  ) => Effect.Effect<Awaited<ReturnType<S2["basins"]["getConfig"]>>, S2Error>
  readonly delete: (args: DeleteBasinInput, options?: S2RequestOptions) => Effect.Effect<void, S2Error>
  readonly ensure: (
    args: EnsureBasinInput,
    options?: S2RequestOptions
  ) => Effect.Effect<Awaited<ReturnType<S2["basins"]["ensure"]>>, S2Error>
  readonly reconfigure: (
    args: ReconfigureBasinInput,
    options?: S2RequestOptions
  ) => Effect.Effect<ReconfigureBasinResponse, S2Error>
}

export interface StreamsApi {
  readonly list: (args?: ListStreamsInput, options?: S2RequestOptions) => Effect.Effect<ListStreamsResponse, S2Error>
  readonly listAll: (args?: ListAllStreamsInput, options?: S2RequestOptions) => Stream.Stream<StreamInfo, S2Error>
  readonly create: (
    args: CreateStreamInput,
    options?: S2RequestOptions
  ) => Effect.Effect<Awaited<ReturnType<SdkBasin["streams"]["create"]>>, S2Error>
  readonly getConfig: (
    args: GetStreamConfigInput,
    options?: S2RequestOptions
  ) => Effect.Effect<Awaited<ReturnType<SdkBasin["streams"]["getConfig"]>>, S2Error>
  readonly delete: (args: DeleteStreamInput, options?: S2RequestOptions) => Effect.Effect<void, S2Error>
  readonly ensure: (
    args: EnsureStreamInput,
    options?: S2RequestOptions
  ) => Effect.Effect<Awaited<ReturnType<SdkBasin["streams"]["ensure"]>>, S2Error>
  readonly reconfigure: (
    args: ReconfigureStreamInput,
    options?: S2RequestOptions
  ) => Effect.Effect<ReconfigureStreamResponse, S2Error>
}

export interface AccessTokensApi {
  readonly list: (
    args?: ListAccessTokensInput,
    options?: S2RequestOptions
  ) => Effect.Effect<ListAccessTokensResponse, S2Error>
  readonly listAll: (
    args?: ListAllAccessTokensInput,
    options?: S2RequestOptions
  ) => Stream.Stream<AccessTokenInfo, S2Error>
  readonly issue: (
    args: IssueAccessTokenInput,
    options?: S2RequestOptions
  ) => Effect.Effect<IssueAccessTokenResponse, S2Error>
  readonly revoke: (args: RevokeAccessTokenInput, options?: S2RequestOptions) => Effect.Effect<void, S2Error>
}

export interface LocationsApi {
  readonly list: (options?: S2RequestOptions) => Effect.Effect<ListLocationsResponse, S2Error>
  readonly getDefault: (options?: S2RequestOptions) => Effect.Effect<GetDefaultLocationResponse, S2Error>
  readonly setDefault: (
    args: SetDefaultLocationInput,
    options?: S2RequestOptions
  ) => Effect.Effect<SetDefaultLocationResponse, S2Error>
}

export interface MetricsApi {
  readonly account: (args: AccountMetricsInput, options?: S2RequestOptions) => Effect.Effect<MetricSetResponse, S2Error>
  readonly basin: (args: BasinMetricsInput, options?: S2RequestOptions) => Effect.Effect<MetricSetResponse, S2Error>
  readonly stream: (args: StreamMetricsInput, options?: S2RequestOptions) => Effect.Effect<MetricSetResponse, S2Error>
}

export interface StreamApi {
  readonly raw: SdkStream
  readonly name: string
  readonly checkTail: (options?: S2RequestOptions) => Effect.Effect<TailResponse, S2Error>
  readonly read: <Format extends ReadFormat = "string">(
    input?: ReadInput,
    options?: S2RequestOptions & { readonly as?: Format }
  ) => Effect.Effect<ReadBatch<Format>, S2Error>
  readonly readSession: <Format extends ReadFormat = "string">(
    input?: ReadInput,
    options?: S2RequestOptions & { readonly as?: Format }
  ) => Stream.Stream<ReadRecord<Format>, S2Error>
  readonly append: (input: AppendInput, options?: S2RequestOptions) => Effect.Effect<AppendAck, S2Error>
  readonly appendSession: (
    sessionOptions?: AppendSessionOptions,
    requestOptions?: S2RequestOptions
  ) => Effect.Effect<AppendSessionApi, S2Error, Scope.Scope>
  readonly producer: (
    sessionOptions?: AppendSessionOptions,
    requestOptions?: S2RequestOptions
  ) => Effect.Effect<ProducerApi, S2Error, Scope.Scope>
  readonly serialization: StreamSerializationApi
  readonly withEncryptionKey: (key: EncryptionKeyInput) => StreamApi
  readonly close: Effect.Effect<void>
}

export interface AppendSessionApi {
  readonly raw: AppendSession
  readonly readable: Stream.Stream<AppendAck, S2Error>
  readonly submit: (input: AppendInput) => Effect.Effect<BatchSubmitTicketApi, S2Error>
  readonly acks: Stream.Stream<AppendAck, S2Error>
  readonly lastAckedPosition: Effect.Effect<Option.Option<AppendAck>>
  readonly failureCause: Effect.Effect<Option.Option<S2Error>>
  readonly close: Effect.Effect<void, S2Error>
}

export interface BatchSubmitTicketApi {
  readonly bytes: number
  readonly numRecords: number
  readonly ack: Effect.Effect<AppendAck, S2Error>
}

export interface ProducerApi {
  readonly raw: InstanceType<typeof SdkProducer>
  readonly readable: Stream.Stream<IndexedAppendAck, S2Error>
  readonly submit: (record: AppendRecord) => Effect.Effect<RecordSubmitTicketApi, S2Error>
  readonly close: Effect.Effect<void, S2Error>
}

export interface RecordSubmitTicketApi {
  readonly ack: Effect.Effect<IndexedAppendAck, S2Error>
}

export interface SerializingAppendSessionApi<Message> {
  readonly raw: PatternSerialization.SerializingAppendSession<Message>
  readonly submit: (message: Message) => Effect.Effect<MessageRange, S2Error>
}

export interface StreamSerializationApi {
  readonly appendSession: <Message>(
    serializer: (message: Message) => Uint8Array,
    options?: PatternSerialization.SerializingAppendSessionOptions,
    sessionOptions?: AppendSessionOptions,
    requestOptions?: S2RequestOptions
  ) => Effect.Effect<SerializingAppendSessionApi<Message>, S2Error, Scope.Scope>
  readonly readSession: <Message>(
    deserialize: (payload: Uint8Array) => Message,
    input?: ReadInput,
    requestOptions?: S2RequestOptions,
    options?: PatternSerialization.DeserializingReadSessionOptions
  ) => Stream.Stream<Message, S2Error>
}

export class S2Config extends Context.Service<S2Config, S2ClientOptions>()("effect-s2/S2Client/S2Config") {}

export class S2Client extends Context.Service<S2Client, S2ClientApi>()("effect-s2/S2Client") {}

export const make = Effect.fn("S2Client.make")(function*(options: S2ClientOptions) {
  return makeClient(new S2(options))
})

export const clientLayer = Layer.effect(S2Client, Effect.map(S2Config, (options) => makeClient(new S2(options))))

export const layer = (options: S2ClientOptions) => clientLayer.pipe(Layer.provide(Layer.succeed(S2Config, options)))

export const basin = Effect.fn("S2Client.basin")(function*(name: string) {
  const client = yield* S2Client
  return yield* client.basin(name)
})

export const stream = Effect.fn("S2Client.stream")(function*(
  basinName: string,
  streamName: string,
  options?: StreamOptions
) {
  const client = yield* S2Client
  return yield* client.stream(basinName, streamName, options)
})

export const basins = {
  list: (...args: Parameters<BasinsApi["list"]>) => Effect.flatMap(S2Client, (client) => client.basins.list(...args)),
  listAll: (...args: Parameters<BasinsApi["listAll"]>) =>
    Stream.unwrap(Effect.map(S2Client, (client) => client.basins.listAll(...args))),
  create: (...args: Parameters<BasinsApi["create"]>) =>
    Effect.flatMap(S2Client, (client) => client.basins.create(...args)),
  getConfig: (...args: Parameters<BasinsApi["getConfig"]>) =>
    Effect.flatMap(S2Client, (client) => client.basins.getConfig(...args)),
  delete: (...args: Parameters<BasinsApi["delete"]>) =>
    Effect.flatMap(S2Client, (client) => client.basins.delete(...args)),
  ensure: (...args: Parameters<BasinsApi["ensure"]>) =>
    Effect.flatMap(S2Client, (client) => client.basins.ensure(...args)),
  reconfigure: (...args: Parameters<BasinsApi["reconfigure"]>) =>
    Effect.flatMap(S2Client, (client) => client.basins.reconfigure(...args))
}

export const accessTokens = {
  list: (...args: Parameters<AccessTokensApi["list"]>) =>
    Effect.flatMap(S2Client, (client) => client.accessTokens.list(...args)),
  listAll: (...args: Parameters<AccessTokensApi["listAll"]>) =>
    Stream.unwrap(Effect.map(S2Client, (client) => client.accessTokens.listAll(...args))),
  issue: (...args: Parameters<AccessTokensApi["issue"]>) =>
    Effect.flatMap(S2Client, (client) => client.accessTokens.issue(...args)),
  revoke: (...args: Parameters<AccessTokensApi["revoke"]>) =>
    Effect.flatMap(S2Client, (client) => client.accessTokens.revoke(...args))
}

export const locations = {
  list: (...args: Parameters<LocationsApi["list"]>) =>
    Effect.flatMap(S2Client, (client) => client.locations.list(...args)),
  getDefault: (...args: Parameters<LocationsApi["getDefault"]>) =>
    Effect.flatMap(S2Client, (client) => client.locations.getDefault(...args)),
  setDefault: (...args: Parameters<LocationsApi["setDefault"]>) =>
    Effect.flatMap(S2Client, (client) => client.locations.setDefault(...args))
}

export const metrics = {
  account: (...args: Parameters<MetricsApi["account"]>) =>
    Effect.flatMap(S2Client, (client) => client.metrics.account(...args)),
  basin: (...args: Parameters<MetricsApi["basin"]>) =>
    Effect.flatMap(S2Client, (client) => client.metrics.basin(...args)),
  stream: (...args: Parameters<MetricsApi["stream"]>) =>
    Effect.flatMap(S2Client, (client) => client.metrics.stream(...args))
}

export const patterns = {
  constants: {
    DEDUPE_SEQ_HEADER: PatternSerialization.DEDUPE_SEQ_HEADER,
    DEDUPE_SEQ_HEADER_BYTES: PatternSerialization.DEDUPE_SEQ_HEADER_BYTES,
    DEDUPE_WRITER_UNIQ_ID: PatternSerialization.DEDUPE_WRITER_UNIQ_ID,
    FRAME_BYTES_HEADER: PatternSerialization.FRAME_BYTES_HEADER,
    FRAME_BYTES_HEADER_BYTES: PatternSerialization.FRAME_BYTES_HEADER_BYTES,
    FRAME_RECORDS_HEADER: PatternSerialization.FRAME_RECORDS_HEADER,
    FRAME_RECORDS_HEADER_BYTES: PatternSerialization.FRAME_RECORDS_HEADER_BYTES,
    WRITER_UNIQ_ID: PatternSerialization.WRITER_UNIQ_ID
  },
  u64: {
    encode: (value: bigint | number) => trySync(() => PatternSerialization.encodeU64(value)),
    decode: (bytes: Uint8Array) => trySync(() => PatternSerialization.decodeU64(bytes))
  },
  chunkBytes: (bytes: Uint8Array, maxChunkSize?: number) =>
    trySync(() => PatternSerialization.chunkBytes(bytes, maxChunkSize)),
  frameChunksToRecords: (chunks: ReadonlyArray<Uint8Array>) =>
    trySync(() => PatternSerialization.frameChunksToRecords([...chunks])),
  frameAssembler: (options?: PatternSerialization.FrameAssemblerOptions) =>
    Effect.sync(() => new PatternSerialization.FrameAssembler(options)),
  pushFrameRecord: (
    assembler: PatternSerialization.FrameAssembler,
    record: Parameters<PatternSerialization.FrameAssembler["push"]>[0]
  ) => trySync(() => assembler.push(record)),
  dedupeFilter: () => Effect.sync(() => new PatternSerialization.DedupeFilter()),
  extractDedupeSeq: (...args: Parameters<typeof PatternSerialization.extractDedupeSeq>) =>
    trySync(() => PatternSerialization.extractDedupeSeq(...args)),
  injectDedupeHeaders: (...args: Parameters<typeof PatternSerialization.injectDedupeHeaders>) =>
    trySync(() => PatternSerialization.injectDedupeHeaders(...args)),
  serializingAppendSession: <Message>(
    session: AppendSession,
    serializer: (message: Message) => Uint8Array,
    options?: PatternSerialization.SerializingAppendSessionOptions
  ) =>
    Effect.sync(() =>
      wrapSerializingAppendSession(new PatternSerialization.SerializingAppendSession(session, serializer, options))
    ),
  deserializingReadSession: <Message>(
    session: ReadSession<"bytes">,
    deserialize: (payload: Uint8Array) => Message,
    options?: PatternSerialization.DeserializingReadSessionOptions
  ) =>
    Stream.fromReadableStream({
      evaluate: () => new PatternSerialization.DeserializingReadSession(session, deserialize, options),
      onError: toS2Error
    })
}

const makeClient = (raw: S2): S2ClientApi => ({
  raw,
  basins: makeBasins(raw.basins),
  accessTokens: makeAccessTokens(raw.accessTokens),
  locations: makeLocations(raw.locations),
  metrics: makeMetrics(raw.metrics),
  basin: (name) => Effect.map(trySync(() => raw.basin(name)), makeBasin),
  stream: (basinName, streamName, options) =>
    Effect.map(trySync(() => raw.basin(basinName).stream(streamName, options)), makeStream)
})

const makeBasin = (raw: SdkBasin): BasinApi => ({
  raw,
  name: raw.name,
  streams: makeStreams(raw.streams),
  stream: (name, options) => Effect.map(trySync(() => raw.stream(name, options)), makeStream)
})

const makeBasins = (raw: S2["basins"]): BasinsApi => ({
  list: wrapPromise(raw.list.bind(raw)),
  listAll: wrapAsyncIterable(raw.listAll.bind(raw)),
  create: wrapPromise(raw.create.bind(raw)),
  getConfig: wrapPromise(raw.getConfig.bind(raw)),
  delete: wrapPromise(raw.delete.bind(raw)),
  ensure: wrapPromise(raw.ensure.bind(raw)),
  reconfigure: wrapPromise(raw.reconfigure.bind(raw))
})

const makeStreams = (raw: SdkBasin["streams"]): StreamsApi => {
  const list = wrapPromise(raw.list.bind(raw))
  const listAll = wrapAsyncIterable(raw.listAll.bind(raw))
  const create = wrapPromise(raw.create.bind(raw))
  const getConfig = wrapPromise(raw.getConfig.bind(raw))
  const deleteStream = wrapPromise(raw.delete.bind(raw))
  const ensure = wrapPromise(raw.ensure.bind(raw))
  const reconfigure = wrapPromise(raw.reconfigure.bind(raw))

  return { create, delete: deleteStream, ensure, getConfig, list, listAll, reconfigure }
}

const makeAccessTokens = (raw: S2["accessTokens"]): AccessTokensApi => ({
  list: (args, options) => tryPromise(() => raw.list(args, options)),
  listAll: (args, options) => Stream.fromAsyncIterable(raw.listAll(args, options), toS2Error),
  issue: (args, options) => tryPromise(() => raw.issue(args, options)),
  revoke: (args, options) => tryPromise(() => raw.revoke(args, options))
})

const makeLocations = (raw: S2["locations"]): LocationsApi => ({
  list: (options) => tryPromise(() => raw.list(options)),
  getDefault: (options) => tryPromise(() => raw.getDefault(options)),
  setDefault: (args, options) => tryPromise(() => raw.setDefault(args, options))
})

const makeMetrics = (raw: S2["metrics"]): MetricsApi => ({
  account: (args, options) => tryPromise(() => raw.account(args, options)),
  basin: (args, options) => tryPromise(() => raw.basin(args, options)),
  stream: (args, options) => tryPromise(() => raw.stream(args, options))
})

const makeStream = (raw: SdkStream): StreamApi => ({
  raw,
  name: raw.name,
  checkTail: (options) =>
    withS2Span(
      "effect-s2.check-tail",
      { "s2.stream": raw.name },
      tryPromise(() => raw.checkTail(options))
    ),
  read: (input, options) =>
    withS2Span(
      "effect-s2.read",
      { "s2.stream": raw.name },
      tryPromise(() => raw.read(input, options))
    ),
  readSession: (input, options) =>
    Stream.unwrap(
      Effect.map(acquireReadSession(raw, input, options), (session) => Stream.fromAsyncIterable(session, toS2Error))
    ).pipe(
      Stream.withSpan("effect-s2.read-session", {
        attributes: { "s2.stream": raw.name }
      })
    ),
  append: (input, options) =>
    withS2Span(
      "effect-s2.append",
      { "s2.append.record_count": input.records.length, "s2.stream": raw.name },
      tryPromise(() => raw.append(input, options))
    ),
  appendSession: (sessionOptions, requestOptions) =>
    Effect.map(acquireAppendSession(raw, sessionOptions, requestOptions), wrapAppendSession),
  producer: (sessionOptions, requestOptions) =>
    Effect.map(acquireProducer(raw, sessionOptions, requestOptions), wrapProducer),
  serialization: makeStreamSerialization(raw),
  withEncryptionKey: (key) => makeStream(raw.withEncryptionKey(key)),
  close: Effect.promise(() => raw.close())
})

const acquireReadSession = <Format extends ReadFormat>(
  raw: SdkStream,
  input?: ReadInput,
  options?: S2RequestOptions & { readonly as?: Format }
) =>
  Effect.acquireRelease(
    tryPromise(() => raw.readSession(input, options)),
    (session) => ignoreFinalizerError(Effect.promise(() => session[Symbol.asyncDispose]()))
  )

const acquireAppendSession = (
  raw: SdkStream,
  sessionOptions?: AppendSessionOptions,
  requestOptions?: S2RequestOptions
) =>
  Effect.acquireRelease(
    tryPromise(() => raw.appendSession(sessionOptions, requestOptions)),
    (session) => ignoreFinalizerError(tryPromise(() => session.close()))
  )

const acquireProducer = (
  raw: SdkStream,
  sessionOptions?: AppendSessionOptions,
  requestOptions?: S2RequestOptions
) =>
  Effect.acquireRelease(
    Effect.map(acquireAppendSession(raw, sessionOptions, requestOptions), (session) =>
      new SdkProducer(new BatchTransform(), session)),
    (producer) =>
      ignoreFinalizerError(tryPromise(() => producer.close()))
  )

const wrapAppendSession = (session: AppendSession): AppendSessionApi => ({
  raw: session,
  readable: Stream.fromReadableStream({ evaluate: () => session.readable, onError: toS2Error }),
  submit: (input) =>
    Effect.map(tryPromise(() => session.submit(input)), (ticket) => ({
      bytes: ticket.bytes,
      numRecords: ticket.numRecords,
      ack: tryPromise(() => ticket.ack())
    })),
  acks: Stream.fromAsyncIterable(session.acks(), toS2Error),
  lastAckedPosition: Effect.sync(() => Option.fromNullishOr(session.lastAckedPosition())),
  failureCause: Effect.sync(() => Option.fromNullishOr(session.failureCause())),
  close: tryPromise(() => session.close())
})

const wrapProducer = (producer: InstanceType<typeof SdkProducer>): ProducerApi => ({
  raw: producer,
  readable: Stream.fromReadableStream({ evaluate: () => producer.readable, onError: toS2Error }),
  submit: (record) =>
    Effect.map(tryPromise(() => producer.submit(record)), (ticket) => ({ ack: tryPromise(() => ticket.ack()) })),
  close: tryPromise(() => producer.close())
})

const makeStreamSerialization = (raw: SdkStream): StreamSerializationApi => ({
  appendSession: (serializer, options, sessionOptions, requestOptions) =>
    Effect.map(
      acquireAppendSession(raw, sessionOptions, requestOptions),
      (session) =>
        wrapSerializingAppendSession(new PatternSerialization.SerializingAppendSession(session, serializer, options))
    ),
  readSession: (deserialize, input, requestOptions, options) =>
    Stream.unwrap(
      Effect.map(acquireReadSession(raw, input, { ...requestOptions, as: "bytes" as const }), (session) =>
        patterns.deserializingReadSession(session, deserialize, options))
    )
})

const wrapSerializingAppendSession = <Message>(
  session: PatternSerialization.SerializingAppendSession<Message>
): SerializingAppendSessionApi<Message> => ({
  raw: session,
  submit: (message) => tryPromise(() => session.submit(message))
})
