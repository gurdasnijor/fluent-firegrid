import {
  AppendInput,
  BatchTransform,
  Producer,
  S2,
  S2Environment,
  type AppendAck,
  type ReadOptions,
  type S2RetryConfig,
  type SdkAppendRecord,
  type BatchTransformOptions,
  type AppendSessionOptions,
  type StreamOptions,
  type StreamInfo,
  type Tail,
} from "./internal/sdk.ts"
import type { S2Record, S2RecordBytes } from "./internal/record.ts"
import { toS2Record, toS2RecordBytes } from "./internal/record.ts"
import { fromUnknown, type S2ClientError } from "./S2Error.ts"
import { Config, Context, Effect, Layer, Redacted, Sink, Stream, type Scope } from "effect"

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
}

export interface S2Producer {
  readonly submit: (record: SdkAppendRecord) => Effect.Effect<AppendAck, S2ClientError>
}

export interface AppendSessionConfig {
  readonly maxInflightBytes?: number
  readonly maxInflightBatches?: number
}

export interface S2AppendSession {
  readonly submit: (
    records: ReadonlyArray<SdkAppendRecord>,
    options?: AppendOptions,
  ) => Effect.Effect<AppendAck, S2ClientError>
}

export interface S2ClientApi {
  readonly createStream: (name: string) => Effect.Effect<StreamInfo, S2ClientError>
  readonly checkTail: (name: string) => Effect.Effect<Tail, S2ClientError>
  readonly append: (
    name: string,
    records: ReadonlyArray<SdkAppendRecord>,
    options?: AppendOptions,
  ) => Effect.Effect<AppendAck, S2ClientError>
  readonly read: (name: string, options: ReadOptions) => Stream.Stream<S2Record, S2ClientError>
  readonly readBytes: (
    name: string,
    options: ReadOptions,
  ) => Stream.Stream<S2RecordBytes, S2ClientError>
  readonly appendSession: (
    name: string,
    config?: AppendSessionConfig,
  ) => Effect.Effect<S2AppendSession, S2ClientError, Scope.Scope>
  readonly producer: (
    name: string,
    config?: ProducerConfig,
  ) => Effect.Effect<S2Producer, S2ClientError, Scope.Scope>
}

export class S2Client extends Context.Service<S2Client, S2ClientApi>()("S2Client") {
  static readonly createStream = (
    name: string,
  ): Effect.Effect<StreamInfo, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.createStream(name))

  static readonly checkTail = (name: string): Effect.Effect<Tail, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.checkTail(name))

  static readonly append = (
    name: string,
    records: ReadonlyArray<SdkAppendRecord>,
    options?: AppendOptions,
  ): Effect.Effect<AppendAck, S2ClientError, S2Client> =>
    Effect.flatMap(this, (client) => client.append(name, records, options))

  static readonly read = (
    name: string,
    options: ReadOptions,
  ): Stream.Stream<S2Record, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.read(name, options)))

  static readonly readBytes = (
    name: string,
    options: ReadOptions,
  ): Stream.Stream<S2RecordBytes, S2ClientError, S2Client> =>
    Stream.unwrap(Effect.map(this, (client) => client.readBytes(name, options)))

  static readonly appendSession = (
    name: string,
    config: AppendSessionConfig = {},
  ): Effect.Effect<S2AppendSession, S2ClientError, S2Client | Scope.Scope> =>
    Effect.flatMap(this, (client) => client.appendSession(name, config))

  static readonly producer = (
    name: string,
    config: ProducerConfig = {},
  ): Effect.Effect<S2Producer, S2ClientError, S2Client | Scope.Scope> =>
    Effect.flatMap(this, (client) => client.producer(name, config))

  static readonly sink = (
    s2Producer: S2Producer,
  ): Sink.Sink<void, SdkAppendRecord, never, S2ClientError> => Sink.forEach(s2Producer.submit)

  static get layerConfig(): Layer.Layer<S2Client, Config.ConfigError> {
    return layer({
      accessToken: Config.redacted("S2_ACCESS_TOKEN"),
      basinName: Config.string("S2_BASIN"),
    })
  }

  static readonly layer = layer
}

const resolveOption = <A>(value: Config.Config<A> | A): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value)

const streamOptions = (forceTransport: "fetch" | "s2s" | undefined): StreamOptions =>
  forceTransport === undefined ? {} : { forceTransport }

const batchTransformOptions = (config: ProducerConfig): BatchTransformOptions => ({
  ...(config.lingerDurationMillis === undefined
    ? {}
    : { lingerDurationMillis: config.lingerDurationMillis }),
  ...(config.maxBatchRecords === undefined ? {} : { maxBatchRecords: config.maxBatchRecords }),
  ...(config.maxBatchBytes === undefined ? {} : { maxBatchBytes: config.maxBatchBytes }),
})

const appendSessionOptions = (config: AppendSessionConfig): AppendSessionOptions => ({
  ...(config.maxInflightBytes === undefined ? {} : { maxInflightBytes: config.maxInflightBytes }),
  ...(config.maxInflightBatches === undefined
    ? {}
    : { maxInflightBatches: config.maxInflightBatches }),
})

const liveOptions = (options: {
  readonly accessToken: Redacted.Redacted<string>
  readonly basinName: string
  readonly retry?: S2RetryConfig
  readonly forceTransport?: "fetch" | "s2s"
}) => ({
  accessToken: options.accessToken,
  basinName: options.basinName,
  ...(options.retry === undefined ? {} : { retry: options.retry }),
  ...(options.forceTransport === undefined ? {} : { forceTransport: options.forceTransport }),
})

const makeLiveApi = (input: {
  readonly accessToken: Redacted.Redacted<string>
  readonly basinName: string
  readonly retry?: S2RetryConfig
  readonly forceTransport?: "fetch" | "s2s"
}): S2ClientApi => {
  const environment = S2Environment.parse()
  const retry: S2RetryConfig = {
    ...input.retry,
    appendRetryPolicy: input.retry?.appendRetryPolicy ?? "noSideEffects",
  }
  const client = new S2({
    ...environment,
    accessToken: Redacted.value(input.accessToken),
    retry,
  })
  const basin = client.basin(input.basinName)
  const streamHandle = (name: string) => basin.stream(name, streamOptions(input.forceTransport))

  const createStream = Effect.fn("S2.createStream")(function*(name: string) {
    yield* Effect.tryPromise({
      try: () => basin.streams.create({ stream: name }),
      catch: fromUnknown("createStream"),
    })
    return {
      name,
      createdAt: new Date(),
    }
  })

  const checkTail = Effect.fn("S2.checkTail")(function*(name: string) {
    return yield* Effect.tryPromise({
      try: () => streamHandle(name).checkTail(),
      catch: fromUnknown("checkTail"),
    })
  })

  const append = Effect.fn("S2.append")(function*(
    name: string,
    records: ReadonlyArray<SdkAppendRecord>,
    options?: AppendOptions,
  ) {
    yield* Effect.annotateCurrentSpan({
      stream: name,
      matchSeqNum: options?.matchSeqNum,
    })
    const ack = yield* Effect.tryPromise({
      try: () => streamHandle(name).append(AppendInput.create(records, options)),
      catch: fromUnknown("append"),
    })
    yield* Effect.annotateCurrentSpan({ seqNum: ack.start.seqNum })
    return ack
  })

  const read = (name: string, options: ReadOptions): Stream.Stream<S2Record, S2ClientError> =>
    Stream.unwrap(
      Effect.gen(function*() {
        const handle = streamHandle(name)
        const session = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: () => handle.readSession(options),
            catch: fromUnknown("readSession"),
          }),
          (readSession) => Effect.promise(() => readSession.cancel()),
        )
        return Stream.fromAsyncIterable(session, fromUnknown("read")).pipe(
          Stream.map(toS2Record),
        )
      }),
    ).pipe(Stream.withSpan("S2.read", { attributes: { stream: name } }))

  const readBytes = (
    name: string,
    options: ReadOptions,
  ): Stream.Stream<S2RecordBytes, S2ClientError> =>
    Stream.unwrap(
      Effect.gen(function*() {
        const handle = streamHandle(name)
        const session = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: () => handle.readSession(options, { as: "bytes" }),
            catch: fromUnknown("readSession"),
          }),
          (readSession) => Effect.promise(() => readSession.cancel()),
        )
        return Stream.fromAsyncIterable(session, fromUnknown("read")).pipe(
          Stream.map(toS2RecordBytes),
        )
      }),
    ).pipe(Stream.withSpan("S2.readBytes", { attributes: { stream: name } }))

  const appendSession = Effect.fn("S2.appendSession")(function*(
    name: string,
    config: AppendSessionConfig = {},
  ) {
    const handle = streamHandle(name)
    const sdkSession = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () => handle.appendSession(appendSessionOptions(config)),
        catch: fromUnknown("appendSession"),
      }),
      (session) => Effect.promise(() => session.close()),
    )

    const submit = Effect.fn("S2.appendSession.submit")(function*(
      records: ReadonlyArray<SdkAppendRecord>,
      options?: AppendOptions,
    ) {
      yield* Effect.annotateCurrentSpan({
        stream: name,
        matchSeqNum: options?.matchSeqNum,
      })
      const ticket = yield* Effect.tryPromise({
        try: () => sdkSession.submit(AppendInput.create(records, options)),
        catch: fromUnknown("appendSession.submit"),
      })
      const ack = yield* Effect.tryPromise({
        try: () => ticket.ack(),
        catch: fromUnknown("appendSession.ack"),
      })
      yield* Effect.annotateCurrentSpan({ seqNum: ack.start.seqNum })
      return ack
    })

    return { submit }
  })

  const producer = Effect.fn("S2.producer")(function*(name: string, config: ProducerConfig = {}) {
    const handle = streamHandle(name)
    const sdkProducer = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: async () =>
          new Producer(
            new BatchTransform(batchTransformOptions(config)),
            await handle.appendSession(appendSessionOptions(config)),
            name,
          ),
        catch: fromUnknown("appendSession"),
      }),
      (p) => Effect.promise(() => p.close()),
    )

    const submit = Effect.fn("S2.producer.submit")(function*(record: SdkAppendRecord) {
      yield* Effect.annotateCurrentSpan({ stream: name })
      const ticket = yield* Effect.tryPromise({
        try: () => sdkProducer.submit(record),
        catch: fromUnknown("producer.submit"),
      })
      const ack = yield* Effect.tryPromise({
        try: () => ticket.ack(),
        catch: fromUnknown("producer.ack"),
      })
      yield* Effect.annotateCurrentSpan({ seqNum: ack.seqNum() })
      return ack.batchAppendAck()
    })

    return { submit }
  })

  return {
    createStream,
    checkTail,
    append,
    read,
    readBytes,
    appendSession,
    producer,
  }
}

export function layer(options: {
  readonly accessToken: Config.Config<Redacted.Redacted<string>> | Redacted.Redacted<string>
  readonly basinName: Config.Config<string> | string
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
          ...(options.retry === undefined ? {} : { retry: options.retry }),
          ...(options.forceTransport === undefined
            ? {}
            : { forceTransport: options.forceTransport }),
        }),
      )
    }),
  )
}
