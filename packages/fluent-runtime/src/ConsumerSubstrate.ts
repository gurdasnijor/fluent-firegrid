import {
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "@effect/platform"
import { FetchHttpClient } from "@effect/platform"
import { Context, Data, Effect, Layer, Schema } from "effect"
import { DurableStream, type WriteError } from "effect-durable-streams"

const contentTypeJson = "application/json"

const ConsumerOffsetSchema = Schema.Struct({
  path: Schema.String,
  offset: Schema.String,
})

const AcquiredConsumerSchema = Schema.Struct({
  consumer_id: Schema.String,
  epoch: Schema.Number,
  token: Schema.String,
  streams: Schema.Array(ConsumerOffsetSchema),
  worker: Schema.optional(Schema.String),
})

const ConsumerInfoSchema = Schema.Struct({
  consumer_id: Schema.String,
  state: Schema.String,
  epoch: Schema.Number,
  streams: Schema.Array(ConsumerOffsetSchema),
  namespace: Schema.optional(Schema.String),
  lease_ttl_ms: Schema.Number,
  wake_preference: Schema.Unknown,
})

const AckedConsumerSchema = Schema.Struct({
  ok: Schema.Literal(true),
  token: Schema.String,
})

const ReleasedConsumerSchema = Schema.Struct({
  ok: Schema.Literal(true),
  state: Schema.String,
})

const WakeConfiguredSchema = Schema.Struct({
  ok: Schema.Literal(true),
  wake_preference: Schema.Unknown,
})

const isSuccessStatus = (status: number): boolean =>
  status >= 200 && status < 300

const normalizeBaseUrl = (baseUrl: string): string =>
  baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl

const endpoint = (url: string) => ({ url })

const streamEndpoint = (
  config: DurableConsumerClientConfig,
  routePath: string,
) => endpoint(`${normalizeBaseUrl(config.durableStreamsBaseUrl)}${routePath}`)

export type ConsumerOffset = Schema.Schema.Type<typeof ConsumerOffsetSchema>
export type AcquiredConsumer = Schema.Schema.Type<typeof AcquiredConsumerSchema>
export type ConsumerInfo = Schema.Schema.Type<typeof ConsumerInfoSchema>
export type AckedConsumer = Schema.Schema.Type<typeof AckedConsumerSchema>
export type ReleasedConsumer = Schema.Schema.Type<typeof ReleasedConsumerSchema>

export interface DurableConsumerClientConfig {
  readonly durableStreamsBaseUrl: string
}

export interface RegisterConsumerInput {
  readonly consumerId: string
  readonly streams: ReadonlyArray<string>
  readonly namespace?: string
  readonly leaseTtlMs?: number
}

export interface ConfigurePullWakeInput {
  readonly consumerId: string
  readonly wakeStream: string
}

export interface AcquireConsumerInput {
  readonly consumerId: string
  readonly worker: string
}

export interface AckConsumerInput {
  readonly consumerId: string
  readonly token: string
  readonly offsets: ReadonlyArray<ConsumerOffset>
}

export interface ReleaseConsumerInput {
  readonly consumerId: string
  readonly token: string
}

export interface AppendStreamInput {
  readonly routePath: string
  readonly event: unknown
}

export class DurableConsumerError extends Data.TaggedError("DurableConsumerError")<{
  readonly op: string
  readonly message: string
  readonly status?: number
  readonly body?: string
  readonly cause?: unknown
}> {}

const toConsumerError = (
  op: string,
  message: string,
) =>
  (cause: unknown): DurableConsumerError =>
    new DurableConsumerError({ op, message, cause })

const errorBody = (
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<string> =>
  response.text.pipe(Effect.catchAll(() => Effect.succeed("")))

const expectJson = <A, I, R>(
  op: string,
  schema: Schema.Schema<A, I, R>,
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<A, DurableConsumerError, R> =>
  isSuccessStatus(response.status)
    ? response.json.pipe(
      Effect.mapError(toConsumerError(op, "failed to decode Durable Streams JSON response")),
      Effect.flatMap((body) =>
        Schema.decodeUnknown(schema)(body).pipe(
          Effect.mapError(toConsumerError(op, "Durable Streams response did not match the expected shape")),
        ),
      ),
    )
    : errorBody(response).pipe(
      Effect.flatMap((body) =>
        Effect.fail(
          new DurableConsumerError({
            op,
            message: `Durable Streams ${op} failed with HTTP ${response.status}`,
            status: response.status,
            body,
          }),
        ),
      ),
    )

const requestJson = (
  method: "POST" | "PUT",
  url: string,
  body: unknown,
  headers?: Readonly<Record<string, string>>,
): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.make(method)(url).pipe(
    HttpClientRequest.setHeaders({
      "content-type": contentTypeJson,
      ...(headers ?? {}),
    }),
    HttpClientRequest.bodyUnsafeJson(body),
  )

const execute = (
  request: HttpClientRequest.HttpClientRequest,
): Effect.Effect<HttpClientResponse.HttpClientResponse, DurableConsumerError, HttpClient.HttpClient> =>
  Effect.flatMap(
    HttpClient.HttpClient,
    (client) =>
      client.execute(request).pipe(
        Effect.mapError(toConsumerError("http", "Durable Streams HTTP request failed")),
      ),
  )

const createStream = (
  config: DurableConsumerClientConfig,
  routePath: string,
): Effect.Effect<void, DurableConsumerError, HttpClient.HttpClient> =>
  DurableStream.define({
    endpoint: streamEndpoint(config, routePath),
    schema: Schema.Unknown,
  }).create({ contentType: contentTypeJson }).pipe(
    Effect.mapError(toConsumerError("stream.create", "failed to create Durable Stream")),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.stream_create", {
      attributes: { "durable_streams.route_path": routePath },
    }),
  )

const appendStream = (
  config: DurableConsumerClientConfig,
  input: AppendStreamInput,
): Effect.Effect<{ readonly offset: string }, DurableConsumerError, HttpClient.HttpClient> =>
  DurableStream.define({
    endpoint: streamEndpoint(config, input.routePath),
    schema: Schema.Unknown,
  }).append(input.event).pipe(
    Effect.map(({ offset }) => ({ offset })),
    Effect.mapError((cause: WriteError) =>
      new DurableConsumerError({
        op: "stream.append",
        message: "failed to append Durable Stream event",
        cause,
      }),
    ),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.stream_append", {
      attributes: { "durable_streams.route_path": input.routePath },
    }),
  )

const registerConsumer = (
  config: DurableConsumerClientConfig,
  input: RegisterConsumerInput,
): Effect.Effect<ConsumerInfo, DurableConsumerError, HttpClient.HttpClient> => {
  const body = {
    consumer_id: input.consumerId,
    streams: input.streams,
    ...(input.namespace === undefined ? {} : { namespace: input.namespace }),
    ...(input.leaseTtlMs === undefined ? {} : { lease_ttl_ms: input.leaseTtlMs }),
  }
  return execute(
    requestJson("POST", `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/consumers`, body),
  ).pipe(
    Effect.flatMap(response => expectJson("consumer.register", ConsumerInfoSchema, response)),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.register", {
      attributes: { "durable_streams.consumer.id": input.consumerId },
    }),
  )
}

const bindConsumerHttp = (
  httpClient: HttpClient.HttpClient,
) =>
  <A, E>(
    operation: Effect.Effect<A, E, HttpClient.HttpClient>,
  ): Effect.Effect<A, E> =>
    operation.pipe(
      Effect.provideService(HttpClient.HttpClient, httpClient),
    )

const configurePullWake = (
  config: DurableConsumerClientConfig,
  input: ConfigurePullWakeInput,
): Effect.Effect<void, DurableConsumerError, HttpClient.HttpClient> =>
  execute(
    requestJson(
      "PUT",
      `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/consumers/${
        encodeURIComponent(input.consumerId)
      }/wake`,
      {
        type: "pull-wake",
        wake_stream: input.wakeStream,
      },
    ),
  ).pipe(
    Effect.flatMap(response => expectJson("consumer.configure_pull_wake", WakeConfiguredSchema, response)),
    Effect.asVoid,
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.configure_pull_wake", {
      attributes: {
        "durable_streams.consumer.id": input.consumerId,
        "durable_streams.wake_stream": input.wakeStream,
      },
    }),
  )

const getConsumer = (
  config: DurableConsumerClientConfig,
  consumerId: string,
): Effect.Effect<ConsumerInfo, DurableConsumerError, HttpClient.HttpClient> =>
  execute(
    HttpClientRequest.get(
      `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/consumers/${encodeURIComponent(consumerId)}`,
    ),
  ).pipe(
    Effect.flatMap(response => expectJson("consumer.get", ConsumerInfoSchema, response)),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.get", {
      attributes: { "durable_streams.consumer.id": consumerId },
    }),
  )

const acquireConsumer = (
  config: DurableConsumerClientConfig,
  input: AcquireConsumerInput,
): Effect.Effect<AcquiredConsumer, DurableConsumerError, HttpClient.HttpClient> =>
  execute(
    requestJson(
      "POST",
      `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/consumers/${
        encodeURIComponent(input.consumerId)
      }/acquire`,
      { worker: input.worker },
    ),
  ).pipe(
    Effect.flatMap(response => expectJson("consumer.acquire", AcquiredConsumerSchema, response)),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.acquire", {
      attributes: {
        "durable_streams.consumer.id": input.consumerId,
        "durable_streams.consumer.worker": input.worker,
      },
    }),
  )

const ackConsumer = (
  config: DurableConsumerClientConfig,
  input: AckConsumerInput,
): Effect.Effect<AckedConsumer, DurableConsumerError, HttpClient.HttpClient> =>
  execute(
    requestJson(
      "POST",
      `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/consumers/${
        encodeURIComponent(input.consumerId)
      }/ack`,
      { offsets: input.offsets },
      { authorization: `Bearer ${input.token}` },
    ),
  ).pipe(
    Effect.flatMap(response => expectJson("consumer.ack", AckedConsumerSchema, response)),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.ack", {
      attributes: { "durable_streams.consumer.id": input.consumerId },
    }),
  )

const releaseConsumer = (
  config: DurableConsumerClientConfig,
  input: ReleaseConsumerInput,
): Effect.Effect<ReleasedConsumer, DurableConsumerError, HttpClient.HttpClient> =>
  execute(
    HttpClientRequest.post(
      `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/consumers/${
        encodeURIComponent(input.consumerId)
      }/release`,
    ).pipe(HttpClientRequest.setHeader("authorization", `Bearer ${input.token}`)),
  ).pipe(
    Effect.flatMap(response => expectJson("consumer.release", ReleasedConsumerSchema, response)),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.release", {
      attributes: { "durable_streams.consumer.id": input.consumerId },
    }),
  )

export interface DurableConsumerClientService {
  readonly createStream: (
    routePath: string,
  ) => Effect.Effect<void, DurableConsumerError>
  readonly appendStream: (
    input: AppendStreamInput,
  ) => Effect.Effect<{ readonly offset: string }, DurableConsumerError>
  readonly registerConsumer: (
    input: RegisterConsumerInput,
  ) => Effect.Effect<ConsumerInfo, DurableConsumerError>
  readonly configurePullWake: (
    input: ConfigurePullWakeInput,
  ) => Effect.Effect<void, DurableConsumerError>
  readonly getConsumer: (
    consumerId: string,
  ) => Effect.Effect<ConsumerInfo, DurableConsumerError>
  readonly acquireConsumer: (
    input: AcquireConsumerInput,
  ) => Effect.Effect<AcquiredConsumer, DurableConsumerError>
  readonly ackConsumer: (
    input: AckConsumerInput,
  ) => Effect.Effect<AckedConsumer, DurableConsumerError>
  readonly releaseConsumer: (
    input: ReleaseConsumerInput,
  ) => Effect.Effect<ReleasedConsumer, DurableConsumerError>
}

export class DurableConsumerClient extends Context.Tag("@firegrid/fluent-runtime/ConsumerSubstrate/DurableConsumerClient")<
  DurableConsumerClient,
  DurableConsumerClientService
>() {}

export const makeDurableConsumerClient = (
  config: DurableConsumerClientConfig,
  httpClient: HttpClient.HttpClient,
): DurableConsumerClientService => {
  const provideHttp = bindConsumerHttp(httpClient)

  return {
    createStream: (routePath) => provideHttp(createStream(config, routePath)),
    appendStream: (input) => provideHttp(appendStream(config, input)),
    registerConsumer: (input) => provideHttp(registerConsumer(config, input)),
    configurePullWake: (input) => provideHttp(configurePullWake(config, input)),
    getConsumer: (consumerId) => provideHttp(getConsumer(config, consumerId)),
    acquireConsumer: (input) => provideHttp(acquireConsumer(config, input)),
    ackConsumer: (input) => provideHttp(ackConsumer(config, input)),
    releaseConsumer: (input) => provideHttp(releaseConsumer(config, input)),
  }
}

export const DurableConsumerClientLive = (
  config: DurableConsumerClientConfig,
): Layer.Layer<DurableConsumerClient> =>
  Layer.effect(
    DurableConsumerClient,
    Effect.map(
      HttpClient.HttpClient,
      (httpClient) => makeDurableConsumerClient(config, httpClient),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer))

export const ackAfterDurableProductOutcome = <A, E, R>(
  client: DurableConsumerClientService,
  ack: AckConsumerInput,
  outcome: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | DurableConsumerError, R> =>
  outcome.pipe(
    Effect.tap(() => client.ackConsumer(ack)),
    Effect.withSpan("fluent_runtime.worker_redrive.consumer.ack_after_product", {
      attributes: { "durable_streams.consumer.id": ack.consumerId },
    }),
  )
