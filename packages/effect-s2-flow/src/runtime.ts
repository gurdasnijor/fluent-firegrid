import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import { RemoteChdbSpanExporter } from "@firegrid/observability"
import { SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base"
import {
  AppendInput,
  AppendRecord,
  basin,
  basins,
  layer as S2Layer,
  type S2Error,
  stream as s2Stream,
  type StreamApi
} from "effect-s2"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Random from "effect/Random"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"

const defaultBasin = "effect-s2-flow"
const invocationPrefix = (serviceName: string): string => `${serviceName}.invocation.`
const invocationStream = (serviceName: string, invocationId: string): string =>
  `${invocationPrefix(serviceName)}${invocationId}`

export class FlowError extends Data.TaggedError("FlowError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export interface FlowRuntimeConfig {
  readonly s2Endpoint: string
  readonly basin?: string
}

export class FlowRuntime extends Context.Service<FlowRuntime, Required<FlowRuntimeConfig>>()(
  "effect-s2-flow/runtime/FlowRuntime"
) {
  static readonly layer = (config: FlowRuntimeConfig): Layer.Layer<FlowRuntime> =>
    Layer.succeed(
      FlowRuntime,
      FlowRuntime.of({
        basin: config.basin ?? defaultBasin,
        s2Endpoint: config.s2Endpoint
      })
    )
}

export interface ServiceDefinition<Handlers extends ServiceHandlers> {
  readonly name: string
  readonly handlers: Handlers
}

export type ServiceHandler<Input = any, Output = any> = (
  input: Input
) => Generator<Effect.Effect<any, any, InvocationScope>, Output, any>

export type ServiceHandlers = Record<string, ServiceHandler>

export interface ClientOptions {
  readonly invocationId?: string
}

type HandlerInput<Handler> = Handler extends (input: infer Input) => Generator<any, any, any> ? Input : never

type HandlerOutput<Handler> = Handler extends (input: any) => Generator<any, infer Output, any> ? Output : never

export type ServiceClient<Handlers extends ServiceHandlers> = {
  readonly [Name in keyof Handlers]: (
    input: HandlerInput<Handlers[Name]>
  ) => Effect.Effect<HandlerOutput<Handlers[Name]>, FlowError, FlowRuntime>
}

export const service = <Handlers extends ServiceHandlers>(
  definition: ServiceDefinition<Handlers>
): ServiceDefinition<Handlers> => definition

const s2Layer = (endpoint: string) =>
  S2Layer({
    accessToken: "s2_access_token",
    endpoints: {
      account: endpoint,
      basin: endpoint
    },
    retry: { maxAttempts: 1 }
  })

const flowS2Error = (message: string) => (cause: S2Error): FlowError => new FlowError({ message, cause })

const withS2 = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  runtime: Required<FlowRuntimeConfig>
) => effect.pipe(Effect.provide(s2Layer(runtime.s2Endpoint)))

type FlowRecord =
  | {
    readonly _tag: "Invoke"
    readonly service: string
    readonly method: string
    readonly input: unknown
  }
  | {
    readonly _tag: "StepCompleted"
    readonly stepName: string
    readonly value: unknown
  }
  | {
    readonly _tag: "CheckpointAdvanced"
    readonly nextSeqNum: number
  }
  | {
    readonly _tag: "Completed"
    readonly value: unknown
  }
  | {
    readonly _tag: "Failed"
    readonly message: string
  }

const encodeRecord = (
  record: FlowRecord,
  headers: ReadonlyArray<readonly [string, string]>
): AppendRecord =>
  AppendRecord.string({
    body: JSON.stringify(record),
    headers
  })

const decodeRecord = (body: string): FlowRecord => JSON.parse(body) as FlowRecord

const recordTypeHeader = (type: FlowRecord["_tag"]): ReadonlyArray<readonly [string, string]> => [
  ["effect-s2-flow.record.type", type]
]

const stepHeaders = (type: FlowRecord["_tag"], stepName: string): ReadonlyArray<readonly [string, string]> => [
  ...recordTypeHeader(type),
  ["effect-s2-flow.step.name", stepName]
]

const ensureInvocationStream = Effect.fn("effect-s2-flow.ensureInvocationStream")(function*(
  runtime: Required<FlowRuntimeConfig>,
  streamName: string
) {
  yield* withS2(
    basins.ensure({ basin: runtime.basin }).pipe(
      Effect.mapError(flowS2Error(`failed to ensure basin ${runtime.basin}`))
    ),
    runtime
  )
  yield* withS2(
    basin(runtime.basin).pipe(
      Effect.flatMap((basinApi) => basinApi.streams.ensure({ stream: streamName })),
      Effect.mapError(flowS2Error(`failed to ensure invocation stream ${streamName}`))
    ),
    runtime
  )
  return yield* withS2(
    s2Stream(runtime.basin, streamName).pipe(
      Effect.mapError(flowS2Error(`failed to open invocation stream ${streamName}`))
    ),
    runtime
  )
})

const collectCurrentRecords = Effect.fn("effect-s2-flow.collectCurrentRecords")(function*(streamApi: StreamApi) {
  const tail = yield* streamApi.checkTail().pipe(
    Effect.mapError(flowS2Error("failed to check invocation tail"))
  )
  if (tail.tail.seqNum === 0) {
    return {
      nextSeqNum: 0,
      records: [] as ReadonlyArray<FlowRecord>
    }
  }
  const records = yield* streamApi.readSession({
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: tail.tail.seqNum } }
  }).pipe(
    Stream.runCollect,
    Effect.mapError(flowS2Error("failed to read invocation journal")),
    Effect.map((items) => Array.from(items, (record) => decodeRecord(record.body)))
  )
  return { nextSeqNum: tail.tail.seqNum, records }
})

export const client = <Handlers extends ServiceHandlers>(
  definition: ServiceDefinition<Handlers>,
  options: ClientOptions = {}
): ServiceClient<Handlers> =>
  new Proxy({}, {
    get: (_target, property) =>
      typeof property === "string" && property in definition.handlers
        ? (input: unknown) => invoke(definition.name, property, input, options) as any
        : undefined
  }) as ServiceClient<Handlers>

const invoke = Effect.fn("effect-s2-flow.client.invoke")(function*(
  serviceName: string,
  method: string,
  input: unknown,
  options: ClientOptions
) {
  const runtime = yield* FlowRuntime
  const generated = yield* Random.nextInt
  const invocationId = options.invocationId ?? `${serviceName}-${Math.abs(generated)}`
  const streamName = invocationStream(serviceName, invocationId)
  const streamApi = yield* ensureInvocationStream(runtime, streamName)

  yield* streamApi.append(
    AppendInput.create([
      encodeRecord({
        _tag: "Invoke",
        input,
        method,
        service: serviceName
      }, recordTypeHeader("Invoke"))
    ], { matchSeqNum: 0 })
  ).pipe(
    Effect.mapError(flowS2Error(`failed to append Invoke for ${serviceName}.${method}`)),
    Effect.withSpan("effect-s2-flow.client.invoke", {
      attributes: {
        "effect-s2-flow.invocation.stream": streamName,
        "effect-s2-flow.method": method,
        "effect-s2-flow.service": serviceName
      }
    })
  )

  return yield* streamApi.readSession({
    start: { from: { seqNum: 0 } }
  }).pipe(
    Stream.map((record) => decodeRecord(record.body)),
    Stream.filterMap((record) => {
      if (record._tag === "Completed" || record._tag === "Failed") return Result.succeed(record)
      return Result.fail(record)
    }),
    Stream.runHead,
    Effect.flatMap((completed) =>
      Option.match(completed, {
        onNone: () => new FlowError({ message: `invocation ${streamName} completed without a result` }),
        onSome: (record) =>
          record._tag === "Completed"
            ? Effect.succeed(record.value)
            : new FlowError({ message: `invocation ${streamName} failed: ${record.message}` })
      })
    ),
    Effect.mapError((cause) =>
      cause instanceof FlowError ? cause : new FlowError({
        message: `failed waiting for invocation ${streamName}`,
        cause
      })
    )
  )
})

export interface CurrentInvocationScope {
  readonly streamName: string
  readonly streamApi: StreamApi
  readonly steps: Ref.Ref<ReadonlyMap<string, unknown>>
  readonly nextSeqNum: Ref.Ref<number>
}

export class InvocationScope extends Context.Service<InvocationScope, CurrentInvocationScope>()(
  "effect-s2-flow/runtime/InvocationScope"
) {}

export const run = <A, E, R>(
  name: string,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E | FlowError, R | InvocationScope> =>
  Effect.gen(function*() {
    const scope = yield* InvocationScope
    const steps = yield* Ref.get(scope.steps)
    if (steps.has(name)) {
      return steps.get(name) as A
    }

    const value = yield* effect
    const matchSeqNum = yield* Ref.get(scope.nextSeqNum)
    const checkpointNext = matchSeqNum + 2
    yield* scope.streamApi.append(
      AppendInput.create([
        encodeRecord({
          _tag: "StepCompleted",
          stepName: name,
          value
        }, stepHeaders("StepCompleted", name)),
        encodeRecord({
          _tag: "CheckpointAdvanced",
          nextSeqNum: checkpointNext
        }, recordTypeHeader("CheckpointAdvanced"))
      ], { matchSeqNum })
    ).pipe(
      Effect.tap(() =>
        Effect.annotateCurrentSpan({
          "effect-s2-flow.invocation.stream": scope.streamName,
          "effect-s2-flow.record.type": "StepCompleted",
          "effect-s2-flow.step.name": name
        })
      ),
      Effect.mapError(flowS2Error(`failed to append StepCompleted for ${name}`)),
      Effect.withSpan("effect-s2-flow.journal.append.ack", {
        attributes: {
          "effect-s2-flow.invocation.stream": scope.streamName,
          "effect-s2-flow.record.type": "StepCompleted",
          "effect-s2-flow.step.name": name
        }
      })
    )
    yield* Ref.update(scope.steps, (current) => new Map(current).set(name, value))
    yield* Ref.set(scope.nextSeqNum, checkpointNext)
    return value
  })

export interface ServeOptions {
  readonly services: ReadonlyArray<ServiceDefinition<ServiceHandlers>>
  readonly pollInterval?: Duration.Input
}

const methodEffect = (
  serviceDefinition: ServiceDefinition<ServiceHandlers>,
  method: string,
  input: unknown
): Effect.Effect<unknown, unknown, InvocationScope> => {
  const handler = serviceDefinition.handlers[method]
  if (handler === undefined) {
    return new FlowError({ message: `unknown handler ${serviceDefinition.name}.${method}` })
  }
  return Effect.gen(function*() {
    return yield* handler(input)
  })
}

const processInvocation = Effect.fn("effect-s2-flow.processInvocation")(function*(
  runtime: Required<FlowRuntimeConfig>,
  serviceDefinition: ServiceDefinition<ServiceHandlers>,
  streamName: string
) {
  const streamApi = yield* ensureInvocationStream(runtime, streamName)
  const snapshot = yield* collectCurrentRecords(streamApi).pipe(
    Effect.withSpan("effect-s2-flow.owner.rehydrate", {
      attributes: {
        "effect-s2-flow.invocation.stream": streamName,
        "effect-s2-flow.service": serviceDefinition.name
      }
    })
  )
  if (snapshot.records.some((record) => record._tag === "Completed" || record._tag === "Failed")) return
  const invokeRecord = snapshot.records.find((record) => record._tag === "Invoke")
  if (invokeRecord?._tag !== "Invoke") return

  const steps = new Map<string, unknown>()
  snapshot.records.forEach((record) => {
    if (record._tag === "StepCompleted") {
      steps.set(record.stepName, record.value)
    }
  })

  const stepRef = yield* Ref.make<ReadonlyMap<string, unknown>>(steps)
  const nextSeqNum = yield* Ref.make(snapshot.nextSeqNum)
  const result = yield* Effect.exit(
    methodEffect(serviceDefinition, invokeRecord.method, invokeRecord.input).pipe(
      Effect.provideService(InvocationScope, {
        nextSeqNum,
        steps: stepRef,
        streamApi,
        streamName
      })
    )
  )

  const matchSeqNum = yield* Ref.get(nextSeqNum)
  if (result._tag === "Success") {
    yield* streamApi.append(
      AppendInput.create([
        encodeRecord({
          _tag: "Completed",
          value: result.value
        }, recordTypeHeader("Completed"))
      ], { matchSeqNum })
    ).pipe(
      Effect.mapError(flowS2Error(`failed to append Completed for ${streamName}`)),
      Effect.withSpan("effect-s2-flow.invocation.completed", {
        attributes: {
          "effect-s2-flow.invocation.stream": streamName,
          "effect-s2-flow.service": serviceDefinition.name
        }
      })
    )
    return
  }

  yield* streamApi.append(
    AppendInput.create([
      encodeRecord({
        _tag: "Failed",
        message: String(result.cause)
      }, recordTypeHeader("Failed"))
    ], { matchSeqNum })
  ).pipe(
    Effect.mapError(flowS2Error(`failed to append Failed for ${streamName}`))
  )
})

const discoverInvocations = Effect.fn("effect-s2-flow.discoverInvocations")(function*(
  runtime: Required<FlowRuntimeConfig>,
  serviceDefinition: ServiceDefinition<ServiceHandlers>
) {
  const basinApi = yield* withS2(
    basin(runtime.basin).pipe(
      Effect.mapError(flowS2Error(`failed to open basin ${runtime.basin}`))
    ),
    runtime
  )
  const prefix = invocationPrefix(serviceDefinition.name)
  return yield* basinApi.streams.list({ prefix, limit: 1000 }).pipe(
    Effect.map((response) => response.streams.map((stream) => stream.name)),
    Effect.mapError(flowS2Error(`failed to list invocations for ${serviceDefinition.name}`))
  )
})

export const serve = Effect.fn("effect-s2-flow.serve")(function*(options: ServeOptions) {
  const runtime = yield* FlowRuntime
  yield* withS2(
    basins.ensure({ basin: runtime.basin }).pipe(
      Effect.mapError(flowS2Error(`failed to ensure basin ${runtime.basin}`))
    ),
    runtime
  )
  const active = yield* Ref.make<ReadonlySet<string>>(new Set())
  const pollInterval = options.pollInterval ?? "50 millis"

  const scan = Effect.forEach(options.services, (serviceDefinition) =>
    Effect.gen(function*() {
      const invocations = yield* discoverInvocations(runtime, serviceDefinition)
      yield* Effect.forEach(invocations, (streamName) =>
        Effect.gen(function*() {
          const current = yield* Ref.get(active)
          if (current.has(streamName)) return
          yield* Ref.update(active, (set) => new Set(set).add(streamName))
          yield* processInvocation(runtime, serviceDefinition, streamName).pipe(
            Effect.catch((cause) => Effect.logError(`failed processing ${streamName}`, cause)),
            Effect.forkDetach
          )
        }), { discard: true })
    }), { discard: true })

  return yield* scan.pipe(
    Effect.andThen(Effect.sleep(pollInterval)),
    Effect.forever
  )
})

export const flowRuntimeLayerFromEnv = (): Layer.Layer<FlowRuntime, FlowError> => {
  const s2Endpoint = process.env.S2_ENDPOINT
  if (s2Endpoint === undefined || s2Endpoint === "") {
    return Layer.effect(
      FlowRuntime,
      Effect.fail(new FlowError({ message: "S2_ENDPOINT is required for effect-s2-flow host runtime" }))
    )
  }
  return FlowRuntime.layer({ s2Endpoint })
}

export const hostTraceLayerFromEnv = () => {
  const endpoint = process.env.FIREGRID_OTEL_SPAN_ENDPOINT
  if (endpoint === undefined || endpoint === "") return Layer.empty
  return NodeSdk.layer(() => ({
    resource: {
      serviceName: "effect-s2-flow-host"
    },
    spanProcessor: [new SimpleSpanProcessor(new RemoteChdbSpanExporter(endpoint))]
  }))
}
