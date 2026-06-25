import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import { RemoteChdbSpanExporter } from "@firegrid/observability"
import { SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { AppendInput, type AppendOptions, AppendRecord, type StreamApi } from "effect-s2"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Random from "effect/Random"
import * as Ref from "effect/Ref"
import * as Semaphore from "effect/Semaphore"

import { type BatchTooLarge, FlowError } from "./FlowError.ts"
import {
  appendAtomic,
  defaultBasin,
  encodeRecord,
  ensureBasin,
  ensureInvocationJournalStream,
  flowS2Error,
  invocationPrefix,
  invocationStream,
  listInvocationJournalStreams,
  objectPrefix,
  objectStream,
  readCurrentFenceToken,
  readInvocationJournal,
  recordTypeHeader,
  stateHeaders,
  stepHeaders
} from "./InvocationJournal.ts"

export interface FlowRuntimeConfig {
  readonly s2Endpoint: string
  readonly basin?: string
}

export type FlowRuntimeError = BatchTooLarge | FlowError

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
  readonly kind: "service"
  readonly name: string
  readonly handlers: Handlers
}

export interface ObjectDefinition<Handlers extends ServiceHandlers> {
  readonly kind: "object"
  readonly name: string
  readonly handlers: Handlers
}

export type FlowDefinition<Handlers extends ServiceHandlers> =
  | ServiceDefinition<Handlers>
  | ObjectDefinition<Handlers>

export type ServiceHandler<Input = any, Output = any, Error = any> = (
  input: Input
) => Effect.Effect<Output, Error, InvocationScope>

export type ServiceHandlers = Record<string, ServiceHandler>

export interface ClientOptions {
  readonly invocationId?: string
}

interface ResolvedClientOptions {
  readonly invocationId?: string
  readonly key?: string
}

type HandlerInput<Handler> = Handler extends (input: infer Input) => Effect.Effect<any, any, any> ? Input : never

type HandlerOutput<Handler> = Handler extends (input: any) => Effect.Effect<infer Output, any, any> ? Output : never

export type ServiceClient<Handlers extends ServiceHandlers> = {
  readonly [Name in keyof Handlers]: (
    input: HandlerInput<Handlers[Name]>
  ) => Effect.Effect<HandlerOutput<Handlers[Name]>, FlowError, FlowRuntime>
}

export const service = <Handlers extends ServiceHandlers>(
  definition: Omit<ServiceDefinition<Handlers>, "kind">
): ServiceDefinition<Handlers> => ({ ...definition, kind: "service" })

export const object = <Handlers extends ServiceHandlers>(
  definition: Omit<ObjectDefinition<Handlers>, "kind">
): ObjectDefinition<Handlers> => ({ ...definition, kind: "object" })

export function client<Handlers extends ServiceHandlers>(
  definition: ServiceDefinition<Handlers>,
  options?: ClientOptions
): ServiceClient<Handlers>
export function client<Handlers extends ServiceHandlers>(
  definition: ObjectDefinition<Handlers>,
  key: string,
  options?: ClientOptions
): ServiceClient<Handlers>
export function client<Handlers extends ServiceHandlers>(
  definition: FlowDefinition<Handlers>,
  keyOrOptions: string | ClientOptions = {},
  maybeOptions: ClientOptions = {}
): ServiceClient<Handlers> {
  const options: ResolvedClientOptions = typeof keyOrOptions === "string"
    ? { ...maybeOptions, key: keyOrOptions }
    : keyOrOptions
  return new Proxy({}, {
    get: (_target, property) =>
      typeof property === "string" && property in definition.handlers
        ? (input: unknown) => invoke(definition, property, input, options) as any
        : undefined
  }) as ServiceClient<Handlers>
}

const invoke = Effect.fn("effect-s2-flow.client.invoke")(function*(
  definition: FlowDefinition<ServiceHandlers>,
  method: string,
  input: unknown,
  options: ResolvedClientOptions
) {
  const runtime = yield* FlowRuntime
  const generated = yield* Random.nextInt
  const requestId = options.invocationId ?? `${definition.name}-${Math.abs(generated)}`
  if (definition.kind === "object" && options.key === undefined) {
    return yield* new FlowError({ message: `object client ${definition.name}.${method} requires a key` })
  }
  const streamName = definition.kind === "service"
    ? invocationStream(definition.name, requestId)
    : objectStream(definition.name, options.key!)
  const streamApi = yield* ensureInvocationJournalStream(runtime, streamName)

  const invokeRecords = [
    encodeRecord({
      _tag: "Invoke",
      input,
      method,
      requestId,
      service: definition.name
    }, recordTypeHeader("Invoke"))
  ]
  const journalHasSameInvoke = Effect.fn("effect-s2-flow.client.journalHasSameInvoke")(function*(
    currentStreamApi: StreamApi
  ) {
    const journal = yield* readInvocationJournal(currentStreamApi)
    const existing = journal.records.find((record) => record._tag === "Invoke" && record.requestId === requestId)
    if (existing === undefined) return false
    if (
      existing._tag === "Invoke"
      && existing.service === definition.name
      && existing.method === method
      && Equal.equals(existing.input, input)
    ) {
      return true
    }
    return yield* new FlowError({
      message: `invocation ${requestId} already exists with a different request shape`
    })
  })
  const appendInvoke = Effect.fn("effect-s2-flow.client.appendInvoke")(function*(
    remainingRetries: number,
    currentStreamApi: StreamApi
  ): Generator<Effect.Effect<any, FlowError>, void, any> {
    const exit = yield* Effect.exit(
      currentStreamApi.append(
        definition.kind === "service"
          ? AppendInput.create(invokeRecords, { matchSeqNum: 0 })
          : AppendInput.create(invokeRecords)
      ).pipe(
        Effect.mapError(flowS2Error(`failed to append Invoke for ${definition.name}.${method}`))
      )
    )
    if (exit._tag === "Success") return
    if (definition.kind === "service") {
      const alreadySubmitted = yield* journalHasSameInvoke(currentStreamApi)
      if (alreadySubmitted) return
    }
    if (remainingRetries <= 0) {
      return yield* Effect.failCause(exit.cause)
    }
    yield* Effect.sleep("50 millis")
    const retryStreamApi = yield* ensureInvocationJournalStream(runtime, streamName)
    return yield* appendInvoke(remainingRetries - 1, retryStreamApi)
  })

  yield* appendInvoke(10, streamApi).pipe(
    Effect.withSpan("effect-s2-flow.client.invoke", {
      attributes: {
        "effect-s2-flow.invocation.stream": streamName,
        "effect-s2-flow.method": method,
        "effect-s2-flow.request.id": requestId,
        "effect-s2-flow.service": definition.name
      }
    })
  )

  const awaitCompletion: Effect.Effect<unknown, FlowError> = Effect.suspend(() =>
    readInvocationJournal(streamApi).pipe(
      Effect.flatMap((journal) => {
        const completed = journal.records.find((record) =>
          (record._tag === "Completed" || record._tag === "Failed") && record.requestId === requestId
        )
        if (completed === undefined) {
          return Effect.sleep("25 millis").pipe(Effect.andThen(awaitCompletion))
        }
        if (completed._tag === "Completed") {
          return Effect.succeed(completed.value)
        }
        if (completed._tag === "Failed") {
          return new FlowError({ message: `invocation ${streamName} failed: ${completed.message}` })
        }
        return Effect.sleep("25 millis").pipe(Effect.andThen(awaitCompletion))
      })
    )
  )
  return yield* awaitCompletion
})

export interface CurrentInvocationScope {
  readonly appendLock: Semaphore.Semaphore
  readonly fencingToken: Ref.Ref<string | undefined>
  readonly pendingRecords: Ref.Ref<ReadonlyArray<AppendRecord>>
  readonly requestId: string
  readonly streamName: string
  readonly streamApi: StreamApi
  readonly states: Ref.Ref<ReadonlyMap<string, unknown>>
  readonly steps: Ref.Ref<ReadonlyMap<string, unknown>>
  readonly nextSeqNum: Ref.Ref<number>
}

export class InvocationScope extends Context.Service<InvocationScope, CurrentInvocationScope>()(
  "effect-s2-flow/runtime/InvocationScope"
) {}

const appendOptions = (matchSeqNum: number, fencingToken: string | undefined): AppendOptions =>
  fencingToken === undefined ? { matchSeqNum } : { fencingToken, matchSeqNum }

export const run = <A, E, R>(
  name: string,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E | FlowRuntimeError, R | InvocationScope> =>
  Effect.gen(function*() {
    const scope = yield* InvocationScope
    const steps = yield* Ref.get(scope.steps)
    if (steps.has(name)) {
      return steps.get(name) as A
    }

    const value = yield* effect
    yield* scope.appendLock.withPermit(
      Effect.gen(function*() {
        const matchSeqNum = yield* Ref.get(scope.nextSeqNum)
        const fencingToken = yield* Ref.get(scope.fencingToken)
        const checkpointNext = matchSeqNum + 2
        const ack = yield* appendAtomic(
          scope.streamApi,
          [
            encodeRecord({
              _tag: "StepCompleted",
              requestId: scope.requestId,
              stepName: name,
              value
            }, stepHeaders("StepCompleted", name)),
            encodeRecord({
              _tag: "CheckpointAdvanced",
              nextSeqNum: checkpointNext
            }, recordTypeHeader("CheckpointAdvanced"))
          ],
          appendOptions(matchSeqNum, fencingToken),
          `failed to append StepCompleted for ${name}`
        ).pipe(
          Effect.tap(() =>
            Effect.annotateCurrentSpan({
              "effect-s2-flow.invocation.stream": scope.streamName,
              "effect-s2-flow.fencing.token": fencingToken ?? "",
              "effect-s2-flow.record.type": "StepCompleted",
              "effect-s2-flow.step.name": name
            })
          ),
          Effect.withSpan("effect-s2-flow.journal.append.ack", {
            attributes: {
              "effect-s2-flow.invocation.stream": scope.streamName,
              "effect-s2-flow.fencing.token": fencingToken ?? "",
              "effect-s2-flow.record.type": "StepCompleted",
              "effect-s2-flow.step.name": name
            }
          })
        )
        yield* Ref.update(scope.steps, (current) => new Map(current).set(name, value))
        yield* Ref.set(scope.nextSeqNum, ack.end.seqNum)
      })
    )
    return value
  })

export interface StateHandle<A> {
  readonly get: Effect.Effect<A, FlowError, InvocationScope>
  readonly set: (value: A) => Effect.Effect<void, FlowError, InvocationScope>
  readonly update: (f: (value: A) => A) => Effect.Effect<A, FlowError, InvocationScope>
}

export const state = <A>(name: string, initial: A): StateHandle<A> => ({
  get: Effect.gen(function*() {
    const scope = yield* InvocationScope
    const states = yield* Ref.get(scope.states)
    return (states.has(name) ? states.get(name) : initial) as A
  }),
  set: (value) =>
    Effect.gen(function*() {
      const scope = yield* InvocationScope
      yield* Ref.update(scope.states, (current) => new Map(current).set(name, value))
      yield* Ref.update(scope.pendingRecords, (records) => [
        ...records,
        encodeRecord({
          _tag: "StateChanged",
          stateName: name,
          value
        }, stateHeaders(name))
      ])
    }),
  update: (f) =>
    Effect.gen(function*() {
      const current = yield* state(name, initial).get
      const next = f(current)
      yield* state(name, initial).set(next)
      return next
    })
})

export interface ServeOptions {
  readonly services: ReadonlyArray<FlowDefinition<ServiceHandlers>>
  readonly pollInterval?: Duration.Input
}

const methodEffect = (
  serviceDefinition: FlowDefinition<ServiceHandlers>,
  method: string,
  input: unknown
): Effect.Effect<unknown, unknown, InvocationScope> => {
  const handler = serviceDefinition.handlers[method]
  if (handler === undefined) {
    return new FlowError({ message: `unknown handler ${serviceDefinition.name}.${method}` })
  }
  return handler(input)
}

const hostFenceToken = (): string => (process.env.FIREGRID_HOST_ID ?? `pid-${process.pid}`).slice(0, 36)

const fenceLeaseMillis = 8_000

const fenceRefreshInterval: Duration.Input = "2 seconds"

const hostId = (): string => (process.env.FIREGRID_HOST_ID ?? `pid-${process.pid}`).replace(/:/g, "-").slice(0, 20)

const makeFenceToken = (): string => `${hostId()}:${(Date.now() + fenceLeaseMillis).toString(36)}`

const parseFenceToken = (
  token: string
): { readonly hostId: string; readonly deadlineMillis: number } | undefined => {
  const separator = token.lastIndexOf(":")
  if (separator <= 0) return undefined
  const deadlineMillis = Number.parseInt(token.slice(separator + 1), 36)
  if (!Number.isFinite(deadlineMillis)) return undefined
  return {
    deadlineMillis,
    hostId: token.slice(0, separator)
  }
}

const activeForeignFence = (token: string | undefined): boolean => {
  if (token === undefined) return false
  const parsed = parseFenceToken(token)
  if (parsed === undefined) return token !== hostFenceToken()
  return parsed.hostId !== hostId() && parsed.deadlineMillis > Date.now()
}

const claimObjectFence = Effect.fn("effect-s2-flow.claimObjectFence")(function*(
  streamApi: StreamApi,
  streamName: string
) {
  const expectedToken = yield* readCurrentFenceToken(streamApi)
  if (activeForeignFence(expectedToken)) {
    yield* Effect.void.pipe(
      Effect.withSpan("effect-s2-flow.fence.busy", {
        attributes: {
          "effect-s2-flow.fencing.expected_token": expectedToken ?? "",
          "effect-s2-flow.invocation.stream": streamName
        }
      })
    )
    return undefined
  }
  const token = makeFenceToken()
  const ack = yield* appendAtomic(
    streamApi,
    [AppendRecord.fence(token)],
    { fencingToken: expectedToken ?? "" },
    `failed to claim fence for ${streamName}`
  ).pipe(
    Effect.withSpan("effect-s2-flow.fence.claim", {
      attributes: {
        "effect-s2-flow.fencing.expected_token": expectedToken ?? "",
        "effect-s2-flow.fencing.token": token,
        "effect-s2-flow.invocation.stream": streamName
      }
    })
  )
  return {
    nextSeqNum: ack.end.seqNum,
    token
  }
})

const refreshObjectFence = Effect.fn("effect-s2-flow.refreshObjectFence")(function*(
  streamApi: StreamApi,
  streamName: string,
  appendLock: Semaphore.Semaphore,
  nextSeqNum: Ref.Ref<number>,
  fencingToken: Ref.Ref<string | undefined>
) {
  const refreshOnce = appendLock.withPermit(
    Effect.gen(function*() {
      const previousToken = yield* Ref.get(fencingToken)
      if (previousToken === undefined) return
      const matchSeqNum = yield* Ref.get(nextSeqNum)
      const token = makeFenceToken()
      const ack = yield* appendAtomic(
        streamApi,
        [AppendRecord.fence(token)],
        appendOptions(matchSeqNum, previousToken),
        `failed to refresh fence for ${streamName}`
      ).pipe(
        Effect.withSpan("effect-s2-flow.fence.refresh", {
          attributes: {
            "effect-s2-flow.fencing.previous_token": previousToken,
            "effect-s2-flow.fencing.token": token,
            "effect-s2-flow.invocation.stream": streamName
          }
        })
      )
      yield* Ref.set(fencingToken, token)
      yield* Ref.set(nextSeqNum, ack.end.seqNum)
    })
  )

  return yield* Effect.gen(function*() {
    yield* Effect.sleep(fenceRefreshInterval)
    yield* refreshOnce
  }).pipe(
    Effect.forever
  )
})

const processInvocation = Effect.fn("effect-s2-flow.processInvocation")(function*(
  runtime: Required<FlowRuntimeConfig>,
  serviceDefinition: FlowDefinition<ServiceHandlers>,
  streamName: string
) {
  const streamApi = yield* ensureInvocationJournalStream(runtime, streamName)
  const snapshot = yield* readInvocationJournal(streamApi).pipe(
    Effect.withSpan("effect-s2-flow.owner.rehydrate", {
      attributes: {
        "effect-s2-flow.invocation.stream": streamName,
        "effect-s2-flow.service": serviceDefinition.name
      }
    })
  )
  const completedRequests = new Set(
    snapshot.records.flatMap((record) =>
      record._tag === "Completed" || record._tag === "Failed" ? [record.requestId] : []
    )
  )
  const invokeRecord = snapshot.records.find((record) =>
    record._tag === "Invoke" && !completedRequests.has(record.requestId)
  )
  if (invokeRecord?._tag !== "Invoke") return

  const fence = serviceDefinition.kind === "object"
    ? yield* claimObjectFence(streamApi, streamName)
    : undefined
  if (serviceDefinition.kind === "object" && fence === undefined) return

  const steps = new Map<string, unknown>()
  const states = new Map<string, unknown>()
  snapshot.records.forEach((record) => {
    if (record._tag === "StepCompleted" && record.requestId === invokeRecord.requestId) {
      steps.set(record.stepName, record.value)
    } else if (record._tag === "StateChanged") {
      states.set(record.stateName, record.value)
    }
  })

  const pendingRecords = yield* Ref.make<ReadonlyArray<AppendRecord>>([])
  const stateRef = yield* Ref.make<ReadonlyMap<string, unknown>>(states)
  const stepRef = yield* Ref.make<ReadonlyMap<string, unknown>>(steps)
  const nextSeqNum = yield* Ref.make(fence?.nextSeqNum ?? snapshot.nextSeqNum)
  const fencingToken = yield* Ref.make<string | undefined>(fence?.token)
  const appendLock = yield* Semaphore.make(1)
  const refreshFiber = fence === undefined
    ? undefined
    : yield* refreshObjectFence(streamApi, streamName, appendLock, nextSeqNum, fencingToken).pipe(
      Effect.catch((cause) => Effect.logError(`failed refreshing fence for ${streamName}`, cause)),
      Effect.forkChild
    )
  const result = yield* Effect.exit(
    methodEffect(serviceDefinition, invokeRecord.method, invokeRecord.input).pipe(
      Effect.provideService(InvocationScope, {
        appendLock,
        fencingToken,
        nextSeqNum,
        pendingRecords,
        requestId: invokeRecord.requestId,
        states: stateRef,
        steps: stepRef,
        streamApi,
        streamName
      }),
      Effect.ensuring(refreshFiber === undefined ? Effect.void : Fiber.interrupt(refreshFiber))
    )
  )

  if (result._tag === "Success") {
    yield* appendLock.withPermit(
      Effect.gen(function*() {
        const matchSeqNum = yield* Ref.get(nextSeqNum)
        const token = yield* Ref.get(fencingToken)
        const records = yield* Ref.get(pendingRecords)
        const ack = yield* appendAtomic(
          streamApi,
          [
            ...records,
            encodeRecord({
              _tag: "Completed",
              requestId: invokeRecord.requestId,
              value: result.value
            }, recordTypeHeader("Completed"))
          ],
          appendOptions(matchSeqNum, token),
          `failed to append Completed for ${streamName}`
        ).pipe(
          Effect.withSpan("effect-s2-flow.invocation.completed", {
            attributes: {
              "effect-s2-flow.invocation.stream": streamName,
              "effect-s2-flow.fencing.token": token ?? "",
              "effect-s2-flow.request.id": invokeRecord.requestId,
              "effect-s2-flow.state.record_count": String(records.length),
              "effect-s2-flow.service": serviceDefinition.name
            }
          })
        )
        yield* Ref.set(nextSeqNum, ack.end.seqNum)
      })
    )
    return
  }

  yield* appendLock.withPermit(
    Effect.gen(function*() {
      const matchSeqNum = yield* Ref.get(nextSeqNum)
      const token = yield* Ref.get(fencingToken)
      const ack = yield* appendAtomic(
        streamApi,
        [
          encodeRecord({
            _tag: "Failed",
            requestId: invokeRecord.requestId,
            message: String(result.cause)
          }, recordTypeHeader("Failed"))
        ],
        appendOptions(matchSeqNum, token),
        `failed to append Failed for ${streamName}`
      )
      yield* Ref.set(nextSeqNum, ack.end.seqNum)
    })
  )
})

const discoverInvocations = Effect.fn("effect-s2-flow.discoverInvocations")(function*(
  runtime: Required<FlowRuntimeConfig>,
  serviceDefinition: FlowDefinition<ServiceHandlers>
) {
  const prefix = serviceDefinition.kind === "service"
    ? invocationPrefix(serviceDefinition.name)
    : objectPrefix(serviceDefinition.name)
  return yield* listInvocationJournalStreams(runtime, prefix).pipe(
    Effect.mapError((cause) =>
      cause instanceof FlowError ? cause : new FlowError({
        message: `failed to list invocations for ${serviceDefinition.name}`,
        cause
      })
    )
  )
})

export const serve = Effect.fn("effect-s2-flow.serve")(function*(options: ServeOptions) {
  const runtime = yield* FlowRuntime
  yield* ensureBasin(runtime)
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
            Effect.ensuring(Ref.update(active, (set) => {
              const next = new Set(set)
              next.delete(streamName)
              return next
            })),
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

export const hostLayerFromEnv = () => Layer.mergeAll(flowRuntimeLayerFromEnv(), hostTraceLayerFromEnv())

export const runHostMain = (options: ServeOptions): void =>
  NodeRuntime.runMain(
    serve(options).pipe(
      Effect.provide(hostLayerFromEnv())
    )
  )
