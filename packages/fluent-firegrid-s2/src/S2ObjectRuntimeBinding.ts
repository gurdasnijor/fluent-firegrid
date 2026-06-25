import {
  type CallRequest,
  createTanStackRuntimeBinding,
  type FluentDefinitionBindingOptions,
  FluentFiregridError,
  type FluentRuntimeHost,
  type InvocationBinding,
  type SendReference
} from "@firegrid/fluent-firegrid"
import {
  AppendInput,
  AppendRecord,
  basin,
  basins,
  layer as S2Layer,
  SeqNumMismatchError,
  stream as s2Stream
} from "effect-s2"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"

import {
  createS2ObjectStateBackend,
  type S2ObjectStateAddress,
  type S2ObjectStateBackendConfig,
  type S2ObjectStateOwner
} from "./S2ObjectStateBackend.ts"

export interface S2ObjectRuntimeBindingConfig extends S2ObjectStateBackendConfig {
  readonly now?: () => number
  readonly objectOwnerLeaseMs?: number
}

export interface S2DelayedStartDrainOptions {
  readonly limit?: number
}

export interface S2DelayedStartDrainResult {
  readonly started: number
}

export interface S2ObjectRuntimeBinding extends InvocationBinding<FluentFiregridError> {
  readonly drainDelayedStarts: (
    options?: S2DelayedStartDrainOptions
  ) => Effect.Effect<S2DelayedStartDrainResult, FluentFiregridError>
}

export interface S2FluentDefinitionBindingOptions {
  readonly invocationBinding?: FluentDefinitionBindingOptions["invocationBinding"]
}

type AcceptedEvent = {
  readonly _tag: "Accepted"
  readonly callId: string
  readonly handler: string
  readonly input: unknown
  readonly notBefore?: number
  readonly runId: string
  readonly now: number
}

type StartedEvent = {
  readonly _tag: "Started"
  readonly callId: string
  readonly ownerId: string
  readonly leaseExpiresAt: number
  readonly now: number
}
type CompletedEvent = {
  readonly _tag: "Completed"
  readonly callId: string
  readonly output: unknown
  readonly now: number
}
type ErroredEvent = { readonly _tag: "Errored"; readonly callId: string; readonly error: unknown; readonly now: number }
type StateWaitRegisteredEvent = {
  readonly _tag: "StateWaitRegistered"
  readonly callId: string
  readonly environmentVersion?: string
  readonly key: string
  readonly name: string
  readonly signalName: string
  readonly table: string
  readonly timeoutAt?: number
  readonly timeoutMs?: number
  readonly waitId: string
}
type StateWaitReadyEvent = {
  readonly _tag: "StateWaitReady"
  readonly callId: string
  readonly signalName: string
  readonly value: unknown
  readonly waitId: string
}
type StateWaitDeliveredEvent = {
  readonly _tag: "StateWaitDelivered"
  readonly waitId: string
}

type DelayedStartAcceptedEvent = {
  readonly _tag: "DelayedStartAccepted"
  readonly invocationId: string
  readonly notBefore: number
  readonly now: number
  readonly request: CallRequest
}

type DelayedStartStartedEvent = {
  readonly _tag: "DelayedStartStarted"
  readonly invocationId: string
  readonly leaseExpiresAt: number
  readonly now: number
  readonly ownerId: string
}

type DelayedStartDeliveredEvent = {
  readonly _tag: "DelayedStartDelivered"
  readonly invocationId: string
  readonly now: number
}

type ObjectInvocationEvent =
  | AcceptedEvent
  | StartedEvent
  | CompletedEvent
  | ErroredEvent
  | StateWaitRegisteredEvent
  | StateWaitReadyEvent
  | StateWaitDeliveredEvent

type DelayedStartEvent =
  | DelayedStartAcceptedEvent
  | DelayedStartStartedEvent
  | DelayedStartDeliveredEvent

type RuntimeRunResult = Awaited<ReturnType<NonNullable<FluentRuntimeHost["runtime"]["deliverSignal"]>>>

interface DelayedStartExecution {
  readonly run: {
    readonly error?: unknown
    readonly output?: unknown
    readonly status: string
  }
}

interface DelayedStartHostStore {
  readonly loadExecution: (runId: string) => Promise<DelayedStartExecution | undefined>
}

interface Runtime {
  readonly basinName: string
  readonly layer: ReturnType<typeof S2Layer>
}

interface InvocationProjection {
  readonly accepted: ReadonlyMap<string, AcceptedEvent>
  readonly completed: ReadonlyMap<string, CompletedEvent>
  readonly errored: ReadonlyMap<string, ErroredEvent>
  readonly orderedCallIds: ReadonlyArray<string>
  readonly started: ReadonlyMap<string, StartedEvent>
  readonly stateWaitDelivered: ReadonlySet<string>
  readonly stateWaitReady: ReadonlyMap<string, StateWaitReadyEvent>
  readonly stateWaits: ReadonlyArray<StateWaitRegisteredEvent>
  readonly nextSeqNum: number
}

interface DelayedStartProjection {
  readonly accepted: ReadonlyMap<string, DelayedStartAcceptedEvent>
  readonly delivered: ReadonlySet<string>
  readonly nextSeqNum: number
  readonly orderedInvocationIds: ReadonlyArray<string>
  readonly started: ReadonlyMap<string, DelayedStartStartedEvent>
}

const completionPollAttempts = 12_000
const completionPollInterval = "5 millis"

export const s2FluentDefinitionBindingOptions = (
  config: S2ObjectStateBackendConfig,
  options: S2FluentDefinitionBindingOptions = {}
): FluentDefinitionBindingOptions => ({
  ...(options.invocationBinding === undefined ? {} : { invocationBinding: options.invocationBinding }),
  stateBackendFor: ({ definition, input }) => {
    if (definition._kind !== "object" || input.key === undefined) return undefined
    const owner = s2ObjectStateOwnerFrom(input.stateContext)
    return createS2ObjectStateBackend(
      config,
      { key: input.key, objectName: definition.name },
      owner === undefined ? undefined : { owner }
    )
  }
})

const s2ObjectStateOwnerFrom = (value: unknown): S2ObjectStateOwner | undefined =>
  typeof value === "object"
    && value !== null
    && (value as { readonly _tag?: unknown })._tag === "S2ObjectStateOwner"
    && typeof (value as { readonly callId?: unknown }).callId === "string"
    && typeof (value as { readonly invocationStreamName?: unknown }).invocationStreamName === "string"
    && typeof (value as { readonly ownerId?: unknown }).ownerId === "string"
    ? {
      callId: (value as { readonly callId: string }).callId,
      invocationStreamName: (value as { readonly invocationStreamName: string }).invocationStreamName,
      ownerId: (value as { readonly ownerId: string }).ownerId
    }
    : undefined

export const createS2ObjectRuntimeBinding = (
  host: FluentRuntimeHost,
  config: S2ObjectRuntimeBindingConfig
): S2ObjectRuntimeBinding => {
  let nextCall = 0
  const bindingId = Math.random().toString(36).slice(2)
  const runtime = makeRuntime(config)
  const direct = createTanStackRuntimeBinding(host, config.now === undefined ? {} : { now: config.now })
  const now = config.now ?? Date.now
  const ownerLeaseMs = config.objectOwnerLeaseMs ?? 30_000
  const delayedStarts = delayedStartStreamName(config)

  const callIdFor = (request: CallRequest): string =>
    request.runId ??
      `${request.kind}:${request.name}:${request.key ?? "no-key"}:${request.handler}:${bindingId}:${nextCall++}`

  const runObject = <Output>(
    request: CallRequest,
    mode: "call" | "send"
  ): Effect.Effect<Output | SendReference<Output>, FluentFiregridError> => {
    if (request.key === undefined || request.key === "") {
      return Effect.fail(
        new FluentFiregridError({ message: `object invocation ${request.name}.${request.handler} requires a key` })
      )
    }
    const key = request.key
    const callId = callIdFor(request)
    const ownerId = `object-owner:${callId}:${Math.random().toString(36).slice(2)}`
    const address = { key, objectName: request.name } satisfies S2ObjectStateAddress
    const streamName = objectInvocationStreamName(config, address)
    return runS2(
      streamName,
      Effect.gen(function*() {
        yield* admit(runtime, streamName, {
          _tag: "Accepted",
          callId,
          handler: request.handler,
          input: request.input,
          ...(request.delayMs === undefined ? {} : { notBefore: now() + request.delayMs }),
          now: now(),
          runId: callId
        })
        if (mode === "send") {
          return {
            handler: request.handler,
            invocationId: callId,
            key,
            kind: request.kind,
            name: request.name
          } satisfies SendReference<Output>
        }
        const completed = yield* waitForCompletion(
          host,
          runtime,
          streamName,
          ownerId,
          now,
          ownerLeaseMs,
          request,
          callId
        )
        return completed.output as Output
      })
    )
  }

  const runDelayedStart = <Output>(
    request: CallRequest,
    mode: "call" | "send"
  ): Effect.Effect<Output | SendReference<Output>, FluentFiregridError> => {
    const invocationId = callIdFor(request)
    return runS2(
      delayedStarts,
      Effect.gen(function*() {
        yield* admitDelayedStart(runtime, delayedStarts, {
          _tag: "DelayedStartAccepted",
          invocationId,
          notBefore: now() + (request.delayMs ?? 0),
          now: now(),
          request: { ...request, runId: invocationId }
        })
        if (mode === "send") {
          return {
            handler: request.handler,
            invocationId,
            ...(request.key === undefined ? {} : { key: request.key }),
            kind: request.kind,
            name: request.name
          } satisfies SendReference<Output>
        }
        return yield* waitForDelayedStartCompletion<Output>(
          host,
          runtime,
          delayedStarts,
          now,
          ownerLeaseMs,
          request,
          invocationId
        )
      })
    )
  }

  const drainDelayedStarts = (
    options: S2DelayedStartDrainOptions = {}
  ): Effect.Effect<S2DelayedStartDrainResult, FluentFiregridError> =>
    runS2(
      delayedStarts,
      drainGenericDelayedStarts(host, runtime, delayedStarts, now, ownerLeaseMs, options.limit ?? 25)
    )

  const binding: S2ObjectRuntimeBinding = {
    call: <Output>(request: CallRequest): Effect.Effect<Output, FluentFiregridError> =>
      request.kind === "object"
        ? runObject<Output>(request, "call").pipe(Effect.map((value) => value as Output))
        : request.delayMs !== undefined && request.delayMs > 0
        ? runDelayedStart<Output>(request, "call").pipe(Effect.map((value) => value as Output))
        : direct.call<Output>(request),
    drainDelayedStarts,
    send: <Output>(request: CallRequest): Effect.Effect<SendReference<Output>, FluentFiregridError> =>
      request.kind === "object"
        ? runObject<Output>(request, "send").pipe(Effect.map((value) => value as SendReference<Output>))
        : request.delayMs !== undefined && request.delayMs > 0
        ? runDelayedStart<Output>(request, "send").pipe(Effect.map((value) => value as SendReference<Output>))
        : direct.send<Output>(request)
  }
  return binding
}

export const delayedStartStreamName = (
  config: Pick<S2ObjectRuntimeBindingConfig, "namespace">
): string =>
  `${sanitize(config.namespace ?? "default")}/delayed-starts`

export const objectInvocationStreamName = (
  config: Pick<S2ObjectRuntimeBindingConfig, "namespace">,
  address: S2ObjectStateAddress
): string =>
  `${sanitize(config.namespace ?? "default")}/obj/${sanitize(address.objectName)}/${sanitize(address.key)}/invocations`

const makeRuntime = (config: S2ObjectRuntimeBindingConfig): Runtime => ({
  basinName: config.basin ?? "fluent-firegrid",
  layer: S2Layer({
    accessToken: config.accessToken ?? "s2_access_token",
    endpoints: {
      account: config.s2Endpoint,
      basin: config.s2Endpoint
    }
  })
})

const runS2 = <A>(
  streamName: string,
  effect: Effect.Effect<A, unknown, never>
): Effect.Effect<A, FluentFiregridError> =>
  effect.pipe(
    Effect.mapError((cause) =>
      cause instanceof FluentFiregridError
        ? cause
        : new FluentFiregridError({ cause, message: `S2 object invocation operation failed for ${streamName}` })
    )
  )

const getStream = (runtime: Runtime, streamName: string) =>
  Effect.provide(
    Effect.gen(function*() {
      yield* basins.ensure({ basin: runtime.basinName })
      const basinApi = yield* basin(runtime.basinName)
      yield* basinApi.streams.ensure({ stream: streamName })
      return yield* s2Stream(runtime.basinName, streamName)
    }),
    runtime.layer
  )

const readProjection = (runtime: Runtime, streamName: string): Effect.Effect<InvocationProjection, unknown> =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    const tail = yield* stream.checkTail()
    const events = new Array<ObjectInvocationEvent>()
    if (tail.tail.seqNum > 0) {
      const records = yield* stream.readSession({
        start: { from: { seqNum: 0 } },
        stop: { limits: { count: tail.tail.seqNum } }
      }).pipe(Stream.runCollect)
      events.push(...Array.from(records, (record) => JSON.parse(record.body) as ObjectInvocationEvent))
    }
    return foldInvocationEvents(events, tail.tail.seqNum)
  }).pipe(
    Effect.catch((cause) =>
      isMissing(cause)
        ? Effect.succeed(foldInvocationEvents([], 0))
        : Effect.fail(cause)
    )
  )

const readDelayedStartProjection = (
  runtime: Runtime,
  streamName: string
): Effect.Effect<DelayedStartProjection, unknown> =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    const tail = yield* stream.checkTail()
    const events = new Array<DelayedStartEvent>()
    if (tail.tail.seqNum > 0) {
      const records = yield* stream.readSession({
        start: { from: { seqNum: 0 } },
        stop: { limits: { count: tail.tail.seqNum } }
      }).pipe(Stream.runCollect)
      events.push(...Array.from(records, (record) => JSON.parse(record.body) as DelayedStartEvent))
    }
    return foldDelayedStartEvents(events, tail.tail.seqNum)
  }).pipe(
    Effect.catch((cause) =>
      isMissing(cause)
        ? Effect.succeed(foldDelayedStartEvents([], 0))
        : Effect.fail(cause)
    )
  )

const foldInvocationEvents = (
  events: ReadonlyArray<ObjectInvocationEvent>,
  nextSeqNum: number
): InvocationProjection => {
  const accepted = new Map<string, AcceptedEvent>()
  const completed = new Map<string, CompletedEvent>()
  const errored = new Map<string, ErroredEvent>()
  const orderedCallIds = new Array<string>()
  const started = new Map<string, StartedEvent>()
  const stateWaitDelivered = new Set<string>()
  const stateWaitReady = new Map<string, StateWaitReadyEvent>()
  const stateWaits = new Array<StateWaitRegisteredEvent>()
  events.forEach((event) => {
    switch (event._tag) {
      case "Accepted": {
        if (!accepted.has(event.callId)) {
          accepted.set(event.callId, event)
          orderedCallIds.push(event.callId)
        }
        break
      }
      case "Started": {
        started.set(event.callId, event)
        break
      }
      case "Completed": {
        completed.set(event.callId, event)
        break
      }
      case "Errored": {
        errored.set(event.callId, event)
        break
      }
      case "StateWaitRegistered": {
        stateWaits.push(event)
        break
      }
      case "StateWaitReady": {
        stateWaitReady.set(event.waitId, event)
        break
      }
      case "StateWaitDelivered": {
        stateWaitDelivered.add(event.waitId)
        break
      }
    }
  })
  return {
    accepted,
    completed,
    errored,
    nextSeqNum,
    orderedCallIds,
    started,
    stateWaitDelivered,
    stateWaitReady,
    stateWaits
  }
}

const foldDelayedStartEvents = (
  events: ReadonlyArray<DelayedStartEvent>,
  nextSeqNum: number
): DelayedStartProjection => {
  const accepted = new Map<string, DelayedStartAcceptedEvent>()
  const delivered = new Set<string>()
  const orderedInvocationIds = new Array<string>()
  const started = new Map<string, DelayedStartStartedEvent>()
  events.forEach((event) => {
    switch (event._tag) {
      case "DelayedStartAccepted": {
        if (!accepted.has(event.invocationId)) {
          accepted.set(event.invocationId, event)
          orderedInvocationIds.push(event.invocationId)
        }
        break
      }
      case "DelayedStartStarted": {
        started.set(event.invocationId, event)
        break
      }
      case "DelayedStartDelivered": {
        delivered.add(event.invocationId)
        break
      }
    }
  })
  return {
    accepted,
    delivered,
    nextSeqNum,
    orderedInvocationIds,
    started
  }
}

const appendEvent = (
  runtime: Runtime,
  streamName: string,
  event: ObjectInvocationEvent,
  matchSeqNum?: number
) =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    return yield* stream.append(
      AppendInput.create(
        [AppendRecord.string({ body: JSON.stringify(event) })],
        matchSeqNum === undefined ? undefined : { matchSeqNum }
      )
    )
  })

const appendDelayedStartEvent = (
  runtime: Runtime,
  streamName: string,
  event: DelayedStartEvent,
  matchSeqNum?: number
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    yield* stream.append(
      AppendInput.create(
        [AppendRecord.string({ body: JSON.stringify(event) })],
        matchSeqNum === undefined ? undefined : { matchSeqNum }
      )
    )
  })

const admitDelayedStart = (
  runtime: Runtime,
  streamName: string,
  event: DelayedStartAcceptedEvent
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 32; attempt += 1) {
      const projection = yield* readDelayedStartProjection(runtime, streamName)
      if (projection.accepted.has(event.invocationId)) return
      const result = yield* appendDelayedStartEvent(runtime, streamName, event, projection.nextSeqNum).pipe(Effect.exit)
      if (result._tag === "Success") return
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* new FluentFiregridError({
      message: `delayed fluent invocation admission CAS failed for ${event.invocationId}`
    })
  })

const appendDelayedStartDelivered = (
  runtime: Runtime,
  streamName: string,
  invocationId: string,
  now: number
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const projection = yield* readDelayedStartProjection(runtime, streamName)
      if (projection.delivered.has(invocationId)) return
      const result = yield* appendDelayedStartEvent(
        runtime,
        streamName,
        { _tag: "DelayedStartDelivered", invocationId, now },
        projection.nextSeqNum
      ).pipe(Effect.exit)
      if (result._tag === "Success") return
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* new FluentFiregridError({
      message: `delayed fluent invocation delivered CAS failed for ${invocationId}`
    })
  })

const appendTerminalEvent = (
  runtime: Runtime,
  streamName: string,
  ownerId: string,
  callId: string,
  event: CompletedEvent | ErroredEvent
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const projection = yield* readProjection(runtime, streamName)
      if (projection.completed.has(callId) || projection.errored.has(callId)) return
      if (projection.started.get(callId)?.ownerId !== ownerId) return
      const result = yield* appendEvent(runtime, streamName, event, projection.nextSeqNum).pipe(Effect.exit)
      if (result._tag === "Success") return
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* new FluentFiregridError({ message: `object invocation terminal CAS failed for ${streamName}` })
  })

const appendStateWaitDelivered = (
  runtime: Runtime,
  streamName: string,
  waitId: string
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const projection = yield* readProjection(runtime, streamName)
      if (projection.stateWaitDelivered.has(waitId)) return
      const result = yield* appendEvent(
        runtime,
        streamName,
        { _tag: "StateWaitDelivered", waitId },
        projection.nextSeqNum
      ).pipe(Effect.exit)
      if (result._tag === "Success") return
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* new FluentFiregridError({ message: `object state wait delivered CAS failed for ${waitId}` })
  })

const admit = (
  runtime: Runtime,
  streamName: string,
  event: AcceptedEvent
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 32; attempt += 1) {
      const projection = yield* readProjection(runtime, streamName)
      if (projection.accepted.has(event.callId)) return
      const result = yield* appendEvent(runtime, streamName, event, projection.nextSeqNum).pipe(Effect.exit)
      if (result._tag === "Success") return
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* new FluentFiregridError({ message: `object invocation admission CAS failed for ${streamName}` })
  })

const drainGenericDelayedStarts = (
  host: FluentRuntimeHost,
  runtime: Runtime,
  streamName: string,
  now: () => number,
  leaseMs: number,
  limit: number
): Effect.Effect<S2DelayedStartDrainResult, unknown> =>
  Effect.gen(function*() {
    let started = 0
    while (started < limit) {
      const projection = yield* readDelayedStartProjection(runtime, streamName)
      const currentTime = now()
      const nextInvocationId = projection.orderedInvocationIds.find((invocationId) => {
        const accepted = projection.accepted.get(invocationId)
        const startedEvent = projection.started.get(invocationId)
        return accepted !== undefined
          && accepted.notBefore <= currentTime
          && !projection.delivered.has(invocationId)
          && (startedEvent === undefined || startedEvent.leaseExpiresAt <= currentTime)
      })
      if (nextInvocationId === undefined) return { started }
      const accepted = projection.accepted.get(nextInvocationId)
      if (accepted === undefined) return { started }
      const ownerId = `delayed-start:${nextInvocationId}:${Math.random().toString(36).slice(2)}`
      const claim = yield* appendDelayedStartEvent(
        runtime,
        streamName,
        {
          _tag: "DelayedStartStarted",
          invocationId: nextInvocationId,
          leaseExpiresAt: currentTime + leaseMs,
          now: currentTime,
          ownerId
        },
        projection.nextSeqNum
      ).pipe(Effect.exit)
      if (claim._tag === "Failure") {
        if (isCasConflict(claim.cause)) continue
        return yield* Effect.failCause(claim.cause)
      }
      const result = yield* startDelayedRequest(host, accepted.request, now()).pipe(Effect.exit)
      if (result._tag === "Failure") {
        return yield* Effect.failCause(result.cause)
      }
      yield* appendDelayedStartDelivered(runtime, streamName, nextInvocationId, now())
      started += 1
    }
    return { started }
  })

const waitForDelayedStartCompletion = <Output>(
  host: FluentRuntimeHost,
  runtime: Runtime,
  streamName: string,
  now: () => number,
  leaseMs: number,
  request: CallRequest,
  invocationId: string
): Effect.Effect<Output, unknown> => {
  const loop = (remaining: number): Effect.Effect<Output, unknown> =>
    Effect.gen(function*() {
      yield* drainGenericDelayedStarts(host, runtime, streamName, now, leaseMs, 25)
      const projection = yield* readDelayedStartProjection(runtime, streamName)
      const accepted = projection.accepted.get(invocationId)
      if (accepted !== undefined && accepted.notBefore > now()) {
        return yield* Effect.sleep(completionPollInterval).pipe(Effect.andThen(loop(remaining - 1)))
      }
      const stored = yield* readDelayedStartCompletion<Output>(host, request, invocationId)
      if (Option.isSome(stored)) return stored.value
      const result = yield* startDelayedRequest(host, { ...request, runId: invocationId }, now()).pipe(Effect.exit)
      if (result._tag === "Success") {
        switch (result.value.kind) {
          case "completed": {
            return result.value.run?.output as Output
          }
          case "errored": {
            return yield* new FluentFiregridError({
              cause: result.value.run?.error,
              message: `delayed fluent invocation ${request.name}.${request.handler} failed`
            })
          }
          case "paused":
          case "running":
          case "not-claimable": {
            const current = yield* readDelayedStartCompletion<Output>(host, request, invocationId)
            if (Option.isSome(current)) return current.value
            if (remaining <= 0) {
              return yield* new FluentFiregridError({
                message: `delayed fluent invocation ${request.name}.${request.handler} did not complete before timeout`
              })
            }
            return yield* Effect.sleep(completionPollInterval).pipe(Effect.andThen(loop(remaining - 1)))
          }
          default: {
            return yield* new FluentFiregridError({
              message: `delayed fluent invocation ${request.name}.${request.handler} could not be attached: ${result.value.kind}`
            })
          }
        }
      }
      return yield* Effect.failCause(result.cause)
    })
  return loop(completionPollAttempts)
}

const readDelayedStartCompletion = <Output>(
  host: FluentRuntimeHost,
  request: CallRequest,
  invocationId: string
): Effect.Effect<Option.Option<Output>, unknown> => {
  const store = delayedStartHostStore(host)
  if (store === undefined) return Effect.succeed(Option.none())
  return Effect.tryPromise({
    try: () => store.loadExecution(invocationId),
    catch: (cause) => cause
  }).pipe(
    Effect.flatMap((execution) => {
      if (execution?.run.status === "finished") {
        return Effect.succeed(Option.some(execution.run.output as Output))
      }
      if (execution?.run.status === "errored") {
        return new FluentFiregridError({
          cause: execution.run.error,
          message: `delayed fluent invocation ${request.name}.${request.handler} failed`
        })
      }
      return Effect.succeed(Option.none())
    })
  )
}

const delayedStartHostStore = (host: FluentRuntimeHost): DelayedStartHostStore | undefined => {
  const store = (host as { readonly store?: unknown }).store
  if (typeof store !== "object" || store === null) return undefined
  const loadExecution = (store as { readonly loadExecution?: unknown }).loadExecution
  return typeof loadExecution === "function"
    ? { loadExecution: loadExecution as DelayedStartHostStore["loadExecution"] }
    : undefined
}

const startDelayedRequest = (
  host: FluentRuntimeHost,
  request: CallRequest,
  now: number
): Effect.Effect<RuntimeRunResult, unknown> =>
  Effect.tryPromise({
    try: () =>
      host.runtime.startRun({
        input: {
          input: request.input,
          ...(request.key === undefined ? {} : { key: request.key })
        },
        now,
        runId: request.runId!,
        workflowId: `${request.kind}:${request.name}:${request.handler}`
      }),
    catch: (cause) => cause
  })

const waitForCompletion = (
  host: FluentRuntimeHost,
  runtime: Runtime,
  streamName: string,
  ownerId: string,
  now: () => number,
  ownerLeaseMs: number,
  request: CallRequest,
  callId: string
): Effect.Effect<CompletedEvent, unknown> => {
  const loop = (remaining: number): Effect.Effect<CompletedEvent, unknown> =>
    Effect.gen(function*() {
      yield* drain(runtime, streamName, ownerId, now, ownerLeaseMs, host, request)
      const projection = yield* readProjection(runtime, streamName)
      const completed = projection.completed.get(callId)
      if (completed !== undefined) return completed
      const errored = projection.errored.get(callId)
      if (errored !== undefined) {
        return yield* new FluentFiregridError({
          cause: errored.error,
          message: `object invocation ${request.name}.${request.handler} failed`
        })
      }
      if (remaining <= 0) {
        return yield* new FluentFiregridError({
          message: `object invocation ${request.name}.${request.handler} did not complete before timeout`
        })
      }
      return yield* Effect.sleep(completionPollInterval).pipe(Effect.andThen(loop(remaining - 1)))
    })
  return loop(completionPollAttempts)
}

const drain = (
  runtime: Runtime,
  streamName: string,
  ownerId: string,
  now: () => number,
  ownerLeaseMs: number,
  host: FluentRuntimeHost,
  request: CallRequest
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    while (true) {
      const projection = yield* readProjection(runtime, streamName)
      const currentTime = now()
      const nextCallId = projection.orderedCallIds.find((callId) =>
        isCallDue(projection, callId, currentTime)
        && stateWaitStatusForCall(projection, callId, currentTime)._tag !== "Pending"
      )
      if (nextCallId === undefined) return
      const accepted = projection.accepted.get(nextCallId)
      if (accepted === undefined) return
      const started = projection.started.get(nextCallId)
      const stateWaitStatus = stateWaitStatusForCall(projection, nextCallId, currentTime)
      if (started !== undefined && started.ownerId !== ownerId && started.leaseExpiresAt > currentTime) return
      if (
        started !== undefined
        && started.ownerId === ownerId
        && started.leaseExpiresAt > currentTime
        && stateWaitStatus._tag !== "Ready"
      ) {
        return
      }
      if (stateWaitStatus._tag === "Ready" && started !== undefined) {
        const result = yield* deliverStateWait(host, {
          callId: nextCallId,
          leaseMs: ownerLeaseMs,
          leaseOwner: started.ownerId,
          now: now(),
          ready: stateWaitStatus.ready,
          request
        }).pipe(Effect.exit)
        if (result._tag === "Failure") {
          yield* appendTerminalEvent(runtime, streamName, started.ownerId, nextCallId, {
            _tag: "Errored",
            callId: nextCallId,
            error: Cause.pretty(result.cause),
            now: now()
          })
          return
        }
        yield* appendStateWaitDelivered(runtime, streamName, stateWaitStatus.ready.waitId)
        if (result.value.kind === "completed") {
          yield* appendTerminalEvent(runtime, streamName, started.ownerId, nextCallId, {
            _tag: "Completed",
            callId: nextCallId,
            now: now(),
            output: result.value.run?.output
          })
        }
        if (result.value.kind === "errored") {
          yield* appendTerminalEvent(runtime, streamName, started.ownerId, nextCallId, {
            _tag: "Errored",
            callId: nextCallId,
            error: result.value.run?.error ?? "state wait resume failed",
            now: now()
          })
        }
        continue
      }
      if (started === undefined || (started.ownerId !== ownerId && started.leaseExpiresAt <= currentTime)) {
        const startResult = yield* appendEvent(
          runtime,
          streamName,
          {
            _tag: "Started",
            callId: nextCallId,
            leaseExpiresAt: currentTime + ownerLeaseMs,
            now: currentTime,
            ownerId
          },
          projection.nextSeqNum
        ).pipe(Effect.exit)
        if (startResult._tag === "Failure") {
          if (isCasConflict(startResult.cause)) continue
          return yield* Effect.failCause(startResult.cause)
        }
      }
      const result = yield* Effect.tryPromise({
        try: () =>
          host.runtime.startRun({
            input: {
              input: accepted.input,
              ...(request.key === undefined ? {} : { key: request.key }),
              stateContext: {
                _tag: "S2ObjectStateOwner",
                callId: nextCallId,
                invocationStreamName: streamName,
                ownerId
              }
            },
            leaseMs: ownerLeaseMs,
            leaseOwner: ownerId,
            now: now(),
            runId: accepted.runId,
            workflowId: `${request.kind}:${request.name}:${accepted.handler}`
          }),
        catch: (cause) => cause
      }).pipe(Effect.exit)
      if (result._tag === "Failure") {
        yield* appendTerminalEvent(runtime, streamName, ownerId, nextCallId, {
          _tag: "Errored",
          callId: nextCallId,
          error: Cause.pretty(result.cause),
          now: now()
        })
        return
      }
      if (result.value.kind === "completed") {
        yield* appendTerminalEvent(runtime, streamName, ownerId, nextCallId, {
          _tag: "Completed",
          callId: nextCallId,
          now: now(),
          output: result.value.run?.output
        })
        continue
      }
      if (result.value.kind === "errored") {
        yield* appendTerminalEvent(runtime, streamName, ownerId, nextCallId, {
          _tag: "Errored",
          callId: nextCallId,
          error: result.value.run?.error ?? "object invocation failed",
          now: now()
        })
        continue
      }
      continue
    }
  })

type StateWaitStatus =
  | { readonly _tag: "None" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Ready"; readonly ready: StateWaitReadyEvent }

const isCallDue = (projection: InvocationProjection, callId: string, now: number): boolean => {
  if (projection.completed.has(callId) || projection.errored.has(callId)) return false
  const accepted = projection.accepted.get(callId)
  return accepted !== undefined && (accepted.notBefore === undefined || accepted.notBefore <= now)
}

const stateWaitStatusForCall = (
  projection: InvocationProjection,
  callId: string,
  now: number
): StateWaitStatus => {
  const wait = projection.stateWaits.find((registered) =>
    registered.callId === callId && !projection.stateWaitDelivered.has(registered.waitId)
  )
  if (wait === undefined) return { _tag: "None" }
  const ready = projection.stateWaitReady.get(wait.waitId)
  if (ready !== undefined) return { _tag: "Ready", ready }
  if (wait.timeoutAt !== undefined && wait.timeoutAt <= now) {
    return {
      _tag: "Ready",
      ready: {
        _tag: "StateWaitReady",
        callId,
        signalName: wait.signalName,
        value: { _tag: "StateWaitTimedOut", name: wait.name },
        waitId: wait.waitId
      }
    }
  }
  return { _tag: "Pending" }
}

const deliverStateWait = (
  host: FluentRuntimeHost,
  args: {
    readonly callId: string
    readonly leaseMs: number
    readonly leaseOwner: string
    readonly now: number
    readonly ready: StateWaitReadyEvent
    readonly request: CallRequest
  }
): Effect.Effect<RuntimeRunResult, FluentFiregridError> => {
  const deliverSignal = host.runtime.deliverSignal
  return deliverSignal === undefined
    ? Effect.fail(
      new FluentFiregridError({ message: "S2 object state waits require a runtime host with deliverSignal" })
    )
    : Effect.tryPromise({
      try: () =>
        deliverSignal({
          leaseMs: args.leaseMs,
          leaseOwner: args.leaseOwner,
          name: args.ready.signalName,
          now: args.now,
          payload: args.ready.value,
          runId: args.callId,
          signalId: `state-wait:${args.ready.waitId}`,
          stepId: args.ready.waitId
        }),
      catch: (cause) =>
        new FluentFiregridError({
          cause,
          message: `object invocation ${args.request.name}.${args.request.handler} state wait resume failed`
        })
    })
}

const isCasConflict = (cause: unknown): boolean =>
  Cause.isCause(cause)
    ? Option.match(Cause.findErrorOption(cause), {
      onNone: () => false,
      onSome: isCasConflict
    })
    : cause instanceof SeqNumMismatchError
      || (typeof cause === "object" && cause !== null &&
        String((cause as { readonly status?: unknown }).status) === "412")

const isMissing = (cause: unknown): boolean =>
  typeof cause === "object"
  && cause !== null
  && (String((cause as { readonly status?: unknown }).status) === "404"
    || String((cause as { readonly status?: unknown }).status) === "416")

const sanitize = (value: string): string => encodeURIComponent(value).replace(/%/g, "_")
