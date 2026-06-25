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

type AcceptedEvent = {
  readonly _tag: "Accepted"
  readonly callId: string
  readonly handler: string
  readonly input: unknown
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

type ObjectInvocationEvent = AcceptedEvent | StartedEvent | CompletedEvent | ErroredEvent

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
  readonly nextSeqNum: number
}

export const s2FluentDefinitionBindingOptions = (
  config: S2ObjectStateBackendConfig
): FluentDefinitionBindingOptions => ({
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
): InvocationBinding<FluentFiregridError> => {
  let nextCall = 0
  const bindingId = Math.random().toString(36).slice(2)
  const runtime = makeRuntime(config)
  const direct = createTanStackRuntimeBinding(host, config.now === undefined ? {} : { now: config.now })
  const now = config.now ?? Date.now
  const ownerLeaseMs = config.objectOwnerLeaseMs ?? 30_000

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

  const binding: InvocationBinding<FluentFiregridError> = {
    call: <Output>(request: CallRequest): Effect.Effect<Output, FluentFiregridError> =>
      request.kind === "object"
        ? runObject<Output>(request, "call").pipe(Effect.map((value) => value as Output))
        : direct.call<Output>(request),
    send: <Output>(request: CallRequest): Effect.Effect<SendReference<Output>, FluentFiregridError> =>
      request.kind === "object"
        ? runObject<Output>(request, "send").pipe(Effect.map((value) => value as SendReference<Output>))
        : direct.send<Output>(request)
  }
  return binding
}

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

const foldInvocationEvents = (
  events: ReadonlyArray<ObjectInvocationEvent>,
  nextSeqNum: number
): InvocationProjection => {
  const accepted = new Map<string, AcceptedEvent>()
  const completed = new Map<string, CompletedEvent>()
  const errored = new Map<string, ErroredEvent>()
  const orderedCallIds = new Array<string>()
  const started = new Map<string, StartedEvent>()
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
    }
  })
  return { accepted, completed, errored, nextSeqNum, orderedCallIds, started }
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

const waitForCompletion = (
  host: FluentRuntimeHost,
  runtime: Runtime,
  streamName: string,
  ownerId: string,
  now: () => number,
  ownerLeaseMs: number,
  request: CallRequest,
  callId: string
): Effect.Effect<CompletedEvent, unknown> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 500; attempt += 1) {
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
      yield* Effect.yieldNow
    }
    return yield* new FluentFiregridError({
      message: `object invocation ${request.name}.${request.handler} did not complete before timeout`
    })
  })

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
      const nextCallId = projection.orderedCallIds.find((callId) =>
        !projection.completed.has(callId) && !projection.errored.has(callId)
      )
      if (nextCallId === undefined) return
      const accepted = projection.accepted.get(nextCallId)
      if (accepted === undefined) return
      const started = projection.started.get(nextCallId)
      const currentTime = now()
      if (started !== undefined && started.ownerId !== ownerId && started.leaseExpiresAt > currentTime) return
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
      return
    }
  })

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
