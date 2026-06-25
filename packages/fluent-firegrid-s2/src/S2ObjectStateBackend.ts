import { FluentFiregridError, type ObjectStateBackend } from "@firegrid/fluent-firegrid"
import { ChangeMessage, MaterializedState } from "@firegrid/fluent-firegrid/state"
import {
  AppendInput,
  AppendRecord,
  basin,
  basins,
  layer as S2Layer,
  SeqNumMismatchError,
  stream as s2Stream
} from "effect-s2"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"

export interface S2ObjectStateBackendConfig {
  readonly s2Endpoint: string
  readonly accessToken?: string
  readonly basin?: string
  readonly namespace?: string
}

export interface S2ObjectStateAddress {
  readonly objectName: string
  readonly key: string
}

export interface S2ObjectStateOwner {
  readonly callId: string
  readonly invocationStreamName: string
  readonly ownerId: string
}

export interface S2ObjectStateBackendOptions {
  readonly owner?: S2ObjectStateOwner
}

interface Runtime {
  readonly basinName: string
  readonly layer: ReturnType<typeof S2Layer>
}

interface Projection {
  readonly appliedTxids: ReadonlySet<string>
  readonly materialized: MaterializedState
  readonly nextSeqNum: number
  readonly reads: ReadonlyMap<string, Option.Option<unknown>>
}

interface InvocationProjection {
  readonly started: ReadonlyMap<string, { readonly ownerId: string }>
}

export const objectStateStreamName = (
  config: Pick<S2ObjectStateBackendConfig, "namespace">,
  address: S2ObjectStateAddress
): string =>
  `${sanitize(config.namespace ?? "default")}/obj/${sanitize(address.objectName)}/${sanitize(address.key)}/state`

const objectInvocationStreamName = (
  config: Pick<S2ObjectStateBackendConfig, "namespace">,
  address: S2ObjectStateAddress
): string =>
  `${sanitize(config.namespace ?? "default")}/obj/${sanitize(address.objectName)}/${sanitize(address.key)}/invocations`

export const createS2ObjectStateBackend = (
  config: S2ObjectStateBackendConfig,
  address: S2ObjectStateAddress,
  backendOptions: S2ObjectStateBackendOptions = {}
): ObjectStateBackend => {
  const runtime = makeRuntime(config)
  const streamName = objectStateStreamName(config, address)
  const owner = backendOptions.owner
  const invocationStreamName = owner?.invocationStreamName
  const run = <A>(effect: Effect.Effect<A, unknown, never>): Effect.Effect<A, FluentFiregridError> =>
    effect.pipe(
      Effect.mapError((cause) =>
        cause instanceof FluentFiregridError
          ? cause
          : new FluentFiregridError({ cause, message: `S2 object state operation failed for ${streamName}` })
      )
  )

  return {
    get: (table, key, options) =>
      run(
        options?.readId === undefined
          ? readProjection(runtime, streamName, invocationStreamName).pipe(
            Effect.map((projection) => projection.materialized.get(table, key))
          )
          : readJournaled(runtime, streamName, table, key, options.readId, owner)
      ),
    set: (table, key, value, options) =>
      run(
        appendChange(runtime, streamName, {
          headers: {
            ...(owner === undefined ? {} : {
              callId: owner.callId,
              ownerId: owner.ownerId
            }),
            operation: "update",
            ...(options?.opId === undefined ? {} : { txid: options.opId })
          },
          key,
          type: table,
          value
        }, owner).pipe(Effect.asVoid)
      ),
    delete: (table, key, options) =>
      run(
        appendChange(runtime, streamName, {
          headers: {
            ...(owner === undefined ? {} : {
              callId: owner.callId,
              ownerId: owner.ownerId
            }),
            operation: "delete",
            ...(options?.opId === undefined ? {} : { txid: options.opId })
          },
          key,
          type: table
        }, owner).pipe(Effect.asVoid)
      )
  }
}

const makeRuntime = (config: S2ObjectStateBackendConfig): Runtime => ({
  basinName: config.basin ?? "fluent-firegrid",
  layer: S2Layer({
    accessToken: config.accessToken ?? "s2_access_token",
    endpoints: {
      account: config.s2Endpoint,
      basin: config.s2Endpoint
    }
  })
})

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

const readInvocationProjection = (
  runtime: Runtime,
  streamName: string
): Effect.Effect<InvocationProjection, unknown> =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    const tail = yield* stream.checkTail()
    const started = new Map<string, { readonly ownerId: string }>()
    if (tail.tail.seqNum <= 0) return { started }
    const records = yield* stream.readSession({
      start: { from: { seqNum: 0 } },
      stop: { limits: { count: tail.tail.seqNum } }
    }).pipe(Stream.runCollect)
    Array.from(records, (record) => JSON.parse(record.body) as {
      readonly _tag?: string
      readonly callId?: string
      readonly ownerId?: string
    }).forEach((event) => {
      if (event._tag === "Started" && event.callId !== undefined && event.ownerId !== undefined) {
        started.set(event.callId, { ownerId: event.ownerId })
      }
    })
    return { started }
  })

const readProjection = (
  runtime: Runtime,
  streamName: string,
  invocationStreamName?: string
): Effect.Effect<Projection, unknown> =>
  Effect.gen(function*() {
    const invocation = invocationStreamName === undefined
      ? undefined
      : yield* readInvocationProjection(runtime, invocationStreamName)
    const stream = yield* getStream(runtime, streamName)
    const tail = yield* stream.checkTail()
    const materialized = MaterializedState.empty()
    const reads = new Map<string, Option.Option<unknown>>()
    const appliedTxids = new Set<string>()
    if (tail.tail.seqNum <= 0) {
      return { appliedTxids, materialized, nextSeqNum: tail.tail.seqNum, reads }
    }
    const records = yield* stream.readSession({
      start: { from: { seqNum: 0 } },
      stop: { limits: { count: tail.tail.seqNum } }
    }).pipe(Stream.runCollect)
    yield* Effect.forEach(
      Array.from(records),
      (record) =>
        ChangeMessage.decode(record.body).pipe(
          Effect.tap((message) =>
            Effect.sync(() => {
              if (!stateMessageOwnerIsCurrent(message, invocation)) return
              if (ChangeMessage.isReadJournaled(message)) {
                reads.set(message.headers.readId, message.headers.present ? Option.some(message.value) : Option.none())
                return
              }
              materialized.apply(message)
              if (ChangeMessage.isChange(message) && message.headers.txid !== undefined) {
                appliedTxids.add(message.headers.txid)
              }
            }))
        ),
      { discard: true }
    )
    return { appliedTxids, materialized, nextSeqNum: tail.tail.seqNum, reads }
  })

const stateMessageOwnerIsCurrent = (
  message: ChangeMessage.Message,
  invocation: InvocationProjection | undefined
): boolean => {
  if (invocation === undefined) return true
  if (!("callId" in message.headers) || message.headers.callId === undefined || message.headers.ownerId === undefined) {
    return true
  }
  return invocation.started.get(message.headers.callId)?.ownerId === message.headers.ownerId
}

const verifyOwner = (
  runtime: Runtime,
  owner: S2ObjectStateOwner | undefined
): Effect.Effect<void, unknown> =>
  owner === undefined
    ? Effect.void
    : readInvocationProjection(runtime, owner.invocationStreamName).pipe(
      Effect.flatMap((projection) =>
        projection.started.get(owner.callId)?.ownerId === owner.ownerId
          ? Effect.void
          : Effect.fail(
            new FluentFiregridError({
              message: `S2 object owner ${owner.ownerId} no longer owns call ${owner.callId}`
            })
          )
      )
    )

const readJournaled = (
  runtime: Runtime,
  streamName: string,
  table: string,
  key: string,
  readId: string,
  owner: S2ObjectStateOwner | undefined
): Effect.Effect<Option.Option<unknown>, unknown> =>
  Effect.gen(function*() {
    let attempts = 0
    while (attempts < 16) {
      attempts += 1
      const projection = yield* readProjection(runtime, streamName, owner?.invocationStreamName)
      const journaled = projection.reads.get(readId)
      if (journaled !== undefined) return journaled

      yield* verifyOwner(runtime, owner)
      const value = projection.materialized.get(table, key)
      const stream = yield* getStream(runtime, streamName)
      const body = yield* ChangeMessage.encode({
        headers: {
          ...(owner === undefined ? {} : {
            callId: owner.callId,
            ownerId: owner.ownerId
          }),
          present: Option.isSome(value),
          read: "journaled",
          readId
        },
        key,
        type: table,
        ...(Option.isNone(value) ? {} : { value: value.value })
      })
      const result = yield* stream.append(
        AppendInput.create([AppendRecord.string({ body })], { matchSeqNum: projection.nextSeqNum })
      ).pipe(Effect.exit)
      if (result._tag === "Success") return value
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* Effect.fail(
      new FluentFiregridError({ message: `S2 object state read journal CAS failed after retries for ${streamName}` })
    )
  })

const appendChange = (
  runtime: Runtime,
  streamName: string,
  message: ChangeMessage.Message,
  owner: S2ObjectStateOwner | undefined
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    let attempts = 0
    while (attempts < 16) {
      attempts += 1
      const projection = yield* readProjection(runtime, streamName, owner?.invocationStreamName)
      const txid = ChangeMessage.isChange(message) ? message.headers.txid : undefined
      if (txid !== undefined && projection.appliedTxids.has(txid)) return
      yield* verifyOwner(runtime, owner)
      const stream = yield* getStream(runtime, streamName)
      const body = yield* ChangeMessage.encode(message)
      const result = yield* stream.append(
        AppendInput.create([AppendRecord.string({ body })], { matchSeqNum: projection.nextSeqNum })
      ).pipe(Effect.exit)
      if (result._tag === "Success") {
        return
      }
      if (!isCasConflict(result.cause)) {
        return yield* Effect.failCause(result.cause)
      }
    }
    return yield* Effect.fail(
      new FluentFiregridError({ message: `S2 object state CAS failed after retries for ${streamName}` })
    )
  })

const isCasConflict = (cause: unknown): boolean =>
  cause instanceof SeqNumMismatchError
  || (typeof cause === "object" && cause !== null && String((cause as { readonly status?: unknown }).status) === "412")

const sanitize = (value: string): string => encodeURIComponent(value).replace(/%/g, "_")

export const readS2ObjectState = (
  config: S2ObjectStateBackendConfig,
  address: S2ObjectStateAddress
): Effect.Effect<MaterializedState, FluentFiregridError> => {
  const runtime = makeRuntime(config)
  const streamName = objectStateStreamName(config, address)
  return readProjection(runtime, streamName, objectInvocationStreamName(config, address)).pipe(
    Effect.map((projection) => projection.materialized),
    Effect.mapError((cause) =>
      cause instanceof FluentFiregridError
        ? cause
        : new FluentFiregridError({ cause, message: `failed to read S2 object state ${streamName}` })
    )
  )
}

export const getS2ObjectStateValue = (
  config: S2ObjectStateBackendConfig,
  address: S2ObjectStateAddress,
  table: string,
  key: string
): Effect.Effect<Option.Option<unknown>, FluentFiregridError> =>
  readS2ObjectState(config, address).pipe(Effect.map((state) => state.get(table, key)))
