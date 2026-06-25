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
import type * as Option from "effect/Option"
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

interface Runtime {
  readonly basinName: string
  readonly layer: ReturnType<typeof S2Layer>
}

interface Projection {
  readonly materialized: MaterializedState
  readonly nextSeqNum: number
}

export const objectStateStreamName = (
  config: Pick<S2ObjectStateBackendConfig, "namespace">,
  address: S2ObjectStateAddress
): string =>
  `${sanitize(config.namespace ?? "default")}/obj/${sanitize(address.objectName)}/${sanitize(address.key)}/state`

export const createS2ObjectStateBackend = (
  config: S2ObjectStateBackendConfig,
  address: S2ObjectStateAddress
): ObjectStateBackend => {
  const runtime = makeRuntime(config)
  const streamName = objectStateStreamName(config, address)
  const run = <A>(effect: Effect.Effect<A, unknown, never>): Effect.Effect<A, FluentFiregridError> =>
    effect.pipe(
      Effect.mapError((cause) =>
        cause instanceof FluentFiregridError
          ? cause
          : new FluentFiregridError({ cause, message: `S2 object state operation failed for ${streamName}` })
      )
    )

  return {
    get: (table, key) =>
      run(
        readProjection(runtime, streamName).pipe(
          Effect.map((projection) => projection.materialized.get(table, key))
        )
      ),
    set: (table, key, value) =>
      run(
        appendChange(runtime, streamName, {
          headers: { operation: "update" },
          key,
          type: table,
          value
        }).pipe(Effect.asVoid)
      ),
    delete: (table, key) =>
      run(
        appendChange(runtime, streamName, {
          headers: { operation: "delete" },
          key,
          type: table
        }).pipe(Effect.asVoid)
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

const readProjection = (runtime: Runtime, streamName: string): Effect.Effect<Projection, unknown> =>
  Effect.gen(function*() {
    const stream = yield* getStream(runtime, streamName)
    const tail = yield* stream.checkTail()
    const materialized = MaterializedState.empty()
    if (tail.tail.seqNum <= 0) {
      return { materialized, nextSeqNum: tail.tail.seqNum }
    }
    const records = yield* stream.readSession({
      start: { from: { seqNum: 0 } },
      stop: { limits: { count: tail.tail.seqNum } }
    }).pipe(Stream.runCollect)
    yield* Effect.forEach(
      Array.from(records),
      (record) =>
        ChangeMessage.decode(record.body).pipe(
          Effect.tap((message) => Effect.sync(() => materialized.apply(message)))
        ),
      { discard: true }
    )
    return { materialized, nextSeqNum: tail.tail.seqNum }
  })

const appendChange = (
  runtime: Runtime,
  streamName: string,
  message: ChangeMessage.Message
): Effect.Effect<void, unknown> =>
  Effect.gen(function*() {
    let attempts = 0
    while (attempts < 16) {
      attempts += 1
      const projection = yield* readProjection(runtime, streamName)
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
  return readProjection(runtime, streamName).pipe(
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
