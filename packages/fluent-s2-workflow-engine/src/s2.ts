import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  RangeNotSatisfiableError,
  S2,
  S2Error,
  SeqNumMismatchError,
  type S2Basin,
  type S2Stream,
} from "@s2-dev/streamstore"
import { Context, Effect, Layer } from "effect"
import {
  defaultStreamPrefix,
  S2WorkflowEngineConfigTag,
  type S2WorkflowEngineConfig,
} from "./config.ts"
import { decodeRecord, encodeRecord, type WorkflowRecord } from "./records.ts"

export interface S2WorkflowStore {
  readonly config: S2WorkflowEngineConfig
  readonly ensureStream: (streamName: string) => Effect.Effect<S2Stream>
  readonly append: (
    streamName: string,
    records: ReadonlyArray<WorkflowRecord>,
    options?: { readonly fencingToken?: string | undefined },
  ) => Effect.Effect<void>
  readonly appendFence: (
    streamName: string,
    token: string,
  ) => Effect.Effect<"acquired" | "raced">
  readonly readAll: (streamName: string) => Effect.Effect<ReadonlyArray<WorkflowRecord>>
  readonly listExecutionStreams: () => Effect.Effect<ReadonlyArray<string>>
}

export class S2WorkflowStoreTag extends Context.Service<
  S2WorkflowStoreTag,
  S2WorkflowStore
>()("@firegrid/fluent-s2-workflow-engine/S2WorkflowStore") {}

const toPromise = <A>(evaluate: () => Promise<A>): Effect.Effect<A> =>
  Effect.promise(evaluate)

const tryPromise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, unknown> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (error) => error,
  })

const defectFromUnknown = (error: unknown): Effect.Effect<never> =>
  Effect.sync(() => {
    throw error
  })

const makeS2 = (config: S2WorkflowEngineConfig): S2 => {
  const options: ConstructorParameters<typeof S2>[0] = {
    accessToken: config.accessToken,
    retry: {
      maxAttempts: 3,
      appendRetryPolicy: "noSideEffects",
    },
  }
  if (config.endpoints !== undefined) {
    options.endpoints = config.endpoints
  }
  if (config.requestTimeoutMillis !== undefined) {
    options.requestTimeoutMillis = config.requestTimeoutMillis
  }
  if (config.connectionTimeoutMillis !== undefined) {
    options.connectionTimeoutMillis = config.connectionTimeoutMillis
  }
  return new S2(options)
}

const streamHandle = (basin: S2Basin, streamName: string): S2Stream =>
  basin.stream(streamName, { forceTransport: "fetch" })

const isEmptyRead = (error: unknown): error is RangeNotSatisfiableError | S2Error =>
  error instanceof RangeNotSatisfiableError
  || (error instanceof S2Error && error.status === 404)

const makeStore = (
  config: S2WorkflowEngineConfig,
): Effect.Effect<S2WorkflowStore> =>
  Effect.gen(function*() {
    const s2 = makeS2(config)
    yield* toPromise(() =>
      s2.basins.ensure({
        basin: config.basin,
      }),
    )
    const basin = s2.basin(config.basin)
    const ensured = new Set<string>()

    const ensureStream = (streamName: string): Effect.Effect<S2Stream> =>
      Effect.gen(function*() {
        if (!ensured.has(streamName)) {
          yield* toPromise(() =>
            basin.streams.ensure({
              stream: streamName,
            }),
          )
          ensured.add(streamName)
        }
        return streamHandle(basin, streamName)
      })

    const append = (
      streamName: string,
      records: ReadonlyArray<WorkflowRecord>,
      options?: { readonly fencingToken?: string | undefined },
    ): Effect.Effect<void> =>
      Effect.gen(function*() {
        if (records.length === 0) return
        const stream = yield* ensureStream(streamName)
        const input = AppendInput.create(
          records.map((record) =>
            AppendRecord.string({
              body: encodeRecord(record),
              headers: [["content-type", "application/json"]],
            }),
          ),
          options?.fencingToken === undefined
            ? undefined
            : { fencingToken: options.fencingToken },
        )
        yield* toPromise(() => stream.append(input))
      })

    const appendFence = (
      streamName: string,
      token: string,
    ): Effect.Effect<"acquired" | "raced"> =>
      Effect.gen(function*() {
        const stream = yield* ensureStream(streamName)
        const tail = yield* toPromise(() => stream.checkTail())
        const input = AppendInput.create(
          [AppendRecord.fence(token)],
          { matchSeqNum: tail.tail.seqNum },
        )
        return yield* tryPromise(() => stream.append(input)).pipe(
          Effect.as("acquired" as const),
          Effect.catchIf(
            (error): error is SeqNumMismatchError => error instanceof SeqNumMismatchError,
            () => Effect.succeed("raced" as const),
            defectFromUnknown,
          ),
        )
      })

    const readAll = (streamName: string): Effect.Effect<ReadonlyArray<WorkflowRecord>> =>
      Effect.gen(function*() {
        const stream = streamHandle(basin, streamName)
        const out: Array<WorkflowRecord> = []
        let nextSeqNum = 0
        while (true) {
          const batch = yield* tryPromise(() =>
            stream.read(
              {
                start: { from: { seqNum: nextSeqNum }, clamp: true },
                stop: { limits: { count: 1000 } },
                ignoreCommandRecords: true,
              },
              { as: "string" },
            ),
          ).pipe(
            Effect.catchIf(
              isEmptyRead,
              () => Effect.succeed({ records: [], tail: undefined }),
              defectFromUnknown,
            ),
          )

          if (batch.records.length === 0) {
            return out
          }
          out.push(...batch.records.map((record) => decodeRecord(record.body)))
          const last = batch.records[batch.records.length - 1]
          if (last === undefined) return out
          nextSeqNum = last.seqNum + 1
          if (batch.tail !== undefined && nextSeqNum >= batch.tail.seqNum) {
            return out
          }
        }
      })

    const listExecutionStreams = (): Effect.Effect<ReadonlyArray<string>> =>
      Effect.promise(async () => {
        const iterator = basin.streams.listAll({
          prefix: `${config.streamPrefix ?? defaultStreamPrefix}/executions/`,
        })[Symbol.asyncIterator]()
        const names: Array<string> = []
        while (true) {
          const next = await iterator.next()
          if (next.done === true) {
            return names
          }
          names.push(next.value.name)
        }
      })

    return S2WorkflowStoreTag.of({
      config,
      ensureStream,
      append,
      appendFence,
      readAll,
      listExecutionStreams,
    })
  })

export const layerStore: Layer.Layer<S2WorkflowStoreTag, never, S2WorkflowEngineConfigTag> =
  Layer.effect(S2WorkflowStoreTag)(Effect.flatMap(S2WorkflowEngineConfigTag, makeStore))

export const isFencingTokenMismatch = (error: unknown): boolean =>
  error instanceof FencingTokenMismatchError
