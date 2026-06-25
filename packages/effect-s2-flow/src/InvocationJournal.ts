import {
  AppendInput,
  type AppendOptions,
  AppendRecord,
  basin,
  basins,
  layer as S2Layer,
  MAX_APPEND_BYTES,
  MAX_APPEND_RECORDS,
  meteredBytes,
  type S2Error,
  stream as s2Stream,
  type StreamApi
} from "effect-s2"
import * as Effect from "effect/Effect"

import { BatchTooLarge, FlowError } from "./FlowError.ts"

export const defaultBasin = "effect-s2-flow"

export interface InvocationJournalRuntimeConfig {
  readonly basin: string
  readonly s2Endpoint: string
}

export type InvocationJournalRecord =
  | {
    readonly _tag: "Invoke"
    readonly requestId: string
    readonly service: string
    readonly method: string
    readonly input: unknown
  }
  | {
    readonly _tag: "StepCompleted"
    readonly requestId: string
    readonly stepName: string
    readonly value: unknown
  }
  | {
    readonly _tag: "StateChanged"
    readonly stateName: string
    readonly value: unknown
  }
  | {
    readonly _tag: "TimerSet"
    readonly requestId: string
    readonly timerName: string
    readonly fireAtEpochMillis: number
  }
  | {
    readonly _tag: "TimerFired"
    readonly requestId: string
    readonly timerName: string
  }
  | {
    readonly _tag: "CheckpointAdvanced"
    readonly nextSeqNum: number
  }
  | {
    readonly _tag: "Completed"
    readonly requestId: string
    readonly value: unknown
  }
  | {
    readonly _tag: "Failed"
    readonly requestId: string
    readonly message: string
  }

export interface InvocationJournal {
  readonly nextSeqNum: number
  readonly records: ReadonlyArray<InvocationJournalRecord>
}

export const invocationPrefix = (serviceName: string): string => `${serviceName}.invocation.`

export const invocationStream = (serviceName: string, invocationId: string): string =>
  `${invocationPrefix(serviceName)}${invocationId}`

export const objectPrefix = (objectName: string): string => `${objectName}.object.`

export const objectStream = (objectName: string, key: string): string => `${objectPrefix(objectName)}${key}`

export const encodeRecord = (
  record: InvocationJournalRecord,
  headers: ReadonlyArray<readonly [string, string]>
): AppendRecord =>
  AppendRecord.string({
    body: JSON.stringify(record),
    headers
  })

export const decodeRecord = (body: string): InvocationJournalRecord => JSON.parse(body) as InvocationJournalRecord

export const recordTypeHeader = (type: InvocationJournalRecord["_tag"]): ReadonlyArray<readonly [string, string]> => [
  ["effect-s2-flow.record.type", type]
]

export const stepHeaders = (
  type: InvocationJournalRecord["_tag"],
  stepName: string
): ReadonlyArray<readonly [string, string]> => [
  ...recordTypeHeader(type),
  ["effect-s2-flow.step.name", stepName]
]

export const stateHeaders = (stateName: string): ReadonlyArray<readonly [string, string]> => [
  ...recordTypeHeader("StateChanged"),
  ["effect-s2-flow.state.name", stateName]
]

export const timerHeaders = (
  type: "TimerSet" | "TimerFired",
  timerName: string
): ReadonlyArray<readonly [string, string]> => [
  ...recordTypeHeader(type),
  ["effect-s2-flow.timer.name", timerName]
]

export const s2Layer = (endpoint: string) =>
  S2Layer({
    accessToken: "s2_access_token",
    endpoints: {
      account: endpoint,
      basin: endpoint
    },
    retry: { maxAttempts: 3 }
  })

export const flowS2Error = (message: string) => (cause: S2Error): FlowError => new FlowError({ message, cause })

export const checkAtomicBatch = (records: ReadonlyArray<AppendRecord>): BatchTooLarge | undefined => {
  const bytes = records.reduce((sum, record) => sum + meteredBytes(record), 0)
  if (records.length <= MAX_APPEND_RECORDS && bytes <= MAX_APPEND_BYTES) return undefined
  return new BatchTooLarge({
    bytes,
    maxBytes: MAX_APPEND_BYTES,
    maxRecords: MAX_APPEND_RECORDS,
    records: records.length
  })
}

export const appendAtomic = Effect.fn("effect-s2-flow.invocationJournal.appendAtomic")(function*(
  streamApi: StreamApi,
  records: ReadonlyArray<AppendRecord>,
  options: AppendOptions | undefined,
  errorMessage: string
) {
  const tooLarge = checkAtomicBatch(records)
  if (tooLarge !== undefined) {
    return yield* tooLarge
  }
  return yield* streamApi.append(AppendInput.create(records, options)).pipe(
    Effect.mapError(flowS2Error(errorMessage))
  )
})

export const withS2 = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  runtime: InvocationJournalRuntimeConfig
) => effect.pipe(Effect.provide(s2Layer(runtime.s2Endpoint)))

export const ensureBasin = Effect.fn("effect-s2-flow.invocationJournal.ensureBasin")(function*(
  runtime: InvocationJournalRuntimeConfig
) {
  return yield* withS2(
    basins.ensure({ basin: runtime.basin }).pipe(
      Effect.mapError(flowS2Error(`failed to ensure basin ${runtime.basin}`))
    ),
    runtime
  )
})

export const ensureInvocationJournalStream = Effect.fn("effect-s2-flow.invocationJournal.ensureStream")(function*(
  runtime: InvocationJournalRuntimeConfig,
  streamName: string
) {
  yield* ensureBasin(runtime)
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

const readRecordsFromStart = Effect.fn("effect-s2-flow.invocationJournal.readRecordsFromStart")(
  function*(
    streamApi: StreamApi,
    options: {
      readonly errorMessage: string
      readonly ignoreCommandRecords?: boolean
    }
  ) {
    const tail = yield* streamApi.checkTail().pipe(
      Effect.mapError(flowS2Error("failed to check invocation tail"))
    )
    if (tail.tail.seqNum === 0) return { nextSeqNum: 0, records: [] }
    const batch = yield* streamApi.read({
      start: { from: { seqNum: 0 } },
      ...(options.ignoreCommandRecords === undefined ? {} : { ignoreCommandRecords: options.ignoreCommandRecords }),
      stop: { limits: { count: tail.tail.seqNum } }
    }).pipe(
      Effect.mapError(flowS2Error(options.errorMessage))
    )
    return {
      nextSeqNum: tail.tail.seqNum,
      records: batch.records
    }
  }
)

export const readInvocationJournal = Effect.fn("effect-s2-flow.invocationJournal.read")(
  function*(streamApi: StreamApi) {
    const journal = yield* readRecordsFromStart(streamApi, {
      errorMessage: "failed to read invocation journal",
      ignoreCommandRecords: true
    })
    return {
      nextSeqNum: journal.nextSeqNum,
      records: journal.records.map((record) => decodeRecord(record.body))
    }
  }
)

const isFenceCommand = (headers: ReadonlyArray<readonly [string, string]>): boolean =>
  headers.length === 1 && headers[0]?.[0] === "" && headers[0][1] === "fence"

export const readCurrentFenceToken = Effect.fn("effect-s2-flow.invocationJournal.readCurrentFenceToken")(
  function*(streamApi: StreamApi) {
    const journal = yield* readRecordsFromStart(streamApi, { errorMessage: "failed to read invocation fence" })
    return journal.records.reduce<string | undefined>(
      (token, record) => isFenceCommand(record.headers) ? record.body === "" ? undefined : record.body : token,
      undefined
    )
  }
)

export const listInvocationJournalStreams = Effect.fn("effect-s2-flow.invocationJournal.listStreams")(function*(
  runtime: InvocationJournalRuntimeConfig,
  prefix: string
) {
  const basinApi = yield* withS2(
    basin(runtime.basin).pipe(
      Effect.mapError(flowS2Error(`failed to open basin ${runtime.basin}`))
    ),
    runtime
  )
  return yield* basinApi.streams.list({ prefix, limit: 1000 }).pipe(
    Effect.map((response) => response.streams.map((stream) => stream.name)),
    Effect.mapError(flowS2Error(`failed to list streams for ${prefix}`))
  )
})
