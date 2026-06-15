import {
  AppendInput,
  type AppendAck,
  type ReadOptions,
  type SdkAppendRecord,
  type StreamInfo,
  type Tail,
} from "./internal/sdk.ts"
import type {
  AppendOptions,
  AppendSessionConfig,
  ProducerConfig,
  S2AppendSession,
  S2ClientApi,
  S2Producer,
} from "./S2Client.ts"
import { S2Client } from "./S2Client.ts"
import {
  isStringAppendRecord,
  type S2Record,
  type S2RecordBytes,
} from "./internal/record.ts"
import { S2Conflict, S2NotFound, type S2ClientError } from "./S2Error.ts"
import { Clock, Effect, Layer, PubSub, Stream, SynchronizedRef } from "effect"

interface StoredStringRecord extends S2Record {
  readonly format: "string"
}

interface StoredBytesRecord extends S2RecordBytes {
  readonly format: "bytes"
}

type StoredRecord = StoredStringRecord | StoredBytesRecord

interface StreamState {
  readonly name: string
  readonly createdAt: Date
  readonly records: ReadonlyArray<StoredRecord>
  readonly pubsub: PubSub.PubSub<StoredRecord>
}

type StreamsByName = ReadonlyMap<string, StreamState>

interface AppendCommit {
  readonly ack: AppendAck
  readonly records: ReadonlyArray<StoredRecord>
  readonly pubsub: PubSub.PubSub<StoredRecord>
}

const pubSubCapacity = 1024
const textEncoder = new TextEncoder()

const makeTail = (seqNum: number, timestamp: number): Tail => ({
  tail: {
    seqNum,
    timestamp: new Date(timestamp),
  },
})

const makeAck = (start: number, end: number, timestamp: number): AppendAck => ({
  start: {
    seqNum: start,
    timestamp: new Date(timestamp),
  },
  end: {
    seqNum: end,
    timestamp: new Date(timestamp),
  },
  tail: {
    seqNum: end,
    timestamp: new Date(timestamp),
  },
})

const streamInfo = (state: StreamState): StreamInfo => ({
  name: state.name,
  createdAt: state.createdAt,
})

const createStreamState = (name: string): Effect.Effect<StreamState> =>
  Effect.gen(function*() {
    const pubsub = yield* PubSub.bounded<StoredRecord>(pubSubCapacity)
    const now = yield* Clock.currentTimeMillis
    return {
      name,
      createdAt: new Date(now),
      records: [],
      pubsub,
    }
  })

const notFound = (operation: string, name: string): S2NotFound =>
  S2NotFound.make({
    operation,
    message: `S2 stream not found: ${name}`,
    status: 404,
    cause: name,
  })

const conflictError = (operation: string, message: string, expected: number, observed: number): S2Conflict =>
  S2Conflict.make({
    operation,
    message,
    status: 409,
    expectedSeqNum: expected,
    observedSeqNum: observed,
    cause: message,
  })

const normalizeRecord = (
  record: SdkAppendRecord,
  seqNum: number,
  timestamp: number,
): StoredRecord => {
  if (isStringAppendRecord(record)) {
    return {
      format: "string",
      seqNum,
      timestamp,
      headers: record.headers ?? [],
      body: record.body,
    }
  }
  return {
    format: "bytes",
    seqNum,
    timestamp,
    headers: record.headers ?? [],
    body: record.body,
  }
}

const normalizeRecords = (
  records: ReadonlyArray<SdkAppendRecord>,
  startSeqNum: number,
  timestamp: number,
): ReadonlyArray<StoredRecord> =>
  records.map((record, index) => normalizeRecord(record, startSeqNum + index, timestamp))

const asStringRecord = (record: StoredRecord): S2Record => ({
  seqNum: record.seqNum,
  timestamp: record.timestamp,
  headers: record.format === "string" ? record.headers : [],
  body: record.format === "string" ? record.body : "",
})

const asBytesRecord = (record: StoredRecord): S2RecordBytes => ({
  seqNum: record.seqNum,
  timestamp: record.timestamp,
  headers:
    record.format === "bytes"
      ? record.headers
      : record.headers.map(([key, value]) => [textEncoder.encode(key), textEncoder.encode(value)]),
  body: record.format === "bytes" ? record.body : textEncoder.encode(record.body),
})

const createResult = (info: StreamInfo, streams: StreamsByName): readonly [StreamInfo, StreamsByName] => [
  info,
  streams,
]

const appendResult = (
  commit: AppendCommit,
  streams: StreamsByName,
): readonly [AppendCommit, StreamsByName] => [commit, streams]

const startIndex = (records: ReadonlyArray<StoredRecord>, options: ReadOptions): number => {
  const from = options.start?.from
  if (from === undefined) {
    return 0
  }
  if ("seqNum" in from) {
    return Math.max(0, from.seqNum)
  }
  if ("tailOffset" in from) {
    return Math.max(0, records.length - from.tailOffset)
  }
  const firstIndex = records.findIndex((record) => record.timestamp >= new Date(from.timestamp).getTime())
  return firstIndex === -1 ? records.length : firstIndex
}

const applyStop = <A extends S2Record | S2RecordBytes>(
  stream: Stream.Stream<A>,
  options: ReadOptions,
): Stream.Stream<A> => {
  const count = options.stop?.limits?.count
  if (count !== undefined) {
    return stream.pipe(Stream.take(count))
  }
  const untilTimestamp = options.stop?.untilTimestamp
  if (untilTimestamp !== undefined) {
    const until = new Date(untilTimestamp).getTime()
    return stream.pipe(Stream.takeUntil((record) => record.timestamp >= until))
  }
  return stream
}

const makeApi = Effect.fn("TestS2.makeApi")(function*() {
  const state = yield* SynchronizedRef.make<StreamsByName>(new Map())

  const createStream = Effect.fn("TestS2.createStream")(function*(name: string) {
    return yield* SynchronizedRef.modifyEffect(state, (streams) => {
      const existing = streams.get(name)
      if (existing !== undefined) {
        return Effect.fail(
          S2Conflict.make({
            operation: "createStream",
            message: `S2 stream already exists: ${name}`,
            status: 409,
            cause: name,
          }),
        )
      }
      return createStreamState(name).pipe(
        Effect.map((created) => {
          const next = new Map(streams)
          next.set(name, created)
          return createResult(streamInfo(created), next)
        }),
      )
    })
  })

  const checkTail = Effect.fn("TestS2.checkTail")(function*(name: string) {
    const streams = yield* SynchronizedRef.get(state)
    const stream = streams.get(name)
    if (stream === undefined) {
      return yield* Effect.fail(notFound("checkTail", name))
    }
    const last = stream.records[stream.records.length - 1]
    return makeTail(stream.records.length, last?.timestamp ?? stream.createdAt.getTime())
  })

  const append = Effect.fn("TestS2.append")(function*(
    name: string,
    records: ReadonlyArray<SdkAppendRecord>,
    options?: AppendOptions,
  ) {
    AppendInput.create(records, options)
    const now = yield* Clock.currentTimeMillis
    const commit = yield* SynchronizedRef.modifyEffect(state, (streams) => {
      const stream = streams.get(name)
      if (stream === undefined) {
        return Effect.fail(notFound("append", name))
      }
      const observed = stream.records.length
      if (options?.matchSeqNum !== undefined && options.matchSeqNum !== observed) {
        return Effect.fail(
          conflictError(
            "append",
            `S2 matchSeqNum conflict on ${name}`,
            options.matchSeqNum,
            observed,
          ),
        )
      }
      const normalized = normalizeRecords(records, observed, now)
      const updated: StreamState = {
        ...stream,
        records: [...stream.records, ...normalized],
      }
      const next = new Map(streams)
      next.set(name, updated)
      return Effect.succeed(
        appendResult({
          ack: makeAck(observed, observed + normalized.length, now),
          records: normalized,
          pubsub: stream.pubsub,
        }, next),
      )
    })
    yield* PubSub.publishAll(commit.pubsub, commit.records).pipe(Effect.asVoid)
    return commit.ack
  })

  const read = (name: string, options: ReadOptions): Stream.Stream<S2Record, S2ClientError> =>
    Stream.unwrap(
      Effect.gen(function*() {
        const streams = yield* SynchronizedRef.get(state)
        const stream = streams.get(name)
        if (stream === undefined) {
          return yield* Effect.fail(notFound("read", name))
        }
        const replay = stream.records.slice(startIndex(stream.records, options))
        const replayStream = Stream.fromIterable(replay)
        const follow = Stream.fromPubSub(stream.pubsub)
        const combined =
          options.stop === undefined ? replayStream.pipe(Stream.concat(follow)) : replayStream
        return applyStop(combined.pipe(Stream.map(asStringRecord)), options)
      }),
    )

  const readBytes = (
    name: string,
    options: ReadOptions,
  ): Stream.Stream<S2RecordBytes, S2ClientError> =>
    Stream.unwrap(
      Effect.gen(function*() {
        const streams = yield* SynchronizedRef.get(state)
        const stream = streams.get(name)
        if (stream === undefined) {
          return yield* Effect.fail(notFound("readBytes", name))
        }
        const replay = stream.records.slice(startIndex(stream.records, options))
        const replayStream = Stream.fromIterable(replay)
        const follow = Stream.fromPubSub(stream.pubsub)
        const combined =
          options.stop === undefined ? replayStream.pipe(Stream.concat(follow)) : replayStream
        return applyStop(combined.pipe(Stream.map(asBytesRecord)), options)
      }),
    )

  const appendSession = Effect.fn("TestS2.appendSession")(function*(
    name: string,
    _config: AppendSessionConfig = {},
  ) {
    const session: S2AppendSession = {
      submit: (records, options) => append(name, records, options),
    }
    return session
  })

  const producer = Effect.fn("TestS2.producer")(function*(name: string, _config: ProducerConfig = {}) {
    const s2Producer: S2Producer = {
      submit: (record) => append(name, [record]),
    }
    return s2Producer
  })

  const api: S2ClientApi = {
    createStream,
    checkTail,
    append,
    read,
    readBytes,
    appendSession,
    producer,
  }
  return api
})

export const layer: Layer.Layer<S2Client> = Layer.effect(S2Client, makeApi())
