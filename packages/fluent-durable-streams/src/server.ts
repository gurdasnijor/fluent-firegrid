import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  RangeNotSatisfiableError,
  S2Error,
  SeqNumMismatchError,
  type AppendAck as S2AppendAck,
  type ReadBatch as S2ReadBatch,
  type ReadRecord as S2ReadRecord,
  type StreamPosition as S2StreamPosition,
} from "@s2-dev/streamstore"
import { Effect, Layer, Schema } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import type {
  AppendAck,
  ReadBatch,
  ReadRecord,
  ReadQuery,
  StateMessage,
  StateReadBatch,
  StateRecord,
  StreamPosition,
} from "./api.ts"
import {
  ApiError,
  DurableStreamsApi,
  StateMessage as StateMessageSchema,
} from "./api.ts"
import { S2Profile, type S2ProfileError, type S2ProfileService } from "./s2.ts"
import { tryS2 } from "./errors.ts"

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

const decodeBase64 = (value: string): Uint8Array =>
  Uint8Array.from(atob(value), (char) => char.charCodeAt(0))

const encodeBase64 = (bytes: Uint8Array): string =>
  btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""))

const fromHeaderPair = ([name, value]: readonly [string, string]): readonly [Uint8Array, Uint8Array] => [
  textEncoder.encode(name),
  textEncoder.encode(value),
]

const stringHeader = (name: string, value: string): readonly [string, string] => [name, value]

const toHeaderPair = ([name, value]: readonly [Uint8Array, Uint8Array]): readonly [string, string] => [
  encodeBase64(name),
  encodeBase64(value),
]

const textHeaderValue = (
  headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
  name: string,
): string | undefined => {
  const value = headers.find(([candidate]) => textDecoder.decode(candidate) === name)?.[1]
  return value === undefined ? undefined : textDecoder.decode(value)
}

const streamPosition = (position: S2StreamPosition): StreamPosition =>
  ({
    seqNum: position.seqNum,
    timestamp: position.timestamp.toISOString(),
  })

const appendAck = (ack: S2AppendAck): AppendAck =>
  ({
    start: streamPosition(ack.start),
    end: streamPosition(ack.end),
    tail: streamPosition(ack.tail),
  })

const readRecord = (record: S2ReadRecord<"bytes">): ReadRecord =>
  ({
    seqNum: record.seqNum,
    bodyBase64: encodeBase64(record.body),
    headers: record.headers.map(toHeaderPair),
    timestamp: record.timestamp.toISOString(),
  })

const readBatch = (batch: S2ReadBatch<"bytes">): ReadBatch => {
  const payload = {
    records: batch.records.map(readRecord),
    ...(batch.tail === undefined ? {} : { tail: streamPosition(batch.tail) }),
  }
  return payload
}

const isStateChange = (message: StateMessage): message is Extract<StateMessage, { readonly type: string }> =>
  "type" in message

const optionalStateHeaders = (
  headers: Readonly<{
    readonly txid?: string | undefined
    readonly schema?: string | undefined
  }>,
): ReadonlyArray<readonly [string, string]> => [
  ...(headers.txid === undefined ? [] : [stringHeader("ds-state-txid", headers.txid)]),
  ...(headers.schema === undefined ? [] : [stringHeader("ds-state-schema", headers.schema)]),
]

const stateHeaders = (message: StateMessage): ReadonlyArray<readonly [string, string]> => {
  const base = [
    stringHeader("ds-kind", "state"),
    stringHeader("ds-content-type", "application/vnd.firegrid.state+json"),
  ]
  if (isStateChange(message)) {
    return [
      ...base,
      stringHeader("ds-state-kind", "change"),
      stringHeader("ds-state-type", message.type),
      stringHeader("ds-state-key", message.key),
      stringHeader("ds-state-operation", message.headers.operation),
      ...optionalStateHeaders(message.headers),
    ]
  }
  return [
    ...base,
    stringHeader("ds-state-kind", "control"),
    stringHeader("ds-state-control", message.headers.control),
    ...optionalStateHeaders(message.headers),
  ]
}

const stateAppendRecord = (message: StateMessage): AppendRecord =>
  AppendRecord.string({
    body: JSON.stringify(message),
    headers: stateHeaders(message),
  })

const decodeStateRecord = (record: S2ReadRecord<"bytes">): Effect.Effect<StateRecord, ApiError> => {
  if (textHeaderValue(record.headers, "ds-kind") !== "state") {
    return Effect.fail(new ApiError({
      status: 422,
      message: `S2 record ${record.seqNum} is not a state record`,
      code: "not-state-record",
    }))
  }
  return Schema.decodeUnknownEffect(StateMessageSchema)(JSON.parse(textDecoder.decode(record.body))).pipe(
    Effect.map((message): StateRecord => ({
      seqNum: record.seqNum,
      timestamp: record.timestamp.toISOString(),
      message,
    })),
    Effect.mapError((error) =>
      new ApiError({
        status: 422,
        message: `Invalid state record ${record.seqNum}: ${String(error)}`,
        code: "invalid-state-record",
      }),
    ),
  )
}

const stateReadBatch = (batch: S2ReadBatch<"bytes">): Effect.Effect<StateReadBatch, ApiError> =>
  Effect.forEach(
    batch.records.filter((record) => textHeaderValue(record.headers, "ds-kind") === "state"),
    decodeStateRecord,
  ).pipe(
    Effect.map((records) => ({
      records,
      ...(batch.tail === undefined ? {} : { tail: streamPosition(batch.tail) }),
    })),
  )

const apiError = (error: S2ProfileError): ApiError => {
  if (
    error instanceof S2Error ||
    error instanceof SeqNumMismatchError ||
    error instanceof FencingTokenMismatchError ||
    error instanceof RangeNotSatisfiableError
  ) {
    return new ApiError({
      status: error.status,
      message: error.message,
      ...(error.code === undefined ? {} : { code: error.code }),
    })
  }
  return new ApiError({
    status: 500,
    message: "Unknown S2 error",
  })
}

const catchS2 = <A, R>(effect: Effect.Effect<A, S2ProfileError, R>): Effect.Effect<A, ApiError, R> =>
  Effect.mapError(effect, apiError)

const appendOptions = (
  input: Readonly<{
    readonly matchSeqNum?: number | undefined
    readonly fencingToken?: string | undefined
  }>,
): Readonly<{
  readonly matchSeqNum?: number
  readonly fencingToken?: string
}> => ({
  ...(input.matchSeqNum === undefined ? {} : { matchSeqNum: input.matchSeqNum }),
  ...(input.fencingToken === undefined ? {} : { fencingToken: input.fencingToken }),
})

const readInput = (query: ReadQuery) => ({
  start: query.tailOffset === undefined
    ? query.seqNum === undefined
      ? { clamp: true }
      : { from: { seqNum: query.seqNum }, clamp: true }
    : { from: { tailOffset: query.tailOffset }, clamp: true },
  stop: {
    limits: {
      ...(query.count === undefined ? {} : { count: query.count }),
    },
    ...(query.waitSecs === undefined ? {} : { waitSecs: query.waitSecs }),
  },
  ignoreCommandRecords: query.ignoreCommandRecords ?? true,
})

const appendRecords = (
  profile: S2ProfileService,
  stream: string,
  records: ReadonlyArray<AppendRecord>,
  conditions: Readonly<{
    readonly matchSeqNum?: number | undefined
    readonly fencingToken?: string | undefined
  }>,
) =>
  catchS2(tryS2(() =>
    profile.basin.stream(stream).append(
      AppendInput.create(records, appendOptions(conditions)),
    ),
  ))

const readBytes = (
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
) =>
  catchS2(tryS2(() =>
    profile.basin.stream(stream).read(
      readInput(query),
      { as: "bytes" },
    ),
  ))

export const StreamsLive = HttpApiBuilder.group(
  DurableStreamsApi,
  "Streams",
  (handlers) =>
    handlers
      .handle("ensureStream", ({ params }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* catchS2(tryS2(() => profile.basin.streams.ensure({ stream: params.stream })))
          return {
            result: response.result,
            stream: response.stream.name,
          }
        }))
      .handle("checkTail", ({ params }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* catchS2(tryS2(() => profile.basin.stream(params.stream).checkTail()))
          return streamPosition(response.tail)
        }))
      .handle("append", ({ params, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const records = payload.records.map((record) =>
            AppendRecord.bytes({
              body: decodeBase64(record.bodyBase64),
              headers: (record.headers ?? []).map(fromHeaderPair),
            }),
          )
          const response = yield* appendRecords(profile, params.stream, records, payload)
          return appendAck(response)
        }))
      .handle("appendRaw", ({ params, query, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* appendRecords(profile, params.stream, [AppendRecord.bytes({ body: payload })], query)
          return appendAck(response)
        }))
      .handle("read", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* readBytes(profile, params.stream, query)
          return readBatch(response)
        })),
)

export const StateLive = HttpApiBuilder.group(
  DurableStreamsApi,
  "State",
  (handlers) =>
    handlers
      .handle("appendState", ({ params, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* appendRecords(profile, params.stream, payload.records.map(stateAppendRecord), payload)
          return appendAck(response)
        }))
      .handle("readState", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* readBytes(profile, params.stream, query)
          return yield* stateReadBatch(response)
        })),
)

export const DurableStreamsApiLive = HttpApiBuilder.layer(DurableStreamsApi).pipe(
  Layer.provide(StreamsLive),
  Layer.provide(StateLive),
)
