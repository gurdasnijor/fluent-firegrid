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
import { Effect, Layer } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import type {
  AppendAck,
  ReadBatch,
  ReadRecord,
  StreamPosition,
} from "./api.ts"
import {
  ApiError,
  DurableStreamsApi,
} from "./api.ts"
import { S2Profile, type S2ProfileError } from "./s2.ts"
import { tryS2 } from "./errors.ts"

const textEncoder = new TextEncoder()

const decodeBase64 = (value: string): Uint8Array =>
  Uint8Array.from(atob(value), (char) => char.charCodeAt(0))

const encodeBase64 = (bytes: Uint8Array): string =>
  btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""))

const fromHeaderPair = ([name, value]: readonly [string, string]): readonly [Uint8Array, Uint8Array] => [
  textEncoder.encode(name),
  textEncoder.encode(value),
]

const toHeaderPair = ([name, value]: readonly [Uint8Array, Uint8Array]): readonly [string, string] => [
  encodeBase64(name),
  encodeBase64(value),
]

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
          const response = yield* catchS2(tryS2(() =>
            profile.basin.stream(params.stream).append(
              AppendInput.create(records, {
                ...(payload.matchSeqNum === undefined ? {} : { matchSeqNum: payload.matchSeqNum }),
                ...(payload.fencingToken === undefined ? {} : { fencingToken: payload.fencingToken }),
              }),
            ),
          ))
          return appendAck(response)
        }))
      .handle("appendRaw", ({ params, query, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* catchS2(tryS2(() =>
            profile.basin.stream(params.stream).append(
              AppendInput.create([AppendRecord.bytes({ body: payload })], {
                ...(query.matchSeqNum === undefined ? {} : { matchSeqNum: query.matchSeqNum }),
                ...(query.fencingToken === undefined ? {} : { fencingToken: query.fencingToken }),
              }),
            ),
          ))
          return appendAck(response)
        }))
      .handle("read", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const start = query.tailOffset === undefined
            ? query.seqNum === undefined
              ? { clamp: true }
              : { from: { seqNum: query.seqNum }, clamp: true }
            : { from: { tailOffset: query.tailOffset }, clamp: true }
          const response = yield* catchS2(tryS2(() =>
            profile.basin.stream(params.stream).read(
              {
                start,
                stop: {
                  limits: {
                    ...(query.count === undefined ? {} : { count: query.count }),
                  },
                  ...(query.waitSecs === undefined ? {} : { waitSecs: query.waitSecs }),
                },
                ignoreCommandRecords: query.ignoreCommandRecords ?? true,
              },
              { as: "bytes" },
            ),
          ))
          return readBatch(response)
        })),
)

export const DurableStreamsApiLive = HttpApiBuilder.layer(DurableStreamsApi).pipe(
  Layer.provide(StreamsLive),
)
