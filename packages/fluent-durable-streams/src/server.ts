import { AppendRecord } from "@s2-dev/streamstore"
import { Effect, Layer, Stream } from "effect"
import { HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { DurableStreamsApi } from "./api.ts"
import { tryS2 } from "./errors.ts"
import {
  appendRecords,
  liveReadSse,
  liveStateSse,
  readBytes,
  recordStream,
} from "./s2DataPlane.ts"
import { catchS2 } from "./s2Errors.ts"
import {
  appendAck,
  CLOSE_HEADER,
  decodeBase64,
  fromHeaderPair,
  isCloseRecord,
  readBatch,
  stateAppendRecord,
  stateReadBatch,
  tailResponse,
} from "./s2Records.ts"
import { S2Profile } from "./s2.ts"

const appendToStream = (
  stream: string,
  records: ReadonlyArray<AppendRecord>,
  conditions: Parameters<typeof appendRecords>[3],
) =>
  Effect.gen(function*() {
    const profile = yield* S2Profile
    const response = yield* appendRecords(profile, stream, records, conditions)
    return appendAck(response)
  })

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
          return tailResponse(response.tail)
        }))
      .handle("append", ({ params, payload }) =>
        Effect.gen(function*() {
          const records = payload.records.map((record) =>
            AppendRecord.bytes({
              body: decodeBase64(record.body),
              headers: (record.headers ?? []).map(fromHeaderPair),
            }),
          )
          return yield* appendToStream(params.stream, records, payload)
        }))
      .handle("appendRaw", ({ params, payload, query }) =>
        appendToStream(params.stream, [AppendRecord.bytes({ body: payload })], query))
      .handle("close", ({ params, query }) =>
        appendToStream(
          params.stream,
          [AppendRecord.bytes({ body: new Uint8Array(0), headers: [fromHeaderPair(CLOSE_HEADER)] })],
          query,
        ))
      .handle("read", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* readBytes(profile, params.stream, query)
          return readBatch(response)
        }))
      .handle("readRaw", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const stream = recordStream(profile, params.stream, query).pipe(
            Stream.filter((record) => !isCloseRecord(record)),
            Stream.map((record) => record.body),
          )
          return HttpServerResponse.stream(stream)
        }))
      .handle("readLive", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          return HttpServerResponse.stream(liveReadSse(profile, params.stream, query))
        })),
)

export const StateLive = HttpApiBuilder.group(
  DurableStreamsApi,
  "State",
  (handlers) =>
    handlers
      .handle("appendState", ({ params, payload }) =>
        appendToStream(params.stream, payload.records.map(stateAppendRecord), payload))
      .handle("readState", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* readBytes(profile, params.stream, query)
          return yield* stateReadBatch(response)
        }))
      .handle("readStateLive", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          return HttpServerResponse.stream(liveStateSse(profile, params.stream, query))
        })),
)

export const DurableStreamsApiLive = HttpApiBuilder.layer(DurableStreamsApi).pipe(
  Layer.provide(StreamsLive),
  Layer.provide(StateLive),
)
