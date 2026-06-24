import { AppendInput, type AppendOptions, type AppendSessionApi, S2Client, S2Error, type StreamApi } from "effect-s2"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

import { type FlowError, flowError } from "../runtime/FlowError.ts"
import { appendRecord, decodeRecord, encodeKey } from "./Codec.ts"
import type { EventStreamDef } from "./EventStream.ts"
import { streamName } from "./EventStream.ts"
import type { EventRecord } from "./Record.ts"

const isEmptyReadError = (cause: unknown): boolean =>
  cause instanceof S2Error && (cause.status === 404 || cause.status === 416)

export const physicalStreamName = Effect.fn("stream.physicalStreamName")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  key: K
) {
  const encodedKey = yield* encodeKey(definition, key)
  return streamName(definition, encodedKey)
})

export const open = Effect.fn("stream.open")(function*<K, A>(
  basin: string,
  definition: EventStreamDef<K, A>,
  key: K
) {
  const physicalName = yield* physicalStreamName(definition, key)
  const client = yield* S2Client
  return yield* client.stream(basin, physicalName).pipe(
    Effect.mapError((cause) => flowError("read-session", `failed to open stream ${physicalName}`, cause))
  )
})

export const appendToStream = Effect.fn("stream.appendToStream")(function*<K, A>(
  stream: StreamApi,
  definition: EventStreamDef<K, A>,
  value: A,
  options?: AppendOptions
) {
  const record = yield* appendRecord(definition, value)
  return yield* stream.append(AppendInput.create([record], options)).pipe(
    Effect.mapError((cause) => flowError("write", `failed to append to stream ${stream.name}`, cause))
  )
})

export const append = Effect.fn("stream.append")(function*<K, A>(
  basin: string,
  definition: EventStreamDef<K, A>,
  key: K,
  value: A,
  options?: AppendOptions
) {
  const stream = yield* open(basin, definition, key)
  return yield* appendToStream(stream, definition, value, options)
})

export const appendWithSession = Effect.fn("stream.appendWithSession")(function*<K, A>(
  session: AppendSessionApi,
  definition: EventStreamDef<K, A>,
  key: K,
  value: A,
  options?: AppendOptions
) {
  yield* encodeKey(definition, key)
  const record = yield* appendRecord(definition, value)
  return yield* session.submit(AppendInput.create([record], options)).pipe(
    Effect.mapError((cause) => flowError("write", `failed to submit to append session for ${definition.name}`, cause))
  )
})

export const readSessionFromStream = <K, A>(
  stream: StreamApi,
  definition: EventStreamDef<K, A>,
  key: K,
  fromSeqNum = 0
): Stream.Stream<EventRecord<K, A>, FlowError> =>
  Stream.unwrap(
    Effect.map(encodeKey(definition, key), (encodedKey) =>
      stream.readSession({ start: { from: { seqNum: fromSeqNum }, clamp: true } }).pipe(
        Stream.mapEffect((record) =>
          decodeRecord(definition, key, record)
        ),
        Stream.mapError((cause) =>
          flowError("read-session", `failed to read stream ${streamName(definition, encodedKey)}`, cause)
        ),
        Stream.catch((cause) =>
          isEmptyReadError(cause.cause)
            ? Stream.empty
            : Stream.fail(cause)
        )
      ))
  )

export const readSession = <K, A>(
  basin: string,
  definition: EventStreamDef<K, A>,
  key: K,
  fromSeqNum = 0
): Stream.Stream<EventRecord<K, A>, FlowError, S2Client> =>
  Stream.unwrap(
    Effect.map(open(basin, definition, key), (stream) => readSessionFromStream(stream, definition, key, fromSeqNum))
  )
