import { AppendRecord } from "effect-s2"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

import { flowError } from "../runtime/FlowError.ts"
import type { EventStreamDef } from "./EventStream.ts"
import { streamName } from "./EventStream.ts"

export interface StringReadRecord {
  readonly seqNum: number
  readonly timestamp: Date
  readonly body: string
  readonly headers: ReadonlyArray<readonly [string, string]>
}

export const encodeKey = Effect.fn("stream.encodeKey")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  key: K
) {
  return yield* Schema.encodeEffect(definition.key)(key).pipe(
    Effect.mapError((cause) => flowError("encode", `failed to encode key for stream ${definition.name}`, cause))
  )
})

export const encodeValue = Effect.fn("stream.encodeValue")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  value: A
) {
  return yield* Schema.encodeEffect(definition.value)(value).pipe(
    Effect.mapError((cause) => flowError("encode", `failed to encode value for stream ${definition.name}`, cause))
  )
})

export const appendRecord = Effect.fn("stream.appendRecord")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  value: A
) {
  const body = yield* encodeValue(definition, value)
  return AppendRecord.string({ body })
})

export const decodeRecord = Effect.fn("stream.decodeRecord")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  key: K,
  record: StringReadRecord
) {
  const encodedKey = yield* encodeKey(definition, key)
  const value = yield* Schema.decodeUnknownEffect(definition.value)(record.body).pipe(
    Effect.mapError((cause) => flowError("decode", `failed to decode value for stream ${definition.name}`, cause))
  )
  return {
    stream: streamName(definition, encodedKey),
    key,
    value,
    cursor: {
      stream: streamName(definition, encodedKey),
      seqNum: record.seqNum
    },
    headers: new Map(record.headers)
  }
})
