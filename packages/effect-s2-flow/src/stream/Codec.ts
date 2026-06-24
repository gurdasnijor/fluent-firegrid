import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

import { flowError } from "../runtime/FlowError.ts"
import type { EventStreamDef } from "./EventStream.ts"
import { physicalName } from "./EventStream.ts"
import type { EventRecord } from "./Record.ts"

export interface EncodedEventRecord {
  readonly seqNum: number
  readonly key: string
  readonly body: string
  readonly headers?: ReadonlyArray<readonly [string, string]>
}

export const encodeKey = Effect.fn("FlowStreamCodec.encodeKey")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  key: K
) {
  return yield* Schema.encodeEffect(definition.key)(key).pipe(
    Effect.mapError((cause) => flowError("encode", `failed to encode key for stream ${definition.name}`, cause))
  )
})

export const encodeValue = Effect.fn("FlowStreamCodec.encodeValue")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  value: A
) {
  return yield* Schema.encodeEffect(definition.value)(value).pipe(
    Effect.mapError((cause) => flowError("encode", `failed to encode value for stream ${definition.name}`, cause))
  )
})

export const decodeValue = Effect.fn("FlowStreamCodec.decodeValue")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  body: string
) {
  return yield* Schema.decodeUnknownEffect(definition.value)(body).pipe(
    Effect.mapError((cause) => flowError("decode", `failed to decode value for stream ${definition.name}`, cause))
  )
})

export const encode = Effect.fn("FlowStreamCodec.encode")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  key: K,
  value: A
) {
  const encodedKey = yield* encodeKey(definition, key)
  const body = yield* encodeValue(definition, value)
  return {
    key: encodedKey,
    body
  }
})

export const decodeRecord = Effect.fn("FlowStreamCodec.decodeRecord")(function*<K, A>(
  definition: EventStreamDef<K, A>,
  record: EncodedEventRecord
) {
  const key = yield* Schema.decodeUnknownEffect(definition.key)(record.key).pipe(
    Effect.mapError((cause) => flowError("decode", `failed to decode key for stream ${definition.name}`, cause))
  )
  const value = yield* decodeValue(definition, record.body)
  const stream = physicalName(definition.name, record.key)
  return {
    stream,
    key,
    value,
    cursor: {
      stream,
      seqNum: record.seqNum
    },
    headers: new Map(record.headers ?? [])
  } satisfies EventRecord<K, A>
})
