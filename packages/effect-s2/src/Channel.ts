import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type { S2Record } from "./internal/record.ts"
import { type AppendAck, AppendInput, AppendRecord, type ReadOptions } from "./internal/sdk.ts"
import { type AppendOptions, S2Client } from "./S2Client.ts"
import type { S2ClientError } from "./S2Error.ts"

const JsonValue = Schema.UnknownFromJsonString

export interface DecodedRecord<A> extends S2Record {
  readonly value: A
}

const encodedRecord = <A, I, RD, RE>(
  schema: Schema.Codec<A, I, RD, RE>,
  value: A
) =>
  Effect.gen(function*() {
    const encoded = yield* Schema.encodeEffect(schema)(value)
    const body = yield* Schema.encodeEffect(JsonValue)(encoded)
    return AppendRecord.string({ body })
  })

export const publish = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  value: A
): Effect.Effect<
  AppendAck,
  Schema.SchemaError | S2ClientError,
  S2Client | RE
> => guardedAppend(name, schema, value)

export const readDecoded = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  options: ReadOptions
): Stream.Stream<
  DecodedRecord<A>,
  Schema.SchemaError | S2ClientError,
  S2Client | RD
> =>
  S2Client.read(name, options).pipe(
    Stream.mapEffect((record) =>
      Schema.decodeEffect(JsonValue)(record.body).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(schema)),
        Effect.map((value) => ({ ...record, value }))
      )
    )
  )

export const guardedAppend = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  value: A,
  options?: AppendOptions
): Effect.Effect<
  AppendAck,
  Schema.SchemaError | S2ClientError,
  S2Client | RE
> =>
  Effect.gen(function*() {
    const record = yield* encodedRecord(schema, value)
    return yield* S2Client.append(
      name,
      AppendInput.create([record], options)
    )
  })

export const conditionalAppend = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  value: A,
  matchSeqNum: number
): Effect.Effect<
  AppendAck,
  Schema.SchemaError | S2ClientError,
  S2Client | RE
> => guardedAppend(name, schema, value, { matchSeqNum })
