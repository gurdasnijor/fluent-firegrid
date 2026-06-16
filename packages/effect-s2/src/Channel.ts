import { S2Client, type AppendOptions } from "./S2Client.ts"
import { AppendInput, AppendRecord, type AppendAck, type ReadOptions } from "./internal/sdk.ts"
import type { S2Record } from "./internal/record.ts"
import type { S2ClientError } from "./S2Error.ts"
import { Effect, Schema, Stream } from "effect"

const JsonValue = Schema.UnknownFromJsonString

export interface DecodedRecord<A> extends S2Record {
  readonly value: A
}

const encodedRecord = <A, I, RD, RE>(
  schema: Schema.Codec<A, I, RD, RE>,
  value: A,
) =>
  Effect.gen(function*() {
    const encoded = yield* Schema.encodeEffect(schema)(value)
    const body = yield* Schema.encodeEffect(JsonValue)(encoded)
    return AppendRecord.string({ body })
  })

export const publish = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  value: A,
): Effect.Effect<
  AppendAck,
  Schema.SchemaError | S2ClientError,
  S2Client | RE
> =>
  Effect.gen(function*() {
    const record = yield* encodedRecord(schema, value)
    return yield* S2Client.append(name, AppendInput.create([record]))
  })

export const readDecoded = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  options: ReadOptions,
): Stream.Stream<
  DecodedRecord<A>,
  Schema.SchemaError | S2ClientError,
  S2Client | RD
> =>
  S2Client.read(name, options).pipe(
    Stream.mapEffect((record) =>
      Schema.decodeEffect(JsonValue)(record.body).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(schema)),
        Effect.map((value) => ({ ...record, value })),
      ),
    ),
  )

export const conditionalAppend = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  value: A,
  matchSeqNum: number,
): Effect.Effect<
  AppendAck,
  Schema.SchemaError | S2ClientError,
  S2Client | RE
> =>
  Effect.gen(function*() {
    const options: AppendOptions = { matchSeqNum }
    const record = yield* encodedRecord(schema, value)
    return yield* S2Client.append(
      name,
      AppendInput.create([record], options),
    )
  })
