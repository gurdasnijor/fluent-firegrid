import { S2Client, type AppendOptions } from "./S2Client.ts"
import { AppendInput, AppendRecord, type AppendAck, type ReadOptions } from "./internal/sdk.ts"
import type { S2ClientError } from "./S2Error.ts"
import { Effect, Schema, Stream } from "effect"

const JsonValue = Schema.UnknownFromJsonString

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
    const encoded = yield* Schema.encodeEffect(schema)(value)
    const body = yield* Schema.encodeEffect(JsonValue)(encoded)
    return yield* S2Client.append(name, AppendInput.create([AppendRecord.string({ body })]))
  })

export const readDecoded = <A, I, RD, RE>(
  name: string,
  schema: Schema.Codec<A, I, RD, RE>,
  options: ReadOptions,
): Stream.Stream<
  A,
  Schema.SchemaError | S2ClientError,
  S2Client | RD
> =>
  S2Client.read(name, options).pipe(
    Stream.mapEffect((record) =>
      Schema.decodeEffect(JsonValue)(record.body).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(schema)),
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
    const encoded = yield* Schema.encodeEffect(schema)(value)
    const body = yield* Schema.encodeEffect(JsonValue)(encoded)
    const options: AppendOptions = { matchSeqNum }
    return yield* S2Client.append(
      name,
      AppendInput.create([AppendRecord.string({ body })], options),
    )
  })
