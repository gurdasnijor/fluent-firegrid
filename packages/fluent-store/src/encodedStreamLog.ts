import { Effect, Schema, Sink, Stream } from "effect"
import type { DurableStreamLog } from "./services.ts"
import type { AppendResult, AppendStream, ReadPosition, StreamRecord } from "./streamTypes.ts"
import type { DurableStreamLogError } from "./errors.ts"

type SchemaError = Effect.Error<
  ReturnType<ReturnType<typeof Schema.decodeUnknownEffect<Schema.Schema<unknown>>>>
>

export interface EncodedStreamRecord<A> extends Omit<StreamRecord, "bytes"> {
  readonly value: A
}

export interface EncodedStreamLog<A> {
  readonly append: (
    request: AppendStream,
  ) => Sink.Sink<AppendResult, A, Uint8Array, DurableStreamLogError | SchemaError>
  readonly read: (
    from: ReadPosition,
  ) => Effect.Effect<
    Stream.Stream<EncodedStreamRecord<A>, DurableStreamLogError | SchemaError>,
    DurableStreamLogError
  >
}

export const encodedStreamLog =
  <A, I>(schema: Schema.Codec<A, I>) =>
  (
    log: DurableStreamLog,
    encode: (input: I) => Uint8Array,
    decode: (bytes: Uint8Array) => I,
  ): EncodedStreamLog<A> => {
    const schemaEncode = Schema.encodeEffect(schema)
    const schemaDecode = Schema.decodeEffect(schema)
    return {
      append: (request) =>
        Sink.mapInputEffect(log.append(request), (input: A) =>
          Effect.map(schemaEncode(input), encode),
        ),
      read: (from) =>
        Effect.map(log.read(from), (records) =>
          Stream.mapEffect(records, (record) =>
            Effect.map(schemaDecode(decode(record.bytes)), (value) => ({
              ...record,
              value,
            })),
          ),
        ),
    }
  }
