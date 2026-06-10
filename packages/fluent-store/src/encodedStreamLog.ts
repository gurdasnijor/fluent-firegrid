import { Effect, Schema, Sink, Stream, type ParseResult } from "effect"
import type { DurableStreamLog } from "./services.ts"
import type { AppendResult, AppendStream, ReadPosition, StreamRecord } from "./streamTypes.ts"
import type { DurableStreamLogError } from "./errors.ts"

export interface EncodedStreamRecord<A> extends Omit<StreamRecord, "bytes"> {
  readonly value: A
}

export interface EncodedStreamLog<A> {
  readonly append: (
    request: AppendStream,
  ) => Sink.Sink<AppendResult, A, Uint8Array, DurableStreamLogError | ParseResult.ParseError>
  readonly read: (
    from: ReadPosition,
  ) => Effect.Effect<
    Stream.Stream<EncodedStreamRecord<A>, DurableStreamLogError | ParseResult.ParseError>,
    DurableStreamLogError
  >
}

export const encodedStreamLog =
  <A, I>(schema: Schema.Schema<A, I>) =>
  (
    log: DurableStreamLog,
    encode: (input: I) => Uint8Array,
    decode: (bytes: Uint8Array) => I,
  ): EncodedStreamLog<A> => {
    const schemaEncode = Schema.encode(schema)
    const schemaDecode = Schema.decode(schema)
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
