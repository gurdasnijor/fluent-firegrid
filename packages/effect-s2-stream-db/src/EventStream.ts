import { Effect, Schema, Stream } from "effect"
import { AppendInput, AppendRecord, type AppendAck, S2Client, type S2Record, type StreamConfig } from "effect-s2"
import { S2StreamDbError } from "./errors.ts"

/**
 * A typed, append-only event stream over a single S2 stream — the "stream" half
 * of the stream/table duality (facts, not latest-value-per-key). It is a thin
 * schema/path adapter that delegates all IO to `S2Client`: it owns the value
 * schema, derives the stream path from a key, yields typed `EventRecord`s, and
 * raises `S2StreamDbError`s carrying stream + seq context. It does NOT duplicate
 * basin admin, append sessions, producer backpressure, or SDK retry.
 *
 * Tables (latest-value projections) stay in `StreamDb`. Materialized views over
 * a stream are a later slice; this is the producer/consumer substrate.
 */

/** A decoded event with its S2 cursor. `seqNum` is the resume cursor. */
export interface EventRecord<A> {
  readonly seqNum: number
  readonly timestamp: number
  readonly value: A
}

/** Where to start reading/tailing from (an S2 `seqNum`). Default: head (0). */
export interface ReadFromOptions {
  readonly fromSeq?: number
}

export interface TypedEventStreamInstance<A> {
  /** Append one value; returns the append ack (its `tail.seqNum` is the new tail). */
  readonly append: (value: A) => Effect.Effect<AppendAck, S2StreamDbError>
  /** Append many values as one atomic S2 batch. */
  readonly appendBatch: (values: ReadonlyArray<A>) => Effect.Effect<AppendAck, S2StreamDbError>
  /** Read records from `fromSeq` up to the current tail, then complete (finite). */
  readonly read: (options?: ReadFromOptions) => Stream.Stream<EventRecord<A>, S2StreamDbError>
  /** Read records from `fromSeq` and follow the tail live (does not complete). */
  readonly tail: (options?: ReadFromOptions) => Stream.Stream<EventRecord<A>, S2StreamDbError>
}

/** A key schema: decodes the stream's instance key and encodes it to a path segment. */
export type EventKeySchema = Schema.Codec<unknown, string>

export interface OpenEventStreamOptions {
  readonly config?: StreamConfig
}

/** The static shape of an `EventStream` class. */
export interface EventStreamClass<A, Key extends EventKeySchema> {
  new (): object
  readonly basePath: string
  readonly key: Key
  readonly value: Schema.Codec<A, unknown, never, never>
  /** Open (create-if-absent) the stream at `${basePath}/${encode(key)}`. */
  readonly open: (
    key: Key["Type"],
    options?: OpenEventStreamOptions,
  ) => Effect.Effect<TypedEventStreamInstance<A>, S2StreamDbError, S2Client>
}

const Json = Schema.UnknownFromJsonString

const codecError = (operation: string, message: string) => (cause: unknown): S2StreamDbError =>
  new S2StreamDbError({ operation, message, cause })

const ioError = (operation: string, message: string) => (cause: unknown): S2StreamDbError =>
  new S2StreamDbError({ operation, message, cause })

/**
 * Decode one S2 record body into a typed `EventRecord`. Pure (no IO) — the
 * shared core of `read`/`tail`. A decode failure is a data-plane failure that
 * names the stream and seq number (SDD §6.4).
 */
export const decodeEventRecord = <A>(
  value: Schema.Codec<A, unknown, never, never>,
  stream: string,
  record: { readonly seqNum: number; readonly timestamp: number; readonly body: string },
): Effect.Effect<EventRecord<A>, S2StreamDbError> =>
  Schema.decodeEffect(Json)(record.body).pipe(
    Effect.flatMap((encoded) => Schema.decodeUnknownEffect(value)(encoded)),
    Effect.map((decoded): EventRecord<A> => ({ seqNum: record.seqNum, timestamp: record.timestamp, value: decoded })),
    Effect.mapError(codecError("EventStream.read", `failed to decode event at ${stream}#${record.seqNum}`)),
  )

/** Encode one value into an S2 append record body. Pure (no IO). */
export const encodeEventRecord = <A>(
  value: Schema.Codec<A, unknown, never, never>,
  stream: string,
  event: A,
): Effect.Effect<ReturnType<typeof AppendRecord.string>, S2StreamDbError> =>
  Schema.encodeEffect(value)(event).pipe(
    Effect.flatMap((encoded) => Schema.encodeEffect(Json)(encoded)),
    Effect.map((body) => AppendRecord.string({ body })),
    Effect.mapError(codecError("EventStream.append", `failed to encode event for ${stream}`)),
  )

const makeInstance = <A>(
  client: S2Client["Service"],
  stream: string,
  value: Schema.Codec<A, unknown, never, never>,
): TypedEventStreamInstance<A> => {
  const appendBatch = (values: ReadonlyArray<A>): Effect.Effect<AppendAck, S2StreamDbError> =>
    values.length === 0
      ? Effect.fail(new S2StreamDbError({ operation: "EventStream.appendBatch", message: `cannot append an empty batch to ${stream}`, cause: undefined }))
      : Effect.forEach(values, (event) => encodeEventRecord(value, stream, event)).pipe(
        Effect.flatMap((records) =>
          client.append(stream, AppendInput.create(records)).pipe(
            Effect.mapError(ioError("EventStream.append", `failed to append ${records.length} event(s) to ${stream}`)),
          ),
        ),
      )

  // `read` stops once caught up (waitSecs: 0); `tail` omits the stop and follows
  // the tail live. Command records (trims/fences) are skipped by the session.
  const session = (fromSeq: number | undefined, finite: boolean): Stream.Stream<EventRecord<A>, S2StreamDbError> =>
    client.read(stream, {
      start: { from: { seqNum: fromSeq ?? 0 }, clamp: true },
      ignoreCommandRecords: true,
      ...(finite ? { stop: { waitSecs: 0 } } : {}),
    }).pipe(
      Stream.mapError(ioError("EventStream.read", `failed to read ${stream}`)),
      Stream.mapEffect((record: S2Record) => decodeEventRecord(value, stream, record)),
    )

  return {
    append: (event) => appendBatch([event]),
    appendBatch,
    read: (options) => session(options?.fromSeq, true),
    tail: (options) => session(options?.fromSeq, false),
  }
}

/**
 * Define a typed event stream: one S2 stream of schema-validated facts.
 *
 * @example
 * class CucumberEnvelopes extends EventStream<CucumberEnvelopes>("cucumber/envelopes")(Envelope, CucumberRunId) {}
 * const stream = yield* CucumberEnvelopes.open(runId)
 * yield* stream.append(envelope)
 * stream.tail({ fromSeq: 0 }) // live NDJSON
 */
export const EventStream =
  <_Self = never>(basePath: string) =>
  <A, I, Key extends EventKeySchema = typeof Schema.String>(
    value: Schema.Codec<A, I, never, never>,
    key?: Key,
  ): EventStreamClass<A, Key> => {
    const keySchema = (key ?? Schema.String) as Schema.Codec<unknown, string>
    const valueCodec = value as unknown as Schema.Codec<A, unknown, never, never>
    const encodeKey = (value_: unknown) => Schema.encodeUnknownEffect(keySchema)(value_)

    class EventStreamImpl {
      static readonly basePath = basePath
      static readonly key = keySchema
      static readonly value = valueCodec
      static readonly open = (
        keyValue: Key["Type"],
        options?: OpenEventStreamOptions,
      ): Effect.Effect<TypedEventStreamInstance<A>, S2StreamDbError, S2Client> =>
        encodeKey(keyValue).pipe(
          Effect.mapError(codecError("EventStream.open", `failed to encode key for ${basePath}`)),
          Effect.flatMap((segment) =>
            Effect.gen(function*() {
              const client = yield* S2Client
              const stream = `${basePath}/${segment}`
              yield* client.ensureStream({ stream, ...(options?.config === undefined ? {} : { config: options.config }) }).pipe(
                Effect.mapError(ioError("EventStream.open", `failed to ensure stream ${stream}`)),
              )
              return makeInstance(client, stream, valueCodec)
            }),
          ),
        )
    }
    // Intentional class-factory cast: the static shape plus phantom value type cannot be expressed structurally on a class declaration.
    return EventStreamImpl as unknown as EventStreamClass<A, Key>
  }
