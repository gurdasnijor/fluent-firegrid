import {
  AppendInput,
  type AppendRecord,
  type ReadRecord as S2ReadRecord,
  type ReadSession as S2ReadSession,
} from "@s2-dev/streamstore"
import { Effect, type Scope, Stream } from "effect"
import type { ApiError, ReadQuery } from "./api.ts"
import { tryS2 } from "./errors.ts"
import type { S2ProfileService } from "./s2.ts"
import { catchS2, streamError } from "./s2Errors.ts"
import {
  decodeStateRecord,
  isCloseRecord,
  KIND_HEADER,
  sseBatch,
  sseEvent,
  streamPosition,
  textHeaderValue,
} from "./s2Records.ts"

const appendOptions = (
  input: Readonly<{
    readonly matchSeqNum?: number | undefined
    readonly fencingToken?: string | undefined
  }>,
): Readonly<{
  readonly matchSeqNum?: number
  readonly fencingToken?: string
}> => ({
  ...(input.matchSeqNum === undefined ? {} : { matchSeqNum: input.matchSeqNum }),
  ...(input.fencingToken === undefined ? {} : { fencingToken: input.fencingToken }),
})

const readInput = (query: ReadQuery) => ({
  start: query.tailOffset === undefined
    ? query.seqNum === undefined
      ? { clamp: true }
      : { from: { seqNum: query.seqNum }, clamp: true }
    : { from: { tailOffset: query.tailOffset }, clamp: true },
  stop: {
    limits: {
      ...(query.count === undefined ? {} : { count: query.count }),
    },
    ...(query.waitSecs === undefined ? {} : { waitSecs: query.waitSecs }),
  },
  ignoreCommandRecords: query.ignoreCommandRecords ?? true,
})

export const appendRecords = (
  profile: S2ProfileService,
  stream: string,
  records: ReadonlyArray<AppendRecord>,
  conditions: Readonly<{
    readonly matchSeqNum?: number | undefined
    readonly fencingToken?: string | undefined
  }>,
) =>
  catchS2(tryS2(() =>
    profile.basin.stream(stream).append(
      AppendInput.create(records, appendOptions(conditions)),
    ),
  ))

export const readBytes = (
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
) =>
  catchS2(tryS2(() =>
    profile.basin.stream(stream).read(
      readInput(query),
      { as: "bytes" },
    ),
  ))

const liveReadSession = (
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
): Effect.Effect<S2ReadSession<"bytes">, ApiError, Scope.Scope> =>
  Effect.acquireRelease(
    catchS2(tryS2(() =>
      profile.basin.stream(stream).readSession(
        readInput(query),
        { as: "bytes" },
      ),
    )),
    (session) =>
      Effect.promise(() => session[Symbol.asyncDispose]()).pipe(
        Effect.orDie,
      ),
  )

const boundedRecordStream = (
  session: S2ReadSession<"bytes">,
  query: ReadQuery,
): Stream.Stream<S2ReadRecord<"bytes">, ApiError> =>
  Stream.fromAsyncIterable(session, streamError).pipe(
    Stream.takeUntil(isCloseRecord),
    query.count === undefined ? (records) => records : Stream.take(query.count),
  )

const withLiveRecords = <A>(
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
  make: (
    session: S2ReadSession<"bytes">,
    records: Stream.Stream<S2ReadRecord<"bytes">, ApiError>,
  ) => Stream.Stream<A, ApiError>,
): Stream.Stream<A, ApiError> =>
  Stream.scoped(Stream.unwrap(
    liveReadSession(profile, stream, query).pipe(
      Effect.map((session) => make(session, boundedRecordStream(session, query))),
    ),
  ))

export const recordStream = (
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
): Stream.Stream<S2ReadRecord<"bytes">, ApiError> =>
  withLiveRecords(profile, stream, query, (_session, records) => records)

const liveSse = (
  events: (
    session: S2ReadSession<"bytes">,
    records: Stream.Stream<S2ReadRecord<"bytes">, ApiError>,
  ) => Stream.Stream<Uint8Array, ApiError>,
): (
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
) => Stream.Stream<Uint8Array, ApiError> =>
  (profile, stream, query) => withLiveRecords(profile, stream, query, events)

export const liveReadSse = liveSse((session, records) => {
    const tail = () => session.lastObservedTail()
    return records.pipe(Stream.map((record) => sseBatch(record, tail())))
  })

export const liveStateSse = liveSse((session, records) => {
    const tail = () => session.lastObservedTail()
    return records.pipe(
      Stream.filter((record) => textHeaderValue(record.headers, KIND_HEADER) === "state"),
      Stream.mapEffect((record) => decodeStateRecord(record)),
      Stream.map((record) => {
        const currentTail = tail()
        return sseEvent("batch", {
          records: [record],
          ...(currentTail === undefined ? {} : { tail: streamPosition(currentTail) }),
        })
      }),
    )
  })
