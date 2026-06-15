import {
  AppendInput,
  FencingTokenMismatchError,
  RangeNotSatisfiableError,
  S2,
  SeqNumMismatchError,
  S2Error as SdkS2Error,
  type ReadRecord,
  type S2Stream,
} from "@s2-dev/streamstore"
import { Array, Effect, Layer, Stream } from "effect"
import { AppendCondFailed, S2Error } from "./errors.ts"
import { S2 as S2Tag, type S2Service } from "./s2.ts"

/**
 * The real `S2` Layer — a thin Effect wrapper over the S2 TS SDK, validated
 * against a local `s2-lite` server. The substrate the spike actually runs on.
 *
 * Mapping decisions forced by real S2 behavior (see FINDINGS.md):
 * - `fence`/`trim` are real *command records* that consume seq numbers. The
 *   follow read uses `ignoreCommandRecords: true`; the bounded `drain` reads all
 *   records and filters commands in code so it can paginate by physical seq
 *   number (per-tick fencing makes command records dense). The physical tail (for
 *   `match_seq_num`) always comes from `checkTail`.
 * - A bounded read caps at ~1000 records, so `drain` pages to the tail.
 * - A conditional-append `412` surfaces as `SeqNumMismatchError` (position taken,
 *   `expectedSeqNum` = real tail) or `FencingTokenMismatchError` (lost lease,
 *   `expectedFencingToken` = current fence).
 * - `checkTail`/`read` 404 on a stream that has never been appended to → treated
 *   as an empty stream at tail 0; a start at/after the tail → no records.
 */

export interface S2LiteConfig {
  /** Base URL of the s2-lite server, e.g. `http://127.0.0.1:9100`. */
  readonly endpoint: string
  /** Basin name (≥ 8 bytes per S2). */
  readonly basin: string
  /** s2-lite ignores auth; any non-empty token works. */
  readonly accessToken?: string
}

const dec = new TextDecoder()
const isNotFound = (e: unknown): boolean => e instanceof SdkS2Error && e.status === 404
const isEmptyRead = (e: unknown): boolean => isNotFound(e) || e instanceof RangeNotSatisfiableError

const sdkError = (operation: S2Error["operation"], stream: string) => (cause: unknown): S2Error =>
  new S2Error({
    operation,
    stream,
    details: cause instanceof Error ? cause.message : String(cause),
    cause,
  })

/** fence/trim command records carry an empty-key header (`["", "fence"|"trim"]`). */
const isCommandRecord = (r: ReadRecord<"bytes">): boolean =>
  r.headers.some(([k, v]) => dec.decode(k) === "" && (dec.decode(v) === "fence" || dec.decode(v) === "trim"))

const mapAppendError = (
  e: unknown,
  stream: string,
  opts: { readonly matchSeqNum?: number; readonly fencingToken?: string } | undefined,
): AppendCondFailed | S2Error =>
  e instanceof SeqNumMismatchError
    ? new AppendCondFailed({
        stream,
        ...(opts?.matchSeqNum !== undefined ? { expectedSeqNum: opts.matchSeqNum } : {}),
        actualSeqNum: e.expectedSeqNum,
        reason: "position-taken",
      })
    : e instanceof FencingTokenMismatchError
      ? new AppendCondFailed({
          stream,
          actualSeqNum: 0,
          currentFencingToken: e.expectedFencingToken,
          ...(opts?.fencingToken !== undefined ? { presentedFencingToken: opts.fencingToken } : {}),
          reason: "fence-mismatch",
        })
      : sdkError("append", stream)(e)

// A bounded S2 read caps at ~1000 records, so drain pages to the tail. Pagination
// resumes by *physical* seq number (the last record of any kind), because our
// per-tick fencing makes command records dense — resuming by the last *data*
// record (or relying on `ignoreCommandRecords`) can't advance past a window of
// only command records. `clamp` tolerates a `from` below a trimmed head.
interface Page {
  readonly data: ReadonlyArray<ReadRecord<"bytes">>
  readonly lastSeq: number | null
  readonly tail: number | null
}

const readPage = (
  handle: (stream: string) => S2Stream,
  stream: string,
  from: number,
): Effect.Effect<Page, S2Error> =>
  Effect.tryPromise({
    try: () =>
      handle(stream).read(
        { start: { from: { seqNum: from }, clamp: true }, stop: { limits: { count: 1000 } } },
        { as: "bytes" },
      ),
    catch: (e) => (isEmptyRead(e) ? null : sdkError("read", stream)(e)),
  }).pipe(
    Effect.map((batch): Page => {
      const last = batch.records[batch.records.length - 1]
      return {
        data: batch.records.filter((r) => !isCommandRecord(r)),
        lastSeq: last === undefined ? null : last.seqNum,
        tail: batch.tail === undefined ? null : batch.tail.seqNum,
      }
    }),
    Effect.catchIf((e): e is null => e === null, () => Effect.succeed({ data: [], lastSeq: null, tail: null })),
  )

const physicalTail = (handle: (stream: string) => S2Stream, stream: string): Effect.Effect<number, S2Error> =>
  Effect.tryPromise({
    try: () => handle(stream).checkTail(),
    catch: (e) => (isNotFound(e) ? null : sdkError("checkTail", stream)(e)),
  }).pipe(
    Effect.map((resp) => resp.tail.seqNum),
    Effect.catchIf((e): e is null => e === null, () => Effect.succeed(0)),
  )

const makeService = (handle: (stream: string) => S2Stream): S2Service => {
  // Bound pagination by an up-front tail so a recursive read never starts at/after
  // the tail (which S2 rejects with RangeNotSatisfiable).
  const drainTo = (
    stream: string,
    from: number,
    tail: number,
  ): Effect.Effect<ReadonlyArray<ReadRecord<"bytes">>, S2Error> =>
    Effect.gen(function* () {
      if (from >= tail) return []
      const page = yield* readPage(handle, stream, from)
      if (page.lastSeq === null) return page.data
      const next = page.lastSeq + 1
      if (next >= tail) return page.data
      return Array.appendAll(page.data, yield* drainTo(stream, next, tail))
    })

  const drain = (stream: string, from: number): Effect.Effect<ReadonlyArray<ReadRecord<"bytes">>, S2Error> =>
    physicalTail(handle, stream).pipe(Effect.flatMap((tail) => drainTo(stream, from, tail)))

  return {
    append: (stream, records, opts) =>
      Effect.tryPromise({
        try: () =>
          handle(stream).append(
            AppendInput.create(records, {
              ...(opts?.matchSeqNum !== undefined ? { matchSeqNum: opts.matchSeqNum } : {}),
              ...(opts?.fencingToken !== undefined ? { fencingToken: opts.fencingToken } : {}),
            }),
          ),
        catch: (e) => mapAppendError(e, stream, opts),
      }).pipe(Effect.map((ack) => ({ tail: ack.tail.seqNum }))),

    read: (stream, from, opts) =>
      opts?.follow === true
        ? Stream.unwrap(
            Effect.tryPromise({
              try: () =>
                handle(stream).readSession(
                  { start: { from: { seqNum: from } }, ignoreCommandRecords: true },
                  { as: "bytes" },
                ),
              catch: sdkError("read", stream),
            }).pipe(Effect.map((session) => Stream.fromAsyncIterable(session, sdkError("read", stream)))),
          )
        : drain(stream, from).pipe(Effect.map(Stream.fromIterable), Stream.unwrap),

    checkTail: (stream) => physicalTail(handle, stream),
  }
}

/** Build a live S2 service backed by an s2-lite (or hosted) basin. */
export const make = (config: S2LiteConfig): Effect.Effect<S2Service> =>
  Effect.sync(() => {
    const client = new S2({
      accessToken: config.accessToken ?? "local-token",
      endpoints: { account: config.endpoint, basin: config.endpoint },
    })
    const basin = client.basin(config.basin)
    return makeService((stream) => basin.stream(stream))
  })

export const layerWith = (service: S2Service): Layer.Layer<S2Tag> => Layer.succeed(S2Tag, service)

export const layer = (config: S2LiteConfig): Layer.Layer<S2Tag> => Layer.effect(S2Tag, make(config))
