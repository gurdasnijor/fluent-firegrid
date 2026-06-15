import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  S2,
  SeqNumMismatchError,
  S2Error as SdkS2Error,
  type ReadRecord,
  type S2Stream,
} from "@s2-dev/streamstore"
import { Effect, Layer, Stream } from "effect"
import { AppendCondFailed, S2Error } from "./errors.ts"
import { S2 as S2Tag, type S2Record, type S2Service } from "./s2.ts"

/**
 * The real `S2` Layer — a thin wrapper over the S2 TS SDK, validated against a
 * local `s2-lite` server. This is the substrate the spike actually runs on.
 *
 * Mapping decisions forced by real S2 behavior (see FINDINGS.md):
 * - Journal records are tagged with a `kind=journal` header. `fence`/`trim` are
 *   real *command records* that consume seq numbers and appear on read with
 *   `["", "fence"]` / `["", "trim"]` headers — so `read` filters them out and the
 *   physical tail (for `match_seq_num`) is taken from `checkTail`, never inferred
 *   from journal-record seq numbers.
 * - A conditional-append `412` surfaces as `SeqNumMismatchError` (position taken,
 *   `expectedSeqNum` = real tail) or `FencingTokenMismatchError` (lost lease,
 *   `expectedFencingToken` = current fence).
 * - `checkTail`/`read` 404 on a stream that has never been appended to → treated
 *   as an empty stream at tail 0.
 */

export interface S2LiteConfig {
  /** Base URL of the s2-lite server, e.g. `http://127.0.0.1:9100`. */
  readonly endpoint: string
  /** Basin name (≥ 8 bytes per S2). */
  readonly basin: string
  /** s2-lite ignores auth; any non-empty token works. */
  readonly accessToken?: string
}

const MARKER_KEY = "kind"
const MARKER_VALUE = "journal"
const enc = new TextEncoder()
const dec = new TextDecoder()

const isNotFound = (e: unknown): boolean => e instanceof SdkS2Error && e.status === 404

const sdkError = (operation: S2Error["operation"], stream: string) => (cause: unknown): S2Error =>
  new S2Error({
    operation,
    stream,
    details: cause instanceof Error ? cause.message : String(cause),
    cause,
  })

/** Keep only our journal records; fence/trim command records have a different header. */
const isJournalRecord = (r: ReadRecord<"bytes">): boolean =>
  r.headers.some(([k, v]) => dec.decode(k) === MARKER_KEY && dec.decode(v) === MARKER_VALUE)

const toS2Record = (r: ReadRecord<"bytes">): S2Record => ({ seqNum: BigInt(r.seqNum), data: r.body })

const mapAppendError = (
  e: unknown,
  stream: string,
  opts: { readonly matchSeqNum?: bigint; readonly fencingToken?: string } | undefined,
): AppendCondFailed | S2Error =>
  e instanceof SeqNumMismatchError
    ? new AppendCondFailed({
        stream,
        ...(opts?.matchSeqNum !== undefined ? { expectedSeqNum: opts.matchSeqNum } : {}),
        actualSeqNum: BigInt(e.expectedSeqNum),
        reason: "position-taken",
      })
    : e instanceof FencingTokenMismatchError
      ? new AppendCondFailed({
          stream,
          actualSeqNum: 0n,
          currentFencingToken: e.expectedFencingToken,
          ...(opts?.fencingToken !== undefined ? { presentedFencingToken: opts.fencingToken } : {}),
          reason: "fence-mismatch",
        })
      : sdkError("append", stream)(e)

const makeService = (handle: (stream: string) => S2Stream): S2Service => {
  const drain = (stream: string, from: bigint): Effect.Effect<ReadonlyArray<ReadRecord<"bytes">>, S2Error> =>
    Effect.tryPromise({
      try: () => handle(stream).read({ start: { from: { seqNum: Number(from) } } }, { as: "bytes" }),
      catch: (e) => (isNotFound(e) ? null : sdkError("read", stream)(e)),
    }).pipe(
      Effect.map((batch) => batch.records),
      Effect.catchIf(
        (e): e is null => e === null,
        () => Effect.succeed([] as ReadonlyArray<ReadRecord<"bytes">>),
      ),
    )

  return {
    append: (stream, records, opts) =>
      Effect.tryPromise({
        try: () =>
          handle(stream).append(
            AppendInput.create(
              records.map((body) =>
                AppendRecord.bytes({ body, headers: [[enc.encode(MARKER_KEY), enc.encode(MARKER_VALUE)]] }),
              ),
              {
                ...(opts?.matchSeqNum !== undefined ? { matchSeqNum: Number(opts.matchSeqNum) } : {}),
                ...(opts?.fencingToken !== undefined ? { fencingToken: opts.fencingToken } : {}),
              },
            ),
          ),
        catch: (e) => mapAppendError(e, stream, opts),
      }).pipe(Effect.map((ack) => ({ tail: BigInt(ack.tail.seqNum) }))),

    read: (stream, from, opts) =>
      opts?.follow === true
        ? Stream.unwrap(
            Effect.tryPromise({
              try: () => handle(stream).readSession({ start: { from: { seqNum: Number(from) } } }, { as: "bytes" }),
              catch: sdkError("read", stream),
            }).pipe(
              Effect.map((session) =>
                Stream.fromAsyncIterable(session, sdkError("read", stream)).pipe(
                  Stream.filter(isJournalRecord),
                  Stream.map(toS2Record),
                ),
              ),
            ),
          )
        : drain(stream, from).pipe(
            Effect.map((records) => Stream.fromIterable(records.filter(isJournalRecord).map(toS2Record))),
            Stream.unwrap,
          ),

    checkTail: (stream) =>
      Effect.tryPromise({
        try: () => handle(stream).checkTail(),
        catch: (e) => (isNotFound(e) ? null : sdkError("checkTail", stream)(e)),
      }).pipe(
        Effect.map((resp) => BigInt(resp.tail.seqNum)),
        Effect.catchIf((e): e is null => e === null, () => Effect.succeed(0n)),
      ),

    checkFence: (stream) =>
      drain(stream, 0n).pipe(
        Effect.map((records) => {
          // fence command records: empty header key, value "fence", body = token.
          const fences = records.filter((r) =>
            r.headers.some(([k, v]) => dec.decode(k) === "" && dec.decode(v) === "fence"),
          )
          const last = fences[fences.length - 1]
          return last === undefined ? null : dec.decode(last.body)
        }),
      ),

    fence: (stream, token) =>
      Effect.tryPromise({
        try: () => handle(stream).append(AppendInput.create([AppendRecord.fence(token)])),
        catch: sdkError("fence", stream),
      }).pipe(Effect.asVoid),

    trim: (stream, upTo) =>
      Effect.tryPromise({
        try: () => handle(stream).append(AppendInput.create([AppendRecord.trim(Number(upTo))])),
        catch: sdkError("trim", stream),
      }).pipe(Effect.asVoid),
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
