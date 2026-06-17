import { Effect, Option, Schema, Stream } from "effect"
import {
  AppendInput,
  AppendRecord,
  conditionalAppend,
  publish,
  readDecoded,
  S2Client,
  S2Conflict,
  S2NotFound,
  S2RangeNotSatisfiable,
  SeqNumMismatchError,
} from "effect-s2"
import { type DurableExecutionError, durableError as toError } from "../errors.ts"
import { ActorEvent, type LogEntry } from "./events.ts"

/**
 * `ActorLog` — the effectful read/write surface of one object stream as an
 * ordered `ActorEvent` log (`LAYERING.7`). It reads via `effect-s2.readDecoded`
 * and writes via `S2Client.append` (`publish`/`conditionalAppend`) on the path
 * derived from the owner key codec — NOT a `StreamDb` table fold. The latest-value
 * projection is folded from this log by the pure `transition` (Model A).
 */
export interface ActorLog {
  /** The S2 stream path (a codec output, never hand-built). */
  readonly streamName: string
  /**
   * Bounded read of the current log from `from` (default 0) to the tail — typed
   * `ActorEvent`s in `seq_num` order, command records (trim/fence) skipped. Does
   * NOT tail; `waitSecs: 0` returns what is durable now and stops.
   */
  readonly read: (from?: number) => Effect.Effect<ReadonlyArray<LogEntry>, DurableExecutionError, S2Client>
  /** Append one event; resolves to its assigned S2 `seq_num`. */
  readonly append: (event: ActorEvent) => Effect.Effect<number, DurableExecutionError, S2Client>
  /**
   * CAS append at `matchSeqNum` (admission serialization, `ADMISSION.4`). `None`
   * means a concurrent writer won the CAS — the caller re-reads and retries.
   */
  readonly casAppend: (
    event: ActorEvent,
    matchSeqNum: number,
  ) => Effect.Effect<Option.Option<number>, DurableExecutionError, S2Client>
  /** The next `seq_num` the stream will assign (`0` for an absent/empty stream). */
  readonly tailSeqNum: Effect.Effect<number, DurableExecutionError, S2Client>
  /** Append an S2 trim command, requesting removal of records before `cursor` (`CHECKPOINTING`). */
  readonly trim: (cursor: number) => Effect.Effect<void, DurableExecutionError, S2Client>
}

// A never-appended stream surfaces as 404 and an empty one as 416. The streaming
// read path can carry the base S2Error (not the S2NotFound/S2RangeNotSatisfiable
// subtype), so also treat those status codes as "nothing durable yet".
const isMissing = (cause: unknown): boolean => {
  if (cause instanceof S2NotFound || cause instanceof S2RangeNotSatisfiable) {
    return true
  }
  const status = (cause as { readonly status?: unknown }).status
  return status === 404 || status === 416
}

const isCasLoss = (cause: unknown): boolean =>
  cause instanceof S2Conflict || cause instanceof SeqNumMismatchError

/** Open the actor log over an already-derived stream path. */
export const openLog = (streamName: string): ActorLog => {
  const readFrom = (from: number): Effect.Effect<ReadonlyArray<LogEntry>, DurableExecutionError, S2Client> =>
    readDecoded(streamName, ActorEvent, {
      start: { from: { seqNum: from }, clamp: true },
      stop: { waitSecs: 0 }, // bounded: return what's durable now, do not tail
      ignoreCommandRecords: true,
    }).pipe(
      Stream.map((record): LogEntry => ({ seqNum: record.seqNum, event: record.value })),
      Stream.runCollect,
      Effect.map((chunk) => Array.from(chunk)),
      Effect.catch((cause) =>
        isMissing(cause) ? Effect.succeed<ReadonlyArray<LogEntry>>([]) : Effect.fail(cause)),
      Effect.mapError(toError("actor.read")),
    )

  const read = (from = 0): Effect.Effect<ReadonlyArray<LogEntry>, DurableExecutionError, S2Client> =>
    // Guard on the tail: a never-appended/empty stream (tail ≤ from) yields []
    // without a streaming read, whose 404 can escape as an unmapped S2Error.
    tailSeqNum.pipe(
      Effect.flatMap((tail) => (from >= tail ? Effect.succeed<ReadonlyArray<LogEntry>>([]) : readFrom(from))),
      Effect.withSpan("effect-s2-durable.log.read", { attributes: { stream: streamName } }),
    )

  const append = (event: ActorEvent): Effect.Effect<number, DurableExecutionError, S2Client> =>
    publish(streamName, ActorEvent, event).pipe(
      Effect.map((ack) => ack.start.seqNum),
      Effect.mapError(toError("actor.append")),
      Effect.withSpan("effect-s2-durable.log.append", { attributes: { stream: streamName, tag: event._tag } }),
    )

  const casAppend = (
    event: ActorEvent,
    matchSeqNum: number,
  ): Effect.Effect<Option.Option<number>, DurableExecutionError, S2Client> =>
    conditionalAppend(streamName, ActorEvent, event, matchSeqNum).pipe(
      Effect.map((ack) => Option.some(ack.start.seqNum)),
      Effect.catch((cause) =>
        isCasLoss(cause) ? Effect.succeedNone : Effect.fail(cause)),
      Effect.mapError(toError("actor.casAppend")),
      Effect.withSpan("effect-s2-durable.log.casAppend", {
        attributes: { stream: streamName, tag: event._tag, matchSeqNum },
      }),
    )

  const tailSeqNum: Effect.Effect<number, DurableExecutionError, S2Client> = S2Client.checkTail(streamName).pipe(
    Effect.map((tail) => tail.tail.seqNum),
    Effect.catch((cause) => (isMissing(cause) ? Effect.succeed(0) : Effect.fail(cause))),
    Effect.mapError(toError("actor.tailSeqNum")),
    Effect.withSpan("effect-s2-durable.log.tailSeqNum", { attributes: { stream: streamName } }),
  )

  const trim = (cursor: number): Effect.Effect<void, DurableExecutionError, S2Client> =>
    S2Client.append(streamName, AppendInput.create([AppendRecord.trim(cursor)])).pipe(
      Effect.asVoid,
      Effect.mapError(toError("actor.trim")),
      Effect.withSpan("effect-s2-durable.log.trim", { attributes: { stream: streamName, cursor } }),
    )

  return { streamName, read, append, casAppend, tailSeqNum, trim }
}

/**
 * Open the actor log for an owner — the owner becomes an S2 path segment ONLY by
 * encoding it through its key codec (`ROUTING.3`), never a hand-built string.
 */
export const logForOwner = <Owner>(
  basePath: string,
  ownerCodec: Schema.Codec<Owner, string>,
  owner: Owner,
): Effect.Effect<ActorLog, DurableExecutionError> =>
  Schema.encodeEffect(ownerCodec)(owner).pipe(
    Effect.map((segment) => openLog(`${basePath}/${segment}`)),
    Effect.mapError(toError("actor.logForOwner")),
  )
