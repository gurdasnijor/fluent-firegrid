import { Effect, Option, Schema, Stream } from "effect"
import {
  conditionalAppend,
  publish,
  readDecoded,
  S2Client,
  S2Conflict,
  S2NotFound,
  S2RangeNotSatisfiable,
  SeqNumMismatchError,
} from "effect-s2"
import { DurableExecutionError, durableError as toError } from "../errors.ts"
import { ActorEvent, type LogEntry } from "./core.ts"

/**
 * The effectful read/write surface of one owner stream as an ordered `ActorEvent`
 * log. Reads via `effect-s2.readDecoded`, writes via `publish`/`conditionalAppend`
 * — never a `StreamDb` table fold. Internal to the runtime's object path.
 */
export interface ActorLog {
  readonly streamName: string
  /** Bounded read of the durable log (no tail-follow); decode failures are tagged distinctly. */
  readonly read: () => Effect.Effect<ReadonlyArray<LogEntry>, DurableExecutionError, S2Client>
  /** Append one event; resolves to its assigned `seq_num`. */
  readonly append: (event: ActorEvent) => Effect.Effect<number, DurableExecutionError, S2Client>
  /** CAS append at `matchSeqNum`; `None` means a concurrent writer won (re-read + retry). */
  readonly casAppend: (
    event: ActorEvent,
    matchSeqNum: number,
  ) => Effect.Effect<Option.Option<number>, DurableExecutionError, S2Client>
  /** The next `seq_num` the stream will assign (`0` for an absent/empty stream). */
  readonly tailSeqNum: Effect.Effect<number, DurableExecutionError, S2Client>
}

// An absent stream surfaces as 404 and an empty one as 416; the streaming read can
// carry the base S2Error rather than the subtype, so match on status too.
const isMissing = (cause: unknown): boolean => {
  if (cause instanceof S2NotFound || cause instanceof S2RangeNotSatisfiable) {
    return true
  }
  const status = (cause as { readonly status?: unknown }).status
  return status === 404 || status === 416
}

const isCasLoss = (cause: unknown): boolean => cause instanceof S2Conflict || cause instanceof SeqNumMismatchError

/** Open the actor log over an already-derived stream path. */
export const openLog = (streamName: string): ActorLog => {
  const tailSeqNum: Effect.Effect<number, DurableExecutionError, S2Client> = S2Client.checkTail(streamName).pipe(
    Effect.map((tail) => tail.tail.seqNum),
    Effect.catch((cause) => (isMissing(cause) ? Effect.succeed(0) : Effect.fail(cause))),
    Effect.mapError(toError("object.tailSeqNum")),
    Effect.withSpan("effect-s2-durable.log.tailSeqNum", { attributes: { stream: streamName } }),
  )

  const readFrom = (from: number): Effect.Effect<ReadonlyArray<LogEntry>, DurableExecutionError, S2Client> =>
    readDecoded(streamName, ActorEvent, {
      start: { from: { seqNum: from }, clamp: true },
      stop: { waitSecs: 0 },
      ignoreCommandRecords: true,
    }).pipe(
      Stream.map((record): LogEntry => ({ seqNum: record.seqNum, event: record.value })),
      Stream.runCollect,
      Effect.map((chunk) => Array.from(chunk)),
      Effect.catch((cause) => (isMissing(cause) ? Effect.succeed<ReadonlyArray<LogEntry>>([]) : Effect.fail(cause))),
      Effect.mapError((cause) =>
        cause instanceof Schema.SchemaError
          ? new DurableExecutionError({
            operation: "object.decode",
            message: `malformed ActorEvent on ${streamName}: ${cause.message}`,
            cause,
          })
          : toError("object.read")(cause),
      ),
    )

  const read = (): Effect.Effect<ReadonlyArray<LogEntry>, DurableExecutionError, S2Client> =>
    // Tail-guard: a never-appended/empty stream yields [] without a streaming read
    // (whose 404 can escape as an unmapped S2Error).
    tailSeqNum.pipe(
      Effect.flatMap((tail) => (tail <= 0 ? Effect.succeed<ReadonlyArray<LogEntry>>([]) : readFrom(0))),
      Effect.withSpan("effect-s2-durable.log.read", { attributes: { stream: streamName } }),
    )

  const append = (event: ActorEvent): Effect.Effect<number, DurableExecutionError, S2Client> =>
    publish(streamName, ActorEvent, event).pipe(
      Effect.map((ack) => ack.start.seqNum),
      Effect.mapError(toError("object.append")),
      Effect.withSpan("effect-s2-durable.log.append", { attributes: { stream: streamName, tag: event._tag } }),
    )

  const casAppend = (
    event: ActorEvent,
    matchSeqNum: number,
  ): Effect.Effect<Option.Option<number>, DurableExecutionError, S2Client> =>
    conditionalAppend(streamName, ActorEvent, event, matchSeqNum).pipe(
      Effect.map((ack) => Option.some(ack.start.seqNum)),
      Effect.catch((cause) => (isCasLoss(cause) ? Effect.succeedNone : Effect.fail(cause))),
      Effect.mapError(toError("object.casAppend")),
      Effect.withSpan("effect-s2-durable.log.casAppend", {
        attributes: { stream: streamName, tag: event._tag, matchSeqNum },
      }),
    )

  return { streamName, read, append, casAppend, tailSeqNum }
}
