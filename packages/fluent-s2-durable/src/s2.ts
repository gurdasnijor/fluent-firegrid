import { Context, Data, type Effect, type Stream } from "effect"
import type { AppendCondFailed, S2Error } from "./errors.ts"

/** A record as read back from S2: the S2-assigned `seqNum` plus opaque bytes. */
export interface S2Record {
  readonly seqNum: bigint
  readonly data: Uint8Array
}

/**
 * One entry in an append batch. Faithful to S2: a batch is a list of records,
 * each of which is either a data record or a *command* (here, `trim`; `fence` has
 * its own method as it is set-once-per-lease). Modelling the batch this way lets
 * the snapshot-and-follow recipe append a `trim` command and the snapshot record
 * **atomically** in one batch, as the S2 docs require.
 */
export type S2Write = Data.TaggedEnum<{
  Record: { readonly body: Uint8Array }
  Trim: { readonly upTo: bigint }
}>
export const S2Write = Data.taggedEnum<S2Write>()

export interface AppendOptions {
  /** The executor lease. Presented on every journal write (coarse zombie-stopper). */
  readonly fencingToken?: string
  /**
   * Exactly-once guard: the append commits only if S2's next assignable seq_num
   * equals this — S2's optimistic concurrency control, the load-bearing precondition.
   */
  readonly matchSeqNum?: bigint
}

/**
 * §5.1 — thin Layer over the S2 TS SDK. The two concurrency primitives are
 * surfaced as append options; everything else is read/observe. This is the
 * "Bifrost" seam (S2 is the log).
 */
export interface S2Service {
  readonly append: (
    stream: string,
    writes: ReadonlyArray<S2Write>,
    opts?: AppendOptions,
  ) => Effect.Effect<{ readonly tail: bigint }, AppendCondFailed | S2Error>

  /** Read session as a Stream; `from` is a seq_num, `follow` keeps it open at the tail. */
  readonly read: (
    stream: string,
    from: bigint,
    opts?: { readonly follow?: boolean },
  ) => Stream.Stream<S2Record, S2Error>

  readonly checkTail: (stream: string) => Effect.Effect<bigint, S2Error>

  /** The fencing token S2 currently holds for the stream (null if never fenced). */
  readonly checkFence: (stream: string) => Effect.Effect<string | null, S2Error>

  /** Issue a fence command record. Cooperative, last-write-wins (S2 does not enforce monotonicity). */
  readonly fence: (stream: string, token: string) => Effect.Effect<void, S2Error>
}

export class S2 extends Context.Service<S2, S2Service>()(
  "@firegrid/fluent-s2-durable/S2",
) {}
