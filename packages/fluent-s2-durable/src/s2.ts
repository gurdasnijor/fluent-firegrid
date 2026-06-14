import { Context, type Effect, type Stream } from "effect"
import type { AppendCondFailed, S2Error } from "./errors.ts"

/** A record as read back from S2: the S2-assigned `seqNum` plus opaque bytes. */
export interface S2Record {
  readonly seqNum: bigint
  readonly data: Uint8Array
}

export interface AppendOptions {
  /** The executor lease. Presented on every journal write (coarse zombie-stopper). */
  readonly fencingToken?: string
  /**
   * Exactly-once guard: the append commits only if S2's next assignable seq_num
   * equals this. This is the load-bearing precondition (Q1 fallback path).
   */
  readonly matchSeqNum?: bigint
}

/**
 * §5.1 — thin Layer over the S2 TS SDK (here, the in-memory `s2-lite`
 * emulation). The two concurrency primitives are surfaced as append options;
 * everything else is read/observe.
 */
export interface S2Service {
  readonly append: (
    stream: string,
    records: ReadonlyArray<Uint8Array>,
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

  /** Issue a fence command record; the highest token presented wins thereafter. */
  readonly fence: (stream: string, token: string) => Effect.Effect<void, S2Error>

  /** Trim everything strictly below `upTo`; the record at `upTo` becomes the new head. */
  readonly trim: (stream: string, upTo: bigint) => Effect.Effect<void, S2Error>
}

export class S2 extends Context.Service<S2, S2Service>()(
  "@firegrid/fluent-s2-durable/S2",
) {}
