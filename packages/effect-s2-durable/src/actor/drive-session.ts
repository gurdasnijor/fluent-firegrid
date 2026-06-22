import { Data, Effect, Schema } from "effect"
import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  guardedAppend,
  randomToken,
  S2Client,
  S2Conflict,
  S2NotFound,
} from "effect-s2"
import { type DurableExecutionError, durableError as toError } from "../errors.ts"
import { ActorEvent } from "./core.ts"

/**
 * The scoped owner-drive session: the ONE place that combines fencing with owner
 * appends (host SDD §7 / `DESIGN.md`). The drainer claims an owner stream by
 * stamping a fence token; thereafter every drive append carries that token, so a
 * peer host that claims the same stream displaces this one. Cross-host single-writer
 * is enforced by S2 (the fence), not by in-process coordination — the in-process
 * lock/started/snapshot guards in the drainer are only for intra-process scheduling
 * and read-after-write lag.
 *
 * Deliberately NO per-append `matchSeqNum`: a seq-num conflict in the middle of a
 * user handler would force out-of-band abort/retry to unwind and replay the handler.
 * The fence is the only owner-driver guard; admission/ingress appends stay outside
 * this session (they must not require the owner token).
 */

const isS2Conflict = Schema.is(S2Conflict)
const isS2NotFound = Schema.is(S2NotFound)

const isAbsent = (cause: unknown): boolean =>
  isS2NotFound(cause) || (cause as { readonly status?: unknown }).status === 404

// A 412 carrying `expectedFencingToken` (or the raw SDK error) is a lost fence — a
// peer claimed the stream — as opposed to a 409 seq-num race (which this path never
// provokes, since drive appends carry no `matchSeqNum`).
const isFenceLoss = (cause: unknown): boolean =>
  (isS2Conflict(cause) && cause.expectedFencingToken !== undefined) || cause instanceof FencingTokenMismatchError

/**
 * A peer host has claimed the owner stream — this host's fence token no longer
 * matches. A typed domain failure (NOT a defect): the drainer catches it at its
 * boundary and stops driving (becomes a follower). It never reaches user handlers —
 * the owner-drive session is the only place that raises or names it.
 */
export class FenceLost extends Data.TaggedError("FenceLost")<{ readonly stream: string }> {}

/**
 * This process's owner fence token. Uses the upstream `randomToken` (a base64
 * token) sized to stay within S2's 36-byte fencing-token limit. Mint once per
 * `InvocationStore` (per host process).
 */
export const freshHostToken = (): string => `host-${randomToken(20)}`

export interface OwnerDriveSession {
  /** The fence token this session holds for the stream. */
  readonly token: string
  /**
   * Append one owner-driver event under the fence, resolving to its assigned
   * `seq_num`. Fails with {@link FenceLost} if a peer has claimed the stream.
   */
  readonly append: (event: ActorEvent) => Effect.Effect<number, DurableExecutionError | FenceLost, S2Client>
}

/**
 * Create the owner stream if absent (idempotent), then confirm it is visible before
 * returning. S2 stream creation is not instantly consistent under load, so a bare
 * create-then-append can 404; retrying `checkTail` until it succeeds closes that
 * window so every subsequent append (admission CAS, fence, drive) is safe. The
 * retries are bounded (not fixed-interval polling) — each attempt is a fresh
 * round-trip whose latency paces the loop.
 */
export const ensureStream = (stream: string): Effect.Effect<void, DurableExecutionError, S2Client> =>
  S2Client.createStream({ stream }).pipe(
    Effect.asVoid,
    Effect.catch((cause) => (isS2Conflict(cause) ? Effect.void : Effect.fail(cause))),
    Effect.andThen(S2Client.checkTail(stream).pipe(Effect.retry({ while: isAbsent, times: 100 }), Effect.asVoid)),
    Effect.mapError(toError("object.ensureStream")),
  )

// Claim the stream by stamping the fence (a fence command targets an existing
// stream, which `ensureStream` has confirmed visible).
const claim = (stream: string, token: string): Effect.Effect<void, DurableExecutionError, S2Client> =>
  S2Client.append(stream, AppendInput.create([AppendRecord.fence(token)])).pipe(
    Effect.asVoid,
    Effect.mapError(toError("object.driveSession.claim")),
  )

/**
 * Open a scoped owner-drive session: ensure the stream exists, claim it with this
 * host's fence token, and return an `append` that stamps every write with the token.
 */
export const openOwnerDriveSession = (
  stream: string,
  token: string,
): Effect.Effect<OwnerDriveSession, DurableExecutionError, S2Client> =>
  Effect.gen(function*() {
    yield* ensureStream(stream)
    yield* claim(stream, token)
    const append = (event: ActorEvent): Effect.Effect<number, DurableExecutionError | FenceLost, S2Client> =>
      guardedAppend(stream, ActorEvent, event, { fencingToken: token }).pipe(
        Effect.map((ack) => ack.start.seqNum),
        Effect.catch((cause): Effect.Effect<never, DurableExecutionError | FenceLost> =>
          isFenceLoss(cause)
            ? Effect.fail(new FenceLost({ stream }))
            : Effect.fail(toError("object.driveSession.append")(cause)),
        ),
        Effect.withSpan("effect-s2-durable.driveSession.append", { attributes: { stream, tag: event._tag } }),
      )
    return { token, append }
  }).pipe(Effect.withSpan("effect-s2-durable.driveSession.open", { attributes: { stream, token } }))
