import { Effect, Option } from "effect"
import type { S2Client } from "effect-s2"
import { DurableExecutionError } from "../errors.ts"
import { isDone } from "./snapshot.ts"
import type { ActorLog } from "./log.ts"
import { replay } from "./snapshot.ts"

/**
 * Admission (`ADMISSION.4/6`): a producer admits an exclusive call by appending an
 * `Accepted` event under a `seq_num` CAS, without hosting the drainer.
 *
 * Idempotency is checked against the callId IDEMPOTENCY PROJECTION folded from the
 * log — currently a pending `Accepted` or a retained `Completed`. (A later slice
 * extends the projection to the latest `Checkpointed` snapshot and the `Expired`
 * watermark; this is deliberately NOT a literal "scan the log for an Accepted",
 * which checkpoint-trim would break.)
 */
export type AdmitResult =
  | { readonly _tag: "Admitted"; readonly seqNum: number }
  | { readonly _tag: "AlreadyPending" }
  | { readonly _tag: "AlreadyCompleted" }

/** Bound on CAS retries before surfacing contention as an error. */
const MAX_CAS_RETRIES = 16

export const admit = (
  log: ActorLog,
  callId: string,
  method: string,
  input: unknown,
): Effect.Effect<AdmitResult, DurableExecutionError, S2Client> => {
  const attempt = (retries: number): Effect.Effect<AdmitResult, DurableExecutionError, S2Client> =>
    Effect.gen(function*() {
      // checkTail FIRST, then read up to it: the CAS at `tail` only lands if nothing
      // was appended since, so the idempotency snapshot can't miss a concurrent admit.
      const tail = yield* log.tailSeqNum
      const snapshot = replay(yield* log.read())
      if (isDone(snapshot, callId)) {
        return { _tag: "AlreadyCompleted" }
      }
      if (snapshot.pending.includes(callId)) {
        return { _tag: "AlreadyPending" }
      }
      const appended = yield* log.casAppend({ _tag: "Accepted", callId, method, input }, tail)
      return yield* Option.match(appended, {
        onSome: (seqNum): Effect.Effect<AdmitResult, DurableExecutionError, S2Client> =>
          Effect.succeed({ _tag: "Admitted", seqNum }),
        onNone: () =>
          retries <= 0
            ? Effect.fail(
              new DurableExecutionError({
                operation: "actor.admit",
                message: `admission CAS contention exhausted for callId ${callId}`,
                cause: undefined,
              }),
            )
            : attempt(retries - 1), // a concurrent producer won the CAS — re-read and retry
      })
    })

  return attempt(MAX_CAS_RETRIES).pipe(
    Effect.withSpan("effect-s2-durable.admit", { attributes: { callId, method } }),
  )
}
