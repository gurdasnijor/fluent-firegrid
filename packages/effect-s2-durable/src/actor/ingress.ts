import { Effect } from "effect"
import type { S2Client } from "effect-s2"
import type { DurableExecutionError } from "../errors.ts"
import type { ActorLog } from "./log.ts"
import { attach as attachSnapshot, type CallStatus, replay } from "./snapshot.ts"

/**
 * Ingress and result views — residency-independent (`INGRESS`, `COMPLETION.4`).
 *
 * `resolveSignal` is just an append: it succeeds whether or not the target call is
 * resident or running (the durable event is the source of truth; an in-process
 * waiter is poked best-effort by the drainer, not here). `attach`/`poll` read the
 * log and fold the projection — no roster, no residency.
 */

/** Append a `SignalResolved` event — durable regardless of the call's residency (`INGRESS.1`). */
export const resolveSignal = (
  log: ActorLog,
  callId: string,
  name: string,
  value: unknown,
): Effect.Effect<void, DurableExecutionError, S2Client> =>
  log.append({ _tag: "SignalResolved", callId, name, value }).pipe(
    Effect.asVoid,
    Effect.withSpan("effect-s2-durable.resolveSignal", { attributes: { callId, name } }),
  )

/** Read the call's status from the projection (`Pending` while unsettled, `COMPLETION.4`). */
export const attach = (
  log: ActorLog,
  callId: string,
): Effect.Effect<CallStatus, DurableExecutionError, S2Client> =>
  log.read().pipe(
    Effect.map((entries) => attachSnapshot(replay(entries), callId)),
    Effect.withSpan("effect-s2-durable.attach", { attributes: { callId } }),
  )
