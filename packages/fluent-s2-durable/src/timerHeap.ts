import { Clock, Context, Duration, Effect, Layer } from "effect"
import { Dispatch } from "./dispatch.ts"

export interface TimerEntry {
  readonly fireAt: number
  readonly execId: string
  readonly name: string
}

/**
 * §5.5 — in-memory durable-timer arming. The journal records `timer-set`;
 * this only schedules the *wakeup*. On fire it pokes Dispatch; the tick then
 * appends `timer-fired` under the lease (single writer). A timer whose `fireAt`
 * already elapsed (e.g. across downtime) is appended immediately by the tick's
 * reconciliation — durable sleep is "at least", not "at exactly".
 *
 * Spike simplification: one detached fiber per armed timer instead of a single
 * `setTimeout` to the nearest entry. Re-arming a timer that already fired is
 * harmless because the tick is idempotent.
 */
export interface TimerHeapService {
  readonly arm: (entry: TimerEntry) => Effect.Effect<void>
}

export class TimerHeap extends Context.Service<TimerHeap, TimerHeapService>()(
  "@firegrid/fluent-s2-durable/TimerHeap",
) {}

const make: Effect.Effect<TimerHeapService, never, Dispatch> = Effect.gen(function* () {
  const dispatch = yield* Effect.service(Dispatch)
  return {
    arm: (entry) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Effect.forkChild(
            Effect.sleep(Duration.millis(Math.max(0, entry.fireAt - now))).pipe(
              Effect.andThen(dispatch.poke(entry.execId)),
            ),
          ),
        ),
        Effect.asVoid,
      ),
  }
})

export const layer: Layer.Layer<TimerHeap, never, Dispatch> = Layer.effect(TimerHeap, make)
