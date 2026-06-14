import { Context, Effect, Layer, Queue } from "effect"

/**
 * §5.5 — in-memory ready-set. `poke` marks an execution ready, `claim` blocks
 * for the next ready execution. Pure cache: on restart the host re-pokes every
 * active execution by folding S2.
 *
 * Deliberately *not* deduped. A dropped poke is a lost wakeup; because `tick` is
 * idempotent (it folds the journal and either makes progress or re-suspends), a
 * redundant poke is merely a wasted fold, never a hang. Poke sources are bounded
 * (timer fire, event resolution, inbox watcher) so this cannot busy-loop.
 */
export interface DispatchService {
  readonly poke: (execId: string) => Effect.Effect<void>
  readonly claim: Effect.Effect<string>
}

export class Dispatch extends Context.Service<Dispatch, DispatchService>()(
  "@firegrid/fluent-s2-durable/Dispatch",
) {}

const make: Effect.Effect<DispatchService> = Effect.gen(function* () {
  const queue = yield* Queue.unbounded<string>()
  return {
    poke: (execId) => Queue.offer(queue, execId).pipe(Effect.asVoid),
    claim: Queue.take(queue),
  }
})

export const layer: Layer.Layer<Dispatch> = Layer.effect(Dispatch, make)
