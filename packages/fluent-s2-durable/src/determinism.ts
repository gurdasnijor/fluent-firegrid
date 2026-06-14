import { Clock, Effect, Layer, Random } from "effect"
import type { SeedData } from "./record.ts"

/**
 * §5.4 — replay-deterministic `Clock`/`Random`, sourced from the journal `seed`.
 * Ordinary `Clock.currentTimeMillis` / `Random.next` inside handler code become
 * durable with no special ctx calls: the same seed replays the same values.
 *
 * The clock is frozen at the seed's base instant — durable waits go through
 * `ctx.sleep` (journaled `fireAt`), never the ambient clock, so freezing it
 * removes a source of replay non-determinism rather than losing anything.
 */

const mulberry32 = (seedInt: number): (() => number) => {
  let a = seedInt >>> 0
  return () => {
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const deterministicClock = (seed: SeedData): Clock.Clock => {
  const ms = seed.epochMillis
  const nanos = BigInt(ms) * 1_000_000n
  return {
    currentTimeMillisUnsafe: () => ms,
    currentTimeMillis: Effect.sync(() => ms),
    currentTimeNanosUnsafe: () => nanos,
    currentTimeNanos: Effect.sync(() => nanos),
    sleep: () => Effect.void,
  }
}

const deterministicRandom = (seed: SeedData): typeof Random.Random.Service => {
  const next = mulberry32(seed.random)
  return {
    nextDoubleUnsafe: () => next(),
    nextIntUnsafe: () => (next() * 0x100000000) | 0,
  }
}

/** Clock + Random layers wired to a journal seed; provide around handler execution. */
export const deterministicLayers = (seed: SeedData): Layer.Layer<never> =>
  Layer.merge(
    Layer.succeed(Clock.Clock, deterministicClock(seed)),
    Layer.succeed(Random.Random, deterministicRandom(seed)),
  )
