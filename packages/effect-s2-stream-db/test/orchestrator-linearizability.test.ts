import { describe, expect, it } from "@effect/vitest"
import { AppendRecord, type S2Client, type S2Record } from "effect-s2"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import { makeOwned } from "../src/index.ts"
import { hasS2, S2LiteLive } from "./s2lite.ts"

// SDD Build Plan step 1: the property-based linearizability gate. Replaying the
// demo's example trace can't catch a concurrency violation, so we generate
// interleaved write/strong-read/eventual-read histories against a single fenced
// owner (the OwnedOrchestrator models a register/monotonic counter) and check
// the headline claims hold under concurrency, against real `s2 lite`.

const COUNT = (state: number, record: S2Record): number =>
  record.headers.some(([name]) => name === "") ? state : state + Number(record.body)

const inc = AppendRecord.string({ body: "1" })

let streamCounter = 0
const freshStream = (label: string): string => `orchestrator-lin/${label}-${++streamCounter}`

const run = <A, E>(program: Effect.Effect<A, E, S2Client | Scope.Scope>): Promise<A> =>
  program.pipe(Effect.scoped, Effect.provide(S2LiteLive), Effect.runPromise)

// A deterministic PRNG (LCG) so generated schedules are reproducible without
// wall-clock/Math.random non-determinism.
const lcg = (seed: number): (() => number) => {
  let s = seed >>> 0
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0
    return s / 0xffffffff
  }
}

describe.skipIf(!hasS2())("Orchestrator — linearizability (s2 lite)", () => {
  it("strong reads are linearizable against a single fenced owner", () =>
    run(Effect.gen(function*() {
      for (const seed of [1, 7, 42, 1337, 90210]) {
        const stream = freshStream(`lin-${seed}`)
        const o = yield* makeOwned<number>({ stream, initial: 0, reduce: COUNT })
        const rnd = lcg(seed)
        const writes = 16 + Math.floor(rnd() * 24) // 16..39

        // Concurrent writers (each +1) interleaved with concurrent strong reads.
        // A strong read defers until applied ≥ tail-at-invocation, so its result
        // must be ≥ the applied prefix observed at invocation, and ≤ total writes.
        const writers = Array.from(
          { length: writes },
          () => o.write([inc]).pipe(Effect.asVoid)
        )
        const reads = Array.from({ length: 12 }, () =>
          Effect.gen(function*() {
            const before = yield* o.applied
            const observed = yield* o.readStrong((s) => s)
            expect(observed).toBeGreaterThanOrEqual(before)
            expect(observed).toBeLessThanOrEqual(writes)
          }))

        yield* Effect.all([...writers, ...reads], { concurrency: "unbounded" })

        // Once every write has acked, the register equals the total exactly —
        // each own record folded once (no loss, no double-apply).
        expect(yield* o.readStrong((s) => s)).toBe(writes)
        expect(yield* o.applied).toBe(writes)
      }
    })))

  it("eventual reads are monotonic under a concurrent writer", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("monotonic")
      const o = yield* makeOwned<number>({ stream, initial: 0, reduce: COUNT })
      const total = 40

      const writer = Effect.forEach(
        Array.from({ length: total }, () => 0),
        () => o.write([inc]).pipe(Effect.asVoid),
        { discard: true }
      )
      // a reader fiber sampling eventual reads while writes land must never see
      // the counter go backwards (the apply prefix only grows).
      const reader = Effect.gen(function*() {
        let last = 0
        for (let i = 0; i < 60; i++) {
          const v = yield* o.readEventual((s) => s)
          expect(v).toBeGreaterThanOrEqual(last)
          expect(v).toBeLessThanOrEqual(total)
          last = v
          yield* Effect.sleep("2 millis")
        }
      })

      yield* Effect.all([writer, reader], { concurrency: "unbounded" })
      expect(yield* o.readStrong((s) => s)).toBe(total)
    })))
})
