import { describe, expect, it } from "@effect/vitest"
import { AppendInput, AppendRecord, type S2Record, S2Client } from "effect-s2"
import type * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import { makeOwned, makeView, type Orchestrator } from "../src/index.ts"
import { hasS2, S2LiteLive } from "./s2lite.ts"

// Layer 1 conformance gate (SDD §1, Build Plan step 1): reproduce the KV-demo's
// externally observable semantics on a single stream — strong vs eventual reads,
// in-order write acks, recover-from-cursor — plus the OwnedOrchestrator's
// read-your-writes guarantee and the "tail reader never double-applies own
// records" invariant. Validated against real `s2 lite` (not a mock).

// A counter register: each data record carries an integer delta; the fold sums
// them. Command records (empty-name header) are skipped, as a real view would.
const COUNT = (state: number, record: S2Record): number =>
  record.headers.some(([name]) => name === "") ? state : state + Number(record.body)

const delta = (n: number): AppendRecord => AppendRecord.string({ body: String(n) })

let streamCounter = 0
const freshStream = (label: string): string => `orchestrator-test/${label}-${++streamCounter}`

const run = <A, E>(program: Effect.Effect<A, E, S2Client | Scope.Scope>): Promise<A> =>
  program.pipe(Effect.scoped, Effect.provide(S2LiteLive), Effect.runPromise)

// Seed a stream with raw foreign appends (a different producer than the owner).
const seed = (stream: string, deltas: ReadonlyArray<number>) =>
  S2Client.append(stream, AppendInput.create(deltas.map(delta)))

// Wait until the orchestrator's applied cursor reaches `target`.
const awaitApplied = <S>(o: Orchestrator<S>, target: number): Effect.Effect<void, Cause.TimeoutError> =>
  o.applied.pipe(
    Effect.flatMap((n) =>
      n >= target ? Effect.void : Effect.sleep("5 millis").pipe(Effect.flatMap(() => awaitApplied(o, target)))
    ),
    Effect.timeout("10 seconds")
  )

describe.skipIf(!hasS2())("Orchestrator — Layer 1 conformance (s2 lite)", () => {
  it("view: eventual read folds appended records on tail", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("view-eventual")
      const o = yield* makeView<number>({ stream, initial: 0, reduce: COUNT })
      yield* seed(stream, [1, 2, 3])
      yield* awaitApplied(o, 3)
      expect(yield* o.readEventual((s) => s)).toBe(6)
    })))

  it("view: getStrong reflects a concurrent foreign write (linearizable read)", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("view-strong")
      const o = yield* makeView<number>({ stream, initial: 0, reduce: COUNT })
      // append, then immediately strong-read: checkTail → defer until applied ≥ tail.
      yield* seed(stream, [10, 5])
      expect(yield* o.readStrong((s) => s)).toBe(15)
    })))

  it("owned: read-your-writes — write resolves after its own ordered apply", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("owned-ryw")
      const o = yield* makeOwned<number>({ stream, initial: 0, reduce: COUNT })
      yield* o.write([delta(5)])
      // the write reply completes only after the record is folded locally, so an
      // immediately-following eventual read already sees it (no tail round-trip).
      expect(yield* o.readEventual((s) => s)).toBe(5)
      yield* o.write([delta(7)])
      expect(yield* o.readEventual((s) => s)).toBe(12)
    })))

  it("owned: own records are applied exactly once (no double-apply)", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("owned-once")
      const o = yield* makeOwned<number>({ stream, initial: 0, reduce: COUNT })
      // ten own writes of +1 each; if the ack path and the tail reader both
      // applied own records, the sum would be 20 and applied would be 20.
      for (let i = 0; i < 10; i++) yield* o.write([delta(1)])
      expect(yield* o.readEventual((s) => s)).toBe(10)
      expect(yield* o.applied).toBe(10)
      // give the tail reader a chance to re-deliver own records, then re-check.
      yield* Effect.sleep("200 millis")
      expect(yield* o.readStrong((s) => s)).toBe(10)
      expect(yield* o.applied).toBe(10)
    })))

  it("owned: write acks are in stream order", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("owned-order")
      const o = yield* makeOwned<number>({ stream, initial: 0, reduce: COUNT })
      const a = yield* o.write([delta(1)])
      const b = yield* o.write([delta(1), delta(1)])
      const c = yield* o.write([delta(1)])
      expect(a.start.seqNum).toBe(0)
      expect(b.start.seqNum).toBe(1) // batch of 2 ⇒ occupies seq 1,2
      expect(c.start.seqNum).toBe(3)
    })))

  it("recover-from-cursor: a fresh orchestrator folds the existing log", () =>
    run(Effect.gen(function*() {
      const stream = freshStream("recover")
      yield* seed(stream, [1, 2, 3, 4]) // tail = 4, full state = 10
      // cold start from 0 with empty initial ⇒ full replay.
      const full = yield* makeView<number>({ stream, initial: 0, reduce: COUNT })
      expect(yield* full.readStrong((s) => s)).toBe(10)
      // cold start from a snapshot point: initial = sum of [1,2], fromCursor = 2.
      const fromSnapshot = yield* makeView<number>({ stream, initial: 3, reduce: COUNT, fromCursor: 2 })
      yield* awaitApplied(fromSnapshot, 4)
      expect(yield* fromSnapshot.readStrong((s) => s)).toBe(10)
    })))

  it("owned and view coexist on distinct streams (the Layer 1 fork)", () =>
    run(Effect.gen(function*() {
      const a = yield* makeOwned<number>({ stream: freshStream("coexist-a"), initial: 0, reduce: COUNT })
      const b = yield* makeView<number>({ stream: freshStream("coexist-b"), initial: 0, reduce: COUNT })
      yield* a.write([delta(2)])
      expect(yield* a.readEventual((s) => s)).toBe(2)
      expect(yield* b.readEventual((s) => s)).toBe(0)
    })))
})
