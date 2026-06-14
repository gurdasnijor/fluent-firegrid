import { Clock, Effect, Random } from "effect"
import { describe, expect, it } from "vitest"
import {
  S2InMemory,
  decodeRecord,
  deterministicLayers,
  encodeRecord,
  fold,
  foldRecords,
  type JournalRecord,
  type SeedData,
} from "../src/index.ts"

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect)

describe("record codec — M1", () => {
  it("round-trips every record kind through bytes", () =>
    run(
      Effect.gen(function* () {
        const records: ReadonlyArray<JournalRecord> = [
          { kind: "seed", seed: { epochMillis: 1, random: 2 }, input: { orderId: "o1" } },
          { kind: "step", op: 0, name: "charge", outcome: { _tag: "ok", value: "c1" } },
          { kind: "step", op: 1, name: "x", outcome: { _tag: "error", error: { msg: "boom" } } },
          { kind: "timer-set", op: 2, name: "cooloff", fireAt: 123 },
          { kind: "timer-fired", op: 2 },
          { kind: "awakeable", op: 3, name: "approval", id: "e#3" },
          { kind: "awakeable-done", op: 3, value: true },
          { kind: "completed", outcome: { _tag: "ok", value: { status: "fulfilled" } } },
        ]
        for (const rec of records) {
          const decoded = yield* decodeRecord(encodeRecord(rec))
          expect(decoded).toEqual(rec)
        }
      }),
    ))
})

describe("journal fold — M1", () => {
  it("builds byOp/tail/seed/input and overwrites timer-set with timer-fired", () => {
    const journal = foldRecords([
      [0n, { kind: "seed", seed: { epochMillis: 7, random: 9 }, input: { orderId: "o1" } }],
      [1n, { kind: "step", op: 0, name: "charge", outcome: { _tag: "ok", value: "c1" } }],
      [2n, { kind: "timer-set", op: 1, name: "cooloff", fireAt: 50 }],
      [3n, { kind: "timer-fired", op: 1 }],
    ])
    expect(journal.tail).toBe(4n)
    expect(journal.seed).toEqual({ epochMillis: 7, random: 9 })
    expect(journal.input).toEqual({ orderId: "o1" })
    expect(journal.byOp.get(0)?.kind).toBe("step")
    expect(journal.byOp.get(1)?.kind).toBe("timer-fired")
    expect(journal.status).toBe("running")
  })

  it("a snapshot record reseeds byOp from head and applies later deltas (AC-5)", () => {
    const journal = foldRecords([
      [
        10n,
        {
          kind: "snapshot",
          covers: 10,
          state: {
            seed: { epochMillis: 1, random: 1 },
            input: { orderId: "o9" },
            records: [{ kind: "step", op: 0, name: "charge", outcome: { _tag: "ok", value: "c1" } }],
          },
        },
      ],
      [11n, { kind: "step", op: 1, name: "fulfill", outcome: { _tag: "ok", value: "f1" } }],
    ])
    // recovery did not need anything below seq 10
    expect(journal.tail).toBe(12n)
    expect(journal.byOp.get(0)?.kind).toBe("step")
    expect(journal.byOp.get(1)?.kind).toBe("step")
    expect(journal.input).toEqual({ orderId: "o9" })
  })

  it("folds a real S2 stream", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.append("wf/x", [
          encodeRecord({ kind: "seed", seed: { epochMillis: 1, random: 1 }, input: 42 }),
          encodeRecord({ kind: "completed", outcome: { _tag: "ok", value: "done" } }),
        ])
        const journal = yield* fold(s2.read("wf/x", 0n))
        expect(journal.status).toBe("completed")
        expect(journal.completed).toEqual({ _tag: "ok", value: "done" })
        expect(journal.input).toBe(42)
      }),
    ))
})

describe("deterministic Clock/Random — §5.4", () => {
  const seed: SeedData = { epochMillis: 1_700_000_000_000, random: 12345 }

  const sample = Effect.gen(function* () {
    const t = yield* Clock.currentTimeMillis
    const a = yield* Random.nextInt
    const b = yield* Random.next
    return { t, a, b }
  })

  it("replays identical time and random sequences from the same seed", () =>
    run(
      Effect.gen(function* () {
        const first = yield* sample.pipe(Effect.provide(deterministicLayers(seed)))
        const second = yield* sample.pipe(Effect.provide(deterministicLayers(seed)))
        expect(first).toEqual(second)
        expect(first.t).toBe(seed.epochMillis)
      }),
    ))
})
