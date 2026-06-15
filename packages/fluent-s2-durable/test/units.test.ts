import { Clock, Effect, HashMap, Option, Random } from "effect"
import { expect, it } from "@effect/vitest"
import {
  Awakeable,
  AwakeableDone,
  Completed,
  Err,
  Ok,
  Seed,
  Snapshot,
  Step,
  TimerFired,
  TimerSet,
  decodeRecord,
  deterministicLayers,
  encodeRecord,
  foldRecords,
  type JournalRecord,
} from "../src/index.ts"

const samples: ReadonlyArray<JournalRecord> = [
  new Seed({ epochMillis: 1, random: 2, input: { orderId: "o1" } }),
  new Step({ name: "charge", outcome: new Ok({ value: "c1" }) }),
  new Step({ name: "x", outcome: new Err({ error: { msg: "boom" } }) }),
  new TimerSet({ name: "cooloff", fireAt: 123 }),
  new TimerFired({ name: "cooloff" }),
  new Awakeable({ name: "approval" }),
  new AwakeableDone({ name: "approval", value: true }),
  new Completed({ outcome: new Ok({ value: { status: "fulfilled" } }) }),
]

it.effect("M1 codec — round-trips every record kind through bytes", () =>
  Effect.forEach(
    samples,
    (rec) =>
      Effect.gen(function* () {
        const bytes = yield* encodeRecord(rec)
        const decoded = yield* decodeRecord(bytes)
        expect(decoded).toEqual(rec)
      }),
    { discard: true },
  ),
)

it.effect("M1 fold — byName keeps the latest record per name", () =>
  Effect.sync(() => {
    const journal = foldRecords([
      new Seed({ epochMillis: 7, random: 9, input: { orderId: "o1" } }),
      new Step({ name: "charge", outcome: new Ok({ value: "c1" }) }),
      new TimerSet({ name: "cooloff", fireAt: 50 }),
      new TimerFired({ name: "cooloff" }),
    ])
    expect(Option.isSome(journal.seed)).toBe(true)
    expect(journal.input).toEqual({ orderId: "o1" })
    expect(Option.getOrUndefined(HashMap.get(journal.byName, "charge"))).toBeInstanceOf(Step)
    expect(Option.getOrUndefined(HashMap.get(journal.byName, "cooloff"))).toBeInstanceOf(TimerFired)
    expect(Option.isNone(journal.completed)).toBe(true)
  }),
)

it.effect("M1 fold — a Snapshot reseeds byName from head and applies later deltas (AC-5)", () =>
  Effect.sync(() => {
    const journal = foldRecords([
      new Snapshot({
        covers: 10,
        records: [new Step({ name: "charge", outcome: new Ok({ value: "c1" }) })],
        seed: new Seed({ epochMillis: 1, random: 1, input: { orderId: "o9" } }),
        input: { orderId: "o9" },
      }),
      new Step({ name: "fulfill", outcome: new Ok({ value: "f1" }) }),
    ])
    expect(Option.getOrUndefined(HashMap.get(journal.byName, "charge"))).toBeInstanceOf(Step)
    expect(Option.getOrUndefined(HashMap.get(journal.byName, "fulfill"))).toBeInstanceOf(Step)
    expect(journal.input).toEqual({ orderId: "o9" })
  }),
)

it.effect("§5.4 determinism — same seed replays identical Clock/Random", () => {
  const seed = new Seed({ epochMillis: 1_700_000_000_000, random: 12345, input: null })
  const sample = Effect.gen(function* () {
    const t = yield* Clock.currentTimeMillis
    const a = yield* Random.nextInt
    const b = yield* Random.next
    return { t, a, b }
  })
  return Effect.gen(function* () {
    const first = yield* sample.pipe(Effect.provide(deterministicLayers(seed)))
    const second = yield* sample.pipe(Effect.provide(deterministicLayers(seed)))
    expect(first).toEqual(second)
    expect(first.t).toBe(seed.epochMillis)
  })
})
