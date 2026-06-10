import { afterAll, beforeAll, bench, describe } from "vitest"
import { Effect, Fiber } from "effect"
import { execute, run } from "../src/index.ts"
import {
  BENCH_OPTS,
  BENCH_SIZES,
  makeEffectRuntime,
  makeMemoryDurableStreamsFetch,
  runScoped,
  streamUrl,
} from "./_helpers.ts"
import type { ExecutionContext, Journal } from "../src/index.ts"
import type { EffectRuntime } from "./_helpers.ts"
import type { DurableExecutionRequirements } from "../src/schema.ts"

let runtime: EffectRuntime
const replayUrls = new Map<string, string>()

const invocation = (url: string): ExecutionContext => ({
  journal: { endpoint: { url } },
})

const replayKey = (name: string, size: number): string => `${name}:${size}`

const expectedSum = (size: number): number => ((size - 1) * size) / 2

const allOperation = (size: number, action: (index: number) => number) =>
  Effect.gen(function* () {
    const effects = Array.from({ length: size }, (_value, index) =>
      run(
        `all-${index}`,
        Effect.sync(() => action(index)),
      ),
    )
    const values = yield* Effect.all(effects, { concurrency: "unbounded" })
    let total = 0
    for (let index = 0; index < values.length; index += 1) {
      total += values[index] ?? 0
    }
    return total
  })

const raceOperation = (size: number, action: (index: number) => number) =>
  Effect.gen(function* () {
    let winner = run(
      "race-0",
      Effect.sync(() => action(0)),
    )
    for (let index = 1; index < size; index += 1) {
      winner = Effect.race(
        winner,
        run(
          `race-${index}`,
          Effect.sync(() => action(index)),
        ),
      )
    }
    return yield* winner
  })

const spawnOperation = (size: number, action: (index: number) => number) =>
  Effect.gen(function* () {
    const fibers = new Array<Fiber.RuntimeFiber<number, unknown>>(size)
    for (let index = 0; index < size; index += 1) {
      fibers[index] = yield* Effect.fork(
        run(
          `spawn-${index}`,
          Effect.sync(() => action(index)),
        ),
      )
    }
    const values = yield* Effect.all(
      fibers.map((fiber) => Fiber.join(fiber)),
      { concurrency: "unbounded" },
    )
    let total = 0
    for (let index = 0; index < values.length; index += 1) {
      total += values[index] ?? 0
    }
    return total
  })

type OperationRequirements = Journal | DurableExecutionRequirements

const seedReplay = async (
  name: string,
  size: number,
  operation: Effect.Effect<number, unknown, OperationRequirements>,
  validate: (value: number) => void,
) => {
  const url = streamUrl(`${name}-replay-${size}`)
  const value = await runScoped(runtime, execute(invocation(url), operation))
  validate(value)
  replayUrls.set(replayKey(name, size), url)
}

beforeAll(async () => {
  runtime = makeEffectRuntime(makeMemoryDurableStreamsFetch())
  for (const size of BENCH_SIZES) {
    await seedReplay(
      "all",
      size,
      allOperation(size, (index) => index),
      (value) => {
        if (value !== expectedSum(size)) {
          throw new Error(`unexpected all total for ${size}: ${value}`)
        }
      },
    )
    await seedReplay(
      "race",
      size,
      raceOperation(size, (index) => index),
      (value) => {
        if (value < 0 || value >= size) {
          throw new Error(`unexpected race winner for ${size}: ${value}`)
        }
      },
    )
    await seedReplay(
      "spawn",
      size,
      spawnOperation(size, (index) => index),
      (value) => {
        if (value !== expectedSum(size)) {
          throw new Error(`unexpected spawn total for ${size}: ${value}`)
        }
      },
    )
  }
}, 30_000)

afterAll(async () => {
  await runtime.dispose()
})

for (const size of BENCH_SIZES) {
  describe(`durable-execution public Effect.all(${size}) replay`, () => {
    bench(
      `execute + Effect.all(${size}x run) replay`,
      async () => {
        const url = replayUrls.get(replayKey("all", size))
        if (url === undefined)
          throw new Error(`missing all replay journal for ${size}`)
        const total = await runScoped(
          runtime,
          execute(
            invocation(url),
            allOperation(size, () => {
              throw new Error("all replay action should not execute")
            }),
          ),
        )
        if (total !== expectedSum(size)) {
          throw new Error(`unexpected all replay total for ${size}: ${total}`)
        }
      },
      BENCH_OPTS,
    )
  })

  describe(`durable-execution public Effect.race(${size}) replay`, () => {
    bench(
      `execute + Effect.race(${size}x run) replay`,
      async () => {
        const url = replayUrls.get(replayKey("race", size))
        if (url === undefined)
          throw new Error(`missing race replay journal for ${size}`)
        const winner = await runScoped(
          runtime,
          execute(
            invocation(url),
            raceOperation(size, () => {
              throw new Error("race replay action should not execute")
            }),
          ),
        )
        if (winner < 0 || winner >= size) {
          throw new Error(
            `unexpected race replay winner for ${size}: ${winner}`,
          )
        }
      },
      BENCH_OPTS,
    )
  })

  describe(`durable-execution public Effect.fork(${size}) replay`, () => {
    bench(
      `execute + Effect.fork(${size}) + Effect.all replay`,
      async () => {
        const url = replayUrls.get(replayKey("spawn", size))
        if (url === undefined)
          throw new Error(`missing spawn replay journal for ${size}`)
        const total = await runScoped(
          runtime,
          execute(
            invocation(url),
            spawnOperation(size, () => {
              throw new Error("spawn replay action should not execute")
            }),
          ),
        )
        if (total !== expectedSum(size)) {
          throw new Error(`unexpected spawn replay total for ${size}: ${total}`)
        }
      },
      BENCH_OPTS,
    )
  })
}
