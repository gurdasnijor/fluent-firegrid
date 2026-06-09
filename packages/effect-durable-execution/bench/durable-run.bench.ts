import { afterAll, beforeAll, bench, describe } from "vitest"
import { Effect } from "effect"
import { execute, run } from "../src/index.ts"
import {
  BENCH_OPTS,
  BENCH_SIZES,
  makeEffectRuntime,
  makeMemoryDurableStreamsFetch,
  runScoped,
  streamUrl,
} from "./_helpers.ts"
import type { ExecutionContext } from "../src/index.ts"
import type { EffectRuntime } from "./_helpers.ts"

let runtime: EffectRuntime
const replayUrls = new Map<number, string>()

const invocation = (url: string): ExecutionContext => ({
  journal: { endpoint: { url } },
})

const durableRunOperation = (size: number, action: (index: number) => number) =>
  Effect.gen(function* () {
    let total = 0
    for (let index = 0; index < size; index += 1) {
      total += yield* run(
        `step-${index}`,
        Effect.sync(() => action(index)),
      )
    }
    return total
  })

beforeAll(async () => {
  runtime = makeEffectRuntime(makeMemoryDurableStreamsFetch())
  for (const size of BENCH_SIZES) {
    const url = streamUrl(`replay-${size}`)
    replayUrls.set(size, url)
    const total = await runScoped(
      runtime,
      execute(
        invocation(url),
        durableRunOperation(size, (index) => index),
      ),
    )
    if (total !== ((size - 1) * size) / 2) {
      throw new Error(`unexpected seed total for ${size}: ${total}`)
    }
  }
}, 30_000)

afterAll(async () => {
  await runtime.dispose()
})

for (const size of BENCH_SIZES) {
  describe(`durable-execution first execution ${size} runs`, () => {
    bench(
      `execute + ${size}x run append`,
      async () => {
        const total = await runScoped(
          runtime,
          execute(
            invocation(streamUrl(`first-${size}`)),
            durableRunOperation(size, (index) => index),
          ),
        )
        if (total !== ((size - 1) * size) / 2) {
          throw new Error(`unexpected first-run total for ${size}: ${total}`)
        }
      },
      BENCH_OPTS,
    )
  })

  describe(`durable-execution replay ${size} runs`, () => {
    bench(
      `execute + ${size}x run replay`,
      async () => {
        const replayUrl = replayUrls.get(size)
        if (replayUrl === undefined) {
          throw new Error(`missing replay journal for ${size}`)
        }
        const total = await runScoped(
          runtime,
          execute(
            invocation(replayUrl),
            durableRunOperation(size, () => {
              throw new Error("replay action should not execute")
            }),
          ),
        )
        if (total !== ((size - 1) * size) / 2) {
          throw new Error(`unexpected replay total for ${size}: ${total}`)
        }
      },
      BENCH_OPTS,
    )
  })
}
