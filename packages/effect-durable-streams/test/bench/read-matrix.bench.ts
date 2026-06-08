import {
  DurableStream as RefDurableStream,
  IdempotentProducer,
} from "@durable-streams/client"
import { Schema } from "effect"
import { afterAll, beforeAll, bench, describe } from "vitest"
import { DurableStream } from "../../src/index.ts"
import {
  makeEffectRuntime,
  runScoped,
  startBenchServer,
  type EffectRuntime,
} from "./harness.ts"

let server: Awaited<ReturnType<typeof startBenchServer>>
let runtime: EffectRuntime
const urls: Record<number, string> = {}

const Msg = Schema.Struct({ n: Schema.Number, label: Schema.String })
const sizes = [1_000, 10_000, 100_000]

const seedStream = async (url: string, n: number) => {
  const ref = new RefDurableStream({ url })
  await ref.create({ contentType: "application/json" })
  const p = new IdempotentProducer(ref, "seed", {
    epoch: 0,
    lingerMs: 5,
    maxBatchBytes: 4 * 1024 * 1024,
    maxInFlight: 1,
  })
  for (let i = 0; i < n; i++) {
    p.append(JSON.stringify({ n: i, label: `item-${i}` }))
  }
  await p.flush()
}

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()
  for (const n of sizes) {
    const url = server.streamUrl(`read-${n}`)
    urls[n] = url
    await seedStream(url, n)
  }
}, 120000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

for (const n of sizes) {
  describe(`catch-up read ${n.toLocaleString()} JSON items`, () => {
    bench("reference @durable-streams/client", async () => {
      const ref = new RefDurableStream({ url: urls[n]! })
      const res = await ref.stream({ offset: "-1", live: false })
      const items = await res.json()
      if (items.length !== n) {
        throw new Error(`expected ${n}, got ${items.length}`)
      }
    })

    bench("effect-durable-streams", async () => {
      const items = await runScoped(
        runtime,
        DurableStream.define({
          endpoint: { url: urls[n]! },
          schema: Msg,
        }).collect,
      )
      if (items.length !== n) {
        throw new Error(`expected ${n}, got ${items.length}`)
      }
    })
  })
}
