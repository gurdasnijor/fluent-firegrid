import {
  DurableStream as RefDurableStream,
  IdempotentProducer,
} from "@durable-streams/client"
import { Effect, Schema } from "effect"
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
let populatedUrl: string

const N = 500
const Msg = Schema.Struct({ n: Schema.Number })

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()
  populatedUrl = server.streamUrl("snap-bench")
  const ref = new RefDurableStream({ url: populatedUrl })
  await ref.create({ contentType: "application/json" })
  const producer = new IdempotentProducer(ref, "seed-snap", {
    epoch: 0,
    lingerMs: 5,
    maxBatchBytes: 4 * 1024 * 1024,
    maxInFlight: 1,
  })
  for (let i = 0; i < N; i++) {
    producer.append(JSON.stringify({ n: i }))
  }
  await producer.flush()
}, 30000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

describe(`snapshot ${N} items + open live`, () => {
  bench("reference @durable-streams/client", async () => {
    // Reference equivalent: HEAD then catch-up read up to that offset.
    const ref = new RefDurableStream({ url: populatedUrl })
    const head = await ref.head()
    const res = await ref.stream({ offset: "-1", live: false })
    const items = await res.json()
    if (items.length !== N) {
      throw new Error(`expected ${N}, got ${items.length}`)
    }
    void head
  })

  bench("effect-durable-streams", async () => {
    const result = await runScoped(
      runtime,
      Effect.gen(function* () {
        const s = DurableStream.define({
          endpoint: { url: populatedUrl },
          schema: Msg,
        })
        const r = yield* s.snapshotThenFollow
        return r.snapshot
      }),
    )
    if (result.length !== N) {
      throw new Error(`expected ${N}, got ${result.length}`)
    }
  })
})
