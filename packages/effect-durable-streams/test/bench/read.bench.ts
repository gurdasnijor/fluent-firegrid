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

const N = 1000 // items pre-populated per stream

const Msg = Schema.Struct({ n: Schema.Number, label: Schema.String })

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()

  // Pre-populate one shared stream — reads are idempotent so we can reuse it
  // across every iteration of every read benchmark. Seed via the reference
  // IdempotentProducer, which packs many appends into one HTTP POST.
  populatedUrl = server.streamUrl("read-bench")
  const ref = new RefDurableStream({ url: populatedUrl })
  await ref.create({ contentType: "application/json" })
  const producer = new IdempotentProducer(ref, "seed-prod", {
    epoch: 0,
    lingerMs: 5,
    maxBatchBytes: 4 * 1024 * 1024,
    maxInFlight: 1,
  })
  for (let i = 0; i < N; i++) {
    producer.append(JSON.stringify({ n: i, label: `item-${i}` }))
  }
  await producer.flush()
}, 30000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

describe(`catch-up read ${N} JSON items`, () => {
  bench("reference @durable-streams/client", async () => {
    const ref = new RefDurableStream({ url: populatedUrl })
    const res = await ref.stream({ offset: "-1", live: false })
    const items = await res.json()
    if (items.length !== N) throw new Error(`expected ${N}, got ${items.length}`)
  })

  bench("effect-durable-streams", async () => {
    const items = await runScoped(
      runtime,
      DurableStream.define({
        endpoint: { url: populatedUrl },
        schema: Msg,
      }).collect,
    )
    if (items.length !== N) throw new Error(`expected ${N}, got ${items.length}`)
  })
})

describe("HEAD metadata round-trip", () => {
  bench("reference @durable-streams/client", async () => {
    const ref = new RefDurableStream({ url: populatedUrl })
    await ref.head()
  })

  bench("effect-durable-streams", async () => {
    await runScoped(
      runtime,
      Effect.flatMap(
        Effect.succeed(
          DurableStream.define({ endpoint: { url: populatedUrl }, schema: Msg }),
        ),
        (s) => s.head,
      ),
    )
  })
})
