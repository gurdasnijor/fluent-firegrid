import {
  DurableStream as RefDurableStream,
  IdempotentProducer,
} from "@durable-streams/client"
import { Effect, Schema, Stream } from "effect"
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

const N = 500 // events per iteration — kept modest so each bench iteration is sub-second
const LINGER_MS = 5
// Match the reference client's behavior (no count cap; bytes-bounded only) so
// both producers send the same number of HTTP requests for the same workload.
const MAX_BATCH = 100000

const Event = Schema.Struct({ n: Schema.Number })

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()
}, 30000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

/**
 * Each iteration creates a fresh stream + fresh producer, writes N events
 * through batching with linger, and waits for flush. Setup is included in
 * the measurement — both clients pay the same setup cost, so the comparison
 * is still apples-to-apples.
 */
describe(`producer ${N} events with linger=${LINGER_MS}ms maxBatch=${MAX_BATCH}`, () => {
  bench("reference @durable-streams/client", async () => {
    const url = server.streamUrl("prod-ref")
    const ref = new RefDurableStream({ url })
    await ref.create({ contentType: "application/json" })
    const producer = new IdempotentProducer(ref, "ref-prod", {
      epoch: 0,
      lingerMs: LINGER_MS,
      maxBatchBytes: 1024 * 1024,
      maxInFlight: 1,
    })
    for (let i = 0; i < N; i++) {
      producer.append(JSON.stringify({ n: i }))
    }
    await producer.flush()
  })

  bench("effect-durable-streams", async () => {
    const url = server.streamUrl("prod-eff")
    await runScoped(
      runtime,
      Effect.gen(function* () {
        const s = DurableStream.define({ endpoint: { url }, schema: Event })
        yield* s.create({ contentType: "application/json" })
        const p = yield* s.producer({
          producerId: "eff-prod",
          epoch: 0,
          lingerMs: LINGER_MS,
          maxBatchSize: MAX_BATCH,
        })
        yield* Stream.fromIterable(
          Array.from({ length: N }, (_, i) => ({ n: i })),
        ).pipe(Stream.run(p))
        yield* p.flush
      }),
    )
  })
})
