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

const N = 500

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
 * Producer throughput across the (maxBatchSize, lingerMs) matrix called out
 * in the PR #148 review. Each iteration creates a fresh stream + producer,
 * pumps N events, and flushes.
 *
 * Apples-to-apples note: the reference `IdempotentProducer` has NO count
 * cap — only `maxBatchBytes` and `lingerMs`. To make each row a fair
 * comparison we map our `maxBatchSize` to the reference's `maxBatchBytes`
 * so both producers send the same number of HTTP requests for the same
 * workload:
 *
 *   - `maxBatchSize: 1`            → ref `maxBatchBytes: 1`   (one event ≫ 1 byte → one batch each)
 *   - `maxBatchSize: 100`          → ref `maxBatchBytes` sized so ~100 events fit
 *   - `maxBatchSize: 1000`/100000  → ref `maxBatchBytes: 4 MiB` (no effective cap)
 *
 * Without this mapping the `batch=1` rows compared "we send 500 HTTP
 * requests" vs "reference sends 1 HTTP request", which inflated the gap
 * by ~300x and obscured the real per-event overhead.
 */
const grid: ReadonlyArray<{
  readonly maxBatchSize: number
  readonly lingerMs: number
  readonly refMaxBatchBytes: number
}> = [
  // Each event encodes to roughly 8 bytes (`{"n":N}`) — a 1-byte cap forces
  // a single event per batch on the reference, matching our maxBatchSize=1.
  { maxBatchSize: 1, lingerMs: 0, refMaxBatchBytes: 1 },
  { maxBatchSize: 1, lingerMs: 5, refMaxBatchBytes: 1 },
  // ~800-byte cap → ~100 events per batch (matching maxBatchSize=100).
  { maxBatchSize: 100, lingerMs: 0, refMaxBatchBytes: 800 },
  { maxBatchSize: 100, lingerMs: 5, refMaxBatchBytes: 800 },
  { maxBatchSize: 100, lingerMs: 10, refMaxBatchBytes: 800 },
  // ~8 KB cap → ~1000 events per batch.
  { maxBatchSize: 1000, lingerMs: 0, refMaxBatchBytes: 8 * 1024 },
  { maxBatchSize: 1000, lingerMs: 5, refMaxBatchBytes: 8 * 1024 },
  // 100k cap is effectively "no cap" for N=500 — keep ref at its default
  // 1 MiB so both producers send a single batch.
  { maxBatchSize: 100000, lingerMs: 0, refMaxBatchBytes: 4 * 1024 * 1024 },
  { maxBatchSize: 100000, lingerMs: 5, refMaxBatchBytes: 4 * 1024 * 1024 },
]

for (const params of grid) {
  describe(`producer ${N} events · batch=${params.maxBatchSize} linger=${params.lingerMs}ms`, () => {
    bench("reference @durable-streams/client", async () => {
      const url = server.streamUrl("prod-ref-grid")
      const ref = new RefDurableStream({ url })
      await ref.create({ contentType: "application/json" })
      const producer = new IdempotentProducer(ref, "ref-prod", {
        epoch: 0,
        lingerMs: params.lingerMs,
        maxBatchBytes: params.refMaxBatchBytes,
        maxInFlight: 1,
      })
      for (let i = 0; i < N; i++) {
        producer.append(JSON.stringify({ n: i }))
      }
      await producer.flush()
    })

    bench("effect-durable-streams", async () => {
      const url = server.streamUrl("prod-eff-grid")
      await runScoped(
        runtime,
        Effect.gen(function* () {
          const s = DurableStream.define({ endpoint: { url }, schema: Event })
          yield* s.create({ contentType: "application/json" })
          const p = yield* s.producer({
            producerId: "eff-prod",
            epoch: 0,
            lingerMs: params.lingerMs,
            maxBatchSize: params.maxBatchSize,
          })
          yield* Stream.fromIterable(
            Array.from({ length: N }, (_, i) => ({ n: i })),
          ).pipe(Stream.run(p))
          yield* p.flush
        }),
      )
    })
  })
}
