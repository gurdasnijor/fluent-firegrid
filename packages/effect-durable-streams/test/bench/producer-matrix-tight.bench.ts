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

// ============================================================================
// Tight producer-matrix bench
// ============================================================================
//
// The original `producer-matrix.bench.ts` creates a fresh stream + a fresh
// producer in EVERY iteration. For high-batch / low-event-count cells the
// setup cost (PUT to create the stream, plus producer-scope acquisition)
// dominates and its variance shows up as 25-50% RMEs.
//
// This file removes setup variance by:
//   1. Pre-creating two streams per cell in `beforeAll` (one per library).
//      Producer-id is unique per iteration so there's no collision.
//   2. Bumping `N` from 500 to 2000 so producer-scope acquisition is at
//      most ~5% of each iter rather than the dominant cost.
//   3. Allocating a longer `time` budget per cell so tinybench can collect
//      enough samples for tight confidence intervals.
//   4. Explicit `warmupIterations` so we don't measure JIT cold starts.

const N = 2000

const Event = Schema.Struct({ n: Schema.Number })

let server: Awaited<ReturnType<typeof startBenchServer>>
let runtime: EffectRuntime

interface Cell {
  readonly id: string
  readonly maxBatchSize: number
  readonly lingerMs: number
  readonly refMaxBatchBytes: number
  refUrl: string
  effUrl: string
}

const cells: Array<Cell> = [
  { id: "b1-l0", maxBatchSize: 1, lingerMs: 0, refMaxBatchBytes: 1, refUrl: "", effUrl: "" },
  { id: "b1-l5", maxBatchSize: 1, lingerMs: 5, refMaxBatchBytes: 1, refUrl: "", effUrl: "" },
  { id: "b100-l0", maxBatchSize: 100, lingerMs: 0, refMaxBatchBytes: 800, refUrl: "", effUrl: "" },
  { id: "b100-l5", maxBatchSize: 100, lingerMs: 5, refMaxBatchBytes: 800, refUrl: "", effUrl: "" },
  { id: "b100-l10", maxBatchSize: 100, lingerMs: 10, refMaxBatchBytes: 800, refUrl: "", effUrl: "" },
  { id: "b1k-l0", maxBatchSize: 1000, lingerMs: 0, refMaxBatchBytes: 8 * 1024, refUrl: "", effUrl: "" },
  { id: "b1k-l5", maxBatchSize: 1000, lingerMs: 5, refMaxBatchBytes: 8 * 1024, refUrl: "", effUrl: "" },
  { id: "b100k-l0", maxBatchSize: 100000, lingerMs: 0, refMaxBatchBytes: 4 * 1024 * 1024, refUrl: "", effUrl: "" },
  { id: "b100k-l5", maxBatchSize: 100000, lingerMs: 5, refMaxBatchBytes: 4 * 1024 * 1024, refUrl: "", effUrl: "" },
]

let iterCounter = 0

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()
  for (const cell of cells) {
    cell.refUrl = server.streamUrl(`tight-ref-${cell.id}`)
    cell.effUrl = server.streamUrl(`tight-eff-${cell.id}`)
    await new RefDurableStream({ url: cell.refUrl }).create({
      contentType: "application/json",
    })
    await runScoped(
      runtime,
      DurableStream.define({ endpoint: { url: cell.effUrl }, schema: Event }).create({
        contentType: "application/json",
      }),
    )
  }
}, 60000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

const benchOptions = {
  time: 2500,
  iterations: 30,
  warmupIterations: 3,
  warmupTime: 500,
}

for (const cell of cells) {
  describe(`tight · N=${N} · batch=${cell.maxBatchSize} linger=${cell.lingerMs}ms`, () => {
    bench(
      "reference @durable-streams/client",
      async () => {
        const ref = new RefDurableStream({ url: cell.refUrl })
        const producer = new IdempotentProducer(ref, `ref-prod-${iterCounter++}`, {
          epoch: 0,
          lingerMs: cell.lingerMs,
          maxBatchBytes: cell.refMaxBatchBytes,
          maxInFlight: 1,
        })
        for (let i = 0; i < N; i++) {
          producer.append(JSON.stringify({ n: i }))
        }
        await producer.flush()
      },
      benchOptions,
    )

    bench(
      "effect-durable-streams",
      async () => {
        await runScoped(
          runtime,
          Effect.gen(function* () {
            const s = DurableStream.define({ endpoint: { url: cell.effUrl }, schema: Event })
            const p = yield* s.producer({
              producerId: `eff-prod-${iterCounter++}`,
              epoch: 0,
              lingerMs: cell.lingerMs,
              maxBatchSize: cell.maxBatchSize,
            })
            yield* Stream.fromIterable(
              Array.from({ length: N }, (_, i) => ({ n: i })),
            ).pipe(Stream.run(p))
            yield* p.flush
          }),
        )
      },
      benchOptions,
    )
  })
}
