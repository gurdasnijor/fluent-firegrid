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

// ============================================================================
// Firegrid-shaped payloads. The exact field set isn't load-bearing — what
// matters is the size class (single-line JSON, multi-line embedded text,
// multi-field deeply-nested structure) since the encode cost varies a lot
// across them.
// ============================================================================

const RuntimeOutputEvent = Schema.Struct({
  attempt: Schema.String,
  ts: Schema.Number,
  channel: Schema.Literal("stdout", "stderr"),
  data: Schema.String,
})

const RuntimeIngressRow = Schema.Struct({
  ts: Schema.Number,
  source: Schema.String,
  delivered: Schema.Boolean,
  rowId: Schema.String,
  body: Schema.Struct({
    headers: Schema.Record({ key: Schema.String, value: Schema.String }),
    payload: Schema.String,
  }),
})

const StateChange = Schema.Struct({
  type: Schema.String,
  key: Schema.String,
  value: Schema.Struct({
    txid: Schema.String,
    timestamp: Schema.Number,
    body: Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  }),
  headers: Schema.Struct({
    operation: Schema.Literal("insert", "update", "delete"),
  }),
})

const samples = {
  output: (i: number) =>
    ({
      attempt: `attempt-${i % 4}`,
      ts: 1700000000 + i,
      channel: i % 2 === 0 ? "stdout" : "stderr",
      data: `event line ${i} with some payload content of reasonable size`,
    }) as const,
  ingress: (i: number) => ({
    ts: 1700000000 + i,
    source: "stdin",
    delivered: false,
    rowId: `row-${i.toString().padStart(8, "0")}`,
    body: {
      headers: { "content-type": "application/json", "x-source": "test" },
      payload: JSON.stringify({ idx: i, label: `payload ${i}` }),
    },
  }),
  state: (i: number) => ({
    type: "user",
    key: `user:${i}`,
    value: {
      txid: `tx-${i}`,
      timestamp: 1700000000 + i,
      body: { name: `name${i}`, email: `u${i}@example.com`, score: i },
    },
    headers: { operation: "insert" as const },
  }),
}

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()
}, 30000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

const variants: Array<{
  name: string
  schema: typeof RuntimeOutputEvent | typeof RuntimeIngressRow | typeof StateChange
  gen: (i: number) => unknown
}> = [
  { name: "runtime-output", schema: RuntimeOutputEvent, gen: samples.output },
  { name: "runtime-ingress-row", schema: RuntimeIngressRow, gen: samples.ingress },
  { name: "state-change", schema: StateChange, gen: samples.state },
]

for (const v of variants) {
  describe(`producer ${N} ${v.name} events · linger=0ms`, () => {
    bench("reference @durable-streams/client", async () => {
      const url = server.streamUrl(`fp-ref-${v.name}`)
      const ref = new RefDurableStream({ url })
      await ref.create({ contentType: "application/json" })
      const p = new IdempotentProducer(ref, "ref-prod", {
        epoch: 0,
        lingerMs: 0,
        maxBatchBytes: 4 * 1024 * 1024,
        maxInFlight: 1,
      })
      for (let i = 0; i < N; i++) {
        p.append(JSON.stringify(v.gen(i)))
      }
      await p.flush()
    })

    bench("effect-durable-streams", async () => {
      const url = server.streamUrl(`fp-eff-${v.name}`)
      await runScoped(
        runtime,
        Effect.gen(function* () {
          const s = DurableStream.define({
            endpoint: { url },
            schema: v.schema as unknown as Schema.Schema<unknown, unknown>,
          })
          yield* s.create({ contentType: "application/json" })
          const p = yield* s.producer({
            producerId: "eff-prod",
            epoch: 0,
            lingerMs: 0,
            maxBatchSize: 100_000,
          })
          yield* Stream.fromIterable(
            Array.from({ length: N }, (_, i) => v.gen(i)),
          ).pipe(Stream.run(p))
          yield* p.flush
        }),
      )
    })
  })
}
