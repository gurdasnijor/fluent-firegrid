/**
 * Facade overhead check: the URL-keyed `DurableStreamClient` facade delegates
 * to the SAME `Reader`/`Writer`/`Producer` core as the curried `define(...)`
 * surface, so per-op cost should be within noise. This bench guards against a
 * regression where the facade accidentally adds a layer of cost.
 */
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "effect"
import { afterAll, beforeAll, bench, describe } from "vitest"
import {
  DurableStream,
  DurableStreamClient,
  DurableStreamClientLayerFetch,
} from "../../src/index.ts"
import { startBenchServer } from "./harness.ts"

let server: Awaited<ReturnType<typeof startBenchServer>>
// Provide BOTH the facade service AND a bare HttpClient — `define(...)` ops
// require `HttpClient` in R, the facade captures it internally.
let runtime: ManagedRuntime.ManagedRuntime<DurableStreamClient | HttpClient.HttpClient, never>

const Event = Schema.Struct({ n: Schema.Number, label: Schema.String })
const N_APPEND = 200
const N_PROD = 500

beforeAll(async () => {
  server = await startBenchServer()
  runtime = ManagedRuntime.make(
    Layer.merge(DurableStreamClientLayerFetch, FetchHttpClient.layer),
  )
}, 30000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

describe(`${N_APPEND} typed appends (define vs facade.withSchema)`, () => {
  bench("define", async () => {
    const s = DurableStream.define({ endpoint: { url: server.streamUrl("ap-def") }, schema: Event })
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        for (let i = 0; i < N_APPEND; i++) yield* s.append({ n: i, label: `x${i}` })
      }),
    )
  })

  bench("facade", async () => {
    const url = server.streamUrl("ap-fac")
    await runtime.runPromise(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        const chat = client.withSchema(Event)
        yield* client.create(url, { contentType: "application/json" })
        for (let i = 0; i < N_APPEND; i++) yield* chat.append(url, { n: i, label: `x${i}` })
      }),
    )
  })
})

describe(`producer ${N_PROD} events (define vs facade)`, () => {
  bench("define", async () => {
    const s = DurableStream.define({ endpoint: { url: server.streamUrl("pr-def") }, schema: Event })
    await runtime.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* s.create({ contentType: "application/json" })
          const p = yield* s.producer({ producerId: "d", lingerMs: 5, maxBatchSize: 100000 })
          yield* Stream.fromIterable(
            Array.from({ length: N_PROD }, (_, i) => ({ n: i, label: `x${i}` })),
          ).pipe(Stream.run(p))
          yield* p.flush
        }),
      ),
    )
  })

  bench("facade", async () => {
    const url = server.streamUrl("pr-fac")
    await runtime.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const chat = (yield* DurableStreamClient).withSchema(Event)
          yield* (yield* DurableStreamClient).create(url, { contentType: "application/json" })
          const p = yield* chat.producer(url, { producerId: "f", lingerMs: 5, maxBatchSize: 100000 })
          yield* Stream.fromIterable(
            Array.from({ length: N_PROD }, (_, i) => ({ n: i, label: `x${i}` })),
          ).pipe(Stream.run(p))
          yield* p.flush
        }),
      ),
    )
  })
})
