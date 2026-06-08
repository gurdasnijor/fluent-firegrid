import { Effect, type Scope, Schema, Stream } from "effect"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import {
  DurableStreamClient,
  DurableStreamClientLayerFetch,
} from "../../src/index.ts"
import { startTestServer, type TestServerHandle } from "./test-server.ts"

let server: TestServerHandle

beforeAll(async () => {
  server = await startTestServer()
})

afterAll(async () => {
  await server.stop()
})

const ChatMessage = Schema.Struct({ user: Schema.String, text: Schema.String })

// Batteries-included: provide ONE layer; no FetchHttpClient wiring at call sites.
const run = <A, E>(eff: Effect.Effect<A, E, DurableStreamClient | Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(eff).pipe(Effect.provide(DurableStreamClientLayerFetch)))

describe("DurableStreamClient facade", () => {
  it("raw append + raw stream session (json accumulate)", async () => {
    const url = server.streamUrl("facade-raw")
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        yield* client.create(url, { contentType: "application/json" })
        yield* client.append(url, JSON.stringify({ user: "alice", text: "hi" }))
        yield* client.append(url, JSON.stringify({ user: "bob", text: "yo" }))
        const items = yield* client.stream(url).json
        expect(items.length).toBe(2)
        expect(items[0]).toEqual({ user: "alice", text: "hi" })
      }),
    )
  })

  it("raw append accepts Uint8Array", async () => {
    const url = server.streamUrl("facade-bytes")
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        yield* client.create(url, { contentType: "application/json" })
        yield* client.append(url, new TextEncoder().encode(JSON.stringify({ user: "u", text: "b" })))
        const items = yield* client.stream(url).json
        expect(items.length).toBe(1)
      }),
    )
  })

  it("jsonBatches carries protocol metadata (offset, upToDate)", async () => {
    const url = server.streamUrl("facade-batches")
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        yield* client.create(url, { contentType: "application/json" })
        yield* client.append(url, JSON.stringify({ user: "a", text: "1" }))
        yield* client.append(url, JSON.stringify({ user: "b", text: "2" }))
        const batches = yield* client
          .stream(url, { live: false })
          .jsonBatches.pipe(Stream.runCollect, Effect.map((c) => Array.from(c)))
        const total = batches.reduce((n, b) => n + b.items.length, 0)
        expect(total).toBe(2)
        expect(batches.at(-1)?.upToDate).toBe(true)
        expect(typeof batches.at(-1)?.offset).toBe("string")
      }),
    )
  })

  it("withSchema gives the typed Stream<A>/Sink surface", async () => {
    const url = server.streamUrl("facade-typed")
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        const chat = client.withSchema(ChatMessage)
        yield* client.create(url, { contentType: "application/json" })
        yield* chat.append(url, { user: "alice", text: "typed" })
        const items = yield* chat.collect(url)
        expect(items).toEqual([{ user: "alice", text: "typed" }])
        // typed read returns decoded A, not unknown
        const first = yield* chat
          .read(url, { live: false })
          .pipe(Stream.take(1), Stream.runCollect, Effect.map((c) => Array.from(c)[0]))
        expect(first).toEqual({ user: "alice", text: "typed" })
      }),
    )
  })

  it("withSchema producer round-trips via Stream.run + accessors", async () => {
    const url = server.streamUrl("facade-producer")
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        const chat = client.withSchema(ChatMessage)
        yield* client.create(url, { contentType: "application/json" })
        const p = yield* chat.producer(url, { producerId: "fp", lingerMs: 20, maxBatchSize: 10 })

        expect(yield* p.epoch).toBe(0)
        expect(yield* p.nextSeq).toBe(0)

        yield* Stream.fromIterable([
          { user: "u1", text: "a" },
          { user: "u2", text: "b" },
          { user: "u3", text: "c" },
        ]).pipe(Stream.run(p))
        yield* p.flush
        expect(yield* p.pendingCount).toBe(0)

        const collected = yield* chat.collect(url)
        expect(collected.map((m) => m.user)).toEqual(["u1", "u2", "u3"])

        // close: flush + stop accepting; append-after-close fails typed.
        yield* p.close
        const after = yield* Effect.exit(p.append({ user: "u4", text: "d" }))
        expect(after._tag).toBe("Failure")
      }),
    )
  }, 10000)

  it("propagates typed NotFound from the facade", async () => {
    const url = `${server.url}/v1/stream/facade-missing-${crypto.randomUUID()}`
    await run(
      Effect.gen(function* () {
        const client = yield* DurableStreamClient
        const result = yield* Effect.exit(client.head(url))
        expect(result._tag).toBe("Failure")
      }),
    )
  })
})
