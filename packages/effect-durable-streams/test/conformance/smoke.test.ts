import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, type Scope, Schema, Stream } from "effect"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { DurableStream } from "../../src/index.ts"
import { startTestServer, type TestServerHandle } from "./test-server.ts"

let server: TestServerHandle

beforeAll(async () => {
  server = await startTestServer()
})

afterAll(async () => {
  await server.stop()
})

const ChatMessage = Schema.Struct({
  user: Schema.String,
  text: Schema.String,
})

type Reqs = FetchHttpClient.Fetch | HttpClient.HttpClient | Scope.Scope

const runtime = <A, E>(eff: Effect.Effect<A, E, Reqs>) =>
  Effect.runPromise(
    Effect.scoped(eff.pipe(Effect.provide(FetchHttpClient.layer))) as unknown as Effect.Effect<A, E, never>,
  )

describe("Phase 1 smoke", () => {
  it("creates a stream, appends, and collects", async () => {
    const url = server.streamUrl("smoke")
    const stream = DurableStream.define({
      endpoint: { url },
      schema: ChatMessage,
    })

    await runtime(
      Effect.gen(function* () {
        yield* stream.create({ contentType: "application/json" })
        yield* stream.append({ user: "alice", text: "hello" })
        yield* stream.append({ user: "bob", text: "world" })
        const items = yield* stream.collect
        expect(items.length).toBe(2)
        expect(items[0]).toEqual({ user: "alice", text: "hello" })
        expect(items[1]).toEqual({ user: "bob", text: "world" })
      }),
    )
  })

  it("snapshotThenFollow returns historic items as snapshot", async () => {
    const url = server.streamUrl("stf")
    const s = DurableStream.define({ endpoint: { url }, schema: ChatMessage })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ user: "a", text: "1" })
        yield* s.append({ user: "b", text: "2" })
        const result = yield* s.snapshotThenFollow
        expect(result.snapshot.length).toBe(2)
        expect(result.snapshot[0]).toEqual({ user: "a", text: "1" })
      }),
    )
  })

  it("idempotent producer via append() calls", async () => {
    const url = server.streamUrl("idem-append")
    const s = DurableStream.define({ endpoint: { url }, schema: ChatMessage })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        const producer = yield* s.producer({
          producerId: "p1",
          lingerMs: 20,
          maxBatchSize: 10,
        })
        yield* producer.append({ user: "u1", text: "a" })
        yield* producer.append({ user: "u2", text: "b" })
        yield* producer.append({ user: "u3", text: "c" })
        yield* producer.flush
        const collected = yield* s.collect
        expect(collected.length).toBe(3)
        expect(collected.map((m) => m.user)).toEqual(["u1", "u2", "u3"])
      }),
    )
  }, 10000)

  it("idempotent producer via Stream.run", async () => {
    const url = server.streamUrl("idem-stream")
    const s = DurableStream.define({ endpoint: { url }, schema: ChatMessage })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        const producer = yield* s.producer({
          producerId: "p2",
          lingerMs: 20,
          maxBatchSize: 10,
        })
        const events = [
          { user: "u1", text: "a" },
          { user: "u2", text: "b" },
          { user: "u3", text: "c" },
        ]
        yield* Stream.fromIterable(events).pipe(Stream.run(producer))
        yield* producer.flush

        const collected = yield* s.collect
        expect(collected.length).toBe(3)
        expect(collected.map((m) => m.user)).toEqual(["u1", "u2", "u3"])
      }),
    )
  }, 10000)

  it("close transitions stream to closed; subsequent appends fail", async () => {
    const url = server.streamUrl("close")
    const s = DurableStream.define({ endpoint: { url }, schema: ChatMessage })

    await runtime(
      Effect.gen(function* () {
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ user: "x", text: "final" })
        const closeResult = yield* s.close()
        expect(typeof closeResult.finalOffset).toBe("string")

        const head = yield* s.head
        expect(head.streamClosed).toBe(true)

        const failResult = yield* Effect.exit(s.append({ user: "y", text: "after-close" }))
        expect(failResult._tag).toBe("Failure")
      }),
    )
  })

  it("returns NotFound for a stream that doesn't exist", async () => {
    const url = `${server.url}/v1/stream/missing-${crypto.randomUUID()}`
    const s = DurableStream.define({
      endpoint: { url },
      schema: ChatMessage,
    })
    await runtime(
      Effect.gen(function* () {
        const result = yield* Effect.exit(s.head)
        expect(result._tag).toBe("Failure")
      }),
    )
  })
})
