import { Effect, Fiber, Stream, pipe } from "effect"
import { describe, expect, it } from "vitest"
import { decodeStreamPath } from "@firegrid/fluent-stream-log"
import * as InMemoryStreamLog from "@firegrid/fluent-stream-log-inmemory"
import { makeClient, makeInProcessChannel, makeServer } from "@firegrid/fluent-durable-streams-spike"

describe("same-process Durable Streams spike", () => {
  it("lets a client drive a same-process server over the in-memory log", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("spike/orders")
        const log = yield* InMemoryStreamLog.make()
        const server = makeServer(log)
        const channel = makeInProcessChannel(server)
        const client = makeClient(channel)
        const stream = client.stream(path, "application/json")

        const create = yield* stream.create({ body: [] })
        const append = yield* stream.append([{ id: 1 }, { id: 2 }])
        const read = yield* stream.readJson("-1")
        const head = yield* stream.head()

        return { append, create, head, read }
      }),
    )

    expect(result.create).toMatchObject({ _tag: "Created" })
    expect(result.append).toMatchObject({ _tag: "Appended" })
    expect(result.read).toMatchObject({
      _tag: "ReadJson",
      items: [{ id: 1 }, { id: 2 }],
      upToDate: true,
      closed: false,
    })
    expect(result.head).toMatchObject({
      _tag: "Head",
      metadata: { contentType: "application/json", closed: false },
    })
  })

  it("preserves producer fencing semantics without a transport layer", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("spike/producer")
        const log = yield* InMemoryStreamLog.make()
        const server = makeServer(log)
        const channel = makeInProcessChannel(server)
        const client = makeClient(channel)
        const stream = client.stream(path, "text/plain")

        yield* stream.create()
        const firstProducer = yield* stream.producer({ producerId: "writer" })
        const first = yield* firstProducer.append("first")
        const duplicate = yield* server.append({
          path,
          contentType: "text/plain",
          body: "first retry",
          producer: { producerId: "writer", epoch: 0, seq: 0 },
        })
        const claim = yield* server.append({
          path,
          contentType: "text/plain",
          body: "claim",
          producer: { producerId: "writer", epoch: 2, seq: 0 },
        })
        const fenced = yield* firstProducer.append("zombie").pipe(Effect.flip)
        const autoClaimProducer = yield* stream.producer({ producerId: "writer", autoClaim: true })
        const afterClaim = yield* autoClaimProducer.append("after claim")
        const afterClaimState = yield* autoClaimProducer.state

        return { afterClaim, afterClaimState, claim, duplicate, fenced, first }
      }),
    )

    expect(result.first).toMatchObject({ _tag: "Appended" })
    expect(result.duplicate).toMatchObject({ _tag: "Duplicate" })
    expect(result.claim).toMatchObject({ _tag: "Appended" })
    expect(result.fenced).toMatchObject({ _tag: "ProducerFenced", currentEpoch: 2 })
    expect(result.afterClaim).toMatchObject({ _tag: "Appended" })
    expect(result.afterClaimState).toEqual({ epoch: 3, seq: 1 })
  })

  it("follows caught-up, records, and closed events in process", async () => {
    const events = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const path = yield* decodeStreamPath("spike/follow")
          const log = yield* InMemoryStreamLog.make()
          const server = makeServer(log)
          const channel = makeInProcessChannel(server)
          const client = makeClient(channel)
          const handle = client.stream(path, "text/plain")

          yield* handle.create()
          const follow = yield* handle.follow("-1")
          const fiber = yield* pipe(follow, Stream.take(3), Stream.runCollect, Effect.forkChild)

          yield* handle.append("hello")
          yield* handle.close()

          return yield* Fiber.join(fiber)
        }),
      ),
    )

    expect(Array.from(events).map((event) => event._tag)).toEqual(["CaughtUp", "Records", "Closed"])
  })
})
