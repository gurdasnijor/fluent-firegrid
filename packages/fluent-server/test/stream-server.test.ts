import { Chunk, Effect, Fiber, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { decodeStreamPath } from "@firegrid/fluent-store"
import * as InMemoryStreamLog from "@firegrid/fluent-store-inmemory"
import { makeEventBus } from "../src/eventBus.ts"
import { makeStreamServer } from "../src/streamServer.ts"

const enc = new TextEncoder()
const dec = new TextDecoder()

describe("fluent-server", () => {
  it("wraps stream log create, append, read, head, and delete semantics", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
        const server = makeStreamServer(log)
        const path = yield* decodeStreamPath("server/orders")
        yield* server.create(path, "text/plain")
        yield* server.append(path, "text/plain", enc.encode("hello"))
        const records = yield* server.read(path).pipe(Effect.flatMap(Stream.runCollect))
        const head = yield* server.head(path)
        const deleted = yield* server.delete(path)
        return {
          body: Chunk.toReadonlyArray(records).map((record) => dec.decode(record.bytes)).join(""),
          head,
          deleted,
        }
      }),
    )

    expect(result.body).toBe("hello")
    expect(result.head.contentType).toBe("text/plain")
    expect(result.deleted._tag).toBe("Deleted")
  })

  it("publishes tail advancement through the event bus", async () => {
    const tail = await Effect.runPromise(
      Effect.gen(function* () {
        const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
        const server = makeStreamServer(log)
        const bus = makeEventBus(log)
        const path = yield* decodeStreamPath("server/tails")
        yield* server.create(path, "text/plain")
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const stream = yield* bus.tailAdvanced()
            const fiber = yield* stream.pipe(Stream.take(1), Stream.runCollect, Effect.fork)
            yield* server.append(path, "text/plain", enc.encode("x"))
            const tails = yield* Fiber.join(fiber)
            return Chunk.toReadonlyArray(tails)[0]
          }),
        )
      }),
    )

    expect(tail?.path).toBe("server/tails")
  })
})
