import { Chunk, Duration, Effect, Fiber, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { makeTransportMessage } from "@firegrid/fluent-transport"
import * as InMemoryTransport from "../src/inMemoryTransport.ts"

describe("In-memory transport", () => {
  it("connects clients and delivers client messages to the server side", async () => {
    const received = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* InMemoryTransport.makeInMemoryServer()
          const connectionFiber = yield* server.connections.pipe(Stream.take(1), Stream.runCollect, Effect.fork)
          const client = yield* server.connector.connect("memory://test")
          const connections = yield* Fiber.join(connectionFiber)
          const connection = Chunk.toReadonlyArray(connections)[0]

          if (connection === undefined) {
            return undefined
          }

          const messagesFiber = yield* connection.transport
            .subscribe()
            .pipe(Effect.flatMap((stream) => stream.pipe(Stream.take(1), Stream.runCollect)), Effect.fork)
          yield* Effect.sleep(Duration.millis(10))
          yield* client.publish(makeTransportMessage("m1", "test", "hello"))
          const messages = yield* Fiber.join(messagesFiber)
          return Chunk.toReadonlyArray(messages)[0]
        }),
      ),
    )

    expect(received?.payload).toBe("hello")
  })

  it("broadcasts server messages to connected clients", async () => {
    const received = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* InMemoryTransport.makeInMemoryServer()
          const client = yield* server.connector.connect("memory://test")
          const messagesFiber = yield* client
            .subscribe()
            .pipe(Effect.flatMap((stream) => stream.pipe(Stream.take(1), Stream.runCollect)), Effect.fork)
          yield* Effect.sleep(Duration.millis(10))
          yield* server.broadcast(makeTransportMessage("m2", "test", "broadcast"))
          const messages = yield* Fiber.join(messagesFiber)
          return Chunk.toReadonlyArray(messages)[0]
        }),
      ),
    )

    expect(received?.payload).toBe("broadcast")
  })
})
