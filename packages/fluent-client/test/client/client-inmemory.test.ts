import { Chunk, Effect, Fiber, Stream } from "effect"
import { describe, expect, it } from "vitest"
import * as DurableStreamsClient from "../../src/client/DurableStreamsClient.ts"
import * as InMemoryStreamLog from "@firegrid/fluent-store-inmemory"
import * as InMemoryTransport from "@firegrid/fluent-transport-inmemory"
import { serveConnection } from "@firegrid/fluent-protocol"

const enc = new TextEncoder()
const dec = new TextDecoder()

describe("DurableStreamsClient", () => {
  it("round-trips create, append, read, head, and delete over in-memory protocol transport", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
          const server = yield* InMemoryTransport.makeInMemoryServer()
          const connectionFiber = yield* server.connections.pipe(Stream.take(1), Stream.runCollect, Effect.fork)
          const transport = yield* server.connector.connect("memory://client-test")
          const connections = yield* Fiber.join(connectionFiber)
          const connection = Chunk.toReadonlyArray(connections)[0]

          if (connection === undefined) {
            return undefined
          }

          yield* serveConnection(log, connection)
          const client = yield* DurableStreamsClient.make(transport)
          yield* client.create("client/orders", "text/plain")
          const append = yield* client.append("client/orders", "text/plain", enc.encode("hello"))
          const records = yield* client.read("client/orders")
          const head = yield* client.head("client/orders")
          const deleted = yield* client.delete("client/orders")

          return {
            append,
            head,
            deleted,
            body: records.map((record) => dec.decode(record.bytes)).join(""),
          }
        }),
      ),
    )

    expect(result).toMatchObject({
      append: { closed: false },
      head: { contentType: "text/plain" },
      deleted: "Deleted",
      body: "hello",
    })
  })
})
