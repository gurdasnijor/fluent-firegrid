import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import * as InMemoryStreamLog from "@firegrid/fluent-store-inmemory"
import { handleCommand } from "../src/serverProtocol.ts"

const enc = new TextEncoder()

describe("server protocol", () => {
  it("runs create, append, and read commands against a DurableStreamLog", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
        yield* handleCommand(log, {
          _tag: "CreateStream",
          path: "protocol/orders",
          contentType: "text/plain",
        })
        yield* handleCommand(log, {
          _tag: "AppendToStream",
          path: "protocol/orders",
          contentType: "text/plain",
          bytes: Array.from(enc.encode("hello")),
        })
        return yield* handleCommand(log, {
          _tag: "ReadStream",
          path: "protocol/orders",
          offset: "-1",
        })
      }),
    )

    expect(result).toMatchObject({
      _tag: "ReadResult",
      records: [{ path: "protocol/orders", bytes: Array.from(enc.encode("hello")) }],
    })
  })
})
