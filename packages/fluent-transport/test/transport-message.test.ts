import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { makeTransportMessage, parseTransportMessage } from "../src/index.ts"

describe("TransportMessage", () => {
  it("keeps payload opaque to the transport layer", async () => {
    const message = makeTransportMessage("m1", "durable.command", "{\"x\":1}", { trace: "t1" })

    expect(message.payload).toBe("{\"x\":1}")
    expect(message.metadata.trace).toBe("t1")
  })

  it("validates only the transport envelope", async () => {
    const decoded = await Effect.runPromise(
      parseTransportMessage({
        id: "m2",
        type: "anything",
        payload: "not-json-but-still-opaque",
      }),
    )

    expect(decoded.payload).toBe("not-json-but-still-opaque")
    expect(decoded.metadata).toEqual({})
  })
})
