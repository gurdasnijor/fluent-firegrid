import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { decodeProtocolEnvelope, encodeProtocolEnvelope } from "../src/codec.ts"

describe("protocol codec", () => {
  it("round-trips command envelopes through opaque transport payloads", async () => {
    const transport = await Effect.runPromise(
      encodeProtocolEnvelope({
        kind: "command",
        id: "c1",
        command: {
          _tag: "CreateStream",
          path: "orders",
          contentType: "application/json",
        },
      }),
    )
    const decoded = await Effect.runPromise(decodeProtocolEnvelope(transport))

    expect(transport.type).toBe("fluent.protocol")
    expect(decoded).toMatchObject({
      kind: "command",
      id: "c1",
      command: { _tag: "CreateStream", path: "orders" },
    })
  })
})
