import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { decodeStreamPath, initialOffset } from "@firegrid/fluent-stream-log"
import { Append, Appended, Create, type ResponseOf } from "../src/index.ts"

describe("protocol algebra", () => {
  it("keeps request tags and typed response classes separate from implementations", async () => {
    const path = await Effect.runPromise(decodeStreamPath("protocol/algebra"))
    const create = new Create({ path, contentType: "text/plain" })
    const append = new Append({ path, contentType: "text/plain", bytes: new Uint8Array([1]) })
    const response: ResponseOf<Append> = new Appended({ nextOffset: initialOffset, closed: false })

    expect(create._tag).toBe("Create")
    expect(append._tag).toBe("Append")
    expect(response._tag).toBe("Appended")
  })
})
