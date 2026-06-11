import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { decodeOffset, initialOffset, makeOffset } from "../src/domainTypes.ts"

describe("Offset", () => {
  it("generates lexicographically sortable offset tokens", () => {
    expect([makeOffset(10), makeOffset(2)].sort()).toEqual([makeOffset(2), makeOffset(10)])
  })

  it("does not decode reserved or HTTP-ambiguous offset tokens", async () => {
    const results = await Promise.all([
      Effect.runPromiseExit(decodeOffset("-1")),
      Effect.runPromiseExit(decodeOffset("now")),
      Effect.runPromiseExit(decodeOffset("a/b")),
      Effect.runPromiseExit(decodeOffset("a,b")),
    ])

    expect(results.every((exit) => exit._tag === "Failure")).toBe(true)
  })

  it("starts at the generated zero offset", () => {
    expect(initialOffset).toBe("00000000000000000000")
  })
})
