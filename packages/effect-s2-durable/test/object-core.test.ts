import { describe, expect, it } from "@effect/vitest"
import { Effect, Option } from "effect"
import {
  callStatus,
  decodeObjectCallId,
  encodeObjectCallId,
  type LogEntry,
  OBJECT_ID_PREFIX,
  pathSegment,
  replay,
} from "../src/actor/core.ts"

// Pure (no S2) invariants for the object call-id routing + projection. S2-backed
// behaviour is proven in Firelab (effect-s2-durable-object-call); these guard the
// routing/collision fixes from PR #25 review at the unit level.

describe("object call-id namespace routing", () => {
  it.effect("encode is namespaced and round-trips through decode", () =>
    Effect.gen(function*() {
      const parts = { object: "counter", key: "acct", method: "add", nonce: "n1" }
      const id = yield* encodeObjectCallId(parts)
      expect(id.startsWith(OBJECT_ID_PREFIX)).toBe(true)
      expect(yield* decodeObjectCallId(id)).toEqual(parts)
    }))

  it.effect("a non-prefixed string of the right JSON shape is NOT decoded as an object id", () =>
    Effect.gen(function*() {
      // exactly the shape an object id encodes, but without the reserved prefix —
      // i.e. a service idempotencyKey that happens to be this JSON must route to
      // the service path, not an owner stream.
      const lookalike = JSON.stringify({ object: "x", key: "k", method: "m", nonce: "n" })
      const result = yield* decodeObjectCallId(lookalike).pipe(Effect.option)
      expect(Option.isNone(result)).toBe(true) // decode fails → service routing
    }))

  it.effect("a plain service id (uuid-like) is not an object id", () =>
    Effect.gen(function*() {
      const result = yield* decodeObjectCallId("3f2504e0-4f89-41d3-9a0c-0305e82c3301").pipe(Effect.option)
      expect(Option.isNone(result)).toBe(true)
    }))
})

describe("owner path segments are collision-safe", () => {
  it("distinct (object, key) pairs never collide on one path", () => {
    // (a/b, c) vs (a, b/c): both join to "a/b/c" without escaping; escaped they differ.
    expect(`${pathSegment("a/b")}/${pathSegment("c")}`).not.toBe(`${pathSegment("a")}/${pathSegment("b/c")}`)
    // and the `%` escape itself stays injective
    expect(pathSegment("a%2Fb")).not.toBe(pathSegment("a/b"))
  })
})

describe("call status distinguishes Unknown from Pending", () => {
  const accepted: LogEntry = { seqNum: 1, event: { _tag: "Accepted", callId: "c1", method: "m", input: 1 } }
  const completed: LogEntry = {
    seqNum: 2,
    event: { _tag: "Completed", callId: "c1", exit: { _tag: "Success", value: 42 } },
  }

  it("a never-admitted callId is Unknown (so attach fails, not loops)", () => {
    expect(callStatus(replay([accepted]), "ghost")._tag).toBe("Unknown")
  })

  it("an admitted-but-unsettled callId is Pending", () => {
    expect(callStatus(replay([accepted]), "c1")._tag).toBe("Pending")
  })

  it("a settled callId reports its Exit", () => {
    expect(callStatus(replay([accepted, completed]), "c1")).toEqual({ _tag: "Success", value: 42 })
  })
})
