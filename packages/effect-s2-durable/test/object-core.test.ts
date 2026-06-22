import { describe, expect, it } from "@effect/vitest"
import { Effect, Option } from "effect"
import {
  callStatus,
  decodeObjectCallId,
  encodeObjectCallId,
  journalValue,
  type LogEntry,
  OBJECT_ID_PREFIX,
  pathSegment,
  replay,
  signalValue,
  unPathSegment,
} from "../src/object/events.ts"
import { workflow, workflowRunId } from "../src/service.ts"

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

  it("unPathSegment round-trips pathSegment for keys containing / and %", () => {
    for (const raw of ["", "plain", "a/b", "a%b", "a%2Fb", "%2F%25", "a/b/c%d", "100%/x"]) {
      expect(unPathSegment(pathSegment(raw))).toBe(raw)
    }
  })
})

describe("journal identity is kind-aware (run vs read cannot collide)", () => {
  // a state.get read journal at step "0", and a run step deliberately NAMED "read/0":
  // under a {callId, step}-only key these would collide and be misinterpreted.
  const readFact: LogEntry = {
    seqNum: 1,
    event: { _tag: "Journaled", callId: "c1", kind: "read", step: "0", value: { present: false, value: null } },
  }
  const runFact: LogEntry = {
    seqNum: 2,
    event: { _tag: "Journaled", callId: "c1", kind: "run", step: "read/0", value: { success: true, value: 99 } },
  }
  const snap = replay([readFact, runFact])

  it("each kind+step resolves to its own fact", () => {
    expect(journalValue(snap, "c1", "read", "0")).toEqual(Option.some({ present: false, value: null }))
    expect(journalValue(snap, "c1", "run", "read/0")).toEqual(Option.some({ success: true, value: 99 }))
  })

  it("a cross-kind lookup misses (no collision)", () => {
    expect(Option.isNone(journalValue(snap, "c1", "run", "0"))).toBe(true)
    expect(Option.isNone(journalValue(snap, "c1", "read", "read/0"))).toBe(true)
  })
})

describe("signals projection (durable ingress, first-write-wins)", () => {
  const first: LogEntry = { seqNum: 1, event: { _tag: "SignalResolved", callId: "c1", name: "approved", value: true } }
  const second: LogEntry = { seqNum: 2, event: { _tag: "SignalResolved", callId: "c1", name: "approved", value: false } }

  it("resolves to the FIRST value (a double-resolve is a no-op)", () => {
    expect(signalValue(replay([first, second]), "c1", "approved")).toEqual(Option.some(true))
  })

  it("an unresolved signal / different name / different call is None", () => {
    expect(Option.isNone(signalValue(replay([first]), "c1", "other"))).toBe(true)
    expect(Option.isNone(signalValue(replay([first]), "c2", "approved"))).toBe(true)
    expect(Option.isNone(signalValue(replay([]), "c1", "approved"))).toBe(true)
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

// The workflow run-once mechanic hinges on `workflowRunId` being DETERMINISTIC: every
// start of the same (workflow, id) must resolve to ONE owner call id so admission dedups
// (at most one run). These pin that contract without S2 (the run-once + already-started
// behaviour itself is proven over a real backend in effect-s2-durable-workflow).
describe("workflow run-id is deterministic (run-once anchor)", () => {
  const wf = workflow({ name: "wf-test", *run(n: number) { return n } })

  it.effect("the same (workflow, id) always derives the SAME run call id", () =>
    Effect.gen(function*() {
      const a = yield* workflowRunId(wf, "order-1")
      const b = yield* workflowRunId(wf, "order-1")
      expect(a).toBe(b)
    }))

  it.effect("distinct ids derive distinct run call ids", () =>
    Effect.gen(function*() {
      const a = yield* workflowRunId(wf, "order-1")
      const b = yield* workflowRunId(wf, "order-2")
      expect(a).not.toBe(b)
    }))

  it.effect("the run call id decodes to the workflow's owner key + reserved `run` method", () =>
    Effect.gen(function*() {
      const id = yield* workflowRunId(wf, "order-1")
      expect(yield* decodeObjectCallId(id)).toEqual({ object: "wf-test", key: "order-1", method: "run", nonce: "order-1" })
    }))

  it("rejects a shared handler named `run` (reserved for the entrypoint)", () => {
    // the `as any` deliberately bypasses the type-level guard to prove the runtime guard fires.
    expect(() =>
      workflow({ name: "wf-clash", *run(n: number) { return n }, handlers: { *run() { return 0 } } as any }),
    ).toThrow(/reserved for the run-once entrypoint/)
  })
})
