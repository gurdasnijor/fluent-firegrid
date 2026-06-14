import { Effect, Fiber, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { S2InMemory } from "../src/index.ts"

const enc = (s: string): Uint8Array => new TextEncoder().encode(s)
const dec = (b: Uint8Array): string => new TextDecoder().decode(b)

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect)

describe("S2 service (s2-lite) — M0", () => {
  it("round-trips append / read / checkTail", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.append("s", [enc("a"), enc("b")])
        const tail = yield* s2.checkTail("s")
        expect(tail).toBe(2n)
        const records = yield* s2.read("s", 0n).pipe(Stream.runCollect)
        expect(records.map((r) => dec(r.data))).toEqual(["a", "b"])
        expect(records.map((r) => r.seqNum)).toEqual([0n, 1n])
      }),
    ))

  it("match_seq_num guards the tail: stale writer gets position-taken (412)", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.append("s", [enc("a")], { matchSeqNum: 0n })
        // two writers computed the same expected seq; first wins, second 412s.
        const result = yield* s2
          .append("s", [enc("b")], { matchSeqNum: 0n })
          .pipe(Effect.result)
        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure" && result.failure._tag === "AppendCondFailed") {
          expect(result.failure.reason).toBe("position-taken")
          expect(result.failure.actualSeqNum).toBe(1n)
        }
      }),
    ))

  it("fencing_token: highest token wins, stale token cannot commit (AC-6 probe)", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.fence("s", "00000000000000000001")
        // worker A holds the lease and commits
        yield* s2.append("s", [enc("a")], { fencingToken: "00000000000000000001", matchSeqNum: 0n })
        // worker B leases with a higher epoch, fencing A out
        yield* s2.fence("s", "00000000000000000002")
        const aLosing = yield* s2
          .append("s", [enc("zombie")], { fencingToken: "00000000000000000001", matchSeqNum: 1n })
          .pipe(Effect.result)
        expect(aLosing._tag).toBe("Failure")
        if (aLosing._tag === "Failure" && aLosing.failure._tag === "AppendCondFailed") {
          expect(aLosing.failure.reason).toBe("fence-mismatch")
          expect(aLosing.failure.currentFencingToken).toBe("00000000000000000002")
        }
        // B commits fine — no double-commit happened
        const bWins = yield* s2.append("s", [enc("b")], {
          fencingToken: "00000000000000000002",
          matchSeqNum: 1n,
        })
        expect(bWins.tail).toBe(2n)
      }),
    ))

  it("combines fencing_token AND match_seq_num on one append (Q1)", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.fence("s", "00000000000000000005")
        const ok = yield* s2.append("s", [enc("a")], {
          fencingToken: "00000000000000000005",
          matchSeqNum: 0n,
        })
        expect(ok.tail).toBe(1n)
      }),
    ))

  it("follow read delivers live appends", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.append("s", [enc("a")])
        const fiber = yield* s2
          .read("s", 0n, { follow: true })
          .pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
        yield* s2.append("s", [enc("b")])
        const arr = yield* Fiber.join(fiber)
        expect(arr.map((r) => dec(r.data))).toEqual(["a", "b"])
      }),
    ))

  it("trim drops history below the cursor but keeps seq numbering", () =>
    run(
      Effect.gen(function* () {
        const s2 = yield* S2InMemory.make
        yield* s2.append("s", [enc("a"), enc("b"), enc("c")])
        yield* s2.trim("s", 2n)
        const records = yield* s2.read("s", 0n).pipe(Stream.runCollect)
        expect(records.map((r) => r.seqNum)).toEqual([2n])
        expect(records.map((r) => dec(r.data))).toEqual(["c"])
        // tail unaffected by trim
        expect(yield* s2.checkTail("s")).toBe(3n)
      }),
    ))
})
