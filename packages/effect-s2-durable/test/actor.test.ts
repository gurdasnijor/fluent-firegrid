import { describe, expect, it } from "@effect/vitest"
import { Option } from "effect"
import { Actor } from "effect-s2-durable"

// The pure transition is testable as `snapshot + event -> snapshot + actions`
// with no S2, timers, fibers, or real handlers (PLANNING.8).

const accepted = (seqNum: number, callId: string, input: unknown = null): Actor.LogEntry => ({
  seqNum,
  event: { _tag: "Accepted", callId, method: "m", input },
})

const completed = (
  seqNum: number,
  callId: string,
  exit: Actor.ActorExit = { _tag: "Success", value: null },
): Actor.LogEntry => ({ seqNum, event: { _tag: "Completed", callId, exit } })

const fold = (entries: ReadonlyArray<Actor.LogEntry>): Actor.ActorSnapshot => Actor.replay(entries)

describe("actor transition (pure)", () => {
  it("admits an exclusive call: idle key starts the head (ADMISSION.1-1 / EXECUTION.1)", () => {
    const [snap, actions] = Actor.transition(Actor.empty, accepted(0, "c"))
    expect(snap.pending).toEqual(["c"])
    expect(Option.getOrNull(snap.active)).toBe("c")
    expect(snap.cursor).toBe(0)
    expect(actions).toEqual([{ _tag: "StartCall", callId: "c" }])
  })

  it("is single-writer: a second admission enqueues, only one StartCall (HANDLERS.2)", () => {
    const s1 = Actor.transition(Actor.empty, accepted(0, "a"))[0]
    const [s2, actions] = Actor.transition(s1, accepted(1, "b"))
    expect(s2.pending).toEqual(["a", "b"]) // seq_num order = append order (ADMISSION.3)
    expect(Option.getOrNull(s2.active)).toBe("a")
    expect(actions).toEqual([]) // busy → no second start
  })

  it("completion derives the advance: next head starts, no dequeue write (COMPLETION.2/3)", () => {
    const s = fold([accepted(0, "a"), accepted(1, "b")])
    const [done, actions] = Actor.transition(s, completed(2, "a"))
    expect(done.results.has("a")).toBe(true) // done iff Completed exists
    expect(done.pending).toEqual(["b"]) // "advance" = re-derive pending
    expect(Option.getOrNull(done.active)).toBe("b")
    expect(actions).toEqual([{ _tag: "StartCall", callId: "b" }])
  })

  it("a drained queue emits a Checkpoint at the safe boundary (CHECKPOINTING.2)", () => {
    const s = fold([accepted(0, "a")])
    const [done, actions] = Actor.transition(s, completed(1, "a"))
    expect(done.pending).toEqual([])
    expect(Option.isNone(done.active)).toBe(true)
    expect(actions).toEqual([{ _tag: "Checkpoint" }])
  })

  it("window-2 is structurally impossible: a re-Accepted completed callId never re-runs (COMPLETION.3)", () => {
    const settled = fold([accepted(0, "a"), completed(1, "a")])
    const [after, actions] = Actor.transition(settled, accepted(2, "a")) // a stray duplicate admission
    expect(after.results.has("a")).toBe(true)
    expect(after.pending).toEqual([]) // not re-queued
    expect(actions).toEqual([]) // not re-run
  })

  it("replay folds history WITHOUT executing actions (RECOVERY.3)", () => {
    // replay discards every action; the only observable is the final snapshot.
    const snap = fold([accepted(0, "a"), completed(1, "a"), accepted(2, "b")])
    expect(snap.results.has("a")).toBe(true)
    expect(snap.pending).toEqual(["b"])
    expect(Option.getOrNull(snap.active)).toBe("b") // b became the head when a completed
  })

  it("recovers the durable head regardless of fiber residency (RECOVERY.4)", () => {
    // after a cold replay, `active` is Some(head) with no resident fiber; recovery
    // must restart it. recoveredHeadActions keys off the durable head, not active===None.
    const snap = fold([accepted(0, "a"), accepted(1, "b")]) // a is active head, mid-flight at crash
    expect(Option.getOrNull(snap.active)).toBe("a")
    expect(Actor.recoveredHeadActions(snap)).toEqual([{ _tag: "StartCall", callId: "a" }])
  })

  it("does not restart a head that is already done", () => {
    const snap = fold([accepted(0, "a"), completed(1, "a")]) // drained, active=None
    expect(Actor.recoveredHeadActions(snap)).toEqual([])
  })

  it("seq_num is the only order; no app-level sequence field (ADMISSION.3)", () => {
    const snap = fold([accepted(5, "a"), accepted(9, "b"), accepted(12, "c")])
    expect(snap.pending).toEqual(["a", "b", "c"]) // append order = the seq_nums we fed
    expect(snap.cursor).toBe(12) // last applied seq_num
  })

  it("StateChanged folds latest-value-per-key into the projection (LAYERING.4)", () => {
    const snap = fold([
      { seqNum: 0, event: { _tag: "StateChanged", table: "balance", key: "acct", value: 1 } },
      { seqNum: 1, event: { _tag: "StateChanged", table: "balance", key: "acct", value: 7 } },
    ])
    expect(snap.state.get("balance/acct")).toBe(7) // latest wins
  })

  it("SignalResolved records an ingress fact and asks to wake the waiter (INGRESS)", () => {
    const [snap, actions] = Actor.transition(Actor.empty, {
      seqNum: 0,
      event: { _tag: "SignalResolved", callId: "c", name: "approval", value: true },
    })
    expect(snap.signals.get("c/approval")).toBe(true)
    expect(actions).toEqual([{ _tag: "WakeWaiter", callId: "c", name: "approval" }])
  })
})

describe("attach / poll views (COMPLETION.4/5, PLANNING.2)", () => {
  it("a pending call is Pending; a completed one is served from results, never re-run", () => {
    const s = fold([accepted(0, "a")])
    expect(Actor.attach(s, "a")).toEqual({ _tag: "Pending" })
    const done = Actor.transition(s, completed(1, "a", { _tag: "Success", value: 42 }))[0]
    expect(Actor.attach(done, "a")).toEqual({ _tag: "Success", value: 42 })
    expect(Actor.poll(done, "a")).toEqual({ _tag: "Success", value: 42 })
  })

  it("normalizes every Exit shape (COMPLETION.5 minus Expired, which needs the horizon)", () => {
    const cases: ReadonlyArray<readonly [Actor.ActorExit, Actor.CallStatus]> = [
      [{ _tag: "Success", value: 1 }, { _tag: "Success", value: 1 }],
      [{ _tag: "Failure", error: "boom" }, { _tag: "Failure", error: "boom" }],
      [{ _tag: "Interrupt" }, { _tag: "Interrupted" }],
      [{ _tag: "Defect", defect: "bug" }, { _tag: "Defect", defect: "bug" }],
    ]
    cases.forEach(([exit, status], i) => {
      const done = fold([accepted(i * 2, "x"), completed(i * 2 + 1, "x", exit)])
      expect(Actor.attach(done, "x")).toEqual(status)
    })
  })
})
