import { describe, expect, it } from "@effect/vitest"
import { Option } from "effect"
import { type ActorEvent, replay, type LogEntry } from "../src/object/machine/index.ts"
import * as Machine from "../src/object/machine/index.ts"

const snapshot = (events: ReadonlyArray<ActorEvent>) =>
  replay(events.map((event, index): LogEntry => ({ seqNum: index + 1, event })))

describe("object state machine", () => {
  it("decide processes commands into events and driver actions", () => {
    const accepted = Machine.decide(snapshot([]), {
      _tag: "Admit",
      callId: "c1",
      method: "add",
      input: 5,
    })

    expect(accepted.result).toEqual({ _tag: "Admitted" })
    expect(accepted.events).toEqual([{ _tag: "Accepted", callId: "c1", method: "add", input: 5 }])

    const events = accepted.events
    const selected = Machine.decide(snapshot(events), {
      _tag: "SelectNextHead",
      accepted: Machine.acceptedHeads(events),
      local: { started: new Set() },
    })

    expect(selected.actions).toEqual([{ _tag: "RunHead", head: { callId: "c1", method: "add", input: 5 } }])
  })

  it("admit emits Accepted for an unknown call", () => {
    const result = Machine.admit(snapshot([]), { callId: "c1", method: "add", input: 5 })

    expect(result.result).toEqual({ _tag: "Admitted" })
    expect(result.events).toEqual([{ _tag: "Accepted", callId: "c1", method: "add", input: 5 }])
    expect(result.actions).toEqual([])
  })

  it("admit returns AlreadyPending for an accepted but incomplete call", () => {
    const snap = snapshot([{ _tag: "Accepted", callId: "c1", method: "add", input: 5 }])

    expect(Machine.admit(snap, { callId: "c1", method: "add", input: 5 })).toEqual({
      result: { _tag: "AlreadyPending" },
      events: [],
      actions: [],
    })
  })

  it("admit returns AlreadyCompleted for a completed call", () => {
    const snap = snapshot([
      { _tag: "Accepted", callId: "c1", method: "add", input: 5 },
      { _tag: "Completed", callId: "c1", exit: { _tag: "Success", value: 5 } },
    ])

    expect(Machine.admit(snap, { callId: "c1", method: "add", input: 5 }).result).toEqual({
      _tag: "AlreadyCompleted",
    })
  })

  it("selectNextHead returns the earliest accepted call not completed and not locally started", () => {
    const events: ReadonlyArray<ActorEvent> = [
      { _tag: "Accepted", callId: "c1", method: "add", input: 1 },
      { _tag: "Accepted", callId: "c2", method: "add", input: 2 },
      { _tag: "Accepted", callId: "c3", method: "add", input: 3 },
      { _tag: "Completed", callId: "c1", exit: { _tag: "Success", value: 1 } },
    ]
    const result = Machine.selectNextHead(snapshot(events), Machine.acceptedHeads(events), {
      started: new Set(["c2"]),
    })

    expect(result.result).toEqual({ callId: "c3", method: "add", input: 3 })
    expect(result.actions).toEqual([{ _tag: "RunHead", head: { callId: "c3", method: "add", input: 3 } }])
  })

  it("selectNextHead returns undefined when all calls are completed or locally started", () => {
    const events: ReadonlyArray<ActorEvent> = [
      { _tag: "Accepted", callId: "c1", method: "add", input: 1 },
      { _tag: "Accepted", callId: "c2", method: "add", input: 2 },
      { _tag: "Completed", callId: "c1", exit: { _tag: "Success", value: 1 } },
    ]

    expect(Machine.selectNextHead(snapshot(events), Machine.acceptedHeads(events), {
      started: new Set(["c2"]),
    }).result).toBeUndefined()
  })

  it("stateGet returns a live value and emits a read journal when no journal exists", () => {
    const snap = snapshot([{ _tag: "StateChanged", op: "set", table: "counters", key: "a", value: 42 }])
    const result = Machine.decide(snap, { _tag: "StateGet", callId: "c1", step: "0", table: "counters", key: "a" })

    expect(result.result).toEqual(Option.some(42))
    expect(result.events).toEqual([{
      _tag: "Journaled",
      callId: "c1",
      kind: "read",
      step: "0",
      value: { present: true, value: 42 },
    }])
  })

  it("stateGet returns recorded journal value even if live state changed", () => {
    const snap = snapshot([
      { _tag: "StateChanged", op: "set", table: "counters", key: "a", value: 42 },
      {
        _tag: "Journaled",
        callId: "c1",
        kind: "read",
        step: "0",
        value: { present: true, value: 42 },
      },
      { _tag: "StateChanged", op: "set", table: "counters", key: "a", value: 99 },
    ])

    const result = Machine.decide(snap, { _tag: "StateGet", callId: "c1", step: "0", table: "counters", key: "a" })

    expect(result.result).toEqual(Option.some(42))
    expect(result.events).toEqual([])
  })

  it("resolveSignal emits SignalResolved and NotifySignalWaiter when unresolved", () => {
    const result = Machine.decide(snapshot([]), { _tag: "ResolveSignal", callId: "c1", name: "approved", value: true })

    expect(result.events).toEqual([{ _tag: "SignalResolved", callId: "c1", name: "approved", value: true }])
    expect(result.actions).toEqual([{ _tag: "NotifySignalWaiter", callId: "c1", name: "approved" }])
  })

  it("resolveSignal emits no event when the signal is already resolved", () => {
    const snap = snapshot([{ _tag: "SignalResolved", callId: "c1", name: "approved", value: true }])

    expect(Machine.decide(snap, { _tag: "ResolveSignal", callId: "c1", name: "approved", value: false })).toEqual({
      result: undefined,
      events: [],
      actions: [],
    })
  })

  it("complete emits Completed once and no-ops if already completed", () => {
    const first = Machine.complete(snapshot([]), "c1", { _tag: "Success", value: 7 })
    expect(first.events).toEqual([{ _tag: "Completed", callId: "c1", exit: { _tag: "Success", value: 7 } }])

    const second = Machine.complete(snapshot(first.events), "c1", { _tag: "Success", value: 8 })
    expect(second.events).toEqual([])
  })
})
