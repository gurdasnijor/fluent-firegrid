import * as Option from "effect/Option"
import {
  type ActorEvent,
  type ActorExit,
  type ActorSnapshot,
  type CallStatus,
  callStatus,
  isDone,
  journalValue,
  signalValue,
  stateValue
} from "./model.ts"

/**
 * Pure object-owner command processing.
 *
 * The machine boundary is:
 *
 *   ActorSnapshot + ObjectCommand + local driver facts
 *     -> result value
 *     -> durable ActorEvent[]
 *     -> effectful ObjectDriverAction[]
 *
 * This file contains no S2 client, Effect services, locks, fibers, or waiters.
 * `ObjectOwnerDriver` is the interpreter that appends emitted events and runs
 * emitted actions.
 */

export type AdmitResult =
  | { readonly _tag: "Admitted" }
  | { readonly _tag: "AlreadyPending" }
  | { readonly _tag: "AlreadyCompleted" }

export interface PendingHead {
  readonly callId: string
  readonly method: string
  readonly input: unknown
}

export interface ObjectLocalState {
  readonly started: ReadonlySet<string>
}

type ObjectDriverAction =
  | { readonly _tag: "RunHead"; readonly head: PendingHead }
  | { readonly _tag: "NotifySignalWaiter"; readonly callId: string; readonly name: string }
  | { readonly _tag: "RegisterSignalWaiter"; readonly callId: string; readonly name: string }
  | { readonly _tag: "DropLocalStarted"; readonly callId: string }

export interface ObjectApplyResult<A> {
  readonly result: A
  readonly events: ReadonlyArray<ActorEvent>
  readonly actions: ReadonlyArray<ObjectDriverAction>
}

export type ObjectCommand =
  | {
    readonly _tag: "Admit"
    readonly callId: string
    readonly method: string
    readonly input: unknown
  }
  | { readonly _tag: "Status"; readonly callId: string }
  | {
    readonly _tag: "SelectNextHead"
    readonly accepted: ReadonlyMap<string, PendingHead>
    readonly local: ObjectLocalState
  }
  | { readonly _tag: "StateSet"; readonly table: string; readonly key: string; readonly value: unknown }
  | { readonly _tag: "StateDelete"; readonly table: string; readonly key: string }
  | {
    readonly _tag: "StateGet"
    readonly callId: string
    readonly step: string
    readonly table: string
    readonly key: string
  }
  | { readonly _tag: "JournalGet"; readonly callId: string; readonly kind: string; readonly step: string }
  | {
    readonly _tag: "JournalPut"
    readonly callId: string
    readonly kind: string
    readonly step: string
    readonly value: unknown
  }
  | { readonly _tag: "ResolveSignal"; readonly callId: string; readonly name: string; readonly value: unknown }
  | { readonly _tag: "AwaitSignal"; readonly callId: string; readonly name: string }
  | { readonly _tag: "Complete"; readonly callId: string; readonly exit: ActorExit }

export type ObjectDecision = ObjectApplyResult<unknown>

const applied = <A>(
  result: A,
  events: ReadonlyArray<ActorEvent> = [],
  actions: ReadonlyArray<ObjectDriverAction> = []
): ObjectApplyResult<A> => ({ result, events, actions })

export const decide = (snapshot: ActorSnapshot, command: ObjectCommand): ObjectDecision => {
  switch (command._tag) {
    case "Admit":
      return admit(snapshot, command)
    case "Status":
      return applied(status(snapshot, command.callId))
    case "SelectNextHead":
      return selectNextHead(snapshot, command.accepted, command.local)
    case "StateSet":
      return stateSet(command.table, command.key, command.value)
    case "StateDelete":
      return stateDelete(command.table, command.key)
    case "StateGet":
      return stateGet(snapshot, command)
    case "JournalGet":
      return applied(journalGet(snapshot, command.callId, command.kind, command.step))
    case "JournalPut":
      return journalPut(command.callId, command.kind, command.step, command.value)
    case "ResolveSignal":
      return resolveSignal(snapshot, command)
    case "AwaitSignal":
      return awaitSignal(snapshot, command.callId, command.name)
    case "Complete":
      return complete(snapshot, command.callId, command.exit)
  }
}

export const admit = (
  snapshot: ActorSnapshot,
  input: {
    readonly callId: string
    readonly method: string
    readonly input: unknown
  }
): ObjectApplyResult<AdmitResult> => {
  if (snapshot.order.includes(input.callId)) {
    return applied(snapshot.results.has(input.callId) ? { _tag: "AlreadyCompleted" } : { _tag: "AlreadyPending" })
  }

  return applied(
    { _tag: "Admitted" },
    [{ _tag: "Accepted", callId: input.callId, method: input.method, input: input.input }]
  )
}

export const status = (snapshot: ActorSnapshot, callId: string): CallStatus => callStatus(snapshot, callId)

export const acceptedHeads = (events: ReadonlyArray<ActorEvent>): ReadonlyMap<string, PendingHead> =>
  events.reduce((heads, event) => {
    if (event._tag === "Accepted" && !heads.has(event.callId)) {
      return new Map(heads).set(event.callId, { callId: event.callId, method: event.method, input: event.input })
    }
    return heads
  }, new Map<string, PendingHead>())

export const selectNextHead = (
  snapshot: ActorSnapshot,
  accepted: ReadonlyMap<string, PendingHead>,
  local: ObjectLocalState
): ObjectApplyResult<PendingHead | undefined> => {
  const callId = snapshot.order.find((candidate) => !isDone(snapshot, candidate) && !local.started.has(candidate))
  if (callId === undefined) {
    return applied(undefined)
  }

  const head = accepted.get(callId)
  return applied(head, [], head === undefined ? [] : [{ _tag: "RunHead", head }])
}

const stateSet = (table: string, key: string, value: unknown): ObjectApplyResult<void> =>
  applied(undefined, [{ _tag: "StateChanged", op: "set", table, key, value }])

const stateDelete = (table: string, key: string): ObjectApplyResult<void> =>
  applied(undefined, [{ _tag: "StateChanged", op: "delete", table, key }])

const stateGet = (
  snapshot: ActorSnapshot,
  input: {
    readonly callId: string
    readonly step: string
    readonly table: string
    readonly key: string
  }
): ObjectApplyResult<Option.Option<unknown>> => {
  const recorded = journalValue(snapshot, input.callId, "read", input.step)
  if (Option.isSome(recorded)) {
    const record = recorded.value as { readonly present: boolean; readonly value: unknown }
    return applied(record.present ? Option.some(record.value) : Option.none())
  }

  const live = stateValue(snapshot, input.table, input.key)
  const record = { present: Option.isSome(live), value: Option.getOrNull(live) }
  return applied(live, [{
    _tag: "Journaled",
    callId: input.callId,
    kind: "read",
    step: input.step,
    value: record
  }])
}

export const journalGet = (
  snapshot: ActorSnapshot,
  callId: string,
  kind: string,
  step: string
): Option.Option<unknown> => journalValue(snapshot, callId, kind, step)

const journalPut = (callId: string, kind: string, step: string, value: unknown): ObjectApplyResult<void> =>
  applied(undefined, [{ _tag: "Journaled", callId, kind, step, value }])

const resolveSignal = (
  snapshot: ActorSnapshot,
  input: { readonly callId: string; readonly name: string; readonly value: unknown }
): ObjectApplyResult<void> => {
  if (Option.isSome(signalValue(snapshot, input.callId, input.name))) {
    return applied(undefined)
  }

  return applied(
    undefined,
    [{ _tag: "SignalResolved", callId: input.callId, name: input.name, value: input.value }],
    [{ _tag: "NotifySignalWaiter", callId: input.callId, name: input.name }]
  )
}

export type AwaitSignalDecision =
  | { readonly _tag: "Resolved"; readonly value: unknown }
  | { readonly _tag: "Park" }

export const awaitSignal = (
  snapshot: ActorSnapshot,
  callId: string,
  name: string
): ObjectApplyResult<AwaitSignalDecision> => {
  const resolved = signalValue(snapshot, callId, name)
  if (Option.isSome(resolved)) {
    return applied({ _tag: "Resolved", value: resolved.value })
  }

  return applied({ _tag: "Park" }, [], [{ _tag: "RegisterSignalWaiter", callId, name }])
}

export const complete = (snapshot: ActorSnapshot, callId: string, exit: ActorExit): ObjectApplyResult<void> => {
  if (snapshot.results.has(callId)) {
    return applied(undefined)
  }
  return applied(undefined, [{ _tag: "Completed", callId, exit }])
}
