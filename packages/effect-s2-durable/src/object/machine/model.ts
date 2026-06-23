import { Option, Schema } from "effect"

/**
 * Internal object mechanics for `DurableEngine` (consolidation SDD).
 *
 * NOT a public API and NOT a sibling engine: these are the durable-event model
 * the object owner driver folds over. One ordered `ActorEvent` log per
 * `(object, key)` owner stream is the system of record; the latest-value view is a
 * pure projection (`replay`) — never a separate table.
 *
 * Event vocabulary: `Accepted` (admission), `StateChanged` (user state), `Journaled`
 * (run + state-read facts), `SignalResolved` (ingress), `Completed` (result). Timers
 * (`sleep`) and checkpointing land in later batch items.
 */

/** The durable outcome of a settled call (JSON-safe at the event boundary). */
export const ActorExit = Schema.Union([
  // `value` is optional: a void-returning method encodes `undefined`, which JSON
  // drops — the key must be allowed to be absent on decode.
  Schema.TaggedStruct("Success", { value: Schema.optional(Schema.Unknown) }),
  Schema.TaggedStruct("Failure", { error: Schema.String }),
  Schema.TaggedStruct("Interrupt", {}),
  Schema.TaggedStruct("Defect", { defect: Schema.String }),
])
export type ActorExit = typeof ActorExit.Type

/** Admission: a method call entered the owner's FIFO (the head runs exclusively). */
const Accepted = Schema.TaggedStruct("Accepted", {
  callId: Schema.String,
  method: Schema.String,
  // optional: a no-arg method (`*value()`) has `undefined` input (JSON drops it).
  input: Schema.optional(Schema.Unknown),
})

/** A durable user-state mutation (`state(Table).set/delete`). */
const StateChanged = Schema.TaggedStruct("StateChanged", {
  op: Schema.Literals(["set", "delete"]),
  table: Schema.String,
  key: Schema.String,
  value: Schema.optional(Schema.Unknown),
})

/**
 * A journaled per-call fact, replayed verbatim. `kind` namespaces the durable
 * primitive that wrote it (`read` for a `state.get`, `run` for a `run` step; future
 * `sleep`/`signal`), so a `run` step named like a read journal (`run("read/0", …)`)
 * cannot collide with a state-read fact — identity is `(callId, kind, step)`, not a
 * single shared string. (Services avoided this with separate `steps`/`stateReads`
 * tables; the owner log folds both, so the kind must be part of the key.)
 */
const Journaled = Schema.TaggedStruct("Journaled", {
  callId: Schema.String,
  kind: Schema.String,
  step: Schema.String,
  value: Schema.Unknown,
})

/**
 * A durable ingress/wakeup fact: a named promise (signal / awakeable / deferred) was
 * resolved for a call. Residency-independent — appended to the owner stream by
 * `resolveSignal(callId, …)` whether or not the call is currently resident; the row
 * is the source of truth and an in-process waiter is poked best-effort (`INGRESS`).
 */
const SignalResolved = Schema.TaggedStruct("SignalResolved", {
  callId: Schema.String,
  name: Schema.String,
  value: Schema.optional(Schema.Unknown),
})

/** Terminal: the call settled; its result outlives the running fiber. */
const Completed = Schema.TaggedStruct("Completed", {
  callId: Schema.String,
  exit: ActorExit,
})

/** The one event type appended to an owner stream. */
export const ActorEvent = Schema.Union([Accepted, StateChanged, Journaled, SignalResolved, Completed])
export type ActorEvent = typeof ActorEvent.Type

/** A decoded log record: its S2 `seq_num` and event. */
export interface LogEntry {
  readonly seqNum: number
  readonly event: ActorEvent
}

/**
 * Escape a raw string into a single S2 path segment that cannot contain a raw `/`,
 * so distinct `(object, key)` pairs can never collide on one owner path — e.g.
 * `(a/b, c)` ≠ `(a, b/c)`. Escaping `%` before `/` keeps the encoding injective.
 */
export const pathSegment = (raw: string): string => raw.replaceAll("%", "%25").replaceAll("/", "%2F")

/** Reverse `pathSegment` — recover the raw key from an enumerated path segment. */
export const unPathSegment = (segment: string): string => segment.replaceAll("%2F", "/").replaceAll("%25", "%")

// ── projection (pure fold of the log) ────────────────────────────────────────

/** The latest-value view folded from the log. Not durable — always re-derivable. */
export interface ActorSnapshot {
  /** callIds in `Accepted` order; the head is the lowest not-yet-`Completed`. */
  readonly order: ReadonlyArray<string>
  readonly results: ReadonlyMap<string, ActorExit>
  /** table -> key -> latest value. */
  readonly state: ReadonlyMap<string, ReadonlyMap<string, unknown>>
  /** callId -> (kind,step) -> journaled value. */
  readonly journal: ReadonlyMap<string, ReadonlyMap<string, unknown>>
  /** callId -> signal name -> resolved value (durable ingress, first-write-wins). */
  readonly signals: ReadonlyMap<string, ReadonlyMap<string, unknown>>
}

const empty: ActorSnapshot = {
  order: [],
  results: new Map(),
  state: new Map(),
  journal: new Map(),
  signals: new Map(),
}

const setNested = (
  m: ReadonlyMap<string, ReadonlyMap<string, unknown>>,
  outer: string,
  inner: string,
  value: unknown,
): ReadonlyMap<string, ReadonlyMap<string, unknown>> => {
  const next = new Map(m)
  const sub = new Map(next.get(outer) ?? [])
  sub.set(inner, value)
  next.set(outer, sub)
  return next
}

const deleteNested = (
  m: ReadonlyMap<string, ReadonlyMap<string, unknown>>,
  outer: string,
  inner: string,
): ReadonlyMap<string, ReadonlyMap<string, unknown>> => {
  const next = new Map(m)
  const sub = new Map(next.get(outer) ?? [])
  sub.delete(inner)
  next.set(outer, sub)
  return next
}

/** Fold one event into the snapshot. */
export const transition = (snapshot: ActorSnapshot, entry: LogEntry): ActorSnapshot => {
  const event = entry.event
  switch (event._tag) {
    case "Accepted":
      return snapshot.order.includes(event.callId)
        ? snapshot
        : { ...snapshot, order: [...snapshot.order, event.callId] }
    case "Completed": {
      const results = new Map(snapshot.results)
      results.set(event.callId, event.exit)
      return { ...snapshot, results }
    }
    case "StateChanged":
      return {
        ...snapshot,
        state: event.op === "set"
          ? setNested(snapshot.state, event.table, event.key, event.value)
          : deleteNested(snapshot.state, event.table, event.key),
      }
    case "Journaled":
      return { ...snapshot, journal: setNested(snapshot.journal, event.callId, journalKey(event.kind, event.step), event.value) }
    case "SignalResolved": {
      // first-write-wins: a resolution is terminal, a double-resolve is a no-op.
      const sub = snapshot.signals.get(event.callId)
      if (sub !== undefined && sub.has(event.name)) {
        return snapshot
      }
      return { ...snapshot, signals: setNested(snapshot.signals, event.callId, event.name, event.value) }
    }
  }
}

/** Collision-free inner journal key for a `(kind, step)` pair (injective via JSON tuple). */
const journalKey = (kind: string, step: string): string => JSON.stringify([kind, step])

/** Fold an entire log to its projection. */
export const replay = (entries: ReadonlyArray<LogEntry>): ActorSnapshot => entries.reduce(transition, empty)

/** Has this call settled? */
export const isDone = (snapshot: ActorSnapshot, callId: string): boolean => snapshot.results.has(callId)

/** The latest durable value of `table[key]`, if present. */
export const stateValue = (snapshot: ActorSnapshot, table: string, key: string): Option.Option<unknown> =>
  Option.fromNullishOr(snapshot.state.get(table)).pipe(
    Option.flatMap((sub) => (sub.has(key) ? Option.some(sub.get(key)) : Option.none())),
  )

/** A resolved signal's value for `callId`/`name`, if resolved (the value may be `undefined`). */
export const signalValue = (snapshot: ActorSnapshot, callId: string, name: string): Option.Option<unknown> =>
  Option.fromNullishOr(snapshot.signals.get(callId)).pipe(
    Option.flatMap((sub) => (sub.has(name) ? Option.some(sub.get(name)) : Option.none())),
  )

/** A journaled `(kind, step)` value for `callId`, if recorded. */
export const journalValue = (
  snapshot: ActorSnapshot,
  callId: string,
  kind: string,
  step: string,
): Option.Option<unknown> =>
  Option.fromNullishOr(snapshot.journal.get(callId)).pipe(
    Option.flatMap((sub) => {
      const key = journalKey(kind, step)
      return sub.has(key) ? Option.some(sub.get(key)) : Option.none()
    }),
  )

/**
 * The user-visible status of a call, folded from its `Completed` event (if any).
 * `Unknown` = the callId was never admitted to this owner (distinct from `Pending`,
 * an admitted-but-unsettled call) — so `attach` on a bogus id fails instead of
 * looping forever.
 */
export type CallStatus =
  | { readonly _tag: "Unknown" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Success"; readonly value: unknown }
  | { readonly _tag: "Failure"; readonly error: string }
  | { readonly _tag: "Interrupt" }
  | { readonly _tag: "Defect"; readonly defect: string }

/** Project a call's status from the snapshot. */
export const callStatus = (snapshot: ActorSnapshot, callId: string): CallStatus => {
  const exit = snapshot.results.get(callId)
  if (exit === undefined) {
    return snapshot.order.includes(callId) ? { _tag: "Pending" } : { _tag: "Unknown" }
  }
  switch (exit._tag) {
    case "Success":
      return { _tag: "Success", value: exit.value }
    case "Failure":
      return { _tag: "Failure", error: exit.error }
    case "Interrupt":
      return { _tag: "Interrupt" }
    case "Defect":
      return { _tag: "Defect", defect: exit.defect }
  }
}
