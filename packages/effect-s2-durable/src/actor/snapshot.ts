import { Effect, Option } from "effect"
import type { ActorExit, LogEntry } from "./events.ts"

/**
 * The actor projection and its **pure transition** (`PLANNING.7/8`).
 *
 * `transition(snapshot, entry) -> [snapshot, actions]` is the whole decision
 * layer: ordering, admission, completion-derived advance, and checkpoint
 * eligibility — with no I/O. An interpreter (a later slice) runs the emitted
 * actions only after the durable fact exists; the transition never performs
 * effects. Because it is pure, the bulk of runtime behaviour is testable as
 * `snapshot + event -> snapshot + actions` without S2, timers, or fibers.
 */

/** Derived state at an S2 cursor — rebuildable purely by folding the log. */
export interface ActorSnapshot {
  /** Last applied `seq_num` (`-1` before any record). */
  readonly cursor: number
  /** `accepted ∧ ¬completed`, in `seq_num` order. */
  readonly pending: ReadonlyArray<string>
  /**
   * The DURABLE head pointer = `pending[0]` — the call that SHOULD be running.
   * NOT "a fiber is resident": after a cold replay it can be `Some` with no live
   * fiber, which is why recovery restarts it regardless of residency (`RECOVERY.4`).
   */
  readonly active: Option.Option<string>
  /** A call is done iff present here (`COMPLETION.2`). */
  readonly results: ReadonlyMap<string, ActorExit>
  /** Resolved signals, keyed `${callId}/${name}` (`INGRESS`). */
  readonly signals: ReadonlyMap<string, unknown>
  /**
   * User state, keyed `${table}/${key}` — the latest-value-per-key fold over
   * `StateChanged` events (the reused materialization MECHANISM, not `ChangeMessage`
   * internals; `LAYERING.4`).
   */
  readonly state: ReadonlyMap<string, unknown>
}

/** The empty projection — the fold seed (no records applied). */
export const empty: ActorSnapshot = {
  cursor: -1,
  pending: [],
  active: Option.none(),
  results: new Map(),
  signals: new Map(),
  state: new Map(),
}

/** What the pure core asks the effectful shell to do — never decided by the shell. */
export type ActorAction =
  | { readonly _tag: "StartCall"; readonly callId: string }
  | { readonly _tag: "WakeWaiter"; readonly callId: string; readonly name: string }
  | { readonly _tag: "Checkpoint" }

const withMapSet = <V>(map: ReadonlyMap<string, V>, key: string, value: V): ReadonlyMap<string, V> =>
  new Map(map).set(key, value)

/**
 * The pure transition. Completion *derives* the advance (no dequeue write → a
 * completed call cannot be re-run after a crash; window-2 is structurally
 * impossible, `COMPLETION.3`).
 */
export const transition = (
  snapshot: ActorSnapshot,
  entry: LogEntry,
): readonly [ActorSnapshot, ReadonlyArray<ActorAction>] => {
  const event = entry.event
  const cursor = entry.seqNum
  switch (event._tag) {
    case "Accepted": {
      // idempotent by callId: a duplicate (already pending or already settled)
      // never re-admits or re-runs (supports ADMISSION.4 / COMPLETION.3 on replay).
      if (snapshot.results.has(event.callId) || snapshot.pending.includes(event.callId)) {
        return [{ ...snapshot, cursor }, []]
      }
      const pending = [...snapshot.pending, event.callId] // seq_num order = append order
      const base = { ...snapshot, cursor, pending }
      return Option.isNone(snapshot.active)
        ? [{ ...base, active: Option.some(event.callId) }, [{ _tag: "StartCall", callId: event.callId }]]
        : [base, []] // busy → just enqueue (single-writer, HANDLERS.2 / EXECUTION.1)
    }
    case "Completed": {
      const results = withMapSet(snapshot.results, event.callId, event.exit)
      const pending = snapshot.pending.filter((id) => id !== event.callId) // "advance" = re-derive pending
      const head = Option.fromNullishOr(pending[0])
      const base = { ...snapshot, cursor, results, pending, active: head }
      return Option.match(head, {
        onNone: () => [base, [{ _tag: "Checkpoint" }]], // queue drained → safe checkpoint boundary
        onSome: (callId) => [base, [{ _tag: "StartCall", callId }]], // run the next head
      })
    }
    case "SignalResolved": {
      const signals = withMapSet(snapshot.signals, `${event.callId}/${event.name}`, event.value)
      return [{ ...snapshot, cursor, signals }, [{ _tag: "WakeWaiter", callId: event.callId, name: event.name }]]
    }
    case "StateChanged": {
      const state = withMapSet(snapshot.state, `${event.table}/${event.key}`, event.value)
      return [{ ...snapshot, cursor, state }, []]
    }
    case "Journaled":
    case "Checkpointed":
      // journal facts and checkpoint markers only advance the cursor in the pure
      // core; their effectful meaning (resume/replay-from-cursor) is a later slice.
      return [{ ...snapshot, cursor }, []]
  }
}

/**
 * Fold a log into a snapshot, **discarding actions** — the only thing recovery
 * replay does (`RECOVERY.3`). Folding history and running actions would re-fork a
 * call whose `Completed` is also in history; replay must not.
 */
export const replay = (entries: ReadonlyArray<LogEntry>): ActorSnapshot =>
  entries.reduce((snapshot, entry) => transition(snapshot, entry)[0], empty)

/** The durable head — the call that should be running (`pending[0]`). */
export const head = (snapshot: ActorSnapshot): Option.Option<string> => Option.fromNullishOr(snapshot.pending[0])

/** A call is done iff its `Completed` event has been folded in. */
export const isDone = (snapshot: ActorSnapshot, callId: string): boolean => snapshot.results.has(callId)

/**
 * The at-most-one `StartCall` for the recovered durable head — emitted when the
 * head is not done. At cold boot no fiber is resident, so this restarts a call
 * that was active-but-unfinished at the crash (`RECOVERY.4`). Keyed off the
 * durable head, never off `active === None`.
 */
export const recoveredHeadActions = (snapshot: ActorSnapshot): ReadonlyArray<ActorAction> =>
  Option.match(snapshot.active, {
    onNone: () => [],
    onSome: (callId) => (isDone(snapshot, callId) ? [] : [{ _tag: "StartCall", callId }]),
  })

// ── attach / poll — views over the projection (PLANNING.2, COMPLETION.4/5) ──────

/** The normalized result/status view. `Expired` arrives with the idempotency horizon (a later slice). */
export type CallStatus =
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Success"; readonly value: unknown }
  | { readonly _tag: "Failure"; readonly error: unknown }
  | { readonly _tag: "Interrupted" }
  | { readonly _tag: "Defect"; readonly defect: unknown }
  | { readonly _tag: "Expired" }

const normalizeExit = (exit: ActorExit): CallStatus => {
  switch (exit._tag) {
    case "Success":
      return { _tag: "Success", value: exit.value }
    case "Failure":
      return { _tag: "Failure", error: exit.error }
    case "Interrupt":
      return { _tag: "Interrupted" }
    case "Defect":
      return { _tag: "Defect", defect: exit.defect }
  }
}

/**
 * `attach(callId)` reads the result, awaiting while pending. A duplicate
 * completed callId is served from `results` and never re-run (`COMPLETION.4`).
 */
export const attach = (snapshot: ActorSnapshot, callId: string): CallStatus => {
  const settled = snapshot.results.get(callId)
  return settled === undefined ? { _tag: "Pending" } : normalizeExit(settled)
}

/** `poll(callId)` is the non-awaiting twin of `attach` (same projection view). */
export const poll = (snapshot: ActorSnapshot, callId: string): CallStatus => attach(snapshot, callId)

// ── instrumented edges — the firelab production path; the core stays pure ───────

/** Plan one step (instrumented `transition`). */
export const planStep = (
  snapshot: ActorSnapshot,
  entry: LogEntry,
): Effect.Effect<readonly [ActorSnapshot, ReadonlyArray<ActorAction>]> =>
  Effect.sync(() => transition(snapshot, entry)).pipe(
    Effect.withSpan("effect-s2-durable.transition", {
      attributes: { tag: entry.event._tag, seqNum: entry.seqNum },
    }),
  )

/** Fold a log into a snapshot (instrumented `replay`). */
export const replayLog = (entries: ReadonlyArray<LogEntry>): Effect.Effect<ActorSnapshot> =>
  Effect.sync(() => replay(entries)).pipe(
    Effect.withSpan("effect-s2-durable.replay", { attributes: { count: entries.length } }),
  )

/** Read the status view (instrumented `attach`). */
export const attachView = (snapshot: ActorSnapshot, callId: string): Effect.Effect<CallStatus> =>
  Effect.sync(() => attach(snapshot, callId)).pipe(
    Effect.withSpan("effect-s2-durable.attach", { attributes: { callId } }),
  )
