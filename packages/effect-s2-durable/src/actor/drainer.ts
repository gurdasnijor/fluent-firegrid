import { Cause, Context, Effect, Exit, Layer, Option, Ref, Semaphore } from "effect"
import type { S2Client } from "effect-s2"
import { type DurableExecutionError } from "../errors.ts"
import type { ActorExit } from "./events.ts"
import type { ActorLog } from "./log.ts"
import {
  type ActorSnapshot,
  isDone,
  journalValue,
  replay,
  stateValue,
  toCheckpointSnapshot,
  transition,
} from "./snapshot.ts"

/**
 * The serial per-key drainer (`EXECUTION.1`). It folds the log to a snapshot,
 * runs the durable head (the lowest-`seq_num` pending call) to completion by
 * appending its `Completed` event, then re-derives and advances — one call at a
 * time. Replay is fold-only; the head is (re)started from the durable projection,
 * so a crash mid-call resumes (`RECOVERY.3/4`).
 *
 * 3b drains to quiescence synchronously; a forked live-tail interpreter (for
 * concurrent admission while draining) is a refinement, not a semantic change.
 */

/** A handler's durable state surface — the public `state(Table)` set/get/delete. */
export interface HandlerContext {
  readonly state: {
    readonly get: (table: string, key: string) => Effect.Effect<Option.Option<unknown>, DurableExecutionError, S2Client>
    readonly set: (table: string, key: string, value: unknown) => Effect.Effect<void, DurableExecutionError, S2Client>
    readonly delete: (table: string, key: string) => Effect.Effect<void, DurableExecutionError, S2Client>
  }
}

/** An exclusive method body. Its result settles the call; its state writes are journaled events. */
export type Handler = (input: unknown, ctx: HandlerContext) => Effect.Effect<unknown, unknown, S2Client>

/** method name -> handler. */
export type Handlers = Record<string, Handler>

// Map an Effect Exit to the durable ActorExit (success | failure | interrupt | defect).
const toActorExit = (exit: Exit.Exit<unknown, unknown>): ActorExit => {
  if (Exit.isSuccess(exit)) {
    return { _tag: "Success", value: exit.value }
  }
  const cause = exit.cause
  if (Cause.hasInterruptsOnly(cause)) {
    return { _tag: "Interrupt" }
  }
  const failure = Cause.findErrorOption(cause)
  // error/defect are serialized to a JSON-safe string at this boundary (a richer
  // schema-encoded payload is a follow-up; events carry JSON values).
  return Option.isSome(failure)
    ? { _tag: "Failure", error: String(failure.value) }
    : { _tag: "Defect", defect: Cause.pretty(cause) }
}

// A handler's state context. Reads are JOURNALED: the first read records its value
// as a Journaled event; a re-execution after a crash replays that ORIGINAL value
// (so a read-modify-write recomputes against the value first seen, not the
// already-mutated state — no double-apply, EXECUTION.2). Writes append StateChanged
// and advance the in-memory snapshot so a handler sees its own writes.
const makeContext = (
  log: ActorLog,
  snapshotRef: Ref.Ref<ActorSnapshot>,
  callId: string,
  readCounter: Ref.Ref<number>,
): HandlerContext => {
  const applyStateChange = (
    op: "set" | "delete",
    table: string,
    key: string,
    value: unknown,
  ): Effect.Effect<void, DurableExecutionError, S2Client> =>
    Effect.gen(function*() {
      const event = { _tag: "StateChanged" as const, op, table, key, ...(op === "set" ? { value } : {}) }
      const seqNum = yield* log.append(event)
      yield* Ref.update(snapshotRef, (snapshot) => transition(snapshot, { seqNum, event })[0])
    })

  const get = (table: string, key: string): Effect.Effect<Option.Option<unknown>, DurableExecutionError, S2Client> =>
    Effect.gen(function*() {
      const ordinal = yield* Ref.getAndUpdate(readCounter, (n) => n + 1)
      const step = `read/${ordinal}` // calls/<callId>/read/<n> (PLANNING.3 path-aware)
      const snapshot = yield* Ref.get(snapshotRef)
      const recorded = journalValue(snapshot, callId, step)
      if (Option.isSome(recorded)) {
        const record = recorded.value as { readonly present: boolean; readonly value: unknown }
        return record.present ? Option.some(record.value) : Option.none<unknown>()
      }
      // first execution: read live, journal it so the value is replay-stable.
      const live = stateValue(snapshot, table, key)
      const record = { present: Option.isSome(live), value: Option.getOrNull(live) }
      const event = { _tag: "Journaled" as const, callId, step, value: record }
      const seqNum = yield* log.append(event)
      yield* Ref.update(snapshotRef, (s) => transition(s, { seqNum, event })[0])
      return live
    })

  return {
    state: {
      get,
      set: (table, key, value) => applyStateChange("set", table, key, value),
      delete: (table, key) => applyStateChange("delete", table, key, undefined),
    },
  }
}

const runCall = (
  log: ActorLog,
  handlers: Handlers,
  snapshot: ActorSnapshot,
  callId: string,
  method: string,
  input: unknown,
): Effect.Effect<void, DurableExecutionError, S2Client> =>
  Effect.gen(function*() {
    const handler = handlers[method]
    if (handler === undefined) {
      yield* log.append({ _tag: "Completed", callId, exit: { _tag: "Failure", error: `unknown method: ${method}` } })
      return
    }
    const snapshotRef = yield* Ref.make(snapshot)
    const readCounter = yield* Ref.make(0)
    const exit = yield* handler(input, makeContext(log, snapshotRef, callId, readCounter)).pipe(Effect.exit)
    yield* log.append({ _tag: "Completed", callId, exit: toActorExit(exit) })
  }).pipe(Effect.withSpan("effect-s2-durable.runCall", { attributes: { callId, method } }))

/**
 * In-process single-drainer-per-key guard (`EXECUTION.1`): a registry of per-key
 * locks so two concurrent `drain` calls for the same key cannot both run the head.
 * (Cross-process fencing is a later lease/fence slice.) A creation mutex serializes
 * the lazy registry insert so it is race-free. Provided as a service/layer rather
 * than module-global state so the registry has a proper lifetime.
 */
export interface DrainerLocksApi {
  readonly lockFor: (key: string) => Effect.Effect<Semaphore.Semaphore>
}

export class DrainerLocks extends Context.Service<DrainerLocks, DrainerLocksApi>()(
  "effect-s2-durable/DrainerLocks",
) {
  static readonly layer = Layer.effect(
    DrainerLocks,
    Effect.gen(function*() {
      const registry = yield* Ref.make(new Map<string, Semaphore.Semaphore>())
      const creation = yield* Semaphore.make(1)
      const lockFor = (key: string): Effect.Effect<Semaphore.Semaphore> =>
        creation.withPermits(1)(
          Effect.gen(function*() {
            const existing = (yield* Ref.get(registry)).get(key)
            if (existing !== undefined) {
              return existing
            }
            const created = yield* Semaphore.make(1)
            yield* Ref.update(registry, (map) => new Map(map).set(key, created))
            return created
          }),
        )
      return { lockFor }
    }),
  )
}

/**
 * Drain the per-key queue to quiescence: run each pending head to completion in
 * order, under the per-key drainer lock so at most one drainer runs per key.
 */
export const drain = (
  log: ActorLog,
  handlers: Handlers,
): Effect.Effect<void, DurableExecutionError, S2Client | DrainerLocks> => {
  const step = (): Effect.Effect<void, DurableExecutionError, S2Client> =>
    Effect.gen(function*() {
      const entries = yield* log.read()
      const snapshot = replay(entries)
      const head = Option.getOrUndefined(snapshot.active)
      if (head === undefined || isDone(snapshot, head)) {
        return // quiescent — no pending head to run
      }
      const accepted = entries.find((entry) => entry.event._tag === "Accepted" && entry.event.callId === head)
      if (accepted === undefined || accepted.event._tag !== "Accepted") {
        return // unreachable: a pending head always has an Accepted
      }
      yield* runCall(log, handlers, snapshot, head, accepted.event.method, accepted.event.input)
      yield* step() // re-derive pending and advance to the next head
    })

  return Effect.gen(function*() {
    const lock = yield* (yield* DrainerLocks).lockFor(log.streamName)
    yield* lock.withPermits(1)(step())
  }).pipe(Effect.withSpan("effect-s2-durable.drain", { attributes: { stream: log.streamName } }))
}

/**
 * Write a durable checkpoint at the current (drained) boundary: append a
 * `Checkpointed` event carrying the folded snapshot, THEN trim records before it
 * (`CHECKPOINTING.2/4`). Durable-before-trim — the snapshot is durable before
 * anything it represents is removed (`CHECKPOINTING.7`). On reopen, the
 * `Checkpointed` event reseeds the projection, so replay reconstructs an equal
 * snapshot even before the async S2 trim physically purges the older records.
 */
export const checkpoint = (log: ActorLog): Effect.Effect<void, DurableExecutionError, S2Client> =>
  Effect.gen(function*() {
    const snapshot = replay(yield* log.read())
    if (snapshot.cursor < 0) {
      return // nothing durable yet
    }
    const checkpointSeq = yield* log.append({
      _tag: "Checkpointed",
      cursor: snapshot.cursor,
      snapshot: toCheckpointSnapshot(snapshot),
    })
    yield* log.trim(checkpointSeq) // trim everything before the durable Checkpointed event
  }).pipe(Effect.withSpan("effect-s2-durable.checkpoint", { attributes: { stream: log.streamName } }))
