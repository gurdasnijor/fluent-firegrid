import { Cause, Effect, Exit, Option, Ref } from "effect"
import type { S2Client } from "effect-s2"
import { type DurableExecutionError } from "../errors.ts"
import type { ActorExit } from "./events.ts"
import type { ActorLog } from "./log.ts"
import { type ActorSnapshot, isDone, replay, stateValue, transition } from "./snapshot.ts"

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

// A handler's state context: reads from the live in-memory snapshot; writes append
// StateChanged events AND advance the snapshot so a handler sees its own writes.
const makeContext = (
  log: ActorLog,
  snapshotRef: Ref.Ref<ActorSnapshot>,
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

  return {
    state: {
      get: (table, key) => Ref.get(snapshotRef).pipe(Effect.map((snapshot) => stateValue(snapshot, table, key))),
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
    const exit = yield* handler(input, makeContext(log, snapshotRef)).pipe(Effect.exit)
    yield* log.append({ _tag: "Completed", callId, exit: toActorExit(exit) })
  }).pipe(Effect.withSpan("effect-s2-durable.runCall", { attributes: { callId, method } }))

/** Drain the per-key queue to quiescence: run each pending head to completion in order. */
export const drain = (log: ActorLog, handlers: Handlers): Effect.Effect<void, DurableExecutionError, S2Client> => {
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

  return step().pipe(Effect.withSpan("effect-s2-durable.drain", { attributes: { stream: log.streamName } }))
}
