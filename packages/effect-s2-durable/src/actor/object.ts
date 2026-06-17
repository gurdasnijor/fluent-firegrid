import { Context, Deferred, Effect, Layer, Option, Ref, Schema, Semaphore } from "effect"
import { S2Client, S2Conflict } from "effect-s2"
import { StreamDb } from "effect-s2-stream-db"
import { DurableExecutionError, durableError as toError } from "../errors.ts"
import {
  type ActorExit,
  callStatus,
  type CallStatus,
  isDone,
  journalValue,
  type LogEntry,
  type ObjectCallIdParts,
  pathSegment,
  replay,
  signalValue,
  stateValue,
  transition,
  unPathSegment,
} from "./core.ts"
import { type ActorLog, openLog } from "./log.ts"

/**
 * `InvocationStore` — the object-backed durable store the runtime's object call
 * path uses (consolidation SDD). It owns owner-stream access, admission, the
 * exclusive per-key drainer, and the journaled `state` backend. It is NOT exported
 * from the package and is NOT a sibling runtime: only `DurableExecutionRuntime`
 * constructs and calls it.
 *
 * Scope so far: admit + state + run-journal + signal ingress + completion + by-id
 * status. Boot-recovery, timers (`sleep`), and checkpointing land in later batch items.
 */

/** The outcome of admitting a call (idempotent on a re-admit of the same id). */
type AdmitResult =
  | { readonly _tag: "Admitted" }
  | { readonly _tag: "AlreadyPending" }
  | { readonly _tag: "AlreadyCompleted" }

/** The durable surfaces a running object method writes through (state + run journal). */
export interface ObjectStateBackend {
  readonly get: (table: string, key: string) => Effect.Effect<Option.Option<unknown>, DurableExecutionError, S2Client>
  readonly set: (table: string, key: string, value: unknown) => Effect.Effect<void, DurableExecutionError, S2Client>
  readonly delete: (table: string, key: string) => Effect.Effect<void, DurableExecutionError, S2Client>
  /**
   * The per-call journal of durable-primitive facts, namespaced by `kind` (`run`
   * terminal outcomes, `sleep` timer facts, …) and `step`. A recorded fact replays
   * verbatim and is never re-run. `kind` keeps families from colliding.
   */
  readonly journal: {
    readonly get: (kind: string, step: string) => Effect.Effect<Option.Option<unknown>, DurableExecutionError, S2Client>
    readonly put: (
      kind: string,
      step: string,
      value: unknown,
    ) => Effect.Effect<void, DurableExecutionError, S2Client>
  }
  /**
   * Durable named-promise ingress (signal / awakeable / deferred). `await` parks
   * until a `SignalResolved` for `name` is durable on the owner stream (refreshing
   * the projection); `resolve` is the handler-side (deferred.resolve) append.
   */
  readonly signal: {
    readonly await: (name: string) => Effect.Effect<unknown, DurableExecutionError, S2Client>
    readonly resolve: (name: string, value: unknown) => Effect.Effect<void, DurableExecutionError, S2Client>
  }
}

// In-process best-effort wakeup for parked signal awaits (the durable SignalResolved
// is the source of truth; a poke just accelerates a resident waiter — INGRESS.2).
interface SignalPort {
  readonly register: (callId: string, name: string) => Effect.Effect<Deferred.Deferred<void>>
  readonly poke: (callId: string, name: string) => Effect.Effect<void>
  readonly remove: (callId: string, name: string) => Effect.Effect<void>
}

/** Run one accepted call to an `ActorExit` (handler exit captured, never thrown). */
export type RunHead = (call: {
  readonly callId: string
  readonly method: string
  readonly input: unknown
  readonly state: ObjectStateBackend
}) => Effect.Effect<ActorExit, DurableExecutionError, S2Client>

export interface InvocationStoreApi {
  /** Durably admit a call into its owner FIFO (CAS `Accepted`); idempotent by id. */
  readonly admit: (
    callId: string,
    parts: ObjectCallIdParts,
    input: unknown,
  ) => Effect.Effect<AdmitResult, DurableExecutionError, S2Client>
  /** Read a call's status from the owner projection (no residency). */
  readonly status: (
    callId: string,
    parts: ObjectCallIdParts,
  ) => Effect.Effect<CallStatus, DurableExecutionError, S2Client>
  /** Drain an owner's FIFO to quiescence under its exclusive per-key lock. */
  readonly drain: (
    object: string,
    key: string,
    runHead: RunHead,
  ) => Effect.Effect<void, DurableExecutionError, S2Client>
  /**
   * Residency-independent ingress: append a `SignalResolved` to the owner stream
   * (routed by the call id) and best-effort poke a local waiter — succeeds whether
   * or not the call is currently resident (`INGRESS.1`).
   */
  readonly resolveSignal: (
    callId: string,
    parts: ObjectCallIdParts,
    name: string,
    value: unknown,
  ) => Effect.Effect<void, DurableExecutionError, S2Client>
  /**
   * Enumerate the existing owner keys for an object NAME (boot recovery) — reuses
   * `StreamDb.list` for name enumeration only (never a content fold), decoding each
   * path segment back to the raw key (`RECOVERY.1`).
   */
  readonly ownerKeys: (object: string) => Effect.Effect<ReadonlyArray<string>, DurableExecutionError, S2Client>
}

const MAX_CAS_RETRIES = 32

// Derive the owner stream by encoding the key through the owner-key codec (String
// today) — never a hand-built `name:key` identity string. The object name and key
// are each escaped into a single collision-safe path segment (`pathSegment`).
const ownerKeyCodec = Schema.String
const ownerStream = (object: string, key: string): Effect.Effect<string, DurableExecutionError> =>
  Schema.encodeEffect(ownerKeyCodec)(key).pipe(
    Effect.map((segment) => `obj/${pathSegment(object)}/${pathSegment(segment)}`),
    Effect.mapError(toError("object.ownerStream")),
  )

// Create the owner stream if absent (idempotent) so the first admission CAS does
// not depend on conditional-append auto-creating it.
const ensureStream = (stream: string): Effect.Effect<void, DurableExecutionError, S2Client> =>
  S2Client.createStream({ stream }).pipe(
    Effect.asVoid,
    Effect.catch((cause) => (cause instanceof S2Conflict ? Effect.void : Effect.fail(cause))),
    Effect.mapError(toError("object.ensure")),
  )

// The journaled state context for a running call: reads record a `Journaled` fact
// and replay verbatim (crash-stable RMW); writes append `StateChanged` and advance
// the in-memory snapshot so the handler observes its own writes.
const makeBackend = (
  log: ActorLog,
  snapshotRef: Ref.Ref<ReturnType<typeof replay>>,
  callId: string,
  readCounter: Ref.Ref<number>,
  signalPort: SignalPort,
): ObjectStateBackend => {
  const write = (
    op: "set" | "delete",
    table: string,
    key: string,
    value: unknown,
  ): Effect.Effect<void, DurableExecutionError, S2Client> =>
    Effect.gen(function*() {
      const event = { _tag: "StateChanged" as const, op, table, key, ...(op === "set" ? { value } : {}) }
      const seqNum = yield* log.append(event)
      yield* Ref.update(snapshotRef, (snapshot) => transition(snapshot, { seqNum, event }))
    })

  return {
    get: (table, key) =>
      Effect.gen(function*() {
        const step = String(yield* Ref.getAndUpdate(readCounter, (n) => n + 1))
        const snapshot = yield* Ref.get(snapshotRef)
        const recorded = journalValue(snapshot, callId, "read", step)
        if (Option.isSome(recorded)) {
          const record = recorded.value as { readonly present: boolean; readonly value: unknown }
          return record.present ? Option.some(record.value) : Option.none<unknown>()
        }
        const live = stateValue(snapshot, table, key)
        const record = { present: Option.isSome(live), value: Option.getOrNull(live) }
        const event = { _tag: "Journaled" as const, callId, kind: "read", step, value: record }
        const seqNum = yield* log.append(event)
        yield* Ref.update(snapshotRef, (s) => transition(s, { seqNum, event }))
        return live
      }),
    set: (table, key, value) => write("set", table, key, value),
    delete: (table, key) => write("delete", table, key, undefined),
    // the durable-primitive journal, namespaced by `kind` (`run`, `sleep`, …) so a
    // step named like a state-read journal can never collide with one.
    journal: {
      get: (kind, step) =>
        Ref.get(snapshotRef).pipe(Effect.map((snapshot) => journalValue(snapshot, callId, kind, step))),
      put: (kind, step, value) =>
        Effect.gen(function*() {
          const event = { _tag: "Journaled" as const, callId, kind, step, value }
          const seqNum = yield* log.append(event)
          yield* Ref.update(snapshotRef, (snapshot) => transition(snapshot, { seqNum, event }))
        }),
    },
    signal: {
      await: (name) => {
        // refresh the projection from the live log (picks up own writes + ingress
        // appended during the park) and read the resolved value, if any.
        const check = Effect.gen(function*() {
          const live = replay(yield* log.read())
          yield* Ref.set(snapshotRef, live)
          return signalValue(live, callId, name)
        })
        const loop = (): Effect.Effect<unknown, DurableExecutionError, S2Client> =>
          Effect.gen(function*() {
            const resolved = yield* check
            if (Option.isSome(resolved)) {
              return resolved.value
            }
            // register a waiter, RE-CHECK (closes the resolve-before-register race),
            // then park until poked; the durable row remains the source of truth.
            const waiter = yield* signalPort.register(callId, name)
            const again = yield* check
            if (Option.isSome(again)) {
              yield* signalPort.remove(callId, name)
              return again.value
            }
            yield* Deferred.await(waiter)
            yield* signalPort.remove(callId, name)
            return yield* loop()
          })
        return loop()
      },
      resolve: (name, value) =>
        Effect.gen(function*() {
          const event = { _tag: "SignalResolved" as const, callId, name, value }
          const seqNum = yield* log.append(event)
          yield* Ref.update(snapshotRef, (snapshot) => transition(snapshot, { seqNum, event }))
          yield* signalPort.poke(callId, name)
        }),
    },
  }
}

const make = (): Effect.Effect<InvocationStoreApi> =>
  Effect.gen(function*() {
    // per-owner-stream exclusive drainer lock (single-writer admission, EXECUTION.1).
    const locks = yield* Ref.make(new Map<string, Semaphore.Semaphore>())
    // Per-owner authoritative projection cache, updated under the drainer lock. The
    // durable log is the source of truth (rebuilt on restart), but the in-memory
    // snapshot is what the live drainer advances — so a stale read (s2 read-after-
    // write lag) can never make it re-run a settled head (double-apply). The durable
    // read is consulted only to pick up NEW admissions.
    const snapshots = yield* Ref.make(new Map<string, ReturnType<typeof replay>>())
    // In-memory at-most-once guard per owner: a callId is recorded the instant the
    // drainer starts it, so no stale durable read can ever pick it as a head again
    // (double-apply). Mutated only under the per-key drainer lock. Authoritative for
    // process lifetime; rebuilt by replay on restart.
    const started = new Map<string, Set<string>>()
    const startedFor = (stream: string): Set<string> => {
      const existing = started.get(stream)
      if (existing !== undefined) {
        return existing
      }
      const fresh = new Set<string>()
      started.set(stream, fresh)
      return fresh
    }
    // In-process signal waiters (best-effort wakeup; durable SignalResolved is truth).
    // Keyed by an injective JSON tuple of callId + name; a key collision only
    // causes a spurious poke, which the await harmlessly re-checks against the log.
    const waiters = yield* Ref.make(new Map<string, Deferred.Deferred<void>>())
    const waiterKey = (callId: string, name: string): string => JSON.stringify([callId, name])
    const signalPort: SignalPort = {
      register: (callId, name) =>
        Effect.gen(function*() {
          const deferred = yield* Deferred.make<void>()
          yield* Ref.update(waiters, (m) => new Map(m).set(waiterKey(callId, name), deferred))
          return deferred
        }),
      poke: (callId, name) =>
        Ref.get(waiters).pipe(Effect.flatMap((m) => {
          const deferred = m.get(waiterKey(callId, name))
          return deferred === undefined ? Effect.void : Effect.asVoid(Deferred.succeed(deferred, undefined))
        })),
      remove: (callId, name) =>
        Ref.update(waiters, (m) => {
          const next = new Map(m)
          next.delete(waiterKey(callId, name))
          return next
        }),
    }

    const lockCreation = yield* Semaphore.make(1)
    const lockFor = (stream: string): Effect.Effect<Semaphore.Semaphore> =>
      lockCreation.withPermits(1)(
        Effect.gen(function*() {
          const existing = (yield* Ref.get(locks)).get(stream)
          if (existing !== undefined) {
            return existing
          }
          const created = yield* Semaphore.make(1)
          yield* Ref.update(locks, (map) => new Map(map).set(stream, created))
          return created
        }),
      )

    const admit = (
      callId: string,
      parts: ObjectCallIdParts,
      input: unknown,
    ): Effect.Effect<AdmitResult, DurableExecutionError, S2Client> =>
      Effect.gen(function*() {
        const stream = yield* ownerStream(parts.object, parts.key)
        yield* ensureStream(stream) // first admission must not 404 on a fresh owner
        const log = openLog(stream)
        const attempt = (remaining: number): Effect.Effect<AdmitResult, DurableExecutionError, S2Client> =>
          Effect.gen(function*() {
            const entries = yield* log.read()
            const snapshot = replay(entries)
            if (snapshot.order.includes(callId)) {
              return isDone(snapshot, callId)
                ? { _tag: "AlreadyCompleted" as const }
                : { _tag: "AlreadyPending" as const }
            }
            const tail = yield* log.tailSeqNum
            const ack = yield* log.casAppend({ _tag: "Accepted", callId, method: parts.method, input }, tail)
            if (Option.isSome(ack)) {
              return { _tag: "Admitted" as const }
            }
            if (remaining <= 0) {
              return yield* Effect.fail(
                new DurableExecutionError({ operation: "object.admit", message: "admission CAS exhausted", cause: undefined }),
              )
            }
            return yield* attempt(remaining - 1) // a concurrent writer won; re-read and retry
          })
        return yield* attempt(MAX_CAS_RETRIES)
      }).pipe(Effect.withSpan("effect-s2-durable.object.admit", { attributes: { callId, method: parts.method } }))

    const status = (
      callId: string,
      parts: ObjectCallIdParts,
    ): Effect.Effect<CallStatus, DurableExecutionError, S2Client> =>
      Effect.gen(function*() {
        const log = openLog(yield* ownerStream(parts.object, parts.key))
        return callStatus(replay(yield* log.read()), callId)
      }).pipe(Effect.withSpan("effect-s2-durable.object.status", { attributes: { object: parts.object, key: parts.key } }))

    const drain = (
      object: string,
      key: string,
      runHead: RunHead,
    ): Effect.Effect<void, DurableExecutionError, S2Client> =>
      Effect.gen(function*() {
        const stream = yield* ownerStream(object, key)
        const log = openLog(stream)
        const lock = yield* lockFor(stream)

        // Run one head and return the snapshot ADVANCED in memory by its writes +
        // its `Completed`. The head's `Accepted` (method/input) comes from the batch
        // read once at drain start.
        const runOne = (
          entries: ReadonlyArray<LogEntry>,
          snapshot: ReturnType<typeof replay>,
          headCall: string,
        ): Effect.Effect<ReturnType<typeof replay>, DurableExecutionError, S2Client> =>
          Effect.gen(function*() {
            const accepted = entries.find((e) => e.event._tag === "Accepted" && e.event.callId === headCall)
            if (accepted === undefined || accepted.event._tag !== "Accepted") {
              return snapshot // unreachable: a head always has an Accepted
            }
            const snapshotRef = yield* Ref.make(snapshot)
            const readCounter = yield* Ref.make(0)
            const backend = makeBackend(log, snapshotRef, headCall, readCounter, signalPort)
            const exit = yield* runHead({
              callId: headCall,
              method: accepted.event.method,
              input: accepted.event.input,
              state: backend,
            })
            const event = { _tag: "Completed" as const, callId: headCall, exit }
            const seqNum = yield* log.append(event)
            // Advance the live snapshot (state + this completion) WITHOUT a fresh
            // durable read — a re-read here can lag the append and re-run a settled
            // head (double-apply). New admissions are handled by their own drain.
            return transition(yield* Ref.get(snapshotRef), { seqNum, event })
          })

        const startedSet = startedFor(stream)
        const drainFrom = (
          entries: ReadonlyArray<LogEntry>,
          snapshot: ReturnType<typeof replay>,
        ): Effect.Effect<ReturnType<typeof replay>, DurableExecutionError, S2Client> =>
          Effect.gen(function*() {
            // the head is the lowest accepted call that is neither completed nor
            // already started by this process (the in-memory at-most-once guard).
            const headCall = snapshot.order.find((c) => !snapshot.results.has(c) && !startedSet.has(c))
            if (headCall === undefined) {
              return snapshot // batch drained to quiescence
            }
            startedSet.add(headCall) // mark BEFORE running — never selected again
            const advanced = yield* runOne(entries, snapshot, headCall).pipe(
              Effect.tapError(() => Effect.sync(() => startedSet.delete(headCall))), // a failed start may retry
            )
            return yield* drainFrom(entries, advanced)
          })

        yield* lock.withPermits(1)(
          Effect.gen(function*() {
            const entries = yield* log.read()
            const cached = (yield* Ref.get(snapshots)).get(stream)
            // cold start (or restart): fold the durable log. Warm: trust the cached
            // projection (state + completions) and only add NEW admissions from the read.
            const base = cached === undefined
              ? replay(entries)
              : entries.reduce(
                (s, e) => (e.event._tag === "Accepted" && !s.order.includes(e.event.callId) ? transition(s, e) : s),
                cached,
              )
            const advanced = yield* drainFrom(entries, base)
            yield* Ref.update(snapshots, (m) => new Map(m).set(stream, advanced))
          }),
        )
      }).pipe(Effect.withSpan("effect-s2-durable.object.drain", { attributes: { object, key } }))

    const resolveSignal = (
      callId: string,
      parts: ObjectCallIdParts,
      name: string,
      value: unknown,
    ): Effect.Effect<void, DurableExecutionError, S2Client> =>
      Effect.gen(function*() {
        const log = openLog(yield* ownerStream(parts.object, parts.key))
        // residency-independent: the durable append IS the resolution; the poke just
        // accelerates a locally-parked waiter (first-write-wins is enforced by the fold).
        yield* log.append({ _tag: "SignalResolved", callId, name, value })
        yield* signalPort.poke(callId, name)
      }).pipe(Effect.withSpan("effect-s2-durable.resolveSignal", { attributes: { callId, name } }))

    const ownerKeys = (object: string): Effect.Effect<ReadonlyArray<string>, DurableExecutionError, S2Client> =>
      Effect.gen(function*() {
        // a keys-only StreamDb over the object's base path: list() is NAME enumeration
        // (listAllStreams + key-codec decode), NOT a content fold of the owner streams.
        const keysDb = StreamDb(`obj/${pathSegment(object)}`)({}, Schema.String)
        const segments = yield* keysDb.list()
        return segments.map(unPathSegment)
      }).pipe(
        Effect.mapError(toError("object.ownerKeys")),
        Effect.withSpan("effect-s2-durable.object.ownerKeys", { attributes: { object } }),
      )

    return { admit, status, drain, resolveSignal, ownerKeys }
  })

export class InvocationStore extends Context.Service<InvocationStore, InvocationStoreApi>()(
  "effect-s2-durable/InvocationStore",
) {
  static readonly layer = Layer.effect(InvocationStore, make())
}
