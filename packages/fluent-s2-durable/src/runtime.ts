import { Cause, Clock, Effect, Exit, type Layer, Option, Ref, Result, Stream } from "effect"
import { isSuspend, makeCtx, type Handler, type Suspend } from "./context.ts"
import { deterministicLayers } from "./determinism.ts"
import { LostLeaseError, S2Error, StepFailure, type WfError } from "./errors.ts"
import { fold, type Journal } from "./journal.ts"
import {
  decodeRecord,
  encodeRecords,
  type JournalRecord,
  type SeedData,
  type SnapshotState,
} from "./record.ts"
import { Dispatch } from "./dispatch.ts"
import { S2 } from "./s2.ts"
import { TimerHeap } from "./timerHeap.ts"

export type TickOutcome = "idle" | "suspended" | "completed"

export interface Worker<I, O> {
  /** Genesis: write the seed (clock/random + input) once, then poke. */
  readonly start: (execId: string, input: I) => Effect.Effect<void, WfError>
  /** Drive one execution forward by one lease+fold+run cycle. */
  readonly tick: (execId: string) => Effect.Effect<TickOutcome, WfError>
  /** Resolve a `waitForEvent` by appending to the unfenced inbox, then poke. */
  readonly resolveEvent: (
    execId: string,
    name: string,
    value: unknown,
  ) => Effect.Effect<void, S2Error>
  /** Block until the execution records a `completed`, returning its result. */
  readonly awaitResult: (execId: string) => Effect.Effect<O, WfError>
  /** Re-poke active executions after a (re)start so they fold + re-arm. */
  readonly boot: (execIds: ReadonlyArray<string>) => Effect.Effect<void>
  /** Snapshot-and-follow: checkpoint to head and trim history (M5). */
  readonly snapshot: (execId: string) => Effect.Effect<void, WfError>
  /** The host pump: forever { claim; tick }. Survives tick errors; stops on interrupt. */
  readonly runLoop: Effect.Effect<never, WfError>
}

export interface WorkerConfig<I, O, R> {
  readonly handler: Handler<I, O, R>
  readonly handlerLayer: Layer.Layer<R>
}

interface InboxMessage {
  readonly name: string
  readonly value: unknown
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const encodeInbox = (m: InboxMessage): Uint8Array => encoder.encode(JSON.stringify(m))
const decodeInbox = (bytes: Uint8Array): InboxMessage => JSON.parse(decoder.decode(bytes)) as InboxMessage

const wf = (execId: string): string => `wf/${execId}`
const inbox = (execId: string): string => `wf/${execId}/inbox`

export const make = <I, O, R>(
  config: WorkerConfig<I, O, R>,
): Effect.Effect<Worker<I, O>, never, S2 | Dispatch | TimerHeap> =>
  Effect.gen(function* () {
    const s2 = yield* Effect.service(S2)
    const dispatch = yield* Effect.service(Dispatch)
    const timers = yield* Effect.service(TimerHeap)
    // The host's *live* clock, captured before any deterministic Clock is provided
    // to a handler. `wallNow` stamps durable timers (journaled once, never
    // replayed) and seeds the epoch — real time, not the frozen replay clock.
    const hostClock = yield* Effect.service(Clock.Clock)
    const wallNow = (): number => hostClock.currentTimeMillisUnsafe()
    // process-unique, monotonic epoch base so a later-started worker fences out an older one.
    const epochRef = yield* Ref.make(wallNow())

    /**
     * Acquire a lease strictly greater than the stream's current fence. Seeding
     * the epoch from `Date.now()` alone is *not* enough: two workers started in
     * the same millisecond (e.g. a fast restart, or deploy overlap) collide, and
     * a dead worker's higher token would lock out the newcomer. Reading the
     * journal's fence makes lease issuance monotonic across restarts — the
     * journal itself is the coordination point.
     */
    const acquireLease = (stream: string): Effect.Effect<string, S2Error> =>
      Effect.gen(function* () {
        const current = yield* s2.checkFence(stream)
        const currentEpoch = current === null ? 0 : Number(current)
        const local = yield* Ref.getAndUpdate(epochRef, (n) => n + 1)
        const epoch = Math.max(local, currentEpoch + 1)
        const lease = String(epoch).padStart(20, "0")
        yield* s2.fence(stream, lease)
        return lease
      })

    const foldStream = (execId: string): Effect.Effect<Journal, WfError> =>
      fold(s2.read(wf(execId), 0n))

    const appendUnderLease = (
      execId: string,
      lease: string,
      matchSeqNum: bigint,
      recs: ReadonlyArray<JournalRecord>,
    ): Effect.Effect<{ readonly tail: bigint }, LostLeaseError | S2Error> =>
      Effect.catchTag(
        s2.append(wf(execId), encodeRecords(recs), { fencingToken: lease, matchSeqNum }),
        "AppendCondFailed",
        (e) =>
          Effect.fail(
            e.reason === "fence-mismatch"
              ? new LostLeaseError({ execId, lease })
              : new S2Error({
                  operation: "append",
                  stream: wf(execId),
                  details: `unexpected position-taken at ${matchSeqNum} (tail=${e.actualSeqNum})`,
                }),
          ),
      )

    const readInbox = (execId: string): Effect.Effect<ReadonlyArray<InboxMessage>, WfError> =>
      s2.read(inbox(execId), 0n).pipe(
        Stream.map((r) => decodeInbox(r.data)),
        Stream.runCollect,
      )

    /** Fire elapsed timers + fold matching inbox messages into the journal. */
    const reconcile = (
      execId: string,
      lease: string,
      journal: Journal,
    ): Effect.Effect<Journal, WfError> =>
      Effect.gen(function* () {
        const now = wallNow()
        const records = [...journal.byOp.values()]
        const firedTimers: ReadonlyArray<JournalRecord> = records
          .filter(
            (r): r is Extract<JournalRecord, { kind: "timer-set" }> =>
              r.kind === "timer-set" && r.fireAt <= now,
          )
          .map((r) => ({ kind: "timer-fired", op: r.op }))

        const pending = records.filter(
          (r): r is Extract<JournalRecord, { kind: "awakeable" }> => r.kind === "awakeable",
        )
        const resolved: ReadonlyArray<JournalRecord> =
          pending.length === 0
            ? []
            : yield* readInbox(execId).pipe(
                Effect.map((messages) => {
                  // match each pending awakeable to one unconsumed inbox message by name
                  const consumed = new Set<number>()
                  return pending.flatMap((aw) => {
                    const idx = messages.findIndex((m, i) => !consumed.has(i) && m.name === aw.name)
                    if (idx < 0) return []
                    consumed.add(idx)
                    return [{ kind: "awakeable-done" as const, op: aw.op, value: messages[idx]!.value }]
                  })
                }),
              )

        const toAppend = [...firedTimers, ...resolved]
        if (toAppend.length === 0) return journal
        yield* appendUnderLease(execId, lease, journal.tail, toAppend)
        return yield* foldStream(execId)
      })

    const armWaits = (execId: string, journal: Journal): Effect.Effect<void> =>
      Effect.forEach(
        [...journal.byOp.values()],
        (rec) =>
          rec.kind === "timer-set"
            ? rec.fireAt <= wallNow()
              ? dispatch.poke(execId)
              : timers.arm({ fireAt: rec.fireAt, execId, op: rec.op })
            : Effect.void,
        { discard: true },
      )

    /**
     * §4.2 — translate inbox writes (possibly from another process) into a host
     * poke. On suspend with pending awakeables, follow the inbox; when a matching
     * message lands — already present or arriving later — poke this host's
     * Dispatch so the next tick folds it into the journal. Replaying history
     * first means a pre-delivered event fires immediately. Detached, one-shot
     * (`take(1)`), and torn down with the runtime on crash.
     */
    const watchInbox = (execId: string, journal: Journal): Effect.Effect<void> =>
      Effect.gen(function* () {
        const pendingNames = new Set(
          [...journal.byOp.values()]
            .filter((r): r is Extract<JournalRecord, { kind: "awakeable" }> => r.kind === "awakeable")
            .map((r) => r.name),
        )
        if (pendingNames.size === 0) return
        yield* Effect.forkDetach(
          s2.read(inbox(execId), 0n, { follow: true }).pipe(
            Stream.map((r) => decodeInbox(r.data)),
            Stream.filter((m) => pendingNames.has(m.name)),
            Stream.take(1),
            Stream.runDrain,
            Effect.andThen(dispatch.poke(execId)),
            Effect.catchCause(() => Effect.void),
          ),
        )
      })

    const handleSuspend = (
      execId: string,
      lease: string,
      tailRef: Ref.Ref<bigint>,
      susp: Suspend,
    ): Effect.Effect<void, WfError> =>
      Effect.gen(function* () {
        if (susp.scheduled.length > 0) {
          const tail = yield* Ref.get(tailRef)
          yield* appendUnderLease(execId, lease, tail, susp.scheduled)
        }
        const journal = yield* foldStream(execId)
        yield* armWaits(execId, journal)
        yield* watchInbox(execId, journal)
      })

    const runHandler = (
      execId: string,
      lease: string,
      journal: Journal,
      seed: SeedData,
    ): Effect.Effect<TickOutcome, WfError> =>
      Effect.gen(function* () {
        const opRef = yield* Ref.make(0)
        const tailRef = yield* Ref.make(journal.tail)
        const ctx = makeCtx({ s2, stream: wf(execId), execId, lease, journal, opRef, tailRef, wallNow })
        const exit = yield* config.handler(ctx, journal.input as I).pipe(
          Effect.provide(deterministicLayers(seed)),
          Effect.provide(config.handlerLayer),
          Effect.exit,
        )
        if (Exit.isSuccess(exit)) {
          const tail = yield* Ref.get(tailRef)
          yield* appendUnderLease(execId, lease, tail, [
            { kind: "completed", outcome: { _tag: "ok", value: exit.value } },
          ])
          return "completed" as const
        }
        const defect = Cause.findDefect(exit.cause)
        if (Result.isSuccess(defect) && isSuspend(defect.success)) {
          yield* handleSuspend(execId, lease, tailRef, defect.success)
          return "suspended" as const
        }
        // divergence (defect), lost-lease / typed WfError (fail) — surface to the caller.
        return yield* Effect.failCause(exit.cause)
      })

    const tick = (execId: string): Effect.Effect<TickOutcome, WfError> =>
      Effect.gen(function* () {
        const lease = yield* acquireLease(wf(execId))
        const j0 = yield* foldStream(execId)
        if (j0.status === "completed") return "completed" as const
        const seed = j0.seed
        if (seed === null) return "idle" as const
        const journal = yield* reconcile(execId, lease, j0)
        if (journal.status === "completed") return "completed" as const
        return yield* runHandler(execId, lease, journal, seed)
      })

    const start = (execId: string, input: I): Effect.Effect<void, WfError> =>
      Effect.gen(function* () {
        const lease = yield* acquireLease(wf(execId))
        const tail = yield* s2.checkTail(wf(execId))
        if (tail === 0n) {
          const seed: SeedData = {
            epochMillis: wallNow(),
            random: Math.floor(Math.random() * 0x100000000),
          }
          yield* appendUnderLease(execId, lease, 0n, [{ kind: "seed", seed, input }])
        }
        yield* dispatch.poke(execId)
      })

    const resolveEvent = (
      execId: string,
      name: string,
      value: unknown,
    ): Effect.Effect<void, S2Error> =>
      Effect.catchTag(
        s2.append(inbox(execId), [encodeInbox({ name, value })]),
        "AppendCondFailed",
        (e) => Effect.fail(new S2Error({ operation: "append", stream: inbox(execId), details: e.reason })),
      ).pipe(Effect.andThen(dispatch.poke(execId)))

    const awaitResult = (execId: string): Effect.Effect<O, WfError> =>
      Effect.gen(function* () {
        const head = yield* s2.read(wf(execId), 0n, { follow: true }).pipe(
          Stream.mapEffect((r) => decodeRecord(r.data)),
          Stream.filter((rec) => rec.kind === "completed"),
          Stream.runHead,
        )
        if (Option.isNone(head)) {
          return yield* Effect.fail(
            new S2Error({ operation: "read", stream: wf(execId), details: "stream ended before completion" }),
          )
        }
        const rec = head.value
        if (rec.kind === "completed" && rec.outcome._tag === "ok") {
          return rec.outcome.value as O
        }
        return yield* Effect.fail(
          new StepFailure({ name: "<completed>", error: rec.kind === "completed" ? rec.outcome : rec }),
        )
      })

    const boot = (execIds: ReadonlyArray<string>): Effect.Effect<void> =>
      Effect.forEach(execIds, (execId) => dispatch.poke(execId), { discard: true })

    const snapshot = (execId: string): Effect.Effect<void, WfError> =>
      Effect.gen(function* () {
        const lease = yield* acquireLease(wf(execId))
        const journal = yield* foldStream(execId)
        const cursor = journal.tail
        const state: SnapshotState = {
          records: [...journal.byOp.values()],
          seed: journal.seed,
          input: journal.input,
        }
        // atomic-ish: snapshot record lands at `cursor`, then trim everything below it.
        yield* appendUnderLease(execId, lease, cursor, [
          { kind: "snapshot", covers: Number(cursor), state },
        ])
        yield* s2.trim(wf(execId), cursor)
      })

    // Stay alive across a bad tick (fold the journal, try again) but let
    // interrupts through so disposing the runtime actually stops the worker —
    // otherwise a "crashed" host keeps running and re-executes steps.
    const runLoop: Effect.Effect<never, WfError> = Effect.forever(
      dispatch.claim.pipe(
        Effect.flatMap((execId) =>
          tick(execId).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.void,
            ),
          ),
        ),
      ),
    )

    return { start, tick, resolveEvent, awaitResult, boot, snapshot, runLoop }
  })
