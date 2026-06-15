import {
  Array,
  Cause,
  Clock,
  Effect,
  Exit,
  HashMap,
  type Layer,
  Match,
  Option,
  Ref,
  Result,
  Schema,
  Stream,
  pipe,
} from "effect"
import { isSuspend, makeCtx, type Handler, type Suspend } from "./context.ts"
import { deterministicLayers } from "./determinism.ts"
import { CodecError, LostLeaseError, S2Error, StepFailure, type WfError } from "./errors.ts"
import { fold, type Journal } from "./journal.ts"
import {
  Awakeable,
  AwakeableDone,
  Completed,
  Ok,
  Seed,
  Snapshot,
  TimerFired,
  TimerSet,
  decodeRecord,
  encodeRecord,
  encodeRecords,
  type JournalRecord,
} from "./record.ts"
import { Dispatch } from "./dispatch.ts"
import { S2, S2Write } from "./s2.ts"
import { TimerHeap } from "./timerHeap.ts"

export type TickOutcome = "idle" | "suspended" | "completed"

export interface Worker<I, O> {
  /** Genesis: write the seed (clock/random + input) once, then poke. */
  readonly start: (execId: string, input: I) => Effect.Effect<void, WfError>
  /** Drive one execution forward by one lease+fold+run cycle (one StateMachine step). */
  readonly tick: (execId: string) => Effect.Effect<TickOutcome, WfError>
  /** Resolve an `awakeable` by appending to the unfenced inbox, then poke. */
  readonly resolveEvent: (
    execId: string,
    name: string,
    value: unknown,
  ) => Effect.Effect<void, S2Error | CodecError>
  /** Block until the execution records a `Completed`, returning its result. */
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

class InboxMessage extends Schema.Class<InboxMessage>("InboxMessage")({
  name: Schema.String,
  value: Schema.Unknown,
}) {}

const inboxCodec = Schema.fromJsonString(InboxMessage)
const encodeInboxJson = Schema.encodeEffect(inboxCodec)
const decodeInboxJson = Schema.decodeEffect(inboxCodec)
const encoder = new TextEncoder()
const decoder = new TextDecoder()
const toCodecError = (cause: unknown): CodecError =>
  new CodecError({ details: "inbox message codec failure", cause })

const encodeInbox = (m: InboxMessage): Effect.Effect<Uint8Array, CodecError> =>
  encodeInboxJson(m).pipe(Effect.map((s) => encoder.encode(s)), Effect.mapError(toCodecError))
const decodeInbox = (bytes: Uint8Array): Effect.Effect<InboxMessage, CodecError> =>
  decodeInboxJson(decoder.decode(bytes)).pipe(Effect.mapError(toCodecError))

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
    const epochRef = yield* Ref.make(wallNow())

    /**
     * Acquire a lease strictly greater than the stream's current fence. Seeding
     * the epoch from the wall clock alone is *not* enough: two workers started in
     * the same millisecond (a fast restart, or deploy overlap) collide, and a
     * dead worker's higher token would lock out the newcomer. Reading the
     * journal's fence makes lease issuance monotonic across restarts — the journal
     * itself is the coordination point (Restate leadership ≈ Bifrost sealing).
     */
    const acquireLease = (stream: string): Effect.Effect<string, S2Error> =>
      Effect.gen(function* () {
        const current = yield* s2.checkFence(stream)
        const currentEpoch = Option.match(Option.fromNullOr(current), {
          onNone: () => 0,
          onSome: (token) => Number(token),
        })
        const local = yield* Ref.getAndUpdate(epochRef, (n) => n + 1)
        const epoch = Math.max(local, currentEpoch + 1)
        const lease = String(epoch).padStart(20, "0")
        yield* s2.fence(stream, lease)
        return lease
      })

    const foldStream = (execId: string): Effect.Effect<Journal, WfError> =>
      fold(s2.read(wf(execId), 0n))

    const commitWrites = (
      execId: string,
      lease: string,
      matchSeqNum: bigint,
      writes: ReadonlyArray<S2Write>,
    ): Effect.Effect<{ readonly tail: bigint }, LostLeaseError | S2Error> =>
      Effect.catchTag(
        s2.append(wf(execId), writes, { fencingToken: lease, matchSeqNum }),
        "AppendCondFailed",
        (e) =>
          Effect.fail(
            Match.value(e.reason).pipe(
              Match.when("fence-mismatch", () => new LostLeaseError({ execId, lease })),
              Match.orElse(
                () =>
                  new S2Error({
                    operation: "append",
                    stream: wf(execId),
                    details: `unexpected position-taken at ${matchSeqNum} (tail=${e.actualSeqNum})`,
                  }),
              ),
            ),
          ),
      )

    const appendUnderLease = (
      execId: string,
      lease: string,
      matchSeqNum: bigint,
      recs: ReadonlyArray<JournalRecord>,
    ): Effect.Effect<{ readonly tail: bigint }, LostLeaseError | S2Error | CodecError> =>
      encodeRecords(recs).pipe(
        Effect.flatMap((bytes) =>
          commitWrites(execId, lease, matchSeqNum, bytes.map((body) => S2Write.Record({ body }))),
        ),
      )

    const readInbox = (execId: string): Effect.Effect<ReadonlyArray<InboxMessage>, WfError> =>
      s2.read(inbox(execId), 0n).pipe(Stream.mapEffect((r) => decodeInbox(r.data)), Stream.runCollect)

    const ops = (journal: Journal): ReadonlyArray<JournalRecord> =>
      Array.fromIterable(HashMap.values(journal.byName))

    /** Fire elapsed timers + fold matching inbox messages into the journal. */
    const reconcile = (
      execId: string,
      lease: string,
      journal: Journal,
    ): Effect.Effect<Journal, WfError> =>
      Effect.gen(function* () {
        const now = wallNow()
        const all = ops(journal)
        const firedTimers: ReadonlyArray<JournalRecord> = pipe(
          all,
          Array.filter(Schema.is(TimerSet)),
          Array.filter((t) => t.fireAt <= now),
          Array.map((t) => new TimerFired({ name: t.name })),
        )
        const pending = pipe(all, Array.filter(Schema.is(Awakeable)))
        const resolved: ReadonlyArray<JournalRecord> =
          pending.length === 0
            ? []
            : yield* readInbox(execId).pipe(
                Effect.map((messages) =>
                  pipe(
                    pending,
                    Array.flatMap((aw) =>
                      Option.match(Array.findFirst(messages, (m) => m.name === aw.name), {
                        onNone: () => [],
                        onSome: (m) => [new AwakeableDone({ name: aw.name, value: m.value })],
                      }),
                    ),
                  ),
                ),
              )

        const toAppend = [...firedTimers, ...resolved]
        if (toAppend.length === 0) return journal
        // physical tail, not the fold: fence/trim command records consume seq
        // numbers and are filtered out of the fold, so the two can diverge.
        const tail = yield* s2.checkTail(wf(execId))
        yield* appendUnderLease(execId, lease, tail, toAppend)
        return yield* foldStream(execId)
      })

    const armWaits = (execId: string, journal: Journal): Effect.Effect<void> =>
      Effect.forEach(
        ops(journal),
        (rec) =>
          Match.value(rec).pipe(
            Match.tag("TimerSet", (t) => {
              if (t.fireAt <= wallNow()) return dispatch.poke(execId)
              return timers.arm({ fireAt: t.fireAt, execId, name: t.name })
            }),
            Match.orElse(() => Effect.void),
          ),
        { discard: true },
      )

    /**
     * §4.2 — translate inbox writes (possibly from another process) into a host
     * poke. On suspend with pending awakeables, follow the inbox; when a matching
     * message lands — already present or arriving later — poke this host's
     * Dispatch so the next tick folds it into the journal. Detached, one-shot
     * (`take(1)`), torn down with the runtime on crash.
     */
    const watchInbox = (execId: string, journal: Journal): Effect.Effect<void> =>
      Effect.gen(function* () {
        const all = ops(journal)
        const pendingNames = pipe(
          all,
          Array.filter(Schema.is(Awakeable)),
          Array.map((a) => a.name),
        )
        if (pendingNames.length === 0) return
        yield* Effect.forkChild(
          s2.read(inbox(execId), 0n, { follow: true }).pipe(
            Stream.mapEffect((r) => decodeInbox(r.data)),
            Stream.filter((m) => Array.contains(pendingNames, m.name)),
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
      seed: Seed,
    ): Effect.Effect<TickOutcome, WfError> =>
      Effect.gen(function* () {
        const tailRef = yield* Ref.make(yield* s2.checkTail(wf(execId)))
        const ctx = makeCtx({ s2, stream: wf(execId), execId, lease, journal, tailRef, wallNow })
        const exit = yield* config.handler(ctx, journal.input as I).pipe(
          Effect.provide(deterministicLayers(seed)),
          Effect.provide(config.handlerLayer),
          Effect.exit,
        )
        if (Exit.isSuccess(exit)) {
          const tail = yield* Ref.get(tailRef)
          yield* appendUnderLease(execId, lease, tail, [
            new Completed({ outcome: new Ok({ value: exit.value }) }),
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
        if (Option.isSome(j0.completed)) return "completed" as const
        return yield* Option.match(j0.seed, {
          onNone: () => Effect.succeed("idle" as const),
          onSome: (seed) =>
            Effect.gen(function* () {
              const journal = yield* reconcile(execId, lease, j0)
              if (Option.isSome(journal.completed)) return "completed" as const
              return yield* runHandler(execId, lease, journal, seed)
            }),
        })
      })

    const start = (execId: string, input: I): Effect.Effect<void, WfError> =>
      Effect.gen(function* () {
        const lease = yield* acquireLease(wf(execId))
        // Genesis = no seed yet. We can't key off the physical tail because
        // acquireLease has already appended a fence command record.
        const journal = yield* foldStream(execId)
        yield* Option.match(journal.seed, {
          onSome: () => Effect.void,
          onNone: () =>
            Effect.gen(function* () {
              const seed = new Seed({
                epochMillis: wallNow(),
                random: Math.floor(Math.random() * 0x100000000),
                input,
              })
              const tail = yield* s2.checkTail(wf(execId))
              yield* appendUnderLease(execId, lease, tail, [seed])
            }),
        })
        yield* dispatch.poke(execId)
      })

    const resolveEvent = (
      execId: string,
      name: string,
      value: unknown,
    ): Effect.Effect<void, S2Error | CodecError> =>
      Effect.gen(function* () {
        const bytes = yield* encodeInbox(new InboxMessage({ name, value }))
        yield* Effect.catchTag(
          s2.append(inbox(execId), [S2Write.Record({ body: bytes })]),
          "AppendCondFailed",
          (e) => Effect.fail(new S2Error({ operation: "append", stream: inbox(execId), details: e.reason })),
        )
        yield* dispatch.poke(execId)
      })

    const awaitResult = (execId: string): Effect.Effect<O, WfError> =>
      Effect.gen(function* () {
        const head = yield* s2.read(wf(execId), 0n, { follow: true }).pipe(
          Stream.mapEffect((r) => decodeRecord(r.data)),
          Stream.filter(Schema.is(Completed)),
          Stream.runHead,
        )
        return yield* Option.match(head, {
          onNone: () =>
            Effect.fail(
              new S2Error({ operation: "read", stream: wf(execId), details: "stream ended before completion" }),
            ),
          onSome: (rec) =>
            Match.value(rec.outcome).pipe(
              Match.tag("Ok", (o) => Effect.succeed(o.value as O)),
              Match.tag("Err", (e) => Effect.fail(new StepFailure({ name: "<completed>", error: e.error }))),
              Match.exhaustive,
            ),
        })
      })

    const boot = (execIds: ReadonlyArray<string>): Effect.Effect<void> =>
      Effect.forEach(execIds, (execId) => dispatch.poke(execId), { discard: true })

    const snapshot = (execId: string): Effect.Effect<void, WfError> =>
      Effect.gen(function* () {
        const lease = yield* acquireLease(wf(execId))
        const journal = yield* foldStream(execId)
        const cursor = yield* s2.checkTail(wf(execId))
        const snap = new Snapshot({
          covers: Number(cursor),
          records: Array.fromIterable(HashMap.values(journal.byName)),
          seed: Option.getOrElse(journal.seed, () => null),
          input: journal.input,
        })
        const snapBytes = yield* encodeRecord(snap)
        // S2's single-record snapshot recipe: one atomic batch at match_seq_num=cursor
        // — a trim command (head advances to the snapshot) and the snapshot record,
        // durable together. Trim lands at `cursor`, snapshot at `cursor+1`; trimming
        // below `cursor+1` leaves the snapshot as the new head.
        yield* commitWrites(execId, lease, cursor, [
          S2Write.Trim({ upTo: cursor + 1n }),
          S2Write.Record({ body: snapBytes }),
        ])
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
