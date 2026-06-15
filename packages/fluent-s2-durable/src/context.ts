import { AppendRecord } from "@s2-dev/streamstore"
import { Cause, Duration, Effect, Exit, HashMap, Match, Option, Ref } from "effect"
import {
  DivergenceError,
  LostLeaseError,
  S2Error,
  StepFailure,
  type AppendCondFailed,
  type CodecError,
  type WfError,
} from "./errors.ts"
import { fold, type Journal } from "./journal.ts"
import type { S2Service } from "./s2.ts"
import {
  Awakeable,
  Err,
  Ok,
  Step,
  TimerSet,
  encodeRecords,
  recordSignature,
  type JournalRecord,
  type OpRecord,
  type StepOutcome,
} from "./record.ts"

/**
 * §6.3 — the suspend signal. Raised as a *defect* (`Effect.die`) so user-level
 * `catchAll` cannot intercept it (Q4); the host catches it via `Cause.findDefect`.
 * Carries the records to schedule (empty when the wait is already recorded and
 * only needs re-arming by the host from the journal).
 */
export class Suspend {
  readonly _tag = "Suspend"
  constructor(readonly scheduled: ReadonlyArray<JournalRecord>) {}
}

export const isSuspend = (u: unknown): u is Suspend =>
  Match.value(u).pipe(
    Match.when(Match.record, (r) => (r as { readonly _tag?: unknown })._tag === "Suspend"),
    Match.orElse(() => false),
  )

/**
 * Raise a suspend. Travels as a *defect*, not a domain error, on purpose — so
 * user-level `catchAll` cannot intercept it and break resume (SDD Q4).
 */
const suspend = (scheduled: ReadonlyArray<JournalRecord>): Effect.Effect<never> =>
  // eslint-disable-next-line no-restricted-syntax -- intentional control-flow defect, see above
  Effect.die(new Suspend(scheduled))

/**
 * The durable-execution primitive surface (Restate's SDK `ctx`). `run`/`sleep`/
 * `awakeable` are journal-backed; `state`/`call`/`send` are typed seams the
 * user-facing combinator layer (restate-fluent) will grow onto — not built in the
 * spike (SDD non-goals). Each primitive returns a plain `Effect`, so the
 * structured-concurrency layer is just Effect (`Effect.all`/`fork`/`race`).
 */
export interface Ctx {
  readonly run: <A, E, R>(
    name: string,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | WfError, R>
  readonly sleep: (name: string, duration: Duration.Duration) => Effect.Effect<void, WfError>
  readonly awakeable: <A>(name: string) => Effect.Effect<A, WfError>
  /** seam: virtual-object K/V (SDD non-goal) */
  readonly state: {
    readonly get: <A>(key: string) => Effect.Effect<Option.Option<A>, WfError>
    readonly set: (key: string, value: unknown) => Effect.Effect<void, WfError>
    readonly clear: (key: string) => Effect.Effect<void, WfError>
  }
  /** seam: durable RPC (SDD non-goal) */
  readonly call: <A>(target: string, input: unknown) => Effect.Effect<A, WfError>
  readonly send: (target: string, input: unknown) => Effect.Effect<void, WfError>
}

export type Handler<I, O, R = never> = (ctx: Ctx, input: I) => Effect.Effect<O, WfError, R>

export interface CtxDeps {
  readonly s2: S2Service
  readonly stream: string
  readonly execId: string
  readonly lease: string
  readonly journal: Journal
  readonly tailRef: Ref.Ref<number>
  /** real wall-clock, used only to stamp durable timers (journaled once, never replayed). */
  readonly wallNow: () => number
}

const notImplemented = (feature: string): Effect.Effect<never> =>
  // eslint-disable-next-line no-restricted-syntax -- unbuilt seam; calling it is a programming error
  Effect.die(`ctx.${feature} is a seam, not implemented in the spike (SDD non-goal)`)

const outcomeToEffect = <A>(name: string, outcome: StepOutcome): Effect.Effect<A, StepFailure> =>
  Match.value(outcome).pipe(
    Match.tag("Ok", (o) => Effect.succeed(o.value as A)),
    Match.tag("Err", (e) => Effect.fail(new StepFailure({ name, error: e.error }))),
    Match.exhaustive,
  )

const divergence = (
  deps: CtxDeps,
  name: string,
  existing: OpRecord,
  issuing: string,
): Effect.Effect<never> =>
  // eslint-disable-next-line no-restricted-syntax -- divergence is a loud defect, not a domain failure (AC-2)
  Effect.die(
    new DivergenceError({ execId: deps.execId, op: name, expected: recordSignature(existing), actual: issuing }),
  )

/** Append at the live edge under fence + match_seq_num; advance the local tail. */
const appendLive = (
  deps: CtxDeps,
  recs: ReadonlyArray<JournalRecord>,
): Effect.Effect<void, LostLeaseError | S2Error | AppendCondFailed | CodecError> =>
  Effect.gen(function* () {
    const tail = yield* Ref.get(deps.tailRef)
    const bytes = yield* encodeRecords(recs)
    const res = yield* Effect.catchTag(
      deps.s2.append(
        deps.stream,
        bytes.map((body) => AppendRecord.bytes({ body })),
        { fencingToken: deps.lease, matchSeqNum: tail },
      ),
      "AppendCondFailed",
      (e) =>
        Match.value(e.reason).pipe(
          Match.when("fence-mismatch", () =>
            Effect.fail(new LostLeaseError({ execId: deps.execId, lease: deps.lease })),
          ),
          Match.orElse(() => Effect.fail(e)),
        ),
    )
    yield* Ref.set(deps.tailRef, res.tail)
  })

/** §6.2 idempotent resume: a position-taken append means the record already landed. */
const reloadStep = <A>(deps: CtxDeps, name: string): Effect.Effect<A, WfError> =>
  Effect.gen(function* () {
    const journal = yield* fold(deps.s2.read(deps.stream, 0))
    return yield* Option.match(HashMap.get(journal.byName, name), {
      onNone: () =>
        Effect.fail(
          new S2Error({
            operation: "read",
            stream: deps.stream,
            details: `position-taken but no record named ${name}`,
          }),
        ),
      onSome: (rec) =>
        Match.value(rec).pipe(
          Match.tag("Step", (s) => outcomeToEffect<A>(s.name, s.outcome)),
          Match.orElse(() =>
            Effect.fail(
              new S2Error({ operation: "read", stream: deps.stream, details: `${name} is not a step` }),
            ),
          ),
        ),
    })
  })

const liveStep = <A, E, R>(deps: CtxDeps, name: string, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | WfError, R> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(effect)
    if (Exit.isSuccess(exit)) {
      // run-then-journal: the side effect happens exactly once, its result lands.
      yield* appendLive(deps, [new Step({ name, outcome: new Ok({ value: exit.value }) })]).pipe(
        Effect.catchTag("AppendCondFailed", () => reloadStep<A>(deps, name).pipe(Effect.asVoid)),
      )
      return exit.value
    }
    // A typed failure is journaled then re-raised; a defect/interrupt propagates as-is.
    return yield* Option.match(Cause.findErrorOption(exit.cause), {
      onNone: () => Effect.failCause(exit.cause),
      onSome: (error) =>
        appendLive(deps, [new Step({ name, outcome: new Err({ error }) })]).pipe(
          Effect.catchTag("AppendCondFailed", () => Effect.void),
          Effect.andThen(Effect.failCause(exit.cause)),
        ),
    })
  })

const runStep = <A, E, R>(deps: CtxDeps, name: string, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | WfError, R> =>
  Option.match(HashMap.get(deps.journal.byName, name), {
    onNone: () => liveStep(deps, name, effect),
    onSome: (rec) =>
      Match.value(rec).pipe(
        Match.tag("Step", (s) => outcomeToEffect<A>(s.name, s.outcome)),
        Match.orElse((r) => divergence(deps, name, r, "Step")),
      ),
  })

const sleepStep = (
  deps: CtxDeps,
  name: string,
  duration: Duration.Duration,
): Effect.Effect<void, WfError> =>
  Option.match(HashMap.get(deps.journal.byName, name), {
    onNone: () =>
      suspend([new TimerSet({ name, fireAt: deps.wallNow() + Duration.toMillis(duration) })]),
    onSome: (rec) =>
      Match.value(rec).pipe(
        Match.tag("TimerFired", () => Effect.void),
        Match.tag("TimerSet", () => suspend([])),
        Match.orElse((r) => divergence(deps, name, r, "TimerSet")),
      ),
  })

const awakeableStep = <A>(deps: CtxDeps, name: string): Effect.Effect<A, WfError> =>
  Option.match(HashMap.get(deps.journal.byName, name), {
    onNone: () => suspend([new Awakeable({ name })]),
    onSome: (rec) =>
      Match.value(rec).pipe(
        Match.tag("AwakeableDone", (d) => Effect.succeed(d.value as A)),
        Match.tag("Awakeable", () => suspend([])),
        Match.orElse((r) => divergence(deps, name, r, "Awakeable")),
      ),
  })

export const makeCtx = (deps: CtxDeps): Ctx => ({
  run: (name, effect) => runStep(deps, name, effect),
  sleep: (name, duration) => sleepStep(deps, name, duration),
  awakeable: (name) => awakeableStep(deps, name),
  state: {
    get: () => notImplemented("state.get"),
    set: () => notImplemented("state.set"),
    clear: () => notImplemented("state.clear"),
  },
  call: () => notImplemented("call"),
  send: () => notImplemented("send"),
})
