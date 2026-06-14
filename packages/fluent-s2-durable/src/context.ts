import { Cause, Duration, Effect, Exit, Option, Ref, type Schema } from "effect"
import {
  DivergenceError,
  LostLeaseError,
  S2Error,
  StepFailure,
  type AppendCondFailed,
  type WfError,
} from "./errors.ts"
import { fold, type Journal } from "./journal.ts"
import { encodeRecords, recordSignature, type JournalRecord, type StepOutcome } from "./record.ts"
import type { S2Service } from "./s2.ts"

/**
 * §6.3 — the suspend signal. Raised as a *defect* (`Effect.die`) so user-level
 * `catchAll` cannot intercept it (Q4); the host catches it via `Effect.catchDefect`.
 * Carries the records to schedule (may be empty when the wait is already recorded
 * and only needs re-arming by the host from the journal).
 */
export class Suspend {
  readonly _tag = "Suspend"
  constructor(readonly scheduled: ReadonlyArray<JournalRecord>) {}
}

export const isSuspend = (u: unknown): u is Suspend =>
  typeof u === "object" && u !== null && (u as { readonly _tag?: unknown })._tag === "Suspend"

/**
 * Raise a suspend. It travels as a *defect* (not a domain error) on purpose, so
 * user-level `catchAll` cannot intercept it and break resume (SDD Q4).
 */
const suspend = (scheduled: ReadonlyArray<JournalRecord>): Effect.Effect<never> =>
  // eslint-disable-next-line no-restricted-syntax -- intentional control-flow defect, see above
  Effect.die(new Suspend(scheduled))

export interface Ctx {
  readonly run: <A, E, R>(
    name: string,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | WfError, R>
  readonly sleep: (name: string, duration: Duration.Duration) => Effect.Effect<void, WfError>
  readonly waitForEvent: <A>(
    name: string,
    opts?: { readonly schema?: Schema.Schema<A> },
  ) => Effect.Effect<A, WfError>
}

export type Handler<I, O, R = never> = (ctx: Ctx, input: I) => Effect.Effect<O, WfError, R>

export interface CtxDeps {
  readonly s2: S2Service
  readonly stream: string
  readonly execId: string
  readonly lease: string
  readonly journal: Journal
  readonly opRef: Ref.Ref<number>
  readonly tailRef: Ref.Ref<bigint>
  /** real wall-clock, used only to stamp durable timers (journaled once, never replayed). */
  readonly wallNow: () => number
}

const nextOp = (deps: CtxDeps): Effect.Effect<number> =>
  Ref.getAndUpdate(deps.opRef, (n) => n + 1)

/** JSON round-trip a typed error so it survives the journal (lossy: classes flatten). */
const serializeError = (error: unknown): unknown => {
  const cloned: unknown = JSON.parse(JSON.stringify(error) ?? "null")
  return cloned
}

const outcomeToEffect = <A>(name: string, outcome: StepOutcome): Effect.Effect<A, StepFailure> =>
  outcome._tag === "ok"
    ? Effect.succeed(outcome.value as A)
    : Effect.fail(new StepFailure({ name, error: outcome.error }))

/** Append at the live edge under fence + match_seq_num; advance the local tail. */
const appendLive = (
  deps: CtxDeps,
  recs: ReadonlyArray<JournalRecord>,
): Effect.Effect<void, LostLeaseError | S2Error | AppendCondFailed> =>
  Effect.gen(function* () {
    const tail = yield* Ref.get(deps.tailRef)
    const res = yield* Effect.catchTag(
      deps.s2.append(deps.stream, encodeRecords(recs), {
        fencingToken: deps.lease,
        matchSeqNum: tail,
      }),
      "AppendCondFailed",
      (e) =>
        Effect.fail(
          e.reason === "fence-mismatch"
            ? new LostLeaseError({ execId: deps.execId, lease: deps.lease })
            : e,
        ),
    )
    yield* Ref.set(deps.tailRef, res.tail)
  })

/** §6.2 idempotent resume: a position-taken append means the record already landed. */
const reloadOpOutcome = <A>(
  deps: CtxDeps,
  op: number,
  name: string,
): Effect.Effect<A, WfError> =>
  Effect.gen(function* () {
    const j = yield* fold(deps.s2.read(deps.stream, 0n))
    const rec = j.byOp.get(op)
    if (rec !== undefined && rec.kind === "step" && rec.name === name) {
      return yield* outcomeToEffect<A>(name, rec.outcome)
    }
    return yield* Effect.fail(
      new S2Error({
        operation: "read",
        stream: deps.stream,
        details: `position-taken but no step at op ${op}`,
      }),
    )
  })

const divergence = (deps: CtxDeps, op: number, expected: string, actual: string): Effect.Effect<never> =>
  // eslint-disable-next-line no-restricted-syntax -- divergence is a loud defect, not a domain failure (AC-2)
  Effect.die(new DivergenceError({ execId: deps.execId, op, expected, actual }))

const runStep = <A, E, R>(
  deps: CtxDeps,
  name: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | WfError, R> =>
  Effect.gen(function* () {
    const op = yield* nextOp(deps)
    const existing = deps.journal.byOp.get(op)
    if (existing !== undefined) {
      return existing.kind === "step" && existing.name === name
        ? yield* outcomeToEffect<A>(name, existing.outcome)
        : yield* divergence(deps, op, recordSignature(existing), `step:${name}`)
    }
    // live edge: run the effect exactly once, then journal its outcome.
    const exit = yield* Effect.exit(effect)
    return yield* Exit.match(exit, {
      onSuccess: (value: A) =>
        appendLive(deps, [{ kind: "step", op, name, outcome: { _tag: "ok", value } }]).pipe(
          Effect.catchTag("AppendCondFailed", () => reloadOpOutcome<A>(deps, op, name).pipe(Effect.asVoid)),
          Effect.as(value),
        ),
      onFailure: (cause: Cause.Cause<E>) =>
        Option.match(Cause.findErrorOption(cause), {
          onNone: () => Effect.failCause(cause),
          onSome: (error) =>
            appendLive(deps, [
              { kind: "step", op, name, outcome: { _tag: "error", error: serializeError(error) } },
            ]).pipe(
              Effect.catchTag("AppendCondFailed", () => Effect.void),
              Effect.andThen(Effect.failCause(cause)),
            ),
        }),
    })
  })

const sleepStep = (
  deps: CtxDeps,
  name: string,
  duration: Duration.Duration,
): Effect.Effect<void, WfError> =>
  Effect.gen(function* () {
    const op = yield* nextOp(deps)
    const existing = deps.journal.byOp.get(op)
    if (existing?.kind === "timer-fired") return
    if (existing?.kind === "timer-set") return yield* suspend([])
    if (existing !== undefined) {
      return yield* divergence(deps, op, recordSignature(existing), `timer-set:${name}`)
    }
    const fireAt = deps.wallNow() + Duration.toMillis(duration)
    return yield* suspend([{ kind: "timer-set", op, name, fireAt }])
  })

const waitStep = <A>(
  deps: CtxDeps,
  name: string,
): Effect.Effect<A, WfError> =>
  Effect.gen(function* () {
    const op = yield* nextOp(deps)
    const existing = deps.journal.byOp.get(op)
    if (existing?.kind === "awakeable-done") return existing.value as A
    if (existing?.kind === "awakeable") return yield* suspend([])
    if (existing !== undefined) {
      return yield* divergence(deps, op, recordSignature(existing), `awakeable:${name}`)
    }
    const id = `${deps.execId}#${op}`
    return yield* suspend([{ kind: "awakeable", op, name, id }])
  })

export const makeCtx = (deps: CtxDeps): Ctx => ({
  run: (name, effect) => runStep(deps, name, effect),
  sleep: (name, duration) => sleepStep(deps, name, duration),
  waitForEvent: (name) => waitStep(deps, name),
})
