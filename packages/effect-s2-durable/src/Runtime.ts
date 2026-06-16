import {
  Cause,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  HashMap,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  type Scope,
} from "effect"
import { S2Client } from "effect-s2"
import { DurableExecutionError } from "./errors.ts"
import { ExecutionId, RosterDb, WorkflowDb } from "./schema.ts"
import type { AnyHandler, Handler, RetryPolicy, RunOptions } from "./types.ts"

/** The opened per-execution db (success type of `WorkflowDb.open`). */
type WfDb = Effect.Success<ReturnType<typeof WorkflowDb.open>>

/** The active invocation a free primitive (`run`/`sleep`/…) operates on. */
interface Invocation {
  readonly executionId: string
  readonly handlerName: string
  readonly db: WfDb
  readonly inputEncoded: unknown
}

/**
 * The active-invocation slot — a `Context.Reference` (default `None`), so it never
 * surfaces in user `R`. The runtime overrides it around a handler body; the free
 * primitives read it. This is the "active runtime slot" the free surface delegates
 * to (see DESIGN "Free primitive surface"), kept internal — no public escape hatch.
 */
const ActiveInvocation = Context.Reference<Option.Option<Invocation>>(
  "effect-s2-durable/ActiveInvocation",
  { defaultValue: () => Option.none() },
)

/** An in-process owned execution: the live fiber + a waiter for its exit. */
interface RunningEntry {
  readonly fiber: Fiber.Fiber<unknown, unknown>
  readonly deferred: Deferred.Deferred<Exit.Exit<unknown, unknown>>
}

const toError = (operation: string) => (cause: unknown): DurableExecutionError =>
  new DurableExecutionError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
    cause,
  })

const decode = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  encoded: unknown,
): Effect.Effect<A, DurableExecutionError> =>
  Schema.decodeUnknownEffect(schema)(encoded).pipe(Effect.mapError(toError("decode")))

const encode = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  value: A,
): Effect.Effect<I, DurableExecutionError> =>
  Schema.encodeUnknownEffect(schema)(value).pipe(Effect.mapError(toError("encode")))

const scheduleOf = (policy: RetryPolicy): Schedule.Schedule<Duration.Duration> =>
  Schedule.exponential(policy.initialInterval ?? Duration.millis(100), policy.intervalFactor ?? 2)

/** The public engine surface (host ops) plus the primitive ops the free functions delegate to. */
export interface DurableExecutionRuntimeApi {
  /** Genesis + fork: persist the execution + roster rows, then run the handler. */
  readonly submit: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
    input: I,
  ) => Effect.Effect<void, DurableExecutionError, R>
  /** Block until the execution finishes; return its decoded output (or fail). */
  readonly attach: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
  ) => Effect.Effect<O, DurableExecutionError>
  /** Non-blocking read of the completed output, if any. */
  readonly poll: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
  ) => Effect.Effect<Option.Option<O>, DurableExecutionError>
  /** The durable `run` step (delegated to by the `run` free primitive). */
  readonly runStep: <A, E, R, EncodedA, EncodedE>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ) => Effect.Effect<A, E | DurableExecutionError, R>
  /** The decoded handler request (delegated to by the `handlerRequest` free primitive). */
  readonly handlerRequest: <A, I>(
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
}

export class DurableExecutionRuntime
  extends Context.Service<DurableExecutionRuntime, DurableExecutionRuntimeApi>()("DurableExecutionRuntime")
{
  /** The S2-backed runtime layer. Requires an `S2Client`; owns its fiber scope. */
  static get layer(): Layer.Layer<DurableExecutionRuntime, never, S2Client> {
    return Layer.effect(DurableExecutionRuntime)(makeRuntime)
  }
}

const makeRuntime: Effect.Effect<DurableExecutionRuntimeApi, never, S2Client | Scope.Scope> = Effect.gen(function*() {
  const client = yield* S2Client
  // The layer's scope IS the engine's long-lived scope; handler/timer fibers fork
  // into it (SDD §B4) — never into a transient step scope.
  const engineScope = yield* Effect.scope
  const provideClient = <A, Err>(effect: Effect.Effect<A, Err, S2Client>): Effect.Effect<A, Err> =>
    Effect.provideService(effect, S2Client, client)

  // If the roster can't be opened the engine can't start — that's a defect.
  const roster = (yield* provideClient(RosterDb.open("global")).pipe(
    Effect.mapError(toError("open-roster")),
    Effect.orDie,
  )).roster
  const running = yield* Ref.make(HashMap.empty<string, RunningEntry>())

  const openWf = (executionId: string) => provideClient(WorkflowDb.open(ExecutionId.make(executionId)))

  // ── durable step (`run`) ───────────────────────────────────────────────────
  const runStep = <A, E, R, EncodedA, EncodedE>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ): Effect.Effect<A, E | DurableExecutionError, R> =>
    Effect.gen(function*() {
      const activeOpt = yield* ActiveInvocation
      if (Option.isNone(activeOpt)) {
        return yield* Effect.die(
          new DurableExecutionError({
            operation: "run",
            message: `run(${JSON.stringify(key)}) called outside an active handler`,
            cause: undefined,
          }),
        )
      }
      const active = activeOpt.value
      const stepKey = `${active.executionId}/${key}`
      const existing = yield* active.db.steps.get(stepKey).pipe(Effect.mapError(toError("run")))
      if (Option.isSome(existing)) {
        const row = existing.value
        // terminal fact already recorded — replay it, never re-run
        if (row.success) return options?.output ? yield* decode(options.output, row.value) : (row.value as A)
        const error = options?.error ? yield* decode(options.error, row.error) : (row.error as E)
        return yield* Effect.fail(error)
      }
      // no terminal fact: run (retry is pre-terminal), then record the outcome
      const attempted = options?.retry
        ? action.pipe(Effect.retry({ schedule: scheduleOf(options.retry), times: Math.max(0, options.retry.maxAttempts - 1) }))
        : action
      const outcome = yield* Effect.exit(attempted)
      if (Exit.isSuccess(outcome)) {
        const value = options?.output ? yield* encode(options.output, outcome.value) : outcome.value
        yield* active.db.steps.insert({ stepKey, success: true, value }).pipe(Effect.mapError(toError("run")))
        return outcome.value
      }
      // a typed failure (with an error schema) is a terminal StepFailed fact;
      // anything else stays non-terminal and is eligible to run again on replay.
      const failure = Cause.findErrorOption(outcome.cause)
      if (options?.error && Option.isSome(failure)) {
        const error = yield* encode(options.error, failure.value)
        yield* active.db.steps.insert({ stepKey, success: false, error }).pipe(Effect.mapError(toError("run")))
      }
      return yield* outcome
    })

  const handlerRequest = <A, I>(schema: Schema.Codec<A, I, never, never>): Effect.Effect<A, DurableExecutionError> =>
    Effect.gen(function*() {
      const activeOpt = yield* ActiveInvocation
      if (Option.isNone(activeOpt)) {
        return yield* Effect.die(
          new DurableExecutionError({
            operation: "handlerRequest",
            message: "handlerRequest called outside an active handler",
            cause: undefined,
          }),
        )
      }
      return yield* decode(schema, activeOpt.value.inputEncoded)
    })

  // ── completion (SDD §B6): the result must outlive the dropped stream ─────────
  const complete = (
    handler: AnyHandler,
    executionId: string,
    db: WfDb,
    exit: Exit.Exit<unknown, unknown>,
  ): Effect.Effect<void, never> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis
      if (Exit.isSuccess(exit)) {
        const result = yield* encode(handler.output as Schema.Codec<unknown, unknown, never, never>, exit.value)
        // 1. result → roster   2. await its ack (upsert blocks on ack)
        yield* roster.upsert({
          executionId,
          handlerName: handler.name,
          status: "completed",
          result,
          resultAcked: false,
          updatedMs: now,
        })
        // 3. drop the execution stream   4. mark resultAcked
        yield* db.drop
        yield* roster.upsert({
          executionId,
          handlerName: handler.name,
          status: "completed",
          result,
          resultAcked: true,
          updatedMs: now,
        })
      } else {
        yield* roster.upsert({
          executionId,
          handlerName: handler.name,
          status: "failed",
          error: Cause.pretty(exit.cause),
          resultAcked: true,
          updatedMs: now,
        })
        yield* db.drop
      }
      yield* Ref.update(running, HashMap.remove(executionId))
    }).pipe(Effect.mapError(toError("complete")), Effect.orDie)

  const submit = <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
    input: I,
  ): Effect.Effect<void, DurableExecutionError, R> =>
    Effect.gen(function*() {
      const live = yield* Ref.get(running)
      if (HashMap.has(live, executionId)) return // already owned — idempotent

      const db = yield* openWf(executionId).pipe(Effect.mapError(toError("submit")))
      const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
      const now = yield* Clock.currentTimeMillis

      yield* db.executions.insertOrGet({
        executionId,
        handlerName: handler.name,
        input: inputEncoded,
        status: "running",
        suspended: false,
      }).pipe(Effect.mapError(toError("submit")))
      yield* roster.upsert({ executionId, handlerName: handler.name, status: "running", updatedMs: now }).pipe(
        Effect.mapError(toError("submit")),
      )

      const deferred = yield* Deferred.make<Exit.Exit<unknown, unknown>>()
      const invocation: Invocation = { executionId, handlerName: handler.name, db, inputEncoded }
      const body: Effect.Effect<boolean, never, R> = handler.program.pipe(
        Effect.provideService(DurableExecutionRuntime, api),
        Effect.provideService(ActiveInvocation, Option.some(invocation)),
        Effect.exit,
        Effect.flatMap((exit) =>
          complete(handler as AnyHandler, executionId, db, exit).pipe(
            Effect.flatMap(() => Deferred.succeed(deferred, exit)),
          )
        ),
      )
      const fiber = yield* Effect.forkIn(body, engineScope)
      const entry: RunningEntry = { fiber, deferred }
      yield* Ref.update(running, HashMap.set(executionId, entry))
    })

  const attach = <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
  ): Effect.Effect<O, DurableExecutionError> =>
    Effect.gen(function*() {
      const live = yield* Ref.get(running)
      const entry = HashMap.get(live, executionId)
      if (Option.isSome(entry)) {
        const exit = yield* Deferred.await(entry.value.deferred)
        if (Exit.isSuccess(exit)) return exit.value as O
        return yield* Effect.fail(
          new DurableExecutionError({ operation: "attach", message: "execution failed", cause: exit.cause }),
        )
      }
      const row = yield* roster.get(executionId).pipe(Effect.mapError(toError("attach")))
      if (Option.isNone(row)) {
        return yield* Effect.fail(
          new DurableExecutionError({ operation: "attach", message: `unknown execution: ${executionId}`, cause: undefined }),
        )
      }
      if (row.value.status === "completed") {
        return yield* decode(handler.output as Schema.Codec<O, unknown, never, never>, row.value.result)
      }
      if (row.value.status === "failed") {
        return yield* Effect.fail(
          new DurableExecutionError({ operation: "attach", message: row.value.error ?? "execution failed", cause: undefined }),
        )
      }
      return yield* Effect.fail(
        new DurableExecutionError({
          operation: "attach",
          message: `execution ${executionId} is ${row.value.status} with no local waiter`,
          cause: undefined,
        }),
      )
    })

  const poll = <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
  ): Effect.Effect<Option.Option<O>, DurableExecutionError> =>
    roster.get(executionId).pipe(
      Effect.mapError(toError("poll")),
      Effect.flatMap((row) =>
        Option.isSome(row) && row.value.status === "completed"
          ? decode(handler.output as Schema.Codec<O, unknown, never, never>, row.value.result).pipe(Effect.map(Option.some))
          : Effect.succeedNone
      ),
    )

  const api: DurableExecutionRuntimeApi = { submit, attach, poll, runStep, handlerRequest }
  return api
})
