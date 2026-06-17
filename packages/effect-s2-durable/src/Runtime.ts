import {
  Cause,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  type Fiber,
  HashMap,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  type Scope,
  Semaphore,
} from "effect"
import { S2Client } from "effect-s2"
import type { AnyTable, RowOf, TableFacade } from "effect-s2-stream-db"
import { DurableExecutionError, durableError } from "./errors.ts"
import { ExecutionId, type ObjectInboxRow, ObjectStateDb, RosterDb, WorkflowDb } from "./schema.ts"
import type { Handler, RetryPolicy, RunOptions } from "./types.ts"

/** The opened per-execution db (success type of `WorkflowDb.open`). */
type WfDb = Effect.Success<ReturnType<typeof WorkflowDb.open>>

/** The opened per-(object,key) state db: the persistent state + the FIFO inbox. */
type ObjStateDb = Effect.Success<ReturnType<typeof ObjectStateDb.open>>

/** One queued (not-yet-completed) object invocation in a key's FIFO inbox. */
type InboxRow = RowOf<typeof ObjectInboxRow>

/**
 * The durable record store a `state(Table)` binding writes to: the active
 * execution's own stream for a service, or the persistent per-key `ObjectStateDb`
 * for a virtual-object method. Only the generic `table(...)` accessor is needed;
 * both stream-db instances satisfy it structurally.
 */
type StateStore = { readonly table: <Tbl extends AnyTable>(table: Tbl) => TableFacade<RowOf<Tbl>> }

/**
 * The shared per-(object,key) coordination. `state` is the one durable store +
 * inbox for the key (a single instance, so all methods share one materialized
 * fold + CAS tail). `mutex` guards enqueue/drainer-start atomicity; `draining`
 * is whether a serial drainer fiber is currently active for this key.
 */
interface ObjectScope {
  readonly state: ObjStateDb
  readonly mutex: Semaphore.Semaphore
  readonly draining: Ref.Ref<boolean>
}

/** The active invocation a free primitive (`run`/`sleep`/`state`/…) operates on. */
interface Invocation {
  readonly executionId: string
  readonly handlerName: string
  readonly db: WfDb
  /** Where `state(Table)` rows live: the execution stream (service) or per-key store (object). */
  readonly stateDb: StateStore
  /** This object method's `"name:key"`, or `undefined` for a plain service. */
  readonly objectKey: string | undefined
  readonly inputEncoded: unknown
  /** Monotonic per-activation counter for positionally-keyed `run` steps. */
  readonly runSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for journaled `state.get` reads (Option A). */
  readonly readSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for replay-stable `awakeable` ids. */
  readonly awakeSeq: Ref.Ref<number>
}

/**
 * The active-invocation slot — a `Context.Reference` (default `None`), so it never
 * surfaces in user `R`. The runtime overrides it around a handler body; the free
 * primitives read it. This is the "active runtime slot" the free surface delegates
 * to (DESIGN "Free primitive surface"), kept internal — no public escape hatch.
 */
const ActiveInvocation = Context.Reference<Option.Option<Invocation>>(
  "effect-s2-durable/ActiveInvocation",
  { defaultValue: () => Option.none() },
)

/** An in-process owned execution: the live fiber, a result waiter, and its db. */
interface RunningEntry {
  readonly fiber: Fiber.Fiber<unknown, unknown>
  readonly deferred: Deferred.Deferred<Exit.Exit<unknown, unknown>, DurableExecutionError>
  readonly invocation: Invocation
}

const toError = durableError

const fail = (operation: string, message: string): Effect.Effect<never, DurableExecutionError> =>
  Effect.fail(new DurableExecutionError({ operation, message, cause: undefined }))

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

/** The encoded value of a resolved `deferreds` row, if present. */
const resolvedValue = (row: Option.Option<{ readonly value?: unknown }>): Option.Option<unknown> =>
  Option.flatMap(row, (r) => Option.fromNullishOr(r.value))

/** The public engine surface (host ops) plus the primitive ops the free functions delegate to. */
export interface DurableExecutionRuntimeApi {
  /**
   * Genesis + fork: persist the execution + roster rows, then run the handler.
   * `objectKey` (a virtual object's `"name:key"`) routes `state(Table)` to the
   * persistent per-key store and serializes same-key methods (exclusive access).
   */
  readonly submit: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
    input: I,
    objectKey?: string,
  ) => Effect.Effect<void, DurableExecutionError, R>
  /** Block until the execution finishes; decode its output via `schema` (or fail). */
  readonly attach: <A, I>(
    executionId: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  /** Non-blocking read of the completed output, decoded via `schema`, if any. */
  readonly poll: <A, I>(
    executionId: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<Option.Option<A>, DurableExecutionError>
  /** The durable `run` step (delegated to by the `run` free primitive). */
  readonly runStep: <A, E, R, EncodedA, EncodedE>(
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ) => Effect.Effect<A, E | DurableExecutionError, R>
  /** The decoded handler request (delegated to by the `handlerRequest` free primitive). */
  readonly handlerRequest: <A, I>(
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  /** The durable timer (delegated to by the `sleep` free primitive). */
  readonly sleepStep: (name: string, duration: Duration.Duration) => Effect.Effect<void, DurableExecutionError>
  /** State ops (delegated to by the `state(Table)` binding's methods). */
  readonly stateGet: <Tbl extends AnyTable>(
    table: Tbl,
    key: string,
  ) => Effect.Effect<Option.Option<RowOf<Tbl>>, DurableExecutionError>
  readonly stateSet: <Tbl extends AnyTable>(table: Tbl, row: RowOf<Tbl>) => Effect.Effect<void, DurableExecutionError>
  readonly stateDelete: <Tbl extends AnyTable>(table: Tbl, key: string) => Effect.Effect<void, DurableExecutionError>
  /** Park until a named durable promise (signal/deferred/awakeable) is resolved. */
  readonly awaitDeferred: <A, I>(
    name: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  /** Resolve a named durable promise on the active execution (handler-side `deferred.resolve`). */
  readonly resolveLocal: <A, I>(
    name: string,
    schema: Schema.Codec<A, I, never, never>,
    value: A,
  ) => Effect.Effect<void, DurableExecutionError>
  /** Resolve a named durable promise on another execution (ingress `signal`/`awakeable`). */
  readonly resolveExternal: <A, I>(executionId: string, name: string, schema: Schema.Codec<A, I, never, never>, value: A) => Effect.Effect<void, DurableExecutionError>
  /** A fresh replay-stable awakeable id for the active execution. */
  readonly nextAwakeableId: Effect.Effect<string, DurableExecutionError>
}

export class DurableExecutionRuntime
  extends Context.Service<DurableExecutionRuntime, DurableExecutionRuntimeApi>()("DurableExecutionRuntime")
{
  /** The S2-backed runtime layer. Requires an `S2Client`; owns its fiber scope. */
  static layer(
    handlers: ReadonlyArray<RegisteredHandler> = [],
  ): Layer.Layer<DurableExecutionRuntime, DurableExecutionError, S2Client> {
    return Layer.effect(DurableExecutionRuntime)(makeRuntime(handlers))
  }
}

/** A handler the engine can recover by name (program + output schema; no unmet R/E). */
export type RegisteredHandler = Handler<unknown, unknown, never, never>

const makeRuntime = (
  handlers: ReadonlyArray<RegisteredHandler>,
): Effect.Effect<DurableExecutionRuntimeApi, DurableExecutionError, S2Client | Scope.Scope> =>
  Effect.gen(function*() {
    const client = yield* S2Client
    // The layer's scope IS the engine's long-lived scope; handler/timer fibers fork
    // into it (SDD §B4) — never into a transient step scope.
    const engineScope = yield* Effect.scope
    const provideClient = <A, Err>(effect: Effect.Effect<A, Err, S2Client>): Effect.Effect<A, Err> =>
      Effect.provideService(effect, S2Client, client)

    // handlerName → handler, the cold-start lookup for boot recovery.
    const registry = new Map<string, RegisteredHandler>(handlers.map((h) => [h.name, h]))

    // Roster-open failure propagates as the layer's error (a documented start
    // boundary) rather than collapsing to a defect.
    const roster = (yield* provideClient(RosterDb.open("global")).pipe(Effect.mapError(toError("open-roster")))).roster
    const running = yield* Ref.make(HashMap.empty<string, RunningEntry>())
    // Transient in-process waiters, keyed `${executionId}/${name}`. Rebuilt from the
    // durable `deferreds` rows on recovery — the row is truth, the poke is best-effort.
    const waiters = yield* Ref.make(HashMap.empty<string, Deferred.Deferred<void>>())

    const openWf = (executionId: string) => provideClient(WorkflowDb.open(ExecutionId.make(executionId)))
    const waiterKey = (executionId: string, name: string) => `${executionId}/${name}`

    // Per-(object,key) scope: ONE shared `ObjectStateDb` instance (so all methods
    // observe the same materialized state + a single CAS tail — exactly as the
    // roster is one instance) plus coordination for serial draining. Exclusive
    // access is admission control, not a lock over racing bodies: at most one
    // invocation per key is forked at a time; the rest wait in the durable inbox.
    const objectScopes = yield* Ref.make(HashMap.empty<string, ObjectScope>())
    const objectScopesMutex = yield* Semaphore.make(1)
    const objectScopeFor = (objectKey: string): Effect.Effect<ObjectScope, DurableExecutionError> =>
      objectScopesMutex.withPermits(1)(
        Ref.get(objectScopes).pipe(Effect.flatMap((m) =>
          Option.match(HashMap.get(m, objectKey), {
            onSome: (scope) => Effect.succeed(scope),
            onNone: () =>
              Effect.all([
                provideClient(ObjectStateDb.open(objectKey)).pipe(Effect.mapError(toError("open-object-state"))),
                Semaphore.make(1),
                Ref.make(false),
              ]).pipe(
                Effect.map(([state, mutex, draining]): ObjectScope => ({ state, mutex, draining })),
                Effect.tap((scope) => Ref.update(objectScopes, HashMap.set(objectKey, scope))),
              ),
          }),
        )),
      )

    const withActive = (operation: string): Effect.Effect<Invocation, DurableExecutionError> =>
      Effect.flatMap(ActiveInvocation, (opt) =>
        Option.isNone(opt) ? fail(operation, `${operation} called outside an active handler`) : Effect.succeed(opt.value))

    // ── durable step (`run`) ──────────────────────────────────────────────────
    const runStep = <A, E, R, EncodedA, EncodedE>(
      action: Effect.Effect<A, E, R>,
      options?: RunOptions<A, E, EncodedA, EncodedE>,
    ): Effect.Effect<A, E | DurableExecutionError, R> =>
      withActive("run").pipe(Effect.flatMap((active) =>
        Effect.gen(function*() {
          // identity = the optional name, else this step's position in the journal
          const ordinal = yield* Ref.getAndUpdate(active.runSeq, (n) => n + 1)
          const stepKey = `${active.executionId}/${options?.name ?? `run/${ordinal}`}`
          const existing = yield* active.db.steps.get(stepKey).pipe(Effect.mapError(toError("run")))
          if (Option.isSome(existing)) {
            const row = existing.value
            // terminal fact already recorded — replay it, never re-run
            if (row.success) return options?.output ? yield* decode(options.output, row.value) : (row.value as A)
            const error = options?.error ? yield* decode(options.error, row.error) : (row.error as E)
            return yield* Effect.fail(error)
          }
          // no terminal fact: run (retry is pre-terminal), then record the outcome.
          // A run action cannot use durable primitives (the public `run` type forbids
          // `DurableExecutionRuntime` in its `R`), so nothing inside can desync.
          const attempted = options?.retry
            ? action.pipe(
              Effect.retry({ schedule: scheduleOf(options.retry), times: Math.max(0, options.retry.maxAttempts - 1) }),
            )
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
        }),
      ))

    const handlerRequest = <A, I>(schema: Schema.Codec<A, I, never, never>): Effect.Effect<A, DurableExecutionError> =>
      withActive("handlerRequest").pipe(Effect.flatMap((active) => decode(schema, active.inputEncoded)))

    // ── durable timer (`sleep`) ───────────────────────────────────────────────
    // A `clockWakeups` row is the durable fact (`pending` = scheduled, `fired` =
    // elapsed). The handler fiber sleeps inline; on replay a `fired` row
    // short-circuits and a `pending` row recomputes the remaining delay. Re-arming a
    // pending wakeup across a restart is the slice-4 recovery job.
    const sleepStep = (name: string, duration: Duration.Duration): Effect.Effect<void, DurableExecutionError> =>
      withActive("sleep").pipe(Effect.flatMap((active) =>
        Effect.gen(function*() {
          const db = active.db
          const existing = yield* db.clockWakeups.get(name).pipe(Effect.mapError(toError("sleep")))
          if (Option.isSome(existing) && existing.value.status === "fired") return
          const now = yield* Clock.currentTimeMillis
          const deadlineMs = Option.isSome(existing) ? existing.value.deadlineMs : now + Duration.toMillis(duration)
          if (Option.isNone(existing)) {
            yield* db.clockWakeups.insert({ name, deadlineMs, status: "pending" }).pipe(Effect.mapError(toError("sleep")))
          }
          const remaining = Math.max(0, deadlineMs - now)
          if (remaining > 0) yield* Effect.sleep(Duration.millis(remaining))
          yield* db.clockWakeups.upsert({ name, deadlineMs, status: "fired" }).pipe(Effect.mapError(toError("sleep")))
        }),
      ))

    // ── user-defined durable state (`state(Table)`) ───────────────────────────
    // `state.get` is journaled (Option A): a `${execId}/read/${ordinal}` record
    // replays its original value, so a read-modify-write across suspend/resume
    // recomputes against the value seen on first execution. `set`/`delete` are plain
    // (idempotent) writes over `db.table`, scoped structurally to the active db.
    const stateGet = <Tbl extends AnyTable>(
      table: Tbl,
      key: string,
    ): Effect.Effect<Option.Option<RowOf<Tbl>>, DurableExecutionError> =>
      withActive("state.get").pipe(Effect.flatMap((active) =>
        Effect.gen(function*() {
          const ordinal = yield* Ref.getAndUpdate(active.readSeq, (n) => n + 1)
          const readKey = `${active.executionId}/read/${ordinal}`
          // `NullOr(table.schema)` carries `unknown` codec services (generic Struct);
          // pin them off at the Effect boundary, as the db facade does internally.
          const readCodec = Schema.NullOr(table.schema)
          const decodeRead = (encoded: unknown) =>
            (Schema.decodeUnknownEffect(readCodec)(encoded) as Effect.Effect<RowOf<Tbl> | null, Schema.SchemaError>)
              .pipe(Effect.mapError(toError("state.get")))
          const encodeRead = (value: RowOf<Tbl> | null) =>
            (Schema.encodeUnknownEffect(readCodec)(value) as Effect.Effect<unknown, Schema.SchemaError>)
              .pipe(Effect.mapError(toError("state.get")))

          const recorded = yield* active.db.stateReads.get(readKey).pipe(Effect.mapError(toError("state.get")))
          if (Option.isSome(recorded)) {
            return Option.fromNullishOr(yield* decodeRead(recorded.value.value))
          }
          // the durable value lives in the state store (per-key for an object); only
          // the read *journal* lives in the per-execution stream.
          const current = yield* active.stateDb.table(table).get(key).pipe(Effect.mapError(toError("state.get")))
          const encoded = yield* encodeRead(Option.getOrNull(current))
          yield* active.db.stateReads.insert({ readKey, value: encoded }).pipe(Effect.mapError(toError("state.get")))
          return current
        }),
      ))

    const stateSet = <Tbl extends AnyTable>(table: Tbl, row: RowOf<Tbl>): Effect.Effect<void, DurableExecutionError> =>
      withActive("state.set").pipe(
        Effect.flatMap((active) => active.stateDb.table(table).upsert(row)),
        Effect.mapError(toError("state.set")),
      )

    const stateDelete = <Tbl extends AnyTable>(table: Tbl, key: string): Effect.Effect<void, DurableExecutionError> =>
      withActive("state.delete").pipe(
        Effect.flatMap((active) => active.stateDb.table(table).delete(key)),
        Effect.mapError(toError("state.delete")),
      )

    // ── signals / awakeables / deferreds (park-and-resume) ────────────────────
    // One mechanism: a durable `deferreds` row is the resolution (truth); a
    // transient in-process `Deferred` is the wake (best-effort). Resolve writes the
    // row, awaits its ack, *then* pokes (ack-before-poke, §A4). Park checks the row
    // first (handles resolve-before-await and replay), registers a waiter, re-checks
    // (closes the lost-poke race), then awaits and re-reads.
    const poke = (executionId: string, name: string): Effect.Effect<void> =>
      Ref.get(waiters).pipe(Effect.flatMap((map) =>
        Option.match(HashMap.get(map, waiterKey(executionId, name)), {
          onNone: () => Effect.void,
          onSome: (w) => Effect.asVoid(Deferred.succeed(w, undefined)),
        }),
      ))

    const resolveOn = (
      db: WfDb,
      executionId: string,
      name: string,
      encoded: unknown,
    ): Effect.Effect<void, DurableExecutionError> =>
      // first-write-wins: a resolution is terminal, a double-resolve is a no-op
      db.deferreds.insertOrGet({ name, value: encoded }).pipe(
        Effect.mapError(toError("resolve")),
        Effect.andThen(poke(executionId, name)),
      )

    const markSuspended = (active: Invocation, kind: "deferred-wait" | "pending-clock"): Effect.Effect<void> =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Effect.all([
            roster.upsert({
              executionId: active.executionId,
              handlerName: active.handlerName,
              status: "suspended",
              objectKey: active.objectKey,
              suspendKind: kind,
              updatedMs: now,
            }),
            active.db.executions.upsert({
              executionId: active.executionId,
              handlerName: active.handlerName,
              input: active.inputEncoded,
              status: "suspended",
              suspended: true,
            }),
          ], { discard: true }),
        ),
        // suspended markers are recovery hints; never fail the wait on a hint write
        Effect.ignore,
      )

    const awaitDeferred = <A, I>(
      name: string,
      schema: Schema.Codec<A, I, never, never>,
    ): Effect.Effect<A, DurableExecutionError> =>
      withActive("await").pipe(Effect.flatMap((active) =>
        Effect.gen(function*() {
          // read fresh each time — observe the row as of now, not at build time
          const readRow = () =>
            active.db.deferreds.get(name).pipe(Effect.mapError(toError("await")), Effect.map(resolvedValue))
          const first = yield* readRow()
          if (Option.isSome(first)) return yield* decode(schema, first.value)

          const w = yield* Deferred.make<void>()
          const key = waiterKey(active.executionId, name)
          yield* Ref.update(waiters, HashMap.set(key, w))
          yield* markSuspended(active, "deferred-wait")
          // re-check after registering: a resolve between `first` and registration
          // would have poked nothing, so the row is the only signal it landed
          const second = yield* readRow()
          if (Option.isNone(second)) yield* Deferred.await(w)
          yield* Ref.update(waiters, HashMap.remove(key))
          const resolved = yield* readRow()
          return Option.isSome(resolved)
            ? yield* decode(schema, resolved.value)
            : yield* fail("await", `deferred ${JSON.stringify(name)} woke without a value`)
        }),
      ))

    const resolveLocal = <A, I>(
      name: string,
      schema: Schema.Codec<A, I, never, never>,
      value: A,
    ): Effect.Effect<void, DurableExecutionError> =>
      withActive("resolve").pipe(Effect.flatMap((active) =>
        encode(schema, value).pipe(Effect.flatMap((enc) => resolveOn(active.db, active.executionId, name, enc))),
      ))

    const resolveExternal = <A, I>(executionId: string, name: string, schema: Schema.Codec<A, I, never, never>, value: A): Effect.Effect<void, DurableExecutionError> =>
      Ref.get(running).pipe(Effect.flatMap((live) =>
        Option.match(HashMap.get(live, executionId), {
          // single-writer: resolve through the owner's db instance (serialized by
          // its lock), not a second db over the same stream
          onNone: () => fail("resolve", `execution ${executionId} is not running locally`),
          onSome: (entry) =>
            encode(schema, value).pipe(
              Effect.flatMap((enc) => resolveOn(entry.invocation.db, executionId, name, enc)),
            ),
        }),
      ))

    const nextAwakeableId: Effect.Effect<string, DurableExecutionError> = withActive("awakeable").pipe(
      Effect.flatMap((active) =>
        Ref.getAndUpdate(active.awakeSeq, (n) => n + 1).pipe(
          Effect.map((ordinal) => `${active.executionId}/awk/${ordinal}`),
        ),
      ),
    )

    // ── completion (SDD §B6): the result must outlive the dropped stream ───────
    const complete = (
      handler: { readonly name: string; readonly output: Schema.Top },
      executionId: string,
      db: WfDb,
      exit: Exit.Exit<unknown, unknown>,
    ): Effect.Effect<void, DurableExecutionError> =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const outputCodec = handler.output as Schema.Codec<unknown, unknown, never, never>
        if (Exit.isSuccess(exit)) {
          const result = yield* encode(outputCodec, exit.value)
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
      }).pipe(Effect.mapError(toError("complete")))

    // Fork ONE handler body into the engine scope, register it as the live owner,
    // and hand back its completion waiter. Serialization-agnostic: a service forks
    // directly; an object's serial drainer forks the head and awaits the waiter.
    const runExecution = <E, R>(
      handler: Handler<unknown, unknown, E, R>,
      executionId: string,
      db: WfDb,
      inputEncoded: unknown,
      stateDb: StateStore,
      objectKey: string | undefined,
    ): Effect.Effect<Deferred.Deferred<Exit.Exit<unknown, unknown>, DurableExecutionError>, never, R> =>
      Effect.gen(function*() {
        const deferred = yield* Deferred.make<Exit.Exit<unknown, unknown>, DurableExecutionError>()
        const runSeq = yield* Ref.make(0)
        const readSeq = yield* Ref.make(0)
        const awakeSeq = yield* Ref.make(0)
        const invocation: Invocation = {
          executionId,
          handlerName: handler.name,
          db,
          stateDb,
          objectKey,
          inputEncoded,
          runSeq,
          readSeq,
          awakeSeq,
        }
        const body: Effect.Effect<boolean, never, R> = handler.program.pipe(
          Effect.provideService(ActiveInvocation, Option.some(invocation)),
          Effect.provideService(DurableExecutionRuntime, api),
          Effect.exit,
          Effect.flatMap((exit) =>
            // route a completion failure to the result waiter instead of dying
            Effect.matchCauseEffect(complete(handler, executionId, db, exit), {
              onFailure: (cause) => Deferred.failCause(deferred, cause),
              onSuccess: () => Deferred.succeed(deferred, exit),
            }),
          ),
        )
        const fiber = yield* Effect.forkIn(body, engineScope)
        const entry: RunningEntry = { fiber, deferred, invocation }
        yield* Ref.update(running, HashMap.set(executionId, entry))
        return deferred
      })

    // ── virtual-object admission control: a per-key FIFO inbox + serial drainer ──
    // At most ONE invocation per key is forked at a time; the rest wait durably in
    // the inbox. This is what makes exclusivity crash-safe: recovery drains an
    // ordered queue rather than re-racing N incomplete executions (whose journaled
    // state reads would otherwise let a reordered replay clobber an intervening write).
    const nextHead = (scope: ObjectScope): Effect.Effect<Option.Option<InboxRow>, DurableExecutionError> =>
      scope.state.inbox.query((rows) => Option.fromNullishOr([...rows].sort((a, b) => a.seq - b.seq)[0])).pipe(
        Effect.mapError(toError("drain")),
      )

    const drainOne = (objectKey: string, scope: ObjectScope, head: InboxRow): Effect.Effect<void> =>
      Effect.gen(function*() {
        // a head whose roster row is already terminal must NOT re-run: a crash between
        // its completion (roster written, stream dropped) and its inbox dequeue leaves
        // a stale row; re-running it would double-apply the method against live state.
        // Dequeue-only — the same terminal guard `submit` uses, on the drain path.
        const prior = yield* roster.get(head.executionId)
        if (Option.isSome(prior) && (prior.value.status === "completed" || prior.value.status === "failed")) {
          yield* scope.state.inbox.delete(head.executionId)
          return
        }
        const handler = registry.get(head.handlerName)
        if (handler === undefined) {
          // an unregistered handler can't be run — fail it terminally so the queue moves
          const now = yield* Clock.currentTimeMillis
          yield* roster.upsert({
            executionId: head.executionId,
            handlerName: head.handlerName,
            status: "failed",
            error: `no handler ${JSON.stringify(head.handlerName)} registered`,
            resultAcked: true,
            updatedMs: now,
          })
        } else {
          const db = yield* openWf(head.executionId)
          yield* db.executions.insertOrGet({
            executionId: head.executionId,
            handlerName: head.handlerName,
            input: head.input,
            status: "running",
            suspended: false,
            objectKey,
          })
          // fork the head and await its completion (through any parks) before dequeue,
          // so the next method only starts once this one is fully done — single-writer.
          // ignore the exit: a handler failure is still a completion (recorded on the
          // roster); we must always dequeue so the key's queue advances.
          const deferred = yield* runExecution(handler, head.executionId, db, head.input, scope.state, objectKey)
          yield* Deferred.await(deferred).pipe(Effect.ignore)
        }
        yield* scope.state.inbox.delete(head.executionId)
      }).pipe(Effect.mapError(toError("drain")), Effect.ignore) // a single drain failure must not stall the key

    const drainLoop = (objectKey: string, scope: ObjectScope): Effect.Effect<void> =>
      nextHead(scope).pipe(
        Effect.flatMap((head) =>
          Option.match(head, {
            onSome: (h) => drainOne(objectKey, scope, h).pipe(Effect.andThen(drainLoop(objectKey, scope))),
            // empty: re-check under the mutex (closing the enqueue race), then stop
            onNone: () =>
              scope.mutex.withPermits(1)(
                nextHead(scope).pipe(Effect.flatMap((again) =>
                  Option.isNone(again) ? Ref.set(scope.draining, false).pipe(Effect.as(true)) : Effect.succeed(false),
                )),
              ).pipe(Effect.flatMap((stopped) => stopped ? Effect.void : drainLoop(objectKey, scope))),
          }),
        ),
        // on a read failure, release the drainer so a later enqueue can restart it
        Effect.catchCause(() => Ref.set(scope.draining, false)),
      )

    // start a drainer for this key if one isn't already running. CALLER HOLDS `mutex`.
    const ensureDrainerLocked = (objectKey: string, scope: ObjectScope): Effect.Effect<void> =>
      Ref.get(scope.draining).pipe(Effect.flatMap((on) =>
        on ? Effect.void : Ref.set(scope.draining, true).pipe(
          Effect.andThen(Effect.forkIn(drainLoop(objectKey, scope), engineScope)),
          Effect.asVoid,
        ),
      ))

    const enqueueObject = (
      objectKey: string,
      executionId: string,
      handlerName: string,
      inputEncoded: unknown,
    ): Effect.Effect<void, DurableExecutionError> =>
      objectScopeFor(objectKey).pipe(Effect.flatMap((scope) =>
        scope.mutex.withPermits(1)(
          Effect.gen(function*() {
            // append to the durable FIFO (seq = max+1 among the not-yet-completed),
            // publish a roster row so `attach`/`poll` can find it, then ensure draining.
            const seq = yield* scope.state.inbox.query((rows) => rows.reduce((m, r) => Math.max(m, r.seq), 0) + 1)
            yield* scope.state.inbox.insert({ executionId, seq, handlerName, input: inputEncoded })
            const now = yield* Clock.currentTimeMillis
            yield* roster.upsert({ executionId, handlerName, status: "running", objectKey, updatedMs: now })
            yield* ensureDrainerLocked(objectKey, scope)
          }),
        ).pipe(Effect.mapError(toError("enqueue"))),
      ))

    const submit = <I, O, E, R>(
      handler: Handler<I, O, E, R>,
      executionId: string,
      input: I,
      objectKey?: string,
    ): Effect.Effect<void, DurableExecutionError, R> =>
      Effect.gen(function*() {
        const live = yield* Ref.get(running)
        if (HashMap.has(live, executionId)) return // already owned here — idempotent

        // already finished? a terminal roster row means a re-submit must NOT re-run
        // (the execution's stream is gone; the result lives on in the roster).
        const prior = yield* roster.get(executionId).pipe(Effect.mapError(toError("submit")))
        if (Option.isSome(prior) && (prior.value.status === "completed" || prior.value.status === "failed")) return

        const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)

        if (objectKey === undefined) {
          // service: each call is an independent execution — genesis + fork now.
          const db = yield* openWf(executionId).pipe(Effect.mapError(toError("submit")))
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
          yield* runExecution(handler, executionId, db, inputEncoded, db, undefined)
        } else {
          // object: enqueue into the per-key FIFO; the serial drainer runs it in turn.
          // self-register so the live drainer can run it by name (recovery uses the
          // registry seeded by `serviceLayer`).
          // eslint-disable-next-line local/no-launder-cast -- a registered object handler is compiled as Handler<unknown,unknown,never,never>; submit's generic params are existential here
          registry.set(handler.name, handler as unknown as RegisteredHandler)
          yield* enqueueObject(objectKey, executionId, handler.name, inputEncoded)
        }
      })

    // ── boot recovery (SDD §B5): re-drive running/suspended executions ─────────
    // Re-run the handler from the top: `run` short-circuits from its `steps` fact,
    // journaled `state.get` replays, `sleep` recomputes its remaining delay, and a
    // signal/awaitable reads its resolved `deferreds` row or re-parks. The handler
    // is looked up by name in the registry; an unknown name is skipped.
    const recoverExecution = (executionId: string, handlerName: string): Effect.Effect<void> =>
      Option.match(Option.fromNullishOr(registry.get(handlerName)), {
        onNone: () => Effect.void,
        onSome: (handler) =>
          Effect.gen(function*() {
            const db = yield* openWf(executionId)
            const row = yield* db.executions.get(executionId)
            const inputEncoded = Option.match(row, { onNone: () => undefined, onSome: (r) => r.input })
            yield* runExecution(handler, executionId, db, inputEncoded, db, undefined)
          }).pipe(Effect.ignore), // one execution's recovery must not abort boot
      })

    // start (or resume) the serial drainer for an object key — it reads the durable
    // inbox and drives the head, so recovery never re-races queued invocations.
    const recoverObjectKey = (objectKey: string): Effect.Effect<void> =>
      objectScopeFor(objectKey).pipe(
        Effect.flatMap((scope) => scope.mutex.withPermits(1)(ensureDrainerLocked(objectKey, scope))),
        Effect.ignore,
      )

    const bootRecover = roster
      .query((rows) => rows.filter((r) => r.status === "running" || r.status === "suspended"))
      .pipe(
        Effect.flatMap((rows) => {
          // services re-run independently; object methods are drained per key (ordered),
          // never re-run individually — that ordering is the crash-safety guarantee.
          const services = rows.filter((r) => r.objectKey === undefined)
          const objectKeys = [...new Set(rows.flatMap((r) => r.objectKey === undefined ? [] : [r.objectKey]))]
          return Effect.all([
            Effect.forEach(services, (r) => recoverExecution(r.executionId, r.handlerName), { discard: true }),
            Effect.forEach(objectKeys, (k) => recoverObjectKey(k), { discard: true }),
          ], { discard: true })
        }),
        Effect.ignore, // recovery is best-effort; never fail engine startup on it
      )

    const attach = <A, I>(
      executionId: string,
      schema: Schema.Codec<A, I, never, never>,
    ): Effect.Effect<A, DurableExecutionError> =>
      Effect.gen(function*() {
        // if we own it, wait for it to settle — `complete` has written the roster
        // result by the time the waiter resolves, so both paths decode via `schema`.
        const live = yield* Ref.get(running)
        const entry = HashMap.get(live, executionId)
        if (Option.isSome(entry)) {
          const exit = yield* Deferred.await(entry.value.deferred)
          if (Exit.isFailure(exit)) {
            return yield* Effect.fail(
              new DurableExecutionError({
                operation: "attach",
                message: `execution failed: ${Cause.pretty(exit.cause)}`,
                cause: exit.cause,
              }),
            )
          }
          // success: fall through to read + decode the durable roster result
        }
        const row = yield* roster.get(executionId).pipe(Effect.mapError(toError("attach")))
        if (Option.isNone(row)) return yield* fail("attach", `unknown execution: ${executionId}`)
        if (row.value.status === "completed") return yield* decode(schema, row.value.result)
        if (row.value.status === "failed") return yield* fail("attach", row.value.error ?? "execution failed")
        // running/suspended but not (yet) owned here — e.g. an object method still
        // queued in its key's inbox, or one awaiting recovery. Wait for it: re-check
        // the running map (then take the waiter) and the roster until it settles.
        yield* Effect.sleep(Duration.millis(25))
        return yield* attach(executionId, schema)
      })

    const poll = <A, I>(
      executionId: string,
      schema: Schema.Codec<A, I, never, never>,
    ): Effect.Effect<Option.Option<A>, DurableExecutionError> =>
      roster.get(executionId).pipe(
        Effect.mapError(toError("poll")),
        Effect.flatMap((row) =>
          Option.isSome(row) && row.value.status === "completed"
            ? decode(schema, row.value.result).pipe(
              Effect.map(Option.some),
            )
            : Effect.succeedNone,
        ),
      )

    const api: DurableExecutionRuntimeApi = {
      submit,
      attach,
      poll,
      runStep,
      handlerRequest,
      sleepStep,
      stateGet,
      stateSet,
      stateDelete,
      awaitDeferred,
      resolveLocal,
      resolveExternal,
      nextAwakeableId,
    }

    // re-drive any running/suspended executions left by a prior process before
    // serving requests, so a recovered execution is resident (in `running`) and
    // can be `attach`ed / resolved exactly like a freshly-submitted one.
    yield* bootRecover

    return api
  })
