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
} from "effect"
import { S2Client } from "effect-s2"
import type { AnyTable, RowOf, TableFacade } from "effect-s2-stream-db"
import { type ActorExit, decodeObjectCallId, encodeObjectCallId, type ObjectCallIdParts } from "./actor/core.ts"
import { InvocationStore, type ObjectStateBackend, type RunHead } from "./actor/object.ts"
import { DurableExecutionError, durableError } from "./errors.ts"
import { ExecutionId, RosterDb, WorkflowDb } from "./schema.ts"
import type { Handler, RetryPolicy, RunOptions } from "./types.ts"

/** The opened per-execution db (success type of `WorkflowDb.open`). */
type WfDb = Effect.Success<ReturnType<typeof WorkflowDb.open>>

/**
 * The durable record store a service `state(Table)` binding writes to: the active
 * execution's own stream. (Object state moved to the per-owner `ActorEvent` log —
 * see `actor/object.ts`.)
 */
type StateStore = { readonly table: <Tbl extends AnyTable>(table: Tbl) => TableFacade<RowOf<Tbl>> }

/**
 * The active invocation a free primitive (`run`/`sleep`/`state`/…) operates on.
 * A `service` invocation runs against its per-execution `WorkflowDb`; an `object`
 * invocation runs against its owner `ActorEvent` log via the journaled backend
 * (`state`/`run`/`sleep`/`signal`/`deferred`/`awakeable` — all wired).
 */
interface ServiceInvocation {
  readonly kind: "service"
  readonly executionId: string
  readonly handlerName: string
  readonly db: WfDb
  readonly stateDb: StateStore
  readonly inputEncoded: unknown
  /** Monotonic per-activation counter for positionally-keyed `run` steps. */
  readonly runSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for journaled `state.get` reads (Option A). */
  readonly readSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for replay-stable `awakeable` ids. */
  readonly awakeSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for replay-stable child `call`/`send` ids. */
  readonly callSeq: Ref.Ref<number>
}

interface ObjectInvocation {
  readonly kind: "object"
  readonly callId: string
  readonly method: string
  readonly inputEncoded: unknown
  /** The journaled per-owner durable `state` + run-journal + signal surface. */
  readonly state: ObjectStateBackend
  /** Monotonic per-activation counter for positionally-keyed `run` steps. */
  readonly runSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for replay-stable `awakeable` ids. */
  readonly awakeSeq: Ref.Ref<number>
  /** Monotonic per-activation counter for replay-stable child `call`/`send` ids. */
  readonly callSeq: Ref.Ref<number>
}

/** The address of a durable object call target (`call`/`send` between executions). */
export interface CallTarget {
  readonly object: string
  readonly key: string
  readonly method: string
}

type Invocation = ServiceInvocation | ObjectInvocation

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
  // Only service executions register here; object calls settle on their owner log.
  readonly invocation: ServiceInvocation
}

/** A `run` step's terminal outcome (the durable fact replayed instead of re-running). */
interface StepRecord {
  readonly success: boolean
  readonly value?: unknown
  readonly error?: unknown
}

/** A durable timer fact: a scheduled deadline that transitions `pending` → `fired`. */
interface TimerRecord {
  readonly deadlineMs: number
  readonly status: "pending" | "fired"
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

// Object `state(Table)` rows are encoded/decoded through the table schema at the
// log boundary; the durable key is the table's primary-key field value.
// `table.schema` is a generic Struct whose codec services are `unknown`; pin them
// off at the Effect boundary (as the service `state.get` path does internally).
const encodeRowFor = (table: AnyTable, row: unknown): Effect.Effect<unknown, DurableExecutionError> =>
  (Schema.encodeUnknownEffect(table.schema)(row) as Effect.Effect<unknown, Schema.SchemaError>).pipe(
    Effect.mapError(durableError("state.set")),
  )
const decodeRowFor = (table: AnyTable, encoded: unknown): Effect.Effect<unknown, DurableExecutionError> =>
  (Schema.decodeUnknownEffect(table.schema)(encoded) as Effect.Effect<unknown, Schema.SchemaError>).pipe(
    Effect.mapError(durableError("state.get")),
  )
const pkOf = (table: AnyTable, row: unknown): string => String((row as Record<string, unknown>)[table.pkField])

/** The encoded value of a resolved `deferreds` row, if present. */
const resolvedValue = (row: Option.Option<{ readonly value?: unknown }>): Option.Option<unknown> =>
  Option.flatMap(row, (r) => Option.fromNullishOr(r.value))

/** The public engine surface (host ops) plus the primitive ops the free functions delegate to. */
export interface DurableExecutionRuntimeApi {
  /**
   * Genesis + fork. A plain `executionId` is a stateless service execution
   * (genesis + fork now). An `executionId` that decodes as an object call id routes
   * to the per-owner `ActorEvent` log: durably admit the call, then fork the
   * exclusive drainer (`state(Table)` is journaled to the owner stream; same-key
   * methods run serially).
   */
  readonly submit: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    executionId: string,
    input: I,
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
  /** Durable `call` (delegated to by the `call` free primitive): issue a child object call and await its result. */
  readonly callStep: <A, I>(
    target: CallTarget,
    input: unknown,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  /** Durable one-way `send` (delegated to by the `send` free primitive): issue a child object call, returning its id. */
  readonly sendStep: (target: CallTarget, input: unknown) => Effect.Effect<string, DurableExecutionError>
}

export class DurableExecutionRuntime
  extends Context.Service<DurableExecutionRuntime, DurableExecutionRuntimeApi>()("DurableExecutionRuntime")
{
  /**
   * The S2-backed runtime layer. Requires an `S2Client`; owns its fiber scope.
   * `handlers` seed service boot recovery; `objectSeeds` (keyed `${object}/${method}`)
   * seed object boot recovery so a fresh engine can re-drive pending object heads.
   */
  static layer(
    handlers: ReadonlyArray<RegisteredHandler> = [],
    objectSeeds: ReadonlyArray<ObjectHandlerSeed> = [],
  ): Layer.Layer<DurableExecutionRuntime, DurableExecutionError, S2Client> {
    // The object-backed InvocationStore is an internal dependency of the one
    // runtime boundary — provided here, never exported.
    return Layer.effect(DurableExecutionRuntime)(makeRuntime(handlers, objectSeeds)).pipe(
      Layer.provide(InvocationStore.layer),
    )
  }
}

/** A handler the engine can recover by name (program + output schema; no unmet R/E). */
export type RegisteredHandler = Handler<unknown, unknown, never, never>

/** A registered object method, seeding object boot recovery. */
export interface ObjectHandlerSeed {
  readonly object: string
  readonly method: string
  readonly handler: RegisteredHandler
}

const makeRuntime = (
  handlers: ReadonlyArray<RegisteredHandler>,
  objectSeeds: ReadonlyArray<ObjectHandlerSeed>,
): Effect.Effect<DurableExecutionRuntimeApi, DurableExecutionError, S2Client | Scope.Scope | InvocationStore> =>
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

    // The object-backed durable store (admission, exclusive per-key drainer,
    // journaled `state`) for the object call path. Internal to this runtime — never
    // exported as a sibling runtime (consolidation SDD).
    const store = yield* InvocationStore
    // Object handlers keyed `${object}/${method}`, so the drainer can run a key's
    // pending head by name. Seeded at boot (for recovery) and self-registered on submit.
    const objectHandlers = new Map<string, RegisteredHandler>(
      objectSeeds.map((s) => [`${s.object}/${s.method}`, s.handler] as const),
    )
    // the distinct object names to enumerate for boot recovery.
    const objectNames = [...new Set(objectSeeds.map((s) => s.object))]

    const withActive = (operation: string): Effect.Effect<Invocation, DurableExecutionError> =>
      Effect.flatMap(ActiveInvocation, (opt) =>
        Option.isNone(opt) ? fail(operation, `${operation} called outside an active handler`) : Effect.succeed(opt.value))

    // ── durable step (`run`) ──────────────────────────────────────────────────
    // The run-journal stores a step's terminal outcome. For a service it is the
    // per-execution `WorkflowDb.steps` table; for an object it is the owner stream's
    // `Journaled` facts (callId + step are separate event fields — no composed key
    // string). The replay logic over it is identical (see `runStep`).
    const runJournalFor = (active: Invocation) =>
      active.kind === "object"
        ? {
          get: (step: string): Effect.Effect<Option.Option<StepRecord>, DurableExecutionError> =>
            provideClient(active.state.journal.get("run", step)).pipe(Effect.map((o) => o as Option.Option<StepRecord>)),
          put: (step: string, record: StepRecord): Effect.Effect<void, DurableExecutionError> =>
            provideClient(active.state.journal.put("run", step, record)),
        }
        : {
          get: (step: string): Effect.Effect<Option.Option<StepRecord>, DurableExecutionError> =>
            active.db.steps.get(`${active.executionId}/${step}`).pipe(
              Effect.mapError(toError("run")),
              Effect.map((o) => Option.map(o, (r): StepRecord => ({ success: r.success, value: r.value, error: r.error }))),
            ),
          put: (step: string, record: StepRecord): Effect.Effect<void, DurableExecutionError> =>
            active.db.steps.insert({ stepKey: `${active.executionId}/${step}`, ...record }).pipe(
              Effect.mapError(toError("run")),
            ),
        }

    const runStep = <A, E, R, EncodedA, EncodedE>(
      action: Effect.Effect<A, E, R>,
      options?: RunOptions<A, E, EncodedA, EncodedE>,
    ): Effect.Effect<A, E | DurableExecutionError, R> =>
      withActive("run").pipe(Effect.flatMap((active) =>
        Effect.gen(function*() {
          // identity = the optional name, else this step's position in the journal.
          // The run-journal is the per-execution `WorkflowDb.steps` for a service, or
          // the owner stream's `Journaled` facts for an object — same replay logic.
          const ordinal = yield* Ref.getAndUpdate(active.runSeq, (n) => n + 1)
          const stepName = options?.name ?? `run/${ordinal}`
          const journal = runJournalFor(active)
          const existing = yield* journal.get(stepName)
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
            yield* journal.put(stepName, { success: true, value })
            return outcome.value
          }
          // a typed failure (with an error schema) is a terminal StepFailed fact;
          // anything else stays non-terminal and is eligible to run again on replay.
          const failure = Cause.findErrorOption(outcome.cause)
          if (options?.error && Option.isSome(failure)) {
            const error = yield* encode(options.error, failure.value)
            yield* journal.put(stepName, { success: false, error })
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
    // The durable timer store: a service `clockWakeups` row, or an object `kind:"sleep"`
    // journal fact on the owner stream. Same pending→fired replay logic (see `sleepStep`).
    const sleepTimerFor = (active: Invocation) =>
      active.kind === "object"
        ? {
          get: (name: string): Effect.Effect<Option.Option<TimerRecord>, DurableExecutionError> =>
            provideClient(active.state.journal.get("sleep", name)).pipe(Effect.map((o) => o as Option.Option<TimerRecord>)),
          put: (name: string, record: TimerRecord): Effect.Effect<void, DurableExecutionError> =>
            provideClient(active.state.journal.put("sleep", name, record)),
        }
        : {
          get: (name: string): Effect.Effect<Option.Option<TimerRecord>, DurableExecutionError> =>
            active.db.clockWakeups.get(name).pipe(
              Effect.mapError(toError("sleep")),
              Effect.map((o) => Option.map(o, (r): TimerRecord => ({ deadlineMs: r.deadlineMs, status: r.status }))),
            ),
          put: (name: string, record: TimerRecord): Effect.Effect<void, DurableExecutionError> =>
            (record.status === "pending"
              ? active.db.clockWakeups.insert({ name, deadlineMs: record.deadlineMs, status: "pending" })
              : active.db.clockWakeups.upsert({ name, deadlineMs: record.deadlineMs, status: "fired" }))
              .pipe(Effect.mapError(toError("sleep"))),
        }

    const sleepStep = (name: string, duration: Duration.Duration): Effect.Effect<void, DurableExecutionError> =>
      withActive("sleep").pipe(Effect.flatMap((active) =>
        Effect.gen(function*() {
          // a durable timer fact (`pending` deadline → `fired`). For a service it is a
          // `clockWakeups` row; for an object it is a `kind:"sleep"` journal fact on the
          // owner stream. On replay a `fired` fact short-circuits and a `pending` fact
          // recomputes the remaining delay against the recorded deadline.
          const timer = sleepTimerFor(active)
          const existing = yield* timer.get(name)
          if (Option.isSome(existing) && existing.value.status === "fired") return
          const now = yield* Clock.currentTimeMillis
          const deadlineMs = Option.isSome(existing) ? existing.value.deadlineMs : now + Duration.toMillis(duration)
          if (Option.isNone(existing)) {
            yield* timer.put(name, { deadlineMs, status: "pending" })
          }
          const remaining = Math.max(0, deadlineMs - now)
          if (remaining > 0) yield* Effect.sleep(Duration.millis(remaining))
          yield* timer.put(name, { deadlineMs, status: "fired" })
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
        active.kind === "object"
          ? provideClient(
            active.state.get(table.tableName, key).pipe(
              Effect.flatMap((opt) =>
                Option.match(opt, {
                  onNone: () => Effect.succeedNone,
                  onSome: (encoded) =>
                    decodeRowFor(table, encoded).pipe(Effect.map((row) => Option.some(row as RowOf<Tbl>))),
                }),
              ),
            ),
          )
          : Effect.gen(function*() {
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
      withActive("state.set").pipe(Effect.flatMap((active) =>
        active.kind === "object"
          ? provideClient(
            encodeRowFor(table, row).pipe(
              Effect.flatMap((encoded) => active.state.set(table.tableName, pkOf(table, row), encoded)),
            ),
          )
          : active.stateDb.table(table).upsert(row).pipe(Effect.mapError(toError("state.set"))),
      ))

    const stateDelete = <Tbl extends AnyTable>(table: Tbl, key: string): Effect.Effect<void, DurableExecutionError> =>
      withActive("state.delete").pipe(Effect.flatMap((active) =>
        active.kind === "object"
          ? provideClient(active.state.delete(table.tableName, key))
          : active.stateDb.table(table).delete(key).pipe(Effect.mapError(toError("state.delete"))),
      ))

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

    const markSuspended = (active: ServiceInvocation, kind: "deferred-wait" | "pending-clock"): Effect.Effect<void> =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Effect.all([
            roster.upsert({
              executionId: active.executionId,
              handlerName: active.handlerName,
              status: "suspended",
              objectKey: undefined,
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
        active.kind === "object"
          // object: park on a durable `SignalResolved` on the owner stream, then decode.
          ? provideClient(active.state.signal.await(name)).pipe(Effect.flatMap((raw) => decode(schema, raw)))
          : Effect.gen(function*() {
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
        active.kind === "object"
          // handler-side resolve: append `SignalResolved` to the running call's owner stream.
          ? encode(schema, value).pipe(Effect.flatMap((enc) => provideClient(active.state.signal.resolve(name, enc))))
          : encode(schema, value).pipe(Effect.flatMap((enc) => resolveOn(active.db, active.executionId, name, enc))),
      ))

    const resolveExternal = <A, I>(executionId: string, name: string, schema: Schema.Codec<A, I, never, never>, value: A): Effect.Effect<void, DurableExecutionError> =>
      Effect.gen(function*() {
        const parts = yield* decodeParts(executionId)
        if (Option.isSome(parts)) {
          // object ingress is RESIDENCY-INDEPENDENT: route by the call id and append
          // SignalResolved to the owner stream whether or not the call is resident.
          const enc = yield* encode(schema, value)
          return yield* provideClient(store.resolveSignal(executionId, parts.value, name, enc))
        }
        // service: resolve through the resident owner's db (single-writer, serialized).
        const live = yield* Ref.get(running)
        return yield* Option.match(HashMap.get(live, executionId), {
          onNone: () => fail("resolve", `execution ${executionId} is not running locally`),
          onSome: (entry) =>
            encode(schema, value).pipe(Effect.flatMap((enc) => resolveOn(entry.invocation.db, executionId, name, enc))),
        })
      })

    const nextAwakeableId: Effect.Effect<string, DurableExecutionError> = withActive("awakeable").pipe(
      Effect.flatMap((active) =>
        active.kind === "object"
          ? Ref.getAndUpdate(active.awakeSeq, (n) => n + 1).pipe(
            Effect.map((ordinal) => `${active.callId}/awk/${ordinal}`),
          )
          : Ref.getAndUpdate(active.awakeSeq, (n) => n + 1).pipe(
            Effect.map((ordinal) => `${active.executionId}/awk/${ordinal}`),
          ),
      ),
    )

    // ── durable inter-execution calls (`call` / `send`) ───────────────────────
    // A handler issues a child OBJECT call whose id is DETERMINISTIC — derived from
    // the caller id + a per-activation ordinal — so a replay recomputes the same id,
    // admission dedups, and the call is issued exactly once (the result is re-read,
    // never re-issued). No new event: it reuses admission (idempotent) + attach.
    const issueCall = (active: Invocation, target: CallTarget, input: unknown): Effect.Effect<string, DurableExecutionError> =>
      Effect.gen(function*() {
        const ordinal = yield* Ref.getAndUpdate(active.callSeq, (n) => n + 1)
        const parentId = active.kind === "object" ? active.callId : active.executionId
        const parts: ObjectCallIdParts = {
          object: target.object,
          key: target.key,
          method: target.method,
          nonce: `${parentId}/call/${ordinal}`,
        }
        const callId = yield* encodeObjectCallId(parts).pipe(Effect.mapError(toError("call")))
        yield* provideClient(store.admit(callId, parts, input)) // idempotent by callId
        // drive the target's drainer so the call runs (residency-independent dispatch).
        yield* Effect.forkIn(
          provideClient(store.drain(target.object, target.key, makeRunHead(target.object))),
          engineScope,
        )
        return callId
      })

    const callStep = <A, I>(
      target: CallTarget,
      input: unknown,
      schema: Schema.Codec<A, I, never, never>,
    ): Effect.Effect<A, DurableExecutionError> =>
      withActive("call").pipe(Effect.flatMap((active) =>
        issueCall(active, target, input).pipe(Effect.flatMap((callId) => attach(callId, schema))),
      ))

    const sendStep = (target: CallTarget, input: unknown): Effect.Effect<string, DurableExecutionError> =>
      withActive("send").pipe(Effect.flatMap((active) => issueCall(active, target, input)))

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

    // Fork ONE service handler body into the engine scope, register it as the live
    // owner, and hand back its completion waiter. (Object calls do NOT run here —
    // they settle on their owner log via the InvocationStore drainer.)
    const runExecution = <E, R>(
      handler: Handler<unknown, unknown, E, R>,
      executionId: string,
      db: WfDb,
      inputEncoded: unknown,
    ): Effect.Effect<Deferred.Deferred<Exit.Exit<unknown, unknown>, DurableExecutionError>, never, R> =>
      Effect.gen(function*() {
        const deferred = yield* Deferred.make<Exit.Exit<unknown, unknown>, DurableExecutionError>()
        const runSeq = yield* Ref.make(0)
        const readSeq = yield* Ref.make(0)
        const awakeSeq = yield* Ref.make(0)
        const callSeq = yield* Ref.make(0)
        const invocation: ServiceInvocation = {
          kind: "service",
          executionId,
          handlerName: handler.name,
          db,
          stateDb: db,
          inputEncoded,
          runSeq,
          readSeq,
          awakeSeq,
          callSeq,
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

    // ── object call path: admit + exclusive drain on the owner ActorEvent log ──
    // Map a handler Exit to the durable ActorExit. A success value is already
    // encoded by `runObjectBody` so `attach` decodes it symmetrically.
    const toActorExit = (exit: Exit.Exit<unknown, unknown>): ActorExit => {
      if (Exit.isSuccess(exit)) {
        return { _tag: "Success", value: exit.value }
      }
      const cause = exit.cause
      if (Cause.hasInterruptsOnly(cause)) {
        return { _tag: "Interrupt" }
      }
      const failure = Cause.findErrorOption(cause)
      return Option.isSome(failure)
        ? { _tag: "Failure", error: String(failure.value) }
        : { _tag: "Defect", defect: Cause.pretty(cause) }
    }

    // Run one accepted object call with an object-backed invocation: `state` is
    // journaled to the owner stream, other durable primitives fail clearly.
    const runObjectBody = (
      handler: RegisteredHandler,
      callId: string,
      method: string,
      inputEncoded: unknown,
      state: ObjectStateBackend,
    ): Effect.Effect<ActorExit, DurableExecutionError, S2Client> =>
      Effect.gen(function*() {
        const runSeq = yield* Ref.make(0)
        const awakeSeq = yield* Ref.make(0)
        const callSeq = yield* Ref.make(0)
        const invocation: ObjectInvocation = {
          kind: "object",
          callId,
          method,
          inputEncoded,
          state,
          runSeq,
          awakeSeq,
          callSeq,
        }
        const exit = yield* handler.program.pipe(
          Effect.provideService(ActiveInvocation, Option.some(invocation)),
          Effect.provideService(DurableExecutionRuntime, api),
          Effect.exit,
        )
        if (Exit.isSuccess(exit)) {
          const encoded = yield* encode(handler.output as Schema.Codec<unknown, unknown, never, never>, exit.value)
          return { _tag: "Success", value: encoded }
        }
        return toActorExit(exit)
      })

    // The drainer's per-head runner for an object: resolve the head's handler by
    // method and run it; the store appends the resulting `Completed`.
    const makeRunHead = (object: string): RunHead => (call) => {
      const key = `${object}/${call.method}`
      const handler = objectHandlers.get(key)
      return handler === undefined
        ? Effect.succeed<ActorExit>({ _tag: "Failure", error: `no handler ${JSON.stringify(key)} registered` })
        : runObjectBody(handler, call.callId, call.method, call.input, call.state)
    }

    const submit = <I, O, E, R>(
      handler: Handler<I, O, E, R>,
      executionId: string,
      input: I,
    ): Effect.Effect<void, DurableExecutionError, R> =>
      Effect.gen(function*() {
        const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
        // An object call id self-routes to its owner ActorEvent log; any other id is
        // a stateless service execution on the WorkflowDb/roster path.
        const parts = yield* decodeObjectCallId(executionId).pipe(
          Effect.match({ onFailure: () => Option.none<ObjectCallIdParts>(), onSuccess: Option.some }),
        )
        if (Option.isSome(parts)) {
          // eslint-disable-next-line local/no-launder-cast -- a compiled object handler is Handler<unknown,unknown,never,never>; submit's generics are existential here
          objectHandlers.set(`${parts.value.object}/${parts.value.method}`, handler as unknown as RegisteredHandler)
          yield* provideClient(store.admit(executionId, parts.value, inputEncoded)) // durable admission (idempotent)
          // fork the exclusive drainer; it runs the pending head(s) to completion.
          yield* Effect.forkIn(
            provideClient(store.drain(parts.value.object, parts.value.key, makeRunHead(parts.value.object))),
            engineScope,
          )
          return
        }

        // service: each call is an independent execution — genesis + fork now.
        const live = yield* Ref.get(running)
        if (HashMap.has(live, executionId)) return // already owned here — idempotent
        const prior = yield* roster.get(executionId).pipe(Effect.mapError(toError("submit")))
        if (Option.isSome(prior) && (prior.value.status === "completed" || prior.value.status === "failed")) return
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
        yield* runExecution(handler, executionId, db, inputEncoded)
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
            yield* runExecution(handler, executionId, db, inputEncoded)
          }).pipe(
            Effect.withSpan("effect-s2-durable.recover-execution", { attributes: { executionId, handlerName } }),
            Effect.ignore, // one execution's recovery must not abort boot
          ),
      })

    // Service boot recovery: re-drive each running/suspended SERVICE execution.
    // (Objects no longer write the roster, so this query returns service rows only.)
    const bootRecover = roster
      .query((rows) =>
        rows.filter((r) => (r.status === "running" || r.status === "suspended") && r.objectKey === undefined),
      )
      .pipe(
        Effect.flatMap((services) =>
          Effect.forEach(services, (r) => recoverExecution(r.executionId, r.handlerName), { discard: true }),
        ),
        Effect.withSpan("effect-s2-durable.boot-recover"),
        Effect.ignore, // recovery is best-effort; never fail engine startup on it
      )

    // OBJECT boot recovery: for each registered object, enumerate its owner keys and
    // restart a drainer per key. The drainer re-runs the durable head — `run`/`state`/
    // `sleep` facts replay (never re-executed), a parked signal re-parks (RECOVERY.3/4);
    // a key with no pending head drains to a no-op (existence is not liveness,
    // RECOVERY.2). Drains fork into the engine scope so boot does not block on parks.
    const objectBootRecover = Effect.forEach(
      objectNames,
      (object) =>
        provideClient(store.ownerKeys(object)).pipe(
          Effect.flatMap((keys) =>
            Effect.forEach(
              keys,
              (key) => Effect.forkIn(provideClient(store.drain(object, key, makeRunHead(object))), engineScope),
              { discard: true },
            ),
          ),
          Effect.ignore, // one object's recovery must not abort boot
        ),
      { discard: true },
    ).pipe(Effect.withSpan("effect-s2-durable.object.boot-recover"), Effect.ignore)

    // An id that decodes as an object call id routes to the owner projection.
    const decodeParts = (id: string): Effect.Effect<Option.Option<ObjectCallIdParts>, DurableExecutionError> =>
      decodeObjectCallId(id).pipe(
        Effect.match({ onFailure: () => Option.none<ObjectCallIdParts>(), onSuccess: Option.some }),
      )

    // Block on an object call by folding its owner projection until it settles —
    // no residency, no roster (the durable `Completed` event is the source of truth).
    // `Pending` (admitted, unsettled) loops indefinitely; `Unknown` (never admitted)
    // is retried a bounded number of times to absorb a transient read lag right after
    // submit, then fails — a bogus id never loops forever.
    const UNKNOWN_ATTACH_RETRIES = 40
    const attachObject = <A, I>(
      callId: string,
      parts: ObjectCallIdParts,
      schema: Schema.Codec<A, I, never, never>,
      unknownBudget: number,
    ): Effect.Effect<A, DurableExecutionError> =>
      provideClient(store.status(callId, parts)).pipe(Effect.flatMap((st): Effect.Effect<A, DurableExecutionError> => {
        switch (st._tag) {
          case "Success":
            return decode(schema, st.value)
          case "Failure":
            return fail("attach", st.error)
          case "Defect":
            return fail("attach", st.defect)
          case "Interrupt":
            return fail("attach", "call was interrupted")
          case "Pending":
            return Effect.sleep(Duration.millis(25)).pipe(
              Effect.andThen(attachObject(callId, parts, schema, UNKNOWN_ATTACH_RETRIES)),
            )
          case "Unknown":
            return unknownBudget <= 0
              ? fail("attach", `unknown call: ${callId}`)
              : Effect.sleep(Duration.millis(25)).pipe(
                Effect.andThen(attachObject(callId, parts, schema, unknownBudget - 1)),
              )
        }
      }))

    const attach = <A, I>(
      executionId: string,
      schema: Schema.Codec<A, I, never, never>,
    ): Effect.Effect<A, DurableExecutionError> =>
      Effect.gen(function*() {
        const parts = yield* decodeParts(executionId)
        if (Option.isSome(parts)) {
          return yield* attachObject(executionId, parts.value, schema, UNKNOWN_ATTACH_RETRIES)
        }
        // service: if we own it, wait for the waiter; then read + decode the roster.
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
        }
        const row = yield* roster.get(executionId).pipe(Effect.mapError(toError("attach")))
        if (Option.isNone(row)) return yield* fail("attach", `unknown execution: ${executionId}`)
        if (row.value.status === "completed") return yield* decode(schema, row.value.result)
        if (row.value.status === "failed") return yield* fail("attach", row.value.error ?? "execution failed")
        // running/suspended but not (yet) owned here — wait and re-check.
        yield* Effect.sleep(Duration.millis(25))
        return yield* attach(executionId, schema)
      })

    const poll = <A, I>(
      executionId: string,
      schema: Schema.Codec<A, I, never, never>,
    ): Effect.Effect<Option.Option<A>, DurableExecutionError> =>
      Effect.gen(function*() {
        const parts = yield* decodeParts(executionId)
        if (Option.isSome(parts)) {
          const st = yield* provideClient(store.status(executionId, parts.value))
          return st._tag === "Success" ? Option.some(yield* decode(schema, st.value)) : Option.none<A>()
        }
        const row = yield* roster.get(executionId).pipe(Effect.mapError(toError("poll")))
        return Option.isSome(row) && row.value.status === "completed"
          ? Option.some(yield* decode(schema, row.value.result))
          : Option.none<A>()
      })

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
      callStep,
      sendStep,
    }

    // re-drive any running/suspended executions left by a prior process before
    // serving requests, so a recovered execution is resident (in `running`) and
    // can be `attach`ed / resolved exactly like a freshly-submitted one. Objects are
    // re-driven from their owner streams (enumerate keys + restart pending heads).
    yield* bootRecover
    yield* objectBootRecover

    return api
  })
