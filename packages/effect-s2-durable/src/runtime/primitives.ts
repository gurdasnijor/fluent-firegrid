import { Cause, Clock, Context, Deferred, Duration, Effect, Exit, HashMap, Layer, Option, Ref, Schema } from "effect"
import type { AnyTable, RowOf } from "effect-s2-stream-db"
import { encodeObjectCallId, stateValue, type ObjectCallIdParts } from "../actor/core.ts"
import type { DurableExecutionError } from "../errors.ts"
import type { RunOptions } from "../types.ts"
import {
  decode,
  decodeRowFor,
  encode,
  encodeRowFor,
  asServiceFreeDecoder,
  asServiceFreeEncoder,
  fail,
  pkOf,
  resolvedValue,
  scheduleOf,
  sharedForbidden,
  toError,
} from "./helpers.ts"
import { ActiveInvocation, type ObjectInvocation, type ServiceInvocation, type StepRecord, type TimerRecord } from "./invocation.ts"
import { resolveServiceDeferred, serviceWaiterKey } from "./serviceDeferreds.ts"
import { RuntimeState } from "./state.ts"
import { RuntimeStores } from "./stores.ts"

type ResolvePrimitive = <A, I>(
  name: string,
  schema: Schema.Codec<A, I, never, never>,
  value: A,
) => Effect.Effect<void, DurableExecutionError>

export interface PrimitiveInterpreterApi {
  readonly runStep: <A, E, R, EncodedA, EncodedE>(
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ) => Effect.Effect<A, E | DurableExecutionError, R>
  readonly handlerRequest: <A, I>(
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  readonly sleepStep: (name: string, duration: Duration.Duration) => Effect.Effect<void, DurableExecutionError>
  readonly stateGet: <Tbl extends AnyTable>(
    table: Tbl,
    key: string,
  ) => Effect.Effect<Option.Option<RowOf<Tbl>>, DurableExecutionError>
  readonly stateSet: <Tbl extends AnyTable>(table: Tbl, row: RowOf<Tbl>) => Effect.Effect<void, DurableExecutionError>
  readonly stateDelete: <Tbl extends AnyTable>(table: Tbl, key: string) => Effect.Effect<void, DurableExecutionError>
  readonly awaitDeferred: <A, I>(
    name: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  readonly resolveLocal: ResolvePrimitive
  readonly resolvePromise: ResolvePrimitive
  readonly nextAwakeableId: Effect.Effect<string, DurableExecutionError>
}

const withActive = (operation: string) =>
  Effect.flatMap(ActiveInvocation, (opt) =>
    Option.isNone(opt) ? fail(operation, `${operation} called outside an active handler`) : Effect.succeed(opt.value))

const make: Effect.Effect<PrimitiveInterpreterApi, never, RuntimeState | RuntimeStores> = Effect.gen(function*() {
  const runtimeState = yield* RuntimeState
  const stores = yield* RuntimeStores
  const { waiters } = runtimeState
  const { objectStore: store, provideClient, roster } = stores

  const runJournalFor = (active: ServiceInvocation | ObjectInvocation) =>
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
        if (active.kind === "shared") return yield* sharedForbidden("run")
        const ordinal = yield* Ref.getAndUpdate(active.runSeq, (n) => n + 1)
        const stepName = options?.name ?? `run/${ordinal}`
        const journal = runJournalFor(active)
        const existing = yield* journal.get(stepName)
        if (Option.isSome(existing)) {
          const row = existing.value
          if (row.success) return options?.output !== undefined ? yield* decode(options.output, row.value) : (row.value as A)
          const error = options?.error !== undefined ? yield* decode(options.error, row.error) : (row.error as E)
          return yield* Effect.fail(error)
        }

        const attempted = options?.retry !== undefined
          ? action.pipe(
            Effect.retry({ schedule: scheduleOf(options.retry), times: Math.max(0, options.retry.maxAttempts - 1) }),
          )
          : action
        const outcome = yield* Effect.exit(attempted)
        if (Exit.isSuccess(outcome)) {
          const value = options?.output !== undefined ? yield* encode(options.output, outcome.value) : outcome.value
          yield* journal.put(stepName, { success: true, value })
          return outcome.value
        }
        const failure = Cause.findErrorOption(outcome.cause)
        if (options?.error !== undefined && Option.isSome(failure)) {
          const error = yield* encode(options.error, failure.value)
          yield* journal.put(stepName, { success: false, error })
        }
        return yield* outcome
      }),
    ))

  const handlerRequest = <A, I>(schema: Schema.Codec<A, I, never, never>): Effect.Effect<A, DurableExecutionError> =>
    withActive("handlerRequest").pipe(Effect.flatMap((active) => decode(schema, active.inputEncoded)))

  const sleepTimerFor = (active: ServiceInvocation | ObjectInvocation) =>
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
        if (active.kind === "shared") return yield* sharedForbidden("sleep")
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

  const stateGet = <Tbl extends AnyTable>(
    table: Tbl,
    key: string,
  ): Effect.Effect<Option.Option<RowOf<Tbl>>, DurableExecutionError> =>
    withActive("state.get").pipe(Effect.flatMap((active) =>
      active.kind === "shared"
        ? Option.match(stateValue(active.snapshot, table.tableName, key), {
          onNone: () => Effect.succeedNone,
          onSome: (encoded) => decodeRowFor(table, encoded).pipe(Effect.map((row) => Option.some(row as RowOf<Tbl>))),
        })
        : active.kind === "object"
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
          const readCodec = Schema.NullOr(table.schema)
          const decodeRead = (encoded: unknown) =>
            Effect.try({
              try: () => Schema.decodeUnknownSync(asServiceFreeDecoder(readCodec))(encoded),
              catch: toError("state.get"),
            })
          const encodeRead = (value: RowOf<Tbl> | null) =>
            Effect.try({
              try: () => Schema.encodeUnknownSync(asServiceFreeEncoder(readCodec))(value),
              catch: toError("state.get"),
            })

          const recorded = yield* active.db.stateReads.get(readKey).pipe(Effect.mapError(toError("state.get")))
          if (Option.isSome(recorded)) {
            return Option.fromNullishOr(yield* decodeRead(recorded.value.value))
          }
          const current = yield* active.stateDb.table(table).get(key).pipe(Effect.mapError(toError("state.get")))
          const encoded = yield* encodeRead(Option.getOrNull(current))
          yield* active.db.stateReads.insert({ readKey, value: encoded }).pipe(Effect.mapError(toError("state.get")))
          return current
        }),
    ))

  const stateSet = <Tbl extends AnyTable>(table: Tbl, row: RowOf<Tbl>): Effect.Effect<void, DurableExecutionError> =>
    withActive("state.set").pipe(Effect.flatMap((active) =>
      active.kind === "shared"
        ? sharedForbidden("state.set")
        : active.kind === "object"
        ? provideClient(
          encodeRowFor(table, row).pipe(
            Effect.flatMap((encoded) => active.state.set(table.tableName, pkOf(table, row), encoded)),
          ),
        )
        : active.stateDb.table(table).upsert(row).pipe(Effect.mapError(toError("state.set"))),
    ))

  const stateDelete = <Tbl extends AnyTable>(table: Tbl, key: string): Effect.Effect<void, DurableExecutionError> =>
    withActive("state.delete").pipe(Effect.flatMap((active) =>
      active.kind === "shared"
        ? sharedForbidden("state.delete")
        : active.kind === "object"
        ? provideClient(active.state.delete(table.tableName, key))
        : active.stateDb.table(table).delete(key).pipe(Effect.mapError(toError("state.delete"))),
    ))

  const markSuspended = (active: ServiceInvocation, kind: "deferred-wait" | "pending-clock"): Effect.Effect<void, never> =>
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
      Effect.ignore,
    )

  const awaitDeferred = <A, I>(
    name: string,
    schema: Schema.Codec<A, I, never, never>,
  ): Effect.Effect<A, DurableExecutionError> =>
    withActive("await").pipe(Effect.flatMap((active) =>
      active.kind === "shared"
        ? sharedForbidden("await")
        : active.kind === "object"
        ? provideClient(active.state.signal.await(name)).pipe(Effect.flatMap((raw) => decode(schema, raw)))
        : Effect.gen(function*() {
          const readRow = () =>
            active.db.deferreds.get(name).pipe(Effect.mapError(toError("await")), Effect.map(resolvedValue))
          const first = yield* readRow()
          if (Option.isSome(first)) return yield* decode(schema, first.value)

          const w = yield* Deferred.make<void>()
          const key = serviceWaiterKey(active.executionId, name)
          yield* Ref.update(waiters, HashMap.set(key, w))
          return yield* Effect.gen(function*() {
            yield* markSuspended(active, "deferred-wait")
            const second = yield* readRow()
            if (Option.isNone(second)) yield* Deferred.await(w)
            const resolved = yield* readRow()
            return Option.isSome(resolved)
              ? yield* decode(schema, resolved.value)
              : yield* fail("await", `deferred ${JSON.stringify(name)} woke without a value`)
          }).pipe(Effect.ensuring(Ref.update(waiters, HashMap.remove(key))))
        }),
    ))

  const resolveLocal: PrimitiveInterpreterApi["resolveLocal"] = (name, schema, value) =>
    withActive("resolve").pipe(Effect.flatMap((active) =>
      active.kind === "shared"
        ? sharedForbidden("resolve")
        : active.kind === "object"
        ? encode(schema, value).pipe(Effect.flatMap((enc) => provideClient(active.state.signal.resolve(name, enc))))
        : encode(schema, value).pipe(
          Effect.flatMap((enc) => resolveServiceDeferred(waiters, active.db, active.executionId, name, enc)),
        ),
    ))

  const resolvePromise: PrimitiveInterpreterApi["resolvePromise"] = (name, schema, value) =>
    withActive("resolvePromise").pipe(Effect.flatMap((active) =>
      active.kind !== "shared"
        ? fail("resolvePromise", "resolvePromise is only valid inside a shared workflow handler")
        : Effect.gen(function*() {
          const runParts: ObjectCallIdParts = { object: active.object, key: active.key, method: "run", nonce: active.key }
          const runCallId = yield* encodeObjectCallId(runParts).pipe(Effect.mapError(toError("resolvePromise")))
          const enc = yield* encode(schema, value)
          yield* provideClient(store.resolveSignal(runCallId, runParts, name, enc))
        }),
    ))

  const nextAwakeableId: Effect.Effect<string, DurableExecutionError> = withActive("awakeable").pipe(
    Effect.flatMap((active) =>
      active.kind === "shared"
        ? sharedForbidden("awakeable")
        : active.kind === "object"
        ? Ref.getAndUpdate(active.awakeSeq, (n) => n + 1).pipe(Effect.map((ordinal) => `${active.callId}/awk/${ordinal}`))
        : Ref.getAndUpdate(active.awakeSeq, (n) => n + 1).pipe(Effect.map((ordinal) => `${active.executionId}/awk/${ordinal}`)),
    ),
  )

  return {
    runStep,
    handlerRequest,
    sleepStep,
    stateGet,
    stateSet,
    stateDelete,
    awaitDeferred,
    resolveLocal,
    resolvePromise,
    nextAwakeableId,
  }
})

export class PrimitiveInterpreter extends Context.Service<PrimitiveInterpreter, PrimitiveInterpreterApi>()(
  "effect-s2-durable/runtime/primitives/PrimitiveInterpreter",
) {
  static readonly layer: Layer.Layer<PrimitiveInterpreter, never, RuntimeState | RuntimeStores> = Layer.effect(
    PrimitiveInterpreter,
    make,
  )
}
