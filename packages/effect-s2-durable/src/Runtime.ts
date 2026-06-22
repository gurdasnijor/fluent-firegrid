import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  HashMap,
  Layer,
  Option,
  Ref,
  Schema,
} from "effect"
import { type S2Client } from "effect-s2"
import {
  type ActorExit,
  encodeObjectCallId,
  type ObjectCallIdParts,
} from "./actor/core.ts"
import { type AdmitResult, type ObjectStateBackend, type RunHead } from "./actor/object.ts"
import { DurableExecutionError } from "./errors.ts"
import { decodeExecutionAddress, objectPartsOption } from "./runtime/address.ts"
import { CompletionReader, type CompletionReaderApi } from "./runtime/completion.ts"
import {
  decode,
  encode,
  fail,
  sharedForbidden,
  toActorExit,
  toError,
} from "./runtime/helpers.ts"
import {
  ActiveInvocation,
  type Invocation,
  type ObjectHandlerSeed,
  type ObjectInvocation,
  type RegisteredHandler,
  type RunningEntry,
  type ServiceInvocation,
  type SharedObjectInvocation,
  type WfDb,
} from "./runtime/invocation.ts"
import { IngressRouter, type IngressRouterApi } from "./runtime/ingress.ts"
import { PrimitiveInterpreter, type PrimitiveInterpreterApi } from "./runtime/primitives.ts"
import { RuntimeState } from "./runtime/state.ts"
import { RuntimeStores } from "./runtime/stores.ts"
import type { Handler } from "./types.ts"

export type { ObjectHandlerSeed, RegisteredHandler } from "./runtime/invocation.ts"

const isDurableExecutionError = Schema.is(DurableExecutionError)
const isObjectInfrastructureError = (error: DurableExecutionError): boolean => error.operation.startsWith("object.")

/** The address of a durable object call target (`call`/`send` between executions). */
export interface CallTarget {
  readonly object: string
  readonly key: string
  readonly method: string
}

/**
 * The outcome of starting a workflow. A workflow `run` is admitted **at most once**
 * per workflow id: the first start is `"started"`; any later start (while running or
 * after completion) is `"alreadyStarted"` — never a second run (Restate workflow
 * semantics: duplicate start returns already-started, not a deduped second run).
 */
export type WorkflowStartStatus = "started" | "alreadyStarted"

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
  readonly attach: CompletionReaderApi["attach"]
  /** Non-blocking read of the completed output, decoded via `schema`, if any. */
  readonly poll: CompletionReaderApi["poll"]
  /** The durable `run` step (delegated to by the `run` free primitive). */
  readonly runStep: PrimitiveInterpreterApi["runStep"]
  /** The decoded handler request (delegated to by the `handlerRequest` free primitive). */
  readonly handlerRequest: PrimitiveInterpreterApi["handlerRequest"]
  /** The durable timer (delegated to by the `sleep` free primitive). */
  readonly sleepStep: PrimitiveInterpreterApi["sleepStep"]
  /** State ops (delegated to by the `state(Table)` binding's methods). */
  readonly stateGet: PrimitiveInterpreterApi["stateGet"]
  readonly stateSet: PrimitiveInterpreterApi["stateSet"]
  readonly stateDelete: PrimitiveInterpreterApi["stateDelete"]
  /** Park until a named durable promise (signal/deferred/awakeable) is resolved. */
  readonly awaitDeferred: PrimitiveInterpreterApi["awaitDeferred"]
  /** Resolve a named durable promise on the active execution (handler-side `deferred.resolve`). */
  readonly resolveLocal: PrimitiveInterpreterApi["resolveLocal"]
  /** Resolve a named durable promise on another execution (ingress `signal`/`awakeable`). */
  readonly resolveExternal: IngressRouterApi["resolveExternal"]
  /**
   * From a SHARED workflow handler, resolve a durable promise the workflow's `run` body
   * awaits via `signal(name)`. Appends a `SignalResolved` to the run's owner stream
   * (derived from the active shared call's object + key) — an INGRESS append, the one
   * write HANDLERS.5 permits a shared handler (it never mutates user state). Delegated to
   * by the `resolvePromise(...)` free primitive; forbidden outside a shared handler.
   */
  readonly resolvePromise: PrimitiveInterpreterApi["resolvePromise"]
  /** A fresh replay-stable awakeable id for the active execution. */
  readonly nextAwakeableId: PrimitiveInterpreterApi["nextAwakeableId"]
  /**
   * Run a SHARED (read-only) object handler ephemerally over a folded snapshot —
   * no admission, no drainer, concurrent with the exclusive drainer (`EXECUTION.3`).
   * Delegated to by the `sharedClient(...)` proxy.
   */
  readonly sharedCall: <A, I>(
    handler: Handler<unknown, unknown, never, never>,
    object: string,
    key: string,
    input: unknown,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  /** Durable `call`: issue a child object call (encoding input via `inputSchema`) and await its decoded result. */
  readonly callStep: <A, I, B, J>(
    target: CallTarget,
    input: unknown,
    inputSchema: Schema.Codec<B, J, never, never>,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
  /** Durable one-way `send`: issue a child object call (encoding input via `inputSchema`), returning its id. */
  readonly sendStep: <B, J>(
    target: CallTarget,
    input: unknown,
    inputSchema: Schema.Codec<B, J, never, never>,
  ) => Effect.Effect<string, DurableExecutionError>
  /**
   * Start a workflow's `run` at its DETERMINISTIC owner call id (`runCallId`), encoding
   * input via the handler's input codec, and report whether this admitted a fresh run
   * (`"started"`) or hit an existing one (`"alreadyStarted"`). Run-once: a second start
   * never re-runs the body. Attach to the result via `attach(runCallId, …)`.
   */
  readonly workflowStart: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    runCallId: string,
    input: I,
  ) => Effect.Effect<WorkflowStartStatus, DurableExecutionError, R>
}

export class DurableExecutionRuntime
  extends Context.Service<DurableExecutionRuntime, DurableExecutionRuntimeApi>()(
    "effect-s2-durable/Runtime/DurableExecutionRuntime",
  )
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
    const base = Layer.mergeAll(RuntimeState.layer(handlers, objectSeeds), RuntimeStores.layer)
    const internal = Layer.mergeAll(PrimitiveInterpreter.layer, IngressRouter.layer, CompletionReader.layer).pipe(
      Layer.provideMerge(base),
    )
    return Layer.effect(DurableExecutionRuntime)(makeRuntime).pipe(Layer.provide(internal))
  }
}

const makeRuntime = Effect.gen(function*() {
    const runtimeState = yield* RuntimeState
    const stores = yield* RuntimeStores
    const { engineScope, registry, objectHandlers, objectNames, running } = runtimeState
    const { objectStore: store, openWf, provideClient, roster } = stores
    const ingress = yield* IngressRouter
    const completion = yield* CompletionReader

    const withActive = (operation: string): Effect.Effect<Invocation, DurableExecutionError> =>
      Effect.flatMap(ActiveInvocation, (opt) =>
        Option.isNone(opt) ? fail(operation, `${operation} called outside an active handler`) : Effect.succeed(opt.value))

    const primitives = yield* PrimitiveInterpreter

    const runInternalHandler = (
      handler: RegisteredHandler,
      invocation: ObjectInvocation | SharedObjectInvocation,
    ) =>
      handler.program.pipe(
        Effect.provideService(ActiveInvocation, Option.some(invocation)),
        Effect.provideService(DurableExecutionRuntime, api),
        Effect.exit,
      )

    // ── shared (read-only) object handlers ────────────────────────────────────
    const sharedCall: DurableExecutionRuntimeApi["sharedCall"] = (handler, object, key, input, schema) =>
      Effect.gen(function*() {
        // encode the input through the handler's input codec — the same boundary the
        // normal submit path uses, so a transform codec (decoded ≠ encoded) round-trips.
        const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
        // fold the owner stream once; the handler reads this snapshot, never the log.
        const snapshot = yield* provideClient(store.readSnapshot(object, key))
        const invocation: SharedObjectInvocation = { kind: "shared", object, key, method: handler.name, inputEncoded, snapshot }
        const exit = yield* runInternalHandler(handler, invocation)
        if (Exit.isFailure(exit)) {
          return yield* fail("shared", `shared handler failed: ${Cause.pretty(exit.cause)}`)
        }
        const encoded = yield* encode(handler.output as Schema.Codec<unknown, unknown, never, never>, exit.value)
        return yield* decode(schema, encoded)
      }).pipe(Effect.withSpan("effect-s2-durable.object.shared", { attributes: { object, key, method: handler.name } }))

    // ── durable inter-execution calls (`call` / `send`) ───────────────────────
    // A handler issues a child OBJECT call whose id is DETERMINISTIC — derived from
    // the caller id + a per-activation ordinal — so a replay recomputes the same id,
    // admission dedups, and the call is issued exactly once (the result is re-read,
    // never re-issued). No new event: it reuses admission (idempotent) + attach.
    const issueCall = (active: Invocation, target: CallTarget, input: unknown): Effect.Effect<string, DurableExecutionError> =>
      Effect.gen(function*() {
        // a SHARED (read-only) handler has no durable journal to anchor a replay-stable
        // child id — issuing a call from one would not be deterministic. Forbid it.
        if (active.kind === "shared") {
          return yield* sharedForbidden("call")
        }
        // a same-owner self-call (a handler calling its own object+key) would deadlock
        // on the per-key drainer lock — reject it clearly instead.
        if (active.kind === "object") {
          const self = Option.getOrUndefined(yield* objectPartsOption(active.callId))
          if (self !== undefined && self.object === target.object && self.key === target.key) {
            return yield* fail("call", `self-call to the same object/key (${target.object}/${target.key}) is not supported`)
          }
        }
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

    const callStep: DurableExecutionRuntimeApi["callStep"] = (target, input, inputSchema, schema) =>
      withActive("call").pipe(Effect.flatMap((active) =>
        // encode the input through the target's input codec — the same boundary submit
        // uses — so a transform codec (decoded ≠ encoded) round-trips on the target.
        encode(inputSchema, input).pipe(
          Effect.flatMap((enc) => issueCall(active, target, enc)),
          Effect.flatMap((callId) => completion.attach(callId, schema)),
        ),
      ))

    const sendStep: DurableExecutionRuntimeApi["sendStep"] = (target, input, inputSchema) =>
      withActive("send").pipe(Effect.flatMap((active) =>
        encode(inputSchema, input).pipe(Effect.flatMap((enc) => issueCall(active, target, enc))),
      ))

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
        const exit = yield* runInternalHandler(handler, invocation)
        if (Exit.isSuccess(exit)) {
          const encoded = yield* encode(handler.output as Schema.Codec<unknown, unknown, never, never>, exit.value)
          return { _tag: "Success", value: encoded }
        }
        const failure = Cause.findErrorOption(exit.cause)
        if (
          Option.isSome(failure) &&
          isDurableExecutionError(failure.value) &&
          isObjectInfrastructureError(failure.value)
        ) {
          return yield* Effect.fail(failure.value)
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

    // Durably admit an object call onto its owner log + fork the exclusive drainer,
    // returning the admission outcome (`Admitted` once; `AlreadyPending`/`AlreadyCompleted`
    // on a re-admit of the same id). Shared by `submit` (object branch, discards the
    // outcome) and `workflowStart` (which surfaces it as a run-once status).
    const admitObject = (
      handler: RegisteredHandler,
      callId: string,
      parts: ObjectCallIdParts,
      inputEncoded: unknown,
    ): Effect.Effect<AdmitResult, DurableExecutionError> =>
      Effect.gen(function*() {
        objectHandlers.set(`${parts.object}/${parts.method}`, handler)
        const outcome = yield* provideClient(store.admit(callId, parts, inputEncoded)) // idempotent by callId
        // fork the exclusive drainer; it runs the pending head(s) to completion.
        yield* Effect.forkIn(
          provideClient(store.drain(parts.object, parts.key, makeRunHead(parts.object))),
          engineScope,
        )
        return outcome
      })

    const workflowStart: DurableExecutionRuntimeApi["workflowStart"] = (handler, runCallId, input) =>
      Effect.gen(function*() {
        const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
        const address = yield* decodeExecutionAddress(runCallId)
        if (address._tag !== "object") {
          return yield* fail("workflowStart", `workflow run id is not an object call id: ${runCallId}`)
        }
        // Intentional existential handler cast: compiled workflow generics are recovered at the runtime boundary.
        const outcome = yield* admitObject(handler as unknown as RegisteredHandler, runCallId, address.parts, inputEncoded)
        // a fresh `Admitted` is the only "started"; an existing pending/completed run is
        // already-started (run-once: the body is never executed a second time).
        return outcome._tag === "Admitted" ? "started" : "alreadyStarted"
      })

    const submit: DurableExecutionRuntimeApi["submit"] = (handler, executionId, input) =>
      Effect.gen(function*() {
        const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
        // An object call id self-routes to its owner ActorEvent log; any other id is
        // a stateless service execution on the WorkflowDb/roster path.
        const address = yield* decodeExecutionAddress(executionId)
        if (address._tag === "object") {
          // Intentional existential handler cast: compiled object generics are recovered at the runtime boundary.
          yield* admitObject(handler as unknown as RegisteredHandler, executionId, address.parts, inputEncoded)
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

    const api: DurableExecutionRuntimeApi = {
      submit,
      attach: completion.attach,
      poll: completion.poll,
      runStep: primitives.runStep,
      handlerRequest: primitives.handlerRequest,
      sleepStep: primitives.sleepStep,
      stateGet: primitives.stateGet,
      stateSet: primitives.stateSet,
      stateDelete: primitives.stateDelete,
      awaitDeferred: primitives.awaitDeferred,
      resolveLocal: primitives.resolveLocal,
      resolveExternal: ingress.resolveExternal,
      resolvePromise: primitives.resolvePromise,
      nextAwakeableId: primitives.nextAwakeableId,
      sharedCall,
      callStep,
      sendStep,
      workflowStart,
    }

    // re-drive any running/suspended executions left by a prior process before
    // serving requests, so a recovered execution is resident (in `running`) and
    // can be `attach`ed / resolved exactly like a freshly-submitted one. Objects are
    // re-driven from their owner streams (enumerate keys + restart pending heads).
    yield* bootRecover
    yield* objectBootRecover

    return api
  })
