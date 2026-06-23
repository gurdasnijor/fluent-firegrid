import type { S2Client } from "effect-s2"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as HashMap from "effect/HashMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type { Handler } from "../authoring/types.ts"
import { DurableExecutionError } from "../errors.ts"
import { planChildInvocationId } from "../invocation/plan.ts"
import {
  CurrentInvocationScope,
  type InvocationScope,
  type ObjectCallTarget,
  type ServiceCallTarget
} from "../invocation/scope.ts"
import type { ObjectCallIdParts } from "../object/address.ts"
import type { ActorExit } from "../object/machine/index.ts"
import type { AdmitResult, ObjectStateBackend, RunHead } from "../object/owner-driver.ts"
import { decodeExecutionAddress, objectPartsOption } from "./address.ts"
import { DurableEngine, type DurableEngineApi } from "./api.ts"
import {
  ActiveInvocation,
  type Invocation,
  type ObjectHandlerSeed,
  type ObjectInvocation,
  type RegisteredHandler,
  type RunningEntry,
  type ServiceInvocation,
  type SharedObjectInvocation,
  type WfDb
} from "./context.ts"
import { DurableStores } from "./durable-stores.ts"
import { HandlerPrimitives } from "./handler-primitives.ts"
import { decode, encode, fail, sharedForbidden, toActorExit, toError } from "./helpers.ts"
import { ResolutionRouter } from "./resolution-router.ts"
import { ResultReader } from "./result-reader.ts"
import { EngineState } from "./state.ts"

const isDurableExecutionError = Schema.is(DurableExecutionError)
const isObjectInfrastructureError = (error: DurableExecutionError): boolean => error.operation.startsWith("object.")

/**
 * The S2-backed engine layer. Requires an `S2Client`; owns its fiber scope.
 * `handlers` seed service boot recovery; `objectSeeds` seed object owner-stream
 * recovery so a fresh engine can re-drive pending object heads.
 */
export const DurableEngineLive = (
  handlers: ReadonlyArray<RegisteredHandler> = [],
  objectSeeds: ReadonlyArray<ObjectHandlerSeed> = []
): Layer.Layer<DurableEngine, DurableExecutionError, S2Client> => {
  const base = Layer.mergeAll(EngineState.layer(handlers, objectSeeds), DurableStores.layer)
  const internal = Layer.mergeAll(HandlerPrimitives.layer, ResolutionRouter.layer, ResultReader.layer).pipe(
    Layer.provideMerge(base)
  )
  return Layer.effect(DurableEngine)(makeEngine).pipe(Layer.provide(internal))
}

const makeEngine = Effect.gen(function*() {
  const engineState = yield* EngineState
  const stores = yield* DurableStores
  const { engineScope, registry, objectHandlers, objectNames, running } = engineState
  const { objectDriver: store, openWf, provideClient, roster } = stores
  const ingress = yield* ResolutionRouter
  const completion = yield* ResultReader

  const withActive = (operation: string): Effect.Effect<Invocation, DurableExecutionError> =>
    Effect.flatMap(
      ActiveInvocation,
      (opt) =>
        Option.isNone(opt)
          ? fail(operation, `${operation} called outside an active handler`)
          : Effect.succeed(opt.value)
    )

  const primitives = yield* HandlerPrimitives

  const runInternalHandler = (
    handler: RegisteredHandler,
    invocation: ObjectInvocation | SharedObjectInvocation
  ) =>
    handler.program.pipe(
      Effect.provideService(ActiveInvocation, Option.some(invocation)),
      Effect.provideService(CurrentInvocationScope, makeInvocationScope(invocation)),
      Effect.exit
    )

  // ── shared (read-only) object handlers ────────────────────────────────────
  const sharedCall: DurableEngineApi["query"] = (handler, object, key, input, schema) =>
    Effect.gen(function*() {
      // encode the input through the handler's input codec — the same boundary the
      // normal submit path uses, so a transform codec (decoded ≠ encoded) round-trips.
      const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
      // fold the owner stream once; the handler reads this snapshot, never the log.
      const snapshot = yield* provideClient(store.readSnapshot(object, key))
      const invocation: SharedObjectInvocation = {
        kind: "shared",
        object,
        key,
        method: handler.name,
        inputEncoded,
        snapshot
      }
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
  const parentIdOf = (active: Exclude<Invocation, SharedObjectInvocation>): string =>
    active.kind === "object" ? active.callId : active.executionId

  const nextChildId = (
    active: Invocation,
    target: { readonly kind: "service"; readonly name: string; readonly method: string } | {
      readonly kind: "object"
      readonly name: string
      readonly key: string
      readonly method: string
    }
  ): Effect.Effect<string, DurableExecutionError> =>
    Effect.gen(function*() {
      if (active.kind === "shared") {
        return yield* sharedForbidden("call")
      }
      const ordinal = yield* Ref.getAndUpdate(active.callSeq, (n) => n + 1)
      return yield* planChildInvocationId(parentIdOf(active), ordinal, target)
    })

  const issueObjectCall = (
    active: Invocation,
    target: ObjectCallTarget,
    input: unknown
  ): Effect.Effect<string, DurableExecutionError> =>
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
          return yield* fail(
            "call",
            `self-call to the same object/key (${target.object}/${target.key}) is not supported`
          )
        }
      }
      const callId = yield* nextChildId(active, {
        kind: "object",
        name: target.object,
        key: target.key,
        method: target.method
      })
      const callParts = yield* decodeExecutionAddress(callId).pipe(Effect.flatMap((address) =>
        address._tag === "object"
          ? Effect.succeed(address.parts)
          : fail("call", `child object id did not encode an object: ${callId}`)
      ))
      yield* provideClient(store.admit(callId, callParts, input)) // idempotent by callId
      // drive the target's drainer so the call runs (residency-independent dispatch).
      yield* Effect.forkIn(
        provideClient(store.drain(target.object, target.key, makeRunHead(target.object))),
        engineScope
      )
      return callId
    })

  const callObject = <A, I, B, J>(
    target: ObjectCallTarget,
    input: unknown,
    inputSchema: Schema.Codec<B, J, never, never>,
    schema: Schema.Codec<A, I, never, never>
  ) =>
    withActive("call").pipe(Effect.flatMap((active) =>
      // encode the input through the target's input codec — the same boundary submit
      // uses — so a transform codec (decoded ≠ encoded) round-trips on the target.
      encode(inputSchema, input).pipe(
        Effect.flatMap((enc) => issueObjectCall(active, target, enc)),
        Effect.flatMap((callId) => completion.attach(callId, schema))
      )
    ))

  const sendObject = <B, J>(target: ObjectCallTarget, input: unknown, inputSchema: Schema.Codec<B, J, never, never>) =>
    withActive("send").pipe(
      Effect.flatMap((active) =>
        encode(inputSchema, input).pipe(Effect.flatMap((enc) => issueObjectCall(active, target, enc)))
      )
    )

  const issueServiceCall = (
    active: Invocation,
    handler: Handler<unknown, unknown, never, never>,
    target: ServiceCallTarget,
    input: unknown
  ): Effect.Effect<string, DurableExecutionError> =>
    Effect.gen(function*() {
      const executionId = yield* nextChildId(active, {
        kind: "service",
        name: target.service,
        method: target.method
      })
      yield* submit(handler, executionId, input)
      return executionId
    })

  const callService: InvocationScope["calls"]["callService"] = (handler, target, input, schema) =>
    withActive("service.call").pipe(Effect.flatMap((active) =>
      issueServiceCall(active, handler, target, input).pipe(
        Effect.flatMap((executionId) => completion.attach(executionId, schema))
      )
    ))

  const sendService: InvocationScope["calls"]["sendService"] = (handler, target, input) =>
    withActive("service.send").pipe(Effect.flatMap((active) => issueServiceCall(active, handler, target, input)))

  const makeInvocationScope = (_invocation: Invocation): InvocationScope => ({
    request: {
      input: primitives.handlerRequest
    },
    steps: {
      run: primitives.runStep
    },
    clock: {
      sleep: primitives.sleepStep
    },
    state: {
      table: (table) => ({
        get: (key) => primitives.stateGet(table, key),
        set: (row) => primitives.stateSet(table, row),
        delete: (key) => primitives.stateDelete(table, key)
      })
    },
    awakeables: {
      create: (schema) =>
        primitives.nextAwakeableId.pipe(Effect.map((id) => ({
          id,
          promise: primitives.awaitDeferred(id, schema)
        })))
    },
    durablePromises: {
      await: primitives.awaitDeferred,
      resolve: primitives.resolveLocal,
      resolveWorkflow: primitives.resolvePromise
    },
    calls: {
      callService,
      sendService,
      callObject,
      sendObject,
      sharedObject: sharedCall
    }
  })

  // ── completion (SDD §B6): the result must outlive the dropped stream ───────
  const complete = (
    handler: { readonly name: string; readonly output: Schema.Top },
    executionId: string,
    db: WfDb,
    exit: Exit.Exit<unknown, unknown>
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
          updatedMs: now
        })
        // 3. drop the execution stream   4. mark resultAcked
        yield* db.drop
        yield* roster.upsert({
          executionId,
          handlerName: handler.name,
          status: "completed",
          result,
          resultAcked: true,
          updatedMs: now
        })
      } else {
        yield* roster.upsert({
          executionId,
          handlerName: handler.name,
          status: "failed",
          error: Cause.pretty(exit.cause),
          resultAcked: true,
          updatedMs: now
        })
        yield* db.drop
      }
      yield* Ref.update(running, HashMap.remove(executionId))
    }).pipe(Effect.mapError(toError("complete")))

  // Fork ONE service handler body into the engine scope, register it as the live
  // owner, and hand back its completion waiter. (Object calls do NOT run here —
  // they settle on their owner log via the ObjectOwnerDriver drainer.)
  const runExecution = <E, R>(
    handler: Handler<unknown, unknown, E, R>,
    executionId: string,
    db: WfDb,
    inputEncoded: unknown
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
        callSeq
      }
      const body: Effect.Effect<boolean, never, R> = handler.program.pipe(
        Effect.provideService(ActiveInvocation, Option.some(invocation)),
        Effect.provideService(CurrentInvocationScope, makeInvocationScope(invocation)),
        Effect.exit,
        Effect.flatMap((exit) =>
          // route a completion failure to the result waiter instead of dying
          Effect.matchCauseEffect(complete(handler, executionId, db, exit), {
            onFailure: (cause) => Deferred.failCause(deferred, cause),
            onSuccess: () => Deferred.succeed(deferred, exit)
          })
        )
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
    state: ObjectStateBackend
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
        callSeq
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
    inputEncoded: unknown
  ): Effect.Effect<AdmitResult, DurableExecutionError> =>
    Effect.gen(function*() {
      objectHandlers.set(`${parts.object}/${parts.method}`, handler)
      const outcome = yield* provideClient(store.admit(callId, parts, inputEncoded)) // idempotent by callId
      // fork the exclusive drainer; it runs the pending head(s) to completion.
      yield* Effect.forkIn(
        provideClient(store.drain(parts.object, parts.key, makeRunHead(parts.object))),
        engineScope
      )
      return outcome
    })

  const workflowStart: DurableEngineApi["workflowStart"] = (handler, runCallId, input) =>
    Effect.gen(function*() {
      const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
      const address = yield* decodeExecutionAddress(runCallId)
      if (address._tag !== "object") {
        return yield* fail("workflowStart", `workflow run id is not an object call id: ${runCallId}`)
      }
      // Intentional existential handler cast: compiled workflow generics are recovered at the engine boundary.
      const outcome = yield* admitObject(
        handler as unknown as RegisteredHandler,
        runCallId,
        address.parts,
        inputEncoded
      )
      // a fresh `Admitted` is the only "started"; an existing pending/completed run is
      // already-started (run-once: the body is never executed a second time).
      return outcome._tag === "Admitted" ? "started" : "alreadyStarted"
    })

  const submit: DurableEngineApi["submit"] = (handler, executionId, input) =>
    Effect.gen(function*() {
      const inputEncoded = yield* encode(handler.input as Schema.Codec<unknown, unknown, never, never>, input)
      // An object call id self-routes to its owner ActorEvent log; any other id is
      // a stateless service execution on the WorkflowDb/roster path.
      const address = yield* decodeExecutionAddress(executionId)
      if (address._tag === "object") {
        // Intentional existential handler cast: compiled object generics are recovered at the engine boundary.
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
        suspended: false
      }).pipe(Effect.mapError(toError("submit")))
      yield* roster.upsert({ executionId, handlerName: handler.name, status: "running", updatedMs: now }).pipe(
        Effect.mapError(toError("submit"))
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
          Effect.ignore // one execution's recovery must not abort boot
        )
    })

  // Service boot recovery: re-drive each running/suspended SERVICE execution.
  // (Objects no longer write the roster, so this query returns service rows only.)
  const bootRecover = roster
    .query((rows) =>
      rows.filter((r) => (r.status === "running" || r.status === "suspended") && r.objectKey === undefined)
    )
    .pipe(
      Effect.flatMap((services) =>
        Effect.forEach(services, (r) => recoverExecution(r.executionId, r.handlerName), { discard: true })
      ),
      Effect.withSpan("effect-s2-durable.boot-recover"),
      Effect.ignore // recovery is best-effort; never fail engine startup on it
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
            { discard: true }
          )
        ),
        Effect.ignore // one object's recovery must not abort boot
      ),
    { discard: true }
  ).pipe(Effect.withSpan("effect-s2-durable.object.boot-recover"), Effect.ignore)

  const api: DurableEngineApi = {
    submit,
    attach: completion.attach,
    poll: completion.poll,
    query: sharedCall,
    resolveAwakeable: ingress.resolveExternal,
    resolveDurablePromise: ingress.resolveExternal,
    workflowStart
  }

  // re-drive any running/suspended executions left by a prior process before
  // serving requests, so a recovered execution is resident (in `running`) and
  // can be `attach`ed / resolved exactly like a freshly-submitted one. Objects are
  // re-driven from their owner streams (enumerate keys + restart pending heads).
  yield* bootRecover
  yield* objectBootRecover

  return api
})
