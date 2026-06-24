import type { AnyTable, RowOf } from "effect-s2-stream-db"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import type * as Schema from "effect/Schema"
import { DurableEngine } from "../engine/api.ts"
import type { DurableExecutionError } from "../errors.ts"
import { CurrentInvocationScope } from "../invocation/scope.ts"
import type { AwakeableHandle, DeferredHandle, IngressResolve, Run, RunOptions, StateBinding } from "./types.ts"

/**
 * The free primitives — module-level functions over the active invocation scope.
 * There is no custom Operation type; user code is plain `Effect.gen` yielding
 * ordinary Effects.
 */

/**
 * A durable, replay-aware side-effect boundary. The action may use the caller's
 * own services but not the durable engine (enforced by `Run`'s type — see
 * `RunActionViolation`). The cast bridges the impl to that conditional public
 * signature, which the engine impl can't express directly.
 */
type RunImplOptions = RunOptions<unknown, unknown, unknown, unknown>

// Intentional public-surface cast: Run carries a conditional violation brand the impl cannot produce; the implementation always returns the Effect branch.
export const run: Run = ((
  actionOrName: Effect.Effect<unknown, unknown, never> | string,
  actionOrOptions?: Effect.Effect<unknown, unknown, never> | RunImplOptions,
  options?: RunImplOptions
) =>
  Effect.flatMap(CurrentInvocationScope, (scope) => {
    if (typeof actionOrName === "string") {
      const namedOptions: RunImplOptions = { ...options, name: actionOrName }
      return scope.steps.run(actionOrOptions as Effect.Effect<unknown, unknown, never>, namedOptions)
    }
    return scope.steps.run(actionOrName, actionOrOptions as RunImplOptions | undefined)
  })) as unknown as Run

/** The decoded handler request (the active invocation's input). */
export const handlerRequest = <A, I>(
  schema: Schema.Codec<A, I, never, never>
): Effect.Effect<A, DurableExecutionError, CurrentInvocationScope> =>
  Effect.flatMap(CurrentInvocationScope, (scope) => scope.request.input(schema))

/** A durable timer: suspend the step until `duration` has elapsed (replay-safe). */
export const sleep = (
  name: string,
  duration: Duration.Duration
): Effect.Effect<void, DurableExecutionError, CurrentInvocationScope> =>
  Effect.flatMap(CurrentInvocationScope, (scope) => scope.clock.sleep(name, duration))

/**
 * A user-defined durable state collection over the active execution's stream — a
 * key→record store (`get`/`set`/`delete`). Returned **synchronously** (it just
 * names a table); only the operations are Effects.
 */
export const state = <Tbl extends AnyTable>(table: Tbl): StateBinding<RowOf<Tbl>> => ({
  get: (key) => Effect.flatMap(CurrentInvocationScope, (scope) => scope.state.table(table).get(key)),
  set: (row) => Effect.flatMap(CurrentInvocationScope, (scope) => scope.state.table(table).set(row)),
  delete: (key) => Effect.flatMap(CurrentInvocationScope, (scope) => scope.state.table(table).delete(key))
})

/**
 * Receiver-side durable signal: park until an external caller resolves `name` via
 * `resolveSignal(executionId, name, …)`. Returns the decoded value.
 */
export const signal = <A, I>(
  name: string,
  schema: Schema.Codec<A, I, never, never>
): Effect.Effect<A, DurableExecutionError, CurrentInvocationScope> =>
  Effect.flatMap(CurrentInvocationScope, (scope) => scope.durablePromises.await(name, schema))

/**
 * A named, invocation-scoped durable promise resolved by the handler itself.
 * Returned synchronously; `resolve`/`get` are Effects.
 */
export const deferred = <A, I>(name: string, schema: Schema.Codec<A, I, never, never>): DeferredHandle<A> => ({
  resolve: (value) =>
    Effect.flatMap(CurrentInvocationScope, (scope) => scope.durablePromises.resolve(name, schema, value)),
  get: () => Effect.flatMap(CurrentInvocationScope, (scope) => scope.durablePromises.await(name, schema))
})

/**
 * An externally-completed durable handle: `{ id, promise }`. Hand `id` to an
 * ingress client; `promise` parks until `resolveAwakeable(executionId, id, …)`.
 */
export const awakeable = <A, I>(
  schema: Schema.Codec<A, I, never, never>
): Effect.Effect<AwakeableHandle<A>, DurableExecutionError, CurrentInvocationScope> =>
  Effect.flatMap(CurrentInvocationScope, (scope) =>
    Effect.map(scope.awakeables.create(schema), (handle) => ({
      id: handle.id,
      promise: handle.promise
    })))

/** Ingress door: resolve a receiver-side `signal(name)` on `executionId`. */
export const resolveSignal: IngressResolve = (executionId, name, schema, value) =>
  Effect.flatMap(DurableEngine, (rt) => rt.resolveDurablePromise(executionId, name, schema, value))

/**
 * From inside a SHARED workflow handler, resolve a durable promise the workflow's `run`
 * body awaits via `signal(name)`. The workflow is identified by the active shared call's
 * owner (object + key), so a shared signal handler reads `sharedClient(wf, id).approve(v)`
 * naturally. This appends an ingress `SignalResolved` to the run's owner stream — the one
 * write a shared handler may perform (HANDLERS.5); it never mutates user state.
 */
export const resolvePromise = <A, I>(
  name: string,
  schema: Schema.Codec<A, I, never, never>,
  value: A
): Effect.Effect<void, DurableExecutionError, CurrentInvocationScope> =>
  Effect.flatMap(CurrentInvocationScope, (scope) => scope.durablePromises.resolveWorkflow(name, schema, value))

/** Ingress door: resolve an `awakeable()` by its `id` on `executionId`. */
export const resolveAwakeable: IngressResolve = (executionId, id, schema, value) =>
  Effect.flatMap(DurableEngine, (rt) => rt.resolveAwakeable(executionId, id, schema, value))

/** Block until `executionId` finishes, decoding its output via `schema` (restate's `attach`). */
export const attach = <A, I>(
  executionId: string,
  schema: Schema.Codec<A, I, never, never>
): Effect.Effect<A, DurableExecutionError, DurableEngine> =>
  Effect.flatMap(DurableEngine, (rt) => rt.attach(executionId, schema))

/** Non-blocking read of `executionId`'s completed output, decoded via `schema`. */
export const poll = <A, I>(
  executionId: string,
  schema: Schema.Codec<A, I, never, never>
): Effect.Effect<Option.Option<A>, DurableExecutionError, DurableEngine> =>
  Effect.flatMap(DurableEngine, (rt) => rt.poll(executionId, schema))

// Durable inter-execution calls are exposed as typed clients in invocation/client.ts.
