import { type Duration, Effect, type Option, type Schema } from "effect"
import type { AnyTable, RowOf } from "effect-s2-stream-db"
import type { DurableExecutionError } from "./errors.ts"
import { DurableExecutionRuntime } from "./Runtime.ts"
import type { AwakeableHandle, DeferredHandle, IngressResolve, Run, RunOptions, StateBinding } from "./types.ts"

/**
 * The free primitives — module-level functions that read the active-invocation
 * slot (internal) and delegate to the ambient `DurableExecutionRuntime`. There is
 * no `ctx` object and no public runtime accessor; a durable program is plain
 * `Effect.gen` that yields these.
 */

/**
 * A durable, replay-aware side-effect boundary. The action may use the caller's
 * own services but not the durable runtime (enforced by `Run`'s type — see
 * `RunActionViolation`). The cast bridges the impl to that conditional public
 * signature, which the runtime impl can't express directly.
 */
type RunImplOptions = RunOptions<unknown, unknown, unknown, unknown>

// eslint-disable-next-line local/no-launder-cast -- public Run carries a conditional violation brand the impl can't produce; runtime always returns the Effect branch
export const run: Run = ((
  actionOrName: Effect.Effect<unknown, unknown, never> | string,
  actionOrOptions?: Effect.Effect<unknown, unknown, never> | RunImplOptions,
  options?: RunImplOptions,
) =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => {
    if (typeof actionOrName === "string") {
      const namedOptions: RunImplOptions = { ...options, name: actionOrName }
      return rt.runStep(actionOrOptions as Effect.Effect<unknown, unknown, never>, namedOptions)
    }
    return rt.runStep(actionOrName, actionOrOptions as RunImplOptions | undefined)
  })) as unknown as Run

/** The decoded handler request (the active invocation's input). */
export const handlerRequest = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
): Effect.Effect<A, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.handlerRequest(schema))

/** A durable timer: suspend the step until `duration` has elapsed (replay-safe). */
export const sleep = (
  name: string,
  duration: Duration.Duration,
): Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.sleepStep(name, duration))

/**
 * A user-defined durable state collection over the active execution's stream — a
 * key→record store (`get`/`set`/`delete`). Returned **synchronously** (it just
 * names a table); only the operations are Effects.
 */
export const state = <Tbl extends AnyTable>(table: Tbl): StateBinding<RowOf<Tbl>> => ({
  get: (key) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.stateGet(table, key)),
  set: (row) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.stateSet(table, row)),
  delete: (key) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.stateDelete(table, key)),
})

/**
 * Receiver-side durable signal: park until an external caller resolves `name` via
 * `resolveSignal(executionId, name, …)`. Returns the decoded value.
 */
export const signal = <A, I>(
  name: string,
  schema: Schema.Codec<A, I, never, never>,
): Effect.Effect<A, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.awaitDeferred(name, schema))

/**
 * A named, invocation-scoped durable promise resolved by the handler itself.
 * Returned synchronously; `resolve`/`get` are Effects.
 */
export const deferred = <A, I>(name: string, schema: Schema.Codec<A, I, never, never>): DeferredHandle<A> => ({
  resolve: (value) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.resolveLocal(name, schema, value)),
  get: () => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.awaitDeferred(name, schema)),
})

/**
 * An externally-completed durable handle: `{ id, promise }`. Hand `id` to an
 * ingress client; `promise` parks until `resolveAwakeable(executionId, id, …)`.
 */
export const awakeable = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
): Effect.Effect<AwakeableHandle<A>, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.flatMap(DurableExecutionRuntime, (rt) =>
    Effect.map(rt.nextAwakeableId, (id) => ({
      id,
      promise: Effect.flatMap(DurableExecutionRuntime, (r) => r.awaitDeferred(id, schema)),
    })))

/** Ingress door: resolve a receiver-side `signal(name)` on `executionId`. */
export const resolveSignal: IngressResolve = (executionId, name, schema, value) =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.resolveExternal(executionId, name, schema, value))

/** Ingress door: resolve an `awakeable()` by its `id` on `executionId`. */
export const resolveAwakeable: IngressResolve = (executionId, id, schema, value) =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.resolveExternal(executionId, id, schema, value))

/** Block until `executionId` finishes, decoding its output via `schema` (restate's `attach`). */
export const attach = <A, I>(
  executionId: string,
  schema: Schema.Codec<A, I, never, never>,
): Effect.Effect<A, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.attach(executionId, schema))

/** Non-blocking read of `executionId`'s completed output, decoded via `schema`. */
export const poll = <A, I>(
  executionId: string,
  schema: Schema.Codec<A, I, never, never>,
): Effect.Effect<Option.Option<A>, DurableExecutionError, DurableExecutionRuntime> =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.poll(executionId, schema))
