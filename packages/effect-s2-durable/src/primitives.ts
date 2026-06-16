import { type Duration, Effect, type Schema } from "effect"
import type { AnyTable, RowOf } from "effect-s2-stream-db"
import type { DurableExecutionError } from "./errors.ts"
import { DurableExecutionRuntime } from "./Runtime.ts"
import type { Run, StateBinding } from "./types.ts"

/**
 * The free primitives — module-level functions that read the active-invocation
 * slot (internal) and delegate to the ambient `DurableExecutionRuntime`. There is
 * no `ctx` object and no public runtime accessor; a durable program is plain
 * `Effect.gen` that yields these. Slice 1–3 ship `run` / `handlerRequest` /
 * `sleep` / `state`; `signal`/`awakeable`/`deferred` arrive next.
 */

/**
 * A durable, replay-aware side-effect boundary (memoized by `key`). The action may
 * use the caller's own services but not the durable runtime (enforced by `Run`'s
 * type — see `RunActionViolation`); the cast bridges the impl to that signature.
 */
export const run: Run = ((key: string, action: Effect.Effect<unknown, unknown, never>, options?: never) =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.runStep(key, action, options))) as unknown as Run

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
 * key→record store (`get`/`set`/`delete`). `Table` is any `effect-s2-stream-db`
 * table; its rows live alongside the engine's own tables in the one stream.
 *
 * Returns the binding **synchronously** — it just names a table (pure); only the
 * operations are Effects. Reusable as a plain value: `const cart = state(Cart)`.
 */
export const state = <Tbl extends AnyTable>(table: Tbl): StateBinding<RowOf<Tbl>> => ({
  get: (key) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.stateGet(table, key)),
  set: (row) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.stateSet(table, row)),
  delete: (key) => Effect.flatMap(DurableExecutionRuntime, (rt) => rt.stateDelete(table, key)),
})
