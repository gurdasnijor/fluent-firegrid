import { type Duration, Effect, type Schema } from "effect"
import type { DurableExecutionError } from "./errors.ts"
import { DurableExecutionRuntime } from "./Runtime.ts"
import type { Run } from "./types.ts"

/**
 * The free primitives — module-level functions that read the active-invocation
 * slot (internal) and delegate to the ambient `DurableExecutionRuntime`. There is
 * no `ctx` object and no public runtime accessor; a durable program is plain
 * `Effect.gen` that yields these. Slice 1 ships `run` and `handlerRequest`;
 * `sleep`/`signal`/`awakeable`/`deferred`/`state` arrive in later slices.
 */

/** A durable, replay-aware side-effect boundary (memoized by `key`). */
export const run: Run = (key, action, options) =>
  Effect.flatMap(DurableExecutionRuntime, (rt) => rt.runStep(key, action, options))

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
