import type { Duration, Effect, Schema } from "effect"
import type { DurableExecutionError } from "./errors.ts"
import type { DurableExecutionRuntime } from "./Runtime.ts"

/** Retry policy for a `run` step. Controls attempts *before* a terminal fact. */
export interface RetryPolicy {
  readonly maxAttempts: number
  readonly initialInterval?: Duration.Duration
  readonly maxInterval?: Duration.Duration
  readonly intervalFactor?: number
}

/**
 * Options for a `run` step. `output`/`error` are *discharged* schemas
 * (`Schema<A, I, never>`) — durable encode/decode cannot require live services.
 */
export interface RunOptions<A, E = never, EncodedA = unknown, EncodedE = unknown> {
  readonly output?: Schema.Codec<A, EncodedA, never, never>
  readonly error?: Schema.Codec<E, EncodedE, never, never>
  readonly retry?: RetryPolicy
  readonly idempotencyKey?: string
}

/** The `run` free primitive (a durable, replay-aware side-effect boundary). */
export interface Run {
  <A, E, R, EncodedA = unknown, EncodedE = unknown>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ): Effect.Effect<A, E | DurableExecutionError, R | DurableExecutionRuntime>
}

/**
 * A handler definition: stable identity + input/output schemas + the durable
 * program. The program is just an ordinary Effect that additionally requires the
 * `DurableExecutionRuntime` (supplied by the host Layer) — there is no custom
 * program/operation type; user code is plain `Effect.gen` + the free primitives.
 * `handler(...)` is the only definition primitive (no service/object/workflow
 * containers — those belong to hosts above this package).
 */
export interface Handler<I, O, E = never, R = never> {
  readonly name: string
  /** Decodes/encodes the request (typed `I` via the phantom below). */
  readonly input: Schema.Top
  /** Decodes/encodes the result. */
  readonly output: Schema.Top
  readonly program: Effect.Effect<O, E, R | DurableExecutionRuntime>
  /** Phantom carrier for the decoded input type `I`. */
  readonly Input: I
}

/** Any handler definition (existential over its type parameters). */
export type AnyHandler = Handler<any, any, any, any>
