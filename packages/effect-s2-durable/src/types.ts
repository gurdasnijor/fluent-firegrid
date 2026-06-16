import type { Duration, Effect, Option, Schema } from "effect"
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

declare const RunActionViolationId: unique symbol

/**
 * The type a `run` call resolves to when its action illegally requires the
 * durable runtime — a non-`Effect` brand so `yield*`-ing it is a compile error at
 * the `run` call site. Carries a human-readable message in `M`.
 */
export interface RunActionViolation<M extends string> {
  readonly [RunActionViolationId]: M
}

/**
 * The `run` free primitive (a durable, replay-aware side-effect boundary). A run
 * action may use the caller's own services, but **not** the durable runtime: if
 * the action requires `DurableExecutionRuntime` (i.e. it calls `run`/`sleep`/
 * `state`/`signal`), `run` resolves to a `RunActionViolation` instead of an
 * `Effect`, so the misuse is a type error here rather than a runtime fault. This
 * is the Effect analog of Restate's ctx-less `run` closure.
 */
export interface Run {
  <A, E, R, EncodedA = unknown, EncodedE = unknown>(
    key: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ): [DurableExecutionRuntime] extends [R]
    ? RunActionViolation<"a run action cannot use durable primitives (run/sleep/state/signal); use them in the handler body">
    : Effect.Effect<A, E | DurableExecutionError, R | DurableExecutionRuntime>
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

/**
 * A handle to one user-defined durable state collection, scoped to the active
 * execution's stream. `state(Table)` returns this synchronously (it's pure — it
 * just names a table); the *operations* are the Effects. v1 surface is
 * `get`/`set`/`delete` (Restate's minimal key→record shape — `set` is upsert with
 * the primary key carried as a row field). Writes commit on their own ack (a crash
 * can't desync them from the journal because they're ordered after the writes
 * before them). State mutations inside a `run` action are rejected — perform them
 * in the handler body.
 */
export interface StateBinding<Row> {
  readonly get: (key: string) => Effect.Effect<Option.Option<Row>, DurableExecutionError, DurableExecutionRuntime>
  readonly set: (row: Row) => Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime>
  readonly delete: (key: string) => Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime>
}
