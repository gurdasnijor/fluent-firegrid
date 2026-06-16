import type { Duration, Effect, Option, Schema } from "effect"
import type { DurableExecutionError } from "./errors.ts"
import type { DurableExecutionRuntime } from "./Runtime.ts"

/** Retry policy for a `run` step. Controls attempts *before* a terminal fact. */
export interface RetryPolicy {
  readonly maxAttempts: number
  readonly initialInterval?: Duration.Duration
  readonly intervalFactor?: number
  // NOTE: `maxInterval` (delay cap) is intentionally omitted until honored — see
  // `scheduleOf`. Re-add when the schedule actually caps the backoff.
}

/**
 * Options for a `run` step. `output`/`error` are *discharged* schemas
 * (`Schema<A, I, never>`) — durable encode/decode cannot require live services.
 */
export interface RunOptions<A, E = never, EncodedA = unknown, EncodedE = unknown> {
  /**
   * Optional stable name for this step's durable identity. Omit it and the step is
   * keyed by its **position** among the handler's `run` calls (like restate) — fine
   * for code that doesn't change shape mid-flight. Provide a name when you want an
   * identity stable across reordering (or a meaningful journal label).
   */
  readonly name?: string
  readonly output?: Schema.Codec<A, EncodedA, never, never>
  readonly error?: Schema.Codec<E, EncodedE, never, never>
  readonly retry?: RetryPolicy
  // NOTE: `idempotencyKey` (external-effect dedup) is intentionally omitted until
  // the engine surfaces it to the action — the engine can't enforce dedup on an
  // opaque action, so an inert field would mislead. Re-add with a RunAction that
  // receives it.
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

type RunResult<A, E, R> = [DurableExecutionRuntime] extends [R]
  ? RunActionViolation<"a run action cannot use durable primitives (run/sleep/state/signal); use them in the handler body">
  : Effect.Effect<A, E | DurableExecutionError, R | DurableExecutionRuntime>

/**
 * The `run` free primitive (a durable, replay-aware side-effect boundary):
 * `run(action, { name? })` or the compatibility form `run(name, action, options)`.
 * The action runs once; on replay the recorded value is returned without
 * re-running it. The action may use the caller's own services, but **not** the
 * durable runtime: if it requires `DurableExecutionRuntime` (i.e. it calls
 * `run`/`sleep`/`state`/`signal`), `run` resolves to a `RunActionViolation`
 * instead of an `Effect`, so the misuse is a type error here rather than a
 * runtime fault — the Effect analog of restate's ctx-less `run`.
 */
export interface Run {
  <A, E, R, EncodedA = unknown, EncodedE = unknown>(
    name: string,
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ): RunResult<A, E, R>
  <A, E, R, EncodedA = unknown, EncodedE = unknown>(
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ): RunResult<A, E, R>
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
  /** Phantom carrier for the decoded input type `I` (never set at runtime). */
  readonly Input?: I
}

/**
 * A named, invocation-scoped durable promise. `resolve` writes it (handler-side);
 * `get` reads it, parking the handler until it's resolved (replay returns the
 * recorded value). Both are Effects requiring the durable runtime.
 */
export interface DeferredHandle<A> {
  readonly resolve: (value: A) => Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime>
  readonly get: () => Effect.Effect<A, DurableExecutionError, DurableExecutionRuntime>
}

/**
 * An externally-completed durable handle. `id` is a replay-stable opaque token to
 * hand to an ingress client (derived from the execution id + an ordinal, so it's
 * the same on every replay); `promise` parks the handler until the id is resolved.
 */
export interface AwakeableHandle<A> {
  readonly id: string
  readonly promise: Effect.Effect<A, DurableExecutionError, DurableExecutionRuntime>
}

/**
 * An ingress door that resolves a named durable promise (`signal`/`awakeable`) on
 * another execution by key. Shared signature for the `resolveSignal` /
 * `resolveAwakeable` free functions.
 */
export interface IngressResolve {
  <A, I>(executionId: string, name: string, schema: Schema.Codec<A, I, never, never>, value: A): Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime>
}

/**
 * A handle to one user-defined durable state collection, scoped to the active
 * execution's stream. `state(Table)` returns this synchronously (it's pure — it
 * just names a table); the *operations* are the Effects. v1 surface is
 * `get`/`set`/`delete` (Restate's minimal key→record shape — `set` is upsert with
 * the primary key carried as a row field).
 *
 * Durability model: each op is its own single-row atomic commit (apply-on-ack).
 * There is no step-transaction — this engine is replay-from-top (every activation
 * re-runs the handler; `run` short-circuits from its terminal fact, but plain body
 * ops, including `state.set`, *re-execute*). So a torn write self-heals: the next
 * replay re-runs the set and re-commits it. This holds only for **deterministic**
 * writes (value derived from the input + memoized `run` results). A read-modify-
 * write against the *same* key (`set(get(k) + 1)`) is NOT replay-safe yet — replay
 * reads the already-mutated durable value and double-applies; that's the open
 * state-replay-determinism question for the recovery slice (journaled reads vs a
 * determinism constraint). State mutations inside a `run` action are rejected at
 * the type level — perform them in the handler body.
 */
export interface StateBinding<Row> {
  readonly get: (key: string) => Effect.Effect<Option.Option<Row>, DurableExecutionError, DurableExecutionRuntime>
  readonly set: (row: Row) => Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime>
  readonly delete: (key: string) => Effect.Effect<void, DurableExecutionError, DurableExecutionRuntime>
}
