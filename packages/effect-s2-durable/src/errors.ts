import * as Schema from "effect/Schema"

/**
 * The engine's error channel. Wraps the underlying `S2StreamDbError` (state-plane
 * failures), schema decode/encode failures at durable boundaries, and engine-level
 * coordination failures (run-outside-handler, unknown execution) behind one tagged
 * error so callers `catchTag` on a single type.
 */
export class DurableExecutionError extends Schema.TaggedErrorClass<DurableExecutionError>()("DurableExecutionError", {
  /** The operation that failed (`submit` / `run` / `attach` / `complete` / …). */
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.Defect()
}) {}

/** Wrap an unknown cause in a `DurableExecutionError` tagged with the failing operation. */
export const durableError = (operation: string) => (cause: unknown): DurableExecutionError =>
  new DurableExecutionError({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
    cause
  })
