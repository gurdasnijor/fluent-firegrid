import { Schema } from "effect"

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
  cause: Schema.Defect(),
}) {}
