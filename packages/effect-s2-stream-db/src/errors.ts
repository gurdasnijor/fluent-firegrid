import * as Schema from "effect/Schema"

/**
 * The error channel of every `S2StreamDB` operation. Wraps codec failures,
 * conflicts surfaced by `effect-s2` (CAS / fence / throttle), and read/append
 * failures behind one tagged error so callers `catchTag` on a single type.
 */
export class S2StreamDbError extends Schema.TaggedErrorClass<S2StreamDbError>()("S2StreamDbError", {
  /** The operation that failed (`insert` / `get` / `transact` / `compact` / …). */
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.Defect()
}) {}
