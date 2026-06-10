/**
 * Protocol/domain failures raised by the store algebra.
 *
 * These are typed errors in the Effect error channel. Append *protocol
 * conflicts* that participate in PROTOCOL.md §5.2 precedence (closed-stream,
 * content-type mismatch, stream-seq regression, producer rules) are NOT modeled
 * here — they are `Protocol.AppendDecision` variants returned in the success
 * channel so the single decision path keeps its precedence. `ProtocolError`
 * covers store/domain-level failures: missing streams, malformed requests,
 * create conflicts, and retention gaps. HTTP adapters lower these to
 * `HttpApiError` empty errors.
 */
import { Schema } from "effect"

/** The requested stream does not exist. Lowered to HTTP 404. */
export class NotFound
  extends Schema.TaggedError<NotFound>()("NotFound", {
    path: Schema.String,
  })
{}

/**
 * The request was malformed (e.g. partial producer headers, non-integer
 * producer header, an epoch advance presented with a non-zero seq at the HTTP
 * boundary, or an empty JSON array on POST). Lowered to HTTP 400.
 */
export class BadRequest
  extends Schema.TaggedError<BadRequest>()("BadRequest", {
    reason: Schema.String,
  })
{}

/**
 * A create-only `PUT` was attempted with a config that conflicts with the
 * existing stream (e.g. different content type). Lowered to HTTP 409.
 */
export class CreateConflict
  extends Schema.TaggedError<CreateConflict>()("CreateConflict", {
    path: Schema.String,
    reason: Schema.String,
  })
{}

/**
 * The requested offset has been compacted / aged out of retention. Lowered to
 * HTTP 410. (Declared for completeness; retention is out of scope for the
 * memory-store slice and never raised here.)
 */
export class RetentionGone
  extends Schema.TaggedError<RetentionGone>()("RetentionGone", {
    path: Schema.String,
    offset: Schema.String,
  })
{}

export const Failure = Schema.Union(
  NotFound,
  BadRequest,
  CreateConflict,
  RetentionGone,
)
export type ProtocolError = typeof Failure.Type
