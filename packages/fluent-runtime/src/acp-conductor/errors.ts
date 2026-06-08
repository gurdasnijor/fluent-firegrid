import { Data } from "effect"

/**
 * Failure raised by the editor-facing ACP conductor boundary. Carries the ACP
 * operation that failed so it can be lowered to an ACP `RequestError` on the
 * wire and recorded as diagnostic context off the protocol stdout.
 */
export class FiregridAcpError extends Data.TaggedError("FiregridAcpError")<{
  readonly op: string
  readonly message: string
  readonly cause?: unknown
}> {}
