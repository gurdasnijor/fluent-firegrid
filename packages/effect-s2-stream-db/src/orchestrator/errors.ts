import * as Data from "effect/Data"

/**
 * The error channel of the Layer 1 orchestrator. One tagged error so callers
 * `catchTag` on a single type; `reason` narrows the failure class.
 *
 * - `open` — the owned stream could not be ensured at construction.
 * - `write` — an append failed (CAS/fence rejection, transport error, …).
 * - `read` — the tailing reader or a strong read failed.
 * - `readTimeout` — a strong read or write reply was not resolved within
 *   `readDeadline` (a stalled tail reader must not pin a client request).
 */
export class FlowError extends Data.TaggedError("FlowError")<{
  readonly reason: "open" | "write" | "read" | "readTimeout"
  readonly stream: string
  readonly cause?: unknown
}> {
  get message(): string {
    return `orchestrator ${this.reason} failed on stream ${this.stream}`
  }
}
