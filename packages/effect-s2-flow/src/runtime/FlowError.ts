import * as Data from "effect/Data"

export type FlowErrorReason =
  | "check-tail"
  | "read-session"
  | "read-timeout"
  | "write"
  | "write-timeout"

export class FlowError extends Data.TaggedError("FlowError")<{
  readonly reason: FlowErrorReason
  readonly message: string
  readonly cause?: unknown
}> {}

export const flowError = (reason: FlowErrorReason, message: string, cause?: unknown): FlowError =>
  new FlowError({ reason, message, cause })
