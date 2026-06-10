import { Data } from "effect"

export class DurableExecutionError extends Data.TaggedError(
  "DurableExecutionError",
)<{
  readonly message: string
  readonly cause?: unknown
}> {}
