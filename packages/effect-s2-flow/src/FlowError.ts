import * as Data from "effect/Data"

export class FlowError extends Data.TaggedError("FlowError")<{
  readonly message: string
  readonly cause?: unknown
}> {}
