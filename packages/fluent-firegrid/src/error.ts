import * as Data from "effect/Data"

export class FluentFiregridError extends Data.TaggedError("FluentFiregridError")<{
  readonly message: string
  readonly cause?: unknown
}> {}
