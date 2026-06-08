import { Data } from "effect"

// fluent-firegrid-keystone.SUBSTRATE.1
export class FluentFiregridError extends Data.TaggedError("FluentFiregridError")<{
  readonly message: string
  readonly cause?: unknown
}> {}
