import * as Data from "effect/Data"
import * as Schema from "effect/Schema"

export class VerificationError extends Schema.TaggedErrorClass<VerificationError>()(
  "VerificationError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Unknown)
  }
) {}

export class S2LiteError extends Data.TaggedError("S2LiteError")<{
  readonly message: string
  readonly cause?: unknown
}> {}
