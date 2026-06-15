import {
  FencingTokenMismatchError,
  RangeNotSatisfiableError,
  SdkS2Error,
  SeqNumMismatchError,
} from "./internal/sdk.ts"
import { Match, Schema } from "effect"

const statusFromUnknown = (cause: unknown): number | undefined => {
  if (cause instanceof SdkS2Error) {
    return cause.status
  }
  return undefined
}

const expectedSeqNumFromUnknown = (cause: unknown): number | undefined => {
  if (cause instanceof SeqNumMismatchError) {
    return cause.expectedSeqNum
  }
  return undefined
}

const expectedFencingTokenFromUnknown = (cause: unknown): string | undefined => {
  if (cause instanceof FencingTokenMismatchError) {
    return cause.expectedFencingToken
  }
  return undefined
}

const rangeTailSeqNumFromUnknown = (cause: unknown): number | undefined => {
  if (cause instanceof RangeNotSatisfiableError) {
    return cause.tail?.seq_num
  }
  return undefined
}

const messageFromUnknown = (cause: unknown): string =>
  cause instanceof Error ? cause.message : "Unknown S2 error"

export class S2Error extends Schema.TaggedErrorClass<S2Error>()("S2Error", {
  operation: Schema.String,
  message: Schema.String,
  status: Schema.optional(Schema.Number),
  cause: Schema.Defect(),
}) {}

export class S2NotFound extends S2Error.extend<S2NotFound>("S2NotFound")({}) {}

export class S2Conflict extends S2Error.extend<S2Conflict>("S2Conflict")({
  expectedSeqNum: Schema.optional(Schema.Number),
  observedSeqNum: Schema.optional(Schema.Number),
  expectedFencingToken: Schema.optional(Schema.String),
}) {}

export class S2Throttled extends S2Error.extend<S2Throttled>("S2Throttled")({}) {}

export class S2RangeNotSatisfiable extends S2Error.extend<S2RangeNotSatisfiable>(
  "S2RangeNotSatisfiable",
)({
  tailSeqNum: Schema.optional(Schema.Number),
}) {}

export type S2ClientError = S2Error | S2NotFound | S2Conflict | S2Throttled | S2RangeNotSatisfiable

export const conflict = (input: {
  readonly operation: string
  readonly message: string
  readonly status?: number
  readonly expectedSeqNum?: number
  readonly observedSeqNum?: number
  readonly expectedFencingToken?: string
  readonly cause: unknown
}): S2Conflict => S2Conflict.make(input)

export const fromUnknown =
  (operation: string) =>
  (cause: unknown): S2ClientError => {
    const status = statusFromUnknown(cause)
    const input = {
      operation,
      message: messageFromUnknown(cause),
      status,
      cause,
    }

    return Match.value(status).pipe(
      Match.when(404, () => S2NotFound.make(input)),
      Match.when(409, () =>
        S2Conflict.make({
          ...input,
          expectedSeqNum: expectedSeqNumFromUnknown(cause),
          expectedFencingToken: expectedFencingTokenFromUnknown(cause),
        }),
      ),
      Match.when(412, () =>
        S2Conflict.make({
          ...input,
          expectedSeqNum: expectedSeqNumFromUnknown(cause),
          expectedFencingToken: expectedFencingTokenFromUnknown(cause),
        }),
      ),
      Match.when(416, () =>
        S2RangeNotSatisfiable.make({
          ...input,
          tailSeqNum: rangeTailSeqNumFromUnknown(cause),
        }),
      ),
      Match.when(429, () => S2Throttled.make(input)),
      Match.orElse(() => S2Error.make(input)),
    )
  }
