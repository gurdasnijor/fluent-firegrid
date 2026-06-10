import { Data } from "effect"

export class DurableStreamsClientError extends Data.TaggedError("DurableStreamsClientError")<
  Readonly<{
    readonly operation: string
    readonly message: string
    readonly cause?: unknown
  }>
> {}

export class DurableStreamsProtocolFailure extends Data.TaggedError("DurableStreamsProtocolFailure")<
  Readonly<{
    readonly reason: string
    readonly message: string
  }>
> {}
