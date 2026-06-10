import { Data, Predicate } from "effect"
import type { Offset, StreamPath } from "./domainTypes.ts"

export class StreamLogError extends Data.TaggedError("StreamLogError")<
  Readonly<{
    readonly operation: "create" | "append" | "read" | "subscribe" | "head" | "delete"
    readonly path?: string
    readonly details: string
    readonly cause?: unknown
  }>
> {}

export class StreamClosedError extends Data.TaggedError("StreamClosedError")<
  Readonly<{
    readonly path: StreamPath
    readonly finalOffset: Offset
  }>
> {}

export class ContentTypeMismatchError extends Data.TaggedError("ContentTypeMismatchError")<
  Readonly<{
    readonly path: StreamPath
    readonly expected: string
    readonly actual: string
  }>
> {}

export class OffsetConflictError extends Data.TaggedError("OffsetConflictError")<
  Readonly<{
    readonly path: StreamPath
    readonly expectedTailOffset: Offset
    readonly actualTailOffset: Offset
  }>
> {}

export class StreamNotFoundError extends Data.TaggedError("StreamNotFoundError")<
  Readonly<{
    readonly path: StreamPath
  }>
> {}

export class InvalidOffsetError extends Data.TaggedError("InvalidOffsetError")<
  Readonly<{
    readonly path: StreamPath
    readonly offset: string
  }>
> {}

export type DurableStreamLogError =
  | StreamLogError
  | StreamClosedError
  | ContentTypeMismatchError
  | OffsetConflictError
  | StreamNotFoundError
  | InvalidOffsetError

export const isDurableStreamLogError = (error: unknown): error is DurableStreamLogError =>
  Predicate.isTagged(error, "StreamLogError") ||
  Predicate.isTagged(error, "StreamClosedError") ||
  Predicate.isTagged(error, "ContentTypeMismatchError") ||
  Predicate.isTagged(error, "OffsetConflictError") ||
  Predicate.isTagged(error, "StreamNotFoundError") ||
  Predicate.isTagged(error, "InvalidOffsetError")
