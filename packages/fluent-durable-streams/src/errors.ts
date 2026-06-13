import {
  FencingTokenMismatchError,
  RangeNotSatisfiableError,
  S2Error,
  SeqNumMismatchError,
} from "@s2-dev/streamstore"
import { Data, Effect } from "effect"

export class UnknownS2ProfileError extends Data.TaggedError("UnknownS2ProfileError")<
  Readonly<{
    readonly cause: unknown
  }>
> {}

export type S2ProfileError =
  | S2Error
  | SeqNumMismatchError
  | FencingTokenMismatchError
  | RangeNotSatisfiableError
  | UnknownS2ProfileError

export const normalizeS2Error = (cause: unknown): S2ProfileError => {
  if (
    cause instanceof S2Error ||
    cause instanceof SeqNumMismatchError ||
    cause instanceof FencingTokenMismatchError ||
    cause instanceof RangeNotSatisfiableError
  ) {
    return cause
  }
  return new UnknownS2ProfileError({ cause })
}

export const tryS2 = <A>(evaluate: () => Promise<A>): Effect.Effect<A, S2ProfileError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: normalizeS2Error,
  })
