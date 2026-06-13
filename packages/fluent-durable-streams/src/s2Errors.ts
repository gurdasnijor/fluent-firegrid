import {
  FencingTokenMismatchError,
  RangeNotSatisfiableError as S2RangeNotSatisfiableError,
  S2Error,
  SeqNumMismatchError,
} from "@s2-dev/streamstore"
import { Effect } from "effect"
import {
  AppendConditionFailed,
  BadRequestError,
  ConflictError,
  ForbiddenError,
  NotFoundError,
  RangeNotSatisfiableError,
  TimeoutError,
  UpstreamError,
  type ApiError,
} from "./api.ts"
import type { S2ProfileError } from "./s2.ts"

const codeOf = (error: unknown): string | undefined =>
  (error as { readonly code?: string }).code

export const apiError = (error: S2ProfileError): ApiError => {
  const code = codeOf(error)
  const fields = (message: string) => ({
    message,
    ...(code === undefined ? {} : { code }),
  })

  if (error instanceof SeqNumMismatchError) {
    return new AppendConditionFailed({
      ...fields(error.message),
      reason: "seq_num_mismatch",
    })
  }
  if (error instanceof FencingTokenMismatchError) {
    return new AppendConditionFailed({
      ...fields(error.message),
      reason: "fencing_token_mismatch",
    })
  }
  if (error instanceof S2RangeNotSatisfiableError) {
    return new RangeNotSatisfiableError(fields(error.message))
  }
  if (error instanceof S2Error) {
    switch (error.status) {
      case 400:
        return new BadRequestError(fields(error.message))
      case 403:
        return new ForbiddenError(fields(error.message))
      case 404:
        return new NotFoundError(fields(error.message))
      case 408:
        return new TimeoutError(fields(error.message))
      case 409:
        return new ConflictError(fields(error.message))
      case 412:
        return new AppendConditionFailed(fields(error.message))
      case 416:
        return new RangeNotSatisfiableError(fields(error.message))
      default:
        return new UpstreamError(fields(error.message))
    }
  }
  return new UpstreamError({
    message: "Unknown S2 error",
  })
}

export const streamError = (error: unknown): ApiError => {
  if (
    error instanceof S2Error ||
    error instanceof SeqNumMismatchError ||
    error instanceof FencingTokenMismatchError ||
    error instanceof S2RangeNotSatisfiableError
  ) {
    return apiError(error)
  }
  return new UpstreamError({
    message: error instanceof Error ? error.message : "S2 read session failed",
  })
}

export const catchS2 = <A, R>(effect: Effect.Effect<A, S2ProfileError, R>): Effect.Effect<A, ApiError, R> =>
  Effect.mapError(effect, apiError)
