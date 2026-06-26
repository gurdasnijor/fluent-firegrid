import * as Cause from "effect/Cause"
import * as Data from "effect/Data"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"

import { duration, type DurationLike } from "./clients.ts"

export { all, race, raceAll } from "effect/Effect"

export type TimeoutDuration = DurationLike | Duration.Input

export class FluentTimeoutError extends Data.TaggedError("FluentTimeoutError")<{
  readonly duration: TimeoutDuration
  readonly message: string
  readonly cause?: unknown
}> {}

const isDurationLikeObject = (input: TimeoutDuration): input is Exclude<DurationLike, number> =>
  typeof input === "object"
  && input !== null
  && !Array.isArray(input)
  && ("days" in input || "hours" in input || "milliseconds" in input || "minutes" in input || "seconds" in input)

const normalizeTimeoutDuration = (input: TimeoutDuration): Duration.Input =>
  isDurationLikeObject(input) ? duration(input) : input as Duration.Input

const describeTimeoutDuration = (input: TimeoutDuration): string =>
  Duration.format(Duration.fromInputUnsafe(normalizeTimeoutDuration(input)))

export const orTimeout =
  (input: TimeoutDuration) => <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E | FluentTimeoutError, R> =>
    self.pipe(
      Effect.timeout(normalizeTimeoutDuration(input)),
      Effect.mapError((cause) =>
        Cause.isTimeoutError(cause)
          ? new FluentTimeoutError({
            cause,
            duration: input,
            message: `operation timed out after ${describeTimeoutDuration(input)}`
          })
          : cause
      )
    )
