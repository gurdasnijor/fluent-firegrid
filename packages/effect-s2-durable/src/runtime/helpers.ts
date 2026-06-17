import { Cause, Duration, Effect, Exit, Option, Schedule, Schema } from "effect"
import type { AnyTable } from "effect-s2-stream-db"
import type { ActorExit } from "../actor/core.ts"
import { DurableExecutionError, durableError } from "../errors.ts"
import type { RetryPolicy } from "../types.ts"

export const toError = durableError

export const fail = (operation: string, message: string): Effect.Effect<never, DurableExecutionError> =>
  Effect.fail(new DurableExecutionError({ operation, message, cause: undefined }))

export const sharedForbidden = (op: string): Effect.Effect<never, DurableExecutionError> =>
  fail(op, `${op} is not allowed in a shared (read-only) object handler`)

export const decode = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  encoded: unknown,
): Effect.Effect<A, DurableExecutionError> =>
  Schema.decodeUnknownEffect(schema)(encoded).pipe(Effect.mapError(toError("decode")))

export const encode = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  value: A,
): Effect.Effect<I, DurableExecutionError> =>
  Schema.encodeUnknownEffect(schema)(value).pipe(Effect.mapError(toError("encode")))

export const scheduleOf = (policy: RetryPolicy): Schedule.Schedule<Duration.Duration> =>
  Schedule.exponential(policy.initialInterval ?? Duration.millis(100), policy.intervalFactor ?? 2)

export const encodeRowFor = (table: AnyTable, row: unknown): Effect.Effect<unknown, DurableExecutionError> =>
  (Schema.encodeUnknownEffect(table.schema)(row) as Effect.Effect<unknown, Schema.SchemaError>).pipe(
    Effect.mapError(durableError("state.set")),
  )

export const decodeRowFor = (table: AnyTable, encoded: unknown): Effect.Effect<unknown, DurableExecutionError> =>
  (Schema.decodeUnknownEffect(table.schema)(encoded) as Effect.Effect<unknown, Schema.SchemaError>).pipe(
    Effect.mapError(durableError("state.get")),
  )

export const pkOf = (table: AnyTable, row: unknown): string => String((row as Record<string, unknown>)[table.pkField])

export const resolvedValue = (row: Option.Option<{ readonly value?: unknown }>): Option.Option<unknown> =>
  Option.flatMap(row, (r) => Option.fromNullishOr(r.value))

export const toActorExit = (exit: Exit.Exit<unknown, unknown>): ActorExit => {
  if (Exit.isSuccess(exit)) {
    return { _tag: "Success", value: exit.value }
  }
  const cause = exit.cause
  if (Cause.hasInterruptsOnly(cause)) {
    return { _tag: "Interrupt" }
  }
  const failure = Cause.findErrorOption(cause)
  return Option.isSome(failure)
    ? { _tag: "Failure", error: String(failure.value) }
    : { _tag: "Defect", defect: Cause.pretty(cause) }
}
