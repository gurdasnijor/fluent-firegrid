import type { AnyTable } from "effect-s2-stream-db"
import * as Cause from "effect/Cause"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type { RetryPolicy } from "../authoring/types.ts"
import { durableError, DurableExecutionError } from "../errors.ts"
import type { ActorExit } from "../object/machine/index.ts"

export const toError = durableError

export const asServiceFreeEncoder = (schema: Schema.Top): Schema.Encoder<unknown, never> => schema as never

export const asServiceFreeDecoder = (schema: Schema.Top): Schema.Decoder<unknown, never> => schema as never

export const fail = (operation: string, message: string): Effect.Effect<never, DurableExecutionError> =>
  Effect.fail(new DurableExecutionError({ operation, message, cause: undefined }))

export const sharedForbidden = (op: string): Effect.Effect<never, DurableExecutionError> =>
  fail(op, `${op} is not allowed in a shared (read-only) object handler`)

export const decode = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  encoded: unknown
): Effect.Effect<A, DurableExecutionError> =>
  Schema.decodeUnknownEffect(schema)(encoded).pipe(Effect.mapError(toError("decode")))

export const encode = <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  value: A
): Effect.Effect<I, DurableExecutionError> =>
  Schema.encodeUnknownEffect(schema)(value).pipe(Effect.mapError(toError("encode")))

export const scheduleOf = (policy: RetryPolicy): Schedule.Schedule<Duration.Duration> =>
  Schedule.exponential(policy.initialInterval ?? Duration.millis(100), policy.intervalFactor ?? 2)

export const encodeRowFor = (table: AnyTable, row: unknown): Effect.Effect<unknown, DurableExecutionError> =>
  Effect.try({
    try: () => Schema.encodeUnknownSync(asServiceFreeEncoder(table.schema))(row),
    catch: durableError("state.set")
  })

export const decodeRowFor = (table: AnyTable, encoded: unknown): Effect.Effect<unknown, DurableExecutionError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(asServiceFreeDecoder(table.schema))(encoded),
    catch: durableError("state.get")
  })

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
