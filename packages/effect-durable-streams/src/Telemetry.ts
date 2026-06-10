/**
 * Central Durable Streams telemetry helpers.
 *
 * effect-server.TELEMETRY.1 effect-server.TELEMETRY.2
 */
import { Effect, Predicate, String } from "effect"

export type OperationName =
  | "stream.create"
  | "stream.append"
  | "stream.head"
  | "stream.read"
  | "stream.delete"
  | (string & {})

export interface TelemetryOptions {
  readonly operation?: {
    readonly name?: OperationName | undefined
  } | undefined
  readonly stream?: {
    readonly path?: string | undefined
    readonly offset?: string | undefined
    readonly closed?: boolean | undefined
  } | undefined
  readonly producer?: {
    readonly present?: boolean | undefined
  } | undefined
  readonly decision?: {
    readonly name?: string | undefined
  } | undefined
}

export type AttributesWithPrefix<
  Attributes extends Record<string, unknown>,
  Prefix extends string,
> = {
  [Name in keyof Attributes as `${Prefix}.${FormatAttributeName<Name>}`]:
    Attributes[Name]
}

export type FormatAttributeName<T extends string | number | symbol> =
  T extends string ? T extends `${infer First}${infer Rest}`
      ? `${First extends Uppercase<First> ? "_" : ""}${Lowercase<First>}${FormatAttributeName<Rest>}`
      : T
    : never

const addGroup = (
  out: Record<string, unknown>,
  prefix: string,
  fields: Record<string, unknown> | undefined,
) => {
  if (fields === undefined) return
  Object.entries(fields).forEach(([key, value]) => {
    if (Predicate.isNotNullable(value)) {
      out[`${prefix}.${String.camelToSnake(key)}`] = value
    }
  })
}

export const attributes = (
  options: TelemetryOptions,
): Record<string, unknown> => {
  const out: Record<string, unknown> = {
    "durable_streams.system": "durable_streams",
  }
  addGroup(out, "durable_streams.operation", options.operation)
  addGroup(out, "durable_streams.stream", options.stream)
  addGroup(out, "durable_streams.producer", options.producer)
  addGroup(out, "durable_streams.decision", options.decision)
  return out
}

export const annotateCurrentSpan = (
  options: TelemetryOptions,
): Effect.Effect<void> => Effect.annotateCurrentSpan(attributes(options))

export const withSpan = <A, E, R>(
  name: OperationName,
  options: Omit<TelemetryOptions, "operation">,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  effect.pipe(
    Effect.withSpan(name, {
      attributes: attributes({ ...options, operation: { name } }),
    }),
  )
