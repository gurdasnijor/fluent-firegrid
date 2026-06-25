import { Environment } from "@marcbachmann/cel-js"
import * as Effect from "effect/Effect"

import { FluentFiregridError } from "./error.ts"

export interface CelStatePredicate {
  readonly language: "cel"
  readonly expression: string
}

export type StatePredicate = CelStatePredicate

export interface StatePredicateContext {
  readonly row: unknown
  readonly old?: unknown
  readonly change?: {
    readonly key: string
    readonly table: string
    readonly operation: "insert" | "update" | "delete" | "snapshot"
  }
}

export const cel = (expression: string): StatePredicate => ({
  expression,
  language: "cel"
})

const createCelEnvironment = (): Environment =>
  new Environment({
    enableOptionalTypes: true,
    unlistedVariablesAreDyn: true
  })

const causeMessage = (cause: unknown): string => cause instanceof Error ? cause.message : String(cause)

export const validateStatePredicate = (
  predicate: StatePredicate
): Effect.Effect<void, FluentFiregridError> =>
  Effect.try({
    try: () => {
      const result = createCelEnvironment().check(predicate.expression)
      if (!result.valid) {
        throw result.error ?? new Error("invalid CEL expression")
      }
      if (result.type !== "bool") {
        throw new Error(`state wait CEL expression must evaluate to bool, got ${result.type ?? "unknown"}`)
      }
    },
    catch: (cause) =>
      new FluentFiregridError({
        cause,
        message: `invalid state wait predicate: ${causeMessage(cause)}`
      })
  })

export const evaluateStatePredicate = (
  predicate: StatePredicate,
  context: StatePredicateContext
): Effect.Effect<boolean, FluentFiregridError> =>
  validateStatePredicate(predicate).pipe(
    Effect.flatMap(() =>
      Effect.try({
        try: () => {
          const result = createCelEnvironment().evaluate(predicate.expression, context)
          if (typeof result !== "boolean") {
            throw new Error(`state wait CEL expression must evaluate to bool, got ${typeof result}`)
          }
          return result
        },
        catch: (cause) =>
          new FluentFiregridError({
            cause,
            message: `failed to evaluate state wait predicate: ${causeMessage(cause)}`
          })
      })
    )
  )
