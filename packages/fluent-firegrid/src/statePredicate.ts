import { Environment } from "@marcbachmann/cel-js"
import * as Effect from "effect/Effect"

import { FluentFiregridError } from "./error.ts"

export interface CelStatePredicate {
  readonly language: "cel"
  readonly expression: string
}

export type StatePredicate = CelStatePredicate

export interface CelExpressionNode {
  readonly expression: string
  readonly and: (other: CelExpressionInput) => CelExpressionNode
  readonly not: () => CelExpressionNode
  readonly or: (other: CelExpressionInput) => CelExpressionNode
}

export interface CelFieldExpression extends CelExpressionNode {
  readonly eq: (value: CelLiteral) => CelExpressionNode
  readonly greaterThan: (value: CelLiteral) => CelExpressionNode
  readonly greaterThanOrEqual: (value: CelLiteral) => CelExpressionNode
  readonly in: (values: ReadonlyArray<CelLiteral>) => CelExpressionNode
  readonly lessThan: (value: CelLiteral) => CelExpressionNode
  readonly lessThanOrEqual: (value: CelLiteral) => CelExpressionNode
  readonly notEq: (value: CelLiteral) => CelExpressionNode
}

export interface CelExpressionBuilder {
  readonly change: {
    readonly key: CelFieldExpression
    readonly operation: CelFieldExpression
    readonly table: CelFieldExpression
  }
  readonly old: Readonly<Record<string, CelFieldExpression>>
  readonly row: Readonly<Record<string, CelFieldExpression>>
}

export type CelExpressionInput = CelExpressionNode | CelStatePredicate | string

export type CelLiteral = boolean | null | number | string

export interface CelFactory {
  readonly expr: (build: (builder: CelExpressionBuilder) => CelExpressionInput) => StatePredicate
  (expression: string): StatePredicate
}

export interface StatePredicateContext {
  readonly row: unknown
  readonly old?: unknown
  readonly vars?: Readonly<Record<string, unknown>>
  readonly change?: {
    readonly key: string
    readonly table: string
    readonly operation: "insert" | "update" | "delete" | "snapshot"
  }
}

const celPredicate = (expression: string): StatePredicate => ({
  expression,
  language: "cel"
})

export const cel: CelFactory = Object.assign(celPredicate, {
  expr: (build: (builder: CelExpressionBuilder) => CelExpressionInput): StatePredicate =>
    celPredicate(expressionText(build(createCelExpressionBuilder())))
})

const createCelExpressionBuilder = (): CelExpressionBuilder => ({
  change: {
    key: celField("change.key"),
    operation: celField("change.operation"),
    table: celField("change.table")
  },
  old: celFieldScope("old"),
  row: celFieldScope("row")
})

const celFieldScope = (scope: "old" | "row"): Readonly<Record<string, CelFieldExpression>> =>
  new Proxy({}, {
    get: (_target, prop) => celField(`${scope}.${String(prop)}`)
  }) as Readonly<Record<string, CelFieldExpression>>

const celNode = (expression: string): CelExpressionNode => ({
  and: (other) => celNode(`(${expression}) && (${expressionText(other)})`),
  expression,
  not: () => celNode(`!(${expression})`),
  or: (other) => celNode(`(${expression}) || (${expressionText(other)})`)
})

const celField = (path: string): CelFieldExpression => ({
  ...celNode(path),
  eq: (value) => celNode(`${path} == ${literalText(value)}`),
  greaterThan: (value) => celNode(`${path} > ${literalText(value)}`),
  greaterThanOrEqual: (value) => celNode(`${path} >= ${literalText(value)}`),
  in: (values) => celNode(`${path} in [${values.map(literalText).join(", ")}]`),
  lessThan: (value) => celNode(`${path} < ${literalText(value)}`),
  lessThanOrEqual: (value) => celNode(`${path} <= ${literalText(value)}`),
  notEq: (value) => celNode(`${path} != ${literalText(value)}`)
})

const expressionText = (input: CelExpressionInput): string => typeof input === "string" ? input : input.expression

const literalText = (value: CelLiteral): string => typeof value === "string" ? JSON.stringify(value) : String(value)

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
          const result = createCelEnvironment().evaluate(predicate.expression, { ...context.vars, ...context })
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
