import { Brand } from "effect"

export type CelExpression = string & Brand.Brand<"CelExpression">
export type CelPath = string & Brand.Brand<"CelPath">

const CelExpression = Brand.nominal<CelExpression>()
const CelPath = Brand.nominal<CelPath>()

const quote = (value: string | number | boolean): string =>
  typeof value === "string" ? JSON.stringify(value) : String(value)

export const raw = (expression: string): CelExpression =>
  CelExpression(expression)

export const expression = (filter: CelExpression): string =>
  Brand.unbranded(filter)

export const path = (...segments: ReadonlyArray<string>): CelPath =>
  CelPath(segments.map((segment) => segment.replaceAll("`", "\\`")).join("."))

export const eq = (
  left: CelExpression | CelPath,
  right: string | number | boolean,
): CelExpression => raw(`${Brand.unbranded(left)} == ${quote(right)}`)

export const and = (...filters: ReadonlyArray<CelExpression>): CelExpression =>
  raw(filters.map(expression).join(" && "))

export const or = (...filters: ReadonlyArray<CelExpression>): CelExpression =>
  raw(filters.map(expression).join(" || "))

export const CEL = {
  raw,
  expression,
  path,
  eq,
  and,
  or,
}
