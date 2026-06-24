import { ChdbClient } from "@firegrid/observability"
import * as Effect from "effect/Effect"

import { bindTrialSql, expandTraceMacros } from "./TraceViews.ts"
import { VerificationError } from "./VerificationError.ts"

export interface TraceProof {
  readonly name: string
  readonly sql: string
}

export interface TraceOperationMatch {
  readonly operation: string
  readonly status?: string
  readonly attributes?: Record<string, string | number | boolean>
  readonly outputContains?: ReadonlyArray<string>
  readonly count?: number
}

export interface TraceProofResult {
  readonly ok: boolean
  readonly reason?: string
}

export const truthy = (value: unknown): boolean => {
  if (typeof value === "boolean") return value
  if (typeof value === "number") return value !== 0
  if (typeof value === "bigint") return value !== BigInt(0)
  if (typeof value === "string") return value !== "" && value !== "0" && value.toLowerCase() !== "false"
  return value != null
}

export const normalizeProofSql = (sql: string): string => {
  const trimmed = sql.trim().replace(/;+\s*$/u, "")
  if (!/^(select|with)\b/iu.test(trimmed)) {
    throw new VerificationError({ message: "trace proof SQL must be a SELECT or WITH query" })
  }
  if (trimmed.includes(";")) {
    throw new VerificationError({ message: "trace proof SQL must contain a single read-only query" })
  }
  return expandTraceMacros(trimmed)
}

export const traceSql = (name: string, sql: string): TraceProof => ({
  name,
  sql: normalizeProofSql(sql)
})

const sqlString = (value: string): string => `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`

const spanAttributeEquals = (name: string, value: string | number | boolean): string =>
  `SpanAttributes[${sqlString(name)}] = ${sqlString(String(value))}`

export const traceOperation = (name: string, match: TraceOperationMatch): TraceProof => {
  const conditions = [
    "SpanName = 'verification.operation'",
    spanAttributeEquals("firegrid.operation.name", match.operation),
    ...(match.status === undefined ? [] : [spanAttributeEquals("firegrid.operation.status", match.status)]),
    ...Object.entries(match.attributes ?? {}).map(([attribute, value]) => spanAttributeEquals(attribute, value)),
    ...(match.outputContains ?? []).map((fragment) =>
      `position(SpanAttributes['firegrid.operation.output.json'], ${sqlString(fragment)}) > 0`
    )
  ]
  return traceSql(
    name,
    `
      SELECT countIf(
        ${conditions.join("\n        AND ")}
      ) = ${match.count ?? 1} AS ok
      FROM trial_spans
    `
  )
}

export const runTraceProof = Effect.fn("runTraceProof")(function*(
  proof: TraceProof,
  trialId: string
) {
  const chdb = yield* ChdbClient
  const rows = yield* chdb.unsafe<Record<string, unknown>>(bindTrialSql(proof.sql, trialId)).pipe(
    Effect.mapError((cause) =>
      new VerificationError({
        message: `trace proof ${proof.name} query failed`,
        cause
      })
    )
  )
  const row = rows[0]
  if (row === undefined) {
    return yield* new VerificationError({ message: `trace proof ${proof.name} failed: query returned no rows` })
  }
  if ("ok" in row) {
    if (truthy(row.ok)) return
    return yield* new VerificationError({ message: `trace proof ${proof.name} failed: ok was false` })
  }
  const values = Object.values(row)
  if (values.length === 0) {
    return yield* new VerificationError({ message: `trace proof ${proof.name} failed: query returned an empty row` })
  }
  if (truthy(values[0])) return
  return yield* new VerificationError({ message: `trace proof ${proof.name} failed: first column was false` })
})
