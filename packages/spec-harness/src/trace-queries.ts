import { ChdbClient } from "@firegrid/observability"
import { Data, Effect } from "effect"
import { type SqlError } from "effect/unstable/sql/SqlError"
import { type SpanCount } from "./report-state.ts"
import { scenarioTraceWhereSql, truthy } from "./sql-proofs.ts"

export class TraceQueryError extends Data.TaggedError("TraceQueryError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

type TraceQueryFailure = SqlError | TraceQueryError

export interface TraceCoverage {
  readonly spans: number
  readonly traces: number
  readonly evidenceSpans: number
  readonly totalDurationMs: number
  readonly maxDurationMs: number
}

export type TraceProofResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly reason: string }

const parseJsonEachRow = <A>(content: string): ReadonlyArray<A> =>
  content.trim() === ""
    ? []
    : content.trim().split("\n").map((line) => JSON.parse(line) as A)

const boundJsonEachRow = <A>(
  sql: string,
  scenarioId: string,
): Effect.Effect<ReadonlyArray<A>, TraceQueryFailure, ChdbClient> =>
  Effect.gen(function*() {
    const chdb = yield* ChdbClient
    const content = yield* chdb.native.queryBind(sql, { scenario_id: scenarioId }, "JSONEachRow")
    return yield* Effect.try({
      try: () => parseJsonEachRow<A>(content),
      catch: (cause) => new TraceQueryError({ message: "Unable to parse chDB JSONEachRow result", cause }),
    })
  })

export const spanCounts = (scenarioId: string): Effect.Effect<ReadonlyArray<SpanCount>, TraceQueryFailure, ChdbClient> =>
  boundJsonEachRow<SpanCount>(`
SELECT SpanName AS name, count() AS count
FROM otel_traces
WHERE ${scenarioTraceWhereSql}
GROUP BY SpanName
ORDER BY count DESC, SpanName
`, scenarioId)

export const traceCoverage = (scenarioId: string): Effect.Effect<TraceCoverage, TraceQueryFailure, ChdbClient> =>
  Effect.gen(function*() {
    const rows = yield* boundJsonEachRow<TraceCoverage>(`
SELECT
  count() AS spans,
  uniqExact(TraceId) AS traces,
  countIf(SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}) AS evidenceSpans,
  toFloat64(sum(Duration)) / 1000000 AS totalDurationMs,
  toFloat64(max(Duration)) / 1000000 AS maxDurationMs
FROM otel_traces
WHERE ${scenarioTraceWhereSql}
`, scenarioId)
    return rows[0] ?? {
      spans: 0,
      traces: 0,
      evidenceSpans: 0,
      totalDurationMs: 0,
      maxDurationMs: 0,
    }
  })

export const runTraceProofSql = (
  sql: string,
  scenarioId: string,
): Effect.Effect<TraceProofResult, TraceQueryFailure, ChdbClient> =>
  Effect.gen(function*() {
    const rows = yield* boundJsonEachRow<Record<string, unknown>>(sql, scenarioId)
    const row = rows[0]
    if (row === undefined) {
      return { ok: false, reason: "query returned no rows" } as const
    }
    if ("ok" in row) {
      return truthy(row.ok) ? { ok: true } as const : { ok: false, reason: "`ok` was false" } as const
    }
    const values = Object.values(row)
    if (values.length === 0) {
      return { ok: false, reason: "query returned an empty row" } as const
    }
    return truthy(values[0]) ? { ok: true } as const : { ok: false, reason: "first column was false" } as const
  })

export const spansToSummary = (spans: ReadonlyArray<SpanCount>): string =>
  spans.length === 0
    ? "(no spans exported)"
    : spans.map((span) => `${span.count} ${span.name}`).join("\n")
