import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import { ChdbClient } from "@firegrid/trace"
import * as Effect from "effect/Effect"
import { FileSystem } from "effect/FileSystem"
import * as Layer from "effect/Layer"
import { Path } from "effect/Path"

import { bindTrialSql, expandTraceMacros } from "./TraceViews.ts"
import { VerificationError } from "./VerificationError.ts"

interface SpanCount {
  readonly name: string
  readonly count: number
}

interface TraceCoverage {
  readonly spans: number
  readonly traces: number
  readonly evidenceSpans: number
  readonly totalDurationMs: number
  readonly maxDurationMs: number
}

export interface TrialReport {
  readonly trialId: string
  readonly checks: number
  readonly status: "passed" | "failed"
  readonly failedCheck?: string
  readonly failure?: string
  readonly spans: ReadonlyArray<SpanCount>
  readonly coverage: TraceCoverage
}

export interface WrittenTrialReport extends TrialReport {
  readonly path?: string
}

const queryTrial = <A extends object>(
  sql: string,
  trialId: string
): Effect.Effect<ReadonlyArray<A>, VerificationError, ChdbClient> =>
  Effect.gen(function*() {
    const chdb = yield* ChdbClient
    return yield* chdb.unsafe<A>(bindTrialSql(expandTraceMacros(sql), trialId)).pipe(
      Effect.mapError((cause) => new VerificationError({ message: "failed to query trial report data", cause }))
    )
  })

const spanCounts = Effect.fn("Report.spanCounts")(function*(trialId: string) {
  return yield* queryTrial<SpanCount>(
    `
SELECT SpanName AS name, count() AS count
FROM trial_spans
GROUP BY SpanName
ORDER BY count DESC, SpanName
`,
    trialId
  )
})

const traceCoverage = Effect.fn("Report.traceCoverage")(function*(trialId: string) {
  const rows = yield* queryTrial<TraceCoverage>(
    `
SELECT
  count() AS spans,
  uniqExact(TraceId) AS traces,
  countIf(
    SpanAttributes['firegrid.trial.id'] = {trial_id:String}
    OR ResourceAttributes['firegrid.trial.id'] = {trial_id:String}
  ) AS evidenceSpans,
  toFloat64(sum(Duration)) / 1000000 AS totalDurationMs,
  toFloat64(max(Duration)) / 1000000 AS maxDurationMs
FROM trial_spans
`,
    trialId
  )
  return rows[0] ?? {
    evidenceSpans: 0,
    maxDurationMs: 0,
    spans: 0,
    totalDurationMs: 0,
    traces: 0
  }
})

export const spanSummary = Effect.fn("Report.spanSummary")(function*(trialId: string) {
  const counts = yield* spanCounts(trialId)
  return counts.length === 0
    ? "(no spans exported)"
    : counts.map((span) => `${span.count} ${span.name}`).join("\n")
})

const reportFileName = (trialId: string): string => `${trialId.replace(/[^A-Za-z0-9_.-]/g, "-")}.json`

const writeReportFile = Effect.fn("Report.writeReportFile")(function*(
  report: TrialReport,
  reportDir: string | undefined
) {
  if (reportDir === undefined) return undefined
  const fs = yield* FileSystem
  const path = yield* Path
  yield* fs.makeDirectory(reportDir, { recursive: true }).pipe(
    Effect.mapError((cause) =>
      new VerificationError({ message: `failed to create report directory ${reportDir}`, cause })
    )
  )
  const file = path.join(reportDir, reportFileName(report.trialId))
  yield* fs.writeFileString(file, `${JSON.stringify(report, undefined, 2)}\n`).pipe(
    Effect.mapError((cause) => new VerificationError({ message: `failed to write trial report ${file}`, cause }))
  )
  return file
})

const buildTrialReport = Effect.fn("Report.buildTrialReport")(function*(input: {
  readonly trialId: string
  readonly checks: number
  readonly status: "passed" | "failed"
  readonly failedCheck?: string
  readonly failure?: string
}) {
  const spans = yield* spanCounts(input.trialId)
  const coverage = yield* traceCoverage(input.trialId)
  return {
    trialId: input.trialId,
    checks: input.checks,
    status: input.status,
    ...(input.failedCheck === undefined ? {} : { failedCheck: input.failedCheck }),
    ...(input.failure === undefined ? {} : { failure: input.failure }),
    spans,
    coverage
  } satisfies TrialReport
})

export const writeTrialReport = Effect.fn("Report.writeTrialReport")(function*(input: {
  readonly trialId: string
  readonly checks: number
  readonly status: "passed" | "failed"
  readonly reportDir?: string
  readonly failedCheck?: string
  readonly failure?: string
}) {
  const report = yield* buildTrialReport(input)
  const path = yield* writeReportFile(report, input.reportDir).pipe(
    Effect.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer))
  )
  return {
    ...report,
    ...(path === undefined ? {} : { path })
  } satisfies WrittenTrialReport
})
