import { After, AfterAll, Before, BeforeAll, World, type ITestCaseHookParameter, type IWorldOptions } from "@cucumber/cucumber"
import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import { ChdbClient, ChdbSession, ChdbSpanExporter, layer as ChdbLayer } from "@firegrid/observability"
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { Effect, Layer } from "effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import type { S2Client } from "effect-s2"
import { recordScenarioReport, type SpanCount } from "./report-state.ts"
import { S2LiteLive } from "./s2lite.ts"

interface ProofBlock {
  readonly sql: string
}

export type HarnessServices = S2Client | ChdbSession | ChdbClient

// eslint-disable-next-line local/no-module-durable-cache -- Cucumber run-scoped harness runtime; product durability still lives in S2.
let runtime: ManagedRuntime.ManagedRuntime<HarnessServices, unknown> | undefined
// eslint-disable-next-line local/no-module-durable-cache -- Cucumber run-scoped trace exporter handle for per-scenario forceFlush.
let processor: BatchSpanProcessor | undefined

export class FiregridWorld extends World {
  scenarioId = ""
  proofs: Array<ProofBlock> = []

  constructor(options: IWorldOptions) {
    super(options)
  }

  addTraceProof(sql: string): void {
    this.proofs.push({ sql })
  }

  scenarioKey(key: string): string {
    return `${this.scenarioId.replace(/[^A-Za-z0-9_.-]/g, "-")}-${key}`
  }
}

const scenarioIdFor = (scenario: ITestCaseHookParameter): string =>
  scenario.pickle.id

const escapeString = (value: string): string =>
  value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

const bindTraceSql = (sql: string, scenarioId: string): string =>
  sql.replaceAll("{scenario_id:String}", `'${escapeString(scenarioId)}'`)

const truthy = (value: unknown): boolean => {
  if (typeof value === "boolean") return value
  if (typeof value === "number") return value !== 0
  if (typeof value === "bigint") return value !== 0n
  if (typeof value === "string") return value !== "" && value !== "0" && value.toLowerCase() !== "false"
  return value != null
}

const scenarioTraceWhere = `
TraceId IN (
  SELECT TraceId
  FROM otel_traces
  WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
)
`

const spanCounts = (scenarioId: string): Effect.Effect<ReadonlyArray<SpanCount>, unknown, ChdbClient> =>
  Effect.gen(function*() {
    const sql = yield* ChdbClient
    const rows = yield* sql.unsafe<SpanCount>(bindTraceSql(`
SELECT SpanName AS name, count() AS count
FROM otel_traces
WHERE ${scenarioTraceWhere}
GROUP BY SpanName
ORDER BY count DESC, SpanName
`, scenarioId))
    return rows
  })

const traceCoverage = (scenarioId: string): Effect.Effect<{
  readonly spans: number
  readonly traces: number
  readonly evidenceSpans: number
  readonly totalDurationMs: number
  readonly maxDurationMs: number
}, unknown, ChdbClient> =>
  Effect.gen(function*() {
    const sql = yield* ChdbClient
    const rows = yield* sql.unsafe<{
      readonly spans: number
      readonly traces: number
      readonly evidenceSpans: number
      readonly totalDurationMs: number
      readonly maxDurationMs: number
    }>(bindTraceSql(`
SELECT
  count() AS spans,
  uniqExact(TraceId) AS traces,
  countIf(SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}) AS evidenceSpans,
  toFloat64(sum(Duration)) / 1000000 AS totalDurationMs,
  toFloat64(max(Duration)) / 1000000 AS maxDurationMs
FROM otel_traces
WHERE ${scenarioTraceWhere}
`, scenarioId))
    return rows[0] ?? {
      spans: 0,
      traces: 0,
      evidenceSpans: 0,
      totalDurationMs: 0,
      maxDurationMs: 0,
    }
  })

const runTraceProofSql = (
  sql: string,
  scenarioId: string,
): Effect.Effect<{ readonly ok: true } | { readonly ok: false; readonly reason: string }, unknown, ChdbClient> =>
  Effect.gen(function*() {
    const chdb = yield* ChdbClient
    const rows = yield* chdb.unsafe<Record<string, unknown>>(bindTraceSql(sql, scenarioId))
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

const spanSummary = (scenarioId: string): Effect.Effect<string, unknown, ChdbClient> =>
  Effect.gen(function*() {
    const counts = yield* spanCounts(scenarioId)
    return counts.length === 0
      ? "(no spans exported)"
      : counts.map((span) => `${span.count} ${span.name}`).join("\n")
  })

Before(function(this: FiregridWorld, scenario) {
  this.scenarioId = scenarioIdFor(scenario)
  this.proofs = []
})

BeforeAll(async function() {
  let nextProcessor: BatchSpanProcessor | undefined
  const ChdbLive = ChdbLayer({})
  const OtelLive = Layer.unwrap(
    Effect.gen(function*() {
      const session = yield* ChdbSession
      const createdProcessor = new BatchSpanProcessor(new ChdbSpanExporter({ session, table: "otel_traces" }))
      nextProcessor = createdProcessor
      return NodeSdk.layer(() => ({
        resource: { serviceName: "firegrid-cucumber" },
        spanProcessor: [createdProcessor],
      }))
    }),
  )
  runtime = ManagedRuntime.make(Layer.mergeAll(OtelLive, S2LiteLive).pipe(Layer.provideMerge(ChdbLive)))
  await runtime.runPromise(Effect.void)
  if (nextProcessor === undefined) {
    throw new Error("chDB span processor was not initialized")
  }
  processor = nextProcessor
})

After(async function(this: FiregridWorld) {
  const activeRuntime = runtime
  if (activeRuntime === undefined) return
  if (processor !== undefined) {
    await processor.forceFlush()
  }
  const [spans, coverage] = await Promise.all([
    activeRuntime.runPromise(spanCounts(this.scenarioId)),
    activeRuntime.runPromise(traceCoverage(this.scenarioId)),
  ])
  recordScenarioReport({
    scenarioId: this.scenarioId,
    proofs: this.proofs.length,
    spans,
    coverage,
  })
  await Promise.all(this.proofs.map(async(proof) => {
    const result = await activeRuntime.runPromise(runTraceProofSql(proof.sql, this.scenarioId))
    if (result.ok === true) return
    const observed = await activeRuntime.runPromise(spanSummary(this.scenarioId))
    throw new Error(`trace proof failed: ${result.reason}\n\n${proof.sql}\n\nObserved spans:\n${observed}`)
  }))
})

AfterAll(async function() {
  try {
    await runtime?.dispose()
  } finally {
    runtime = undefined
    await processor?.shutdown()
    processor = undefined
  }
})

export const runSpecEffect = <A, E, R extends HarnessServices>(
  world: FiregridWorld,
  effect: Effect.Effect<A, E, R>,
): Promise<A> => {
  if (runtime === undefined) {
    throw new Error("spec runtime is not initialized")
  }
  return runtime.runPromise(
    effect.pipe(
      Effect.withSpan("firegrid.scenario", {
        attributes: {
          "firegrid.scenario.id": world.scenarioId,
        },
      }),
    ),
  )
}
