import { After, Before, setWorldConstructor, World, type ITestCaseHookParameter, type IWorldOptions } from "@cucumber/cucumber"
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

export class FiregridWorld extends World {
  scenarioId = ""
  proofs: Array<ProofBlock> = []
  processor?: BatchSpanProcessor
  runtime?: ManagedRuntime.ManagedRuntime<S2Client | ChdbSession | ChdbClient, unknown>
  streamDb?: unknown
  streamDbKey?: string

  constructor(options: IWorldOptions) {
    super(options)
  }
}

setWorldConstructor(FiregridWorld)

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

const spanCounts = (scenarioId: string): Effect.Effect<ReadonlyArray<SpanCount>, unknown, ChdbClient> =>
  Effect.gen(function*() {
    const sql = yield* ChdbClient
    const rows = yield* sql.unsafe<{ readonly name: string; readonly count: number }>(bindTraceSql(`
SELECT SpanName AS name, count() AS count
FROM otel_traces
WHERE TraceId IN (
  SELECT TraceId
  FROM otel_traces
  WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
)
GROUP BY SpanName
ORDER BY SpanName
`, scenarioId))
    return rows
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

Before(async function(this: FiregridWorld, scenario) {
  this.scenarioId = scenarioIdFor(scenario)
  this.proofs = []
  let processor: BatchSpanProcessor | undefined
  const ChdbLive = ChdbLayer({})
  const OtelLive = Layer.unwrap(
    Effect.gen(function*() {
      const session = yield* ChdbSession
      const nextProcessor = new BatchSpanProcessor(new ChdbSpanExporter({ session, table: "otel_traces" }))
      processor = nextProcessor
      return NodeSdk.layer(() => ({
        resource: { serviceName: "firegrid-cucumber" },
        spanProcessor: [nextProcessor],
      }))
    }),
  )
  this.runtime = ManagedRuntime.make(Layer.mergeAll(OtelLive, S2LiteLive).pipe(Layer.provideMerge(ChdbLive)))
  await this.runtime.runPromise(Effect.void)
  if (processor === undefined) {
    throw new Error("chDB span processor was not initialized")
  }
  this.processor = processor
})

After(async function(this: FiregridWorld) {
  const processor = this.processor
  const runtime = this.runtime
  try {
    if (processor !== undefined) {
      await processor.forceFlush()
    }
    if (runtime !== undefined) {
      const spans = await runtime.runPromise(spanCounts(this.scenarioId))
      recordScenarioReport({
        scenarioId: this.scenarioId,
        proofs: this.proofs.length,
        spans,
      })
      await Promise.all(this.proofs.map(async(proof) => {
        const result = await runtime.runPromise(runTraceProofSql(proof.sql, this.scenarioId))
        if (result.ok === true) return
        const observed = await runtime.runPromise(spanSummary(this.scenarioId))
        throw new Error(`trace proof failed: ${result.reason}\n\n${proof.sql}\n\nObserved spans:\n${observed}`)
      }))
    }
  } finally {
    await this.runtime?.dispose()
    await processor?.shutdown()
  }
})

export const runScenarioEffect = <A, E, R extends S2Client>(
  world: FiregridWorld,
  step: string,
  effect: Effect.Effect<A, E, R>,
): Promise<A> => {
  if (world.runtime === undefined) {
    throw new Error("scenario runtime is not initialized")
  }
  return world.runtime.runPromise(
    effect.pipe(
      Effect.withSpan("cucumber.step", {
        attributes: {
          "firegrid.scenario.id": world.scenarioId,
          "cucumber.step": step,
        },
      }),
    ),
  )
}
