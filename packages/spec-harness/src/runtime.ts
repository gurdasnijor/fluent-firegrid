import { After, AfterAll, Before, BeforeAll, type ITestCaseHookParameter, type IWorld } from "@cucumber/cucumber"
import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import { ChdbClient, ChdbSession, ChdbSpanExporter, layer as ChdbLayer } from "@firegrid/observability"
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { Data, Effect, FileSystem, Layer, Path } from "effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import type { S2Client } from "effect-s2"
import { recordScenarioReport, type SpanCount } from "./report-state.ts"
import { S2LiteLive } from "./s2lite.ts"

interface ProofBlock {
  readonly name?: string
  readonly source?: string
  readonly sql: string
}

interface ScenarioState {
  readonly scenarioId: string
  readonly proofs: Array<ProofBlock>
}

class SqlProofError extends Data.TaggedError("SqlProofError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export type HarnessServices = S2Client | ChdbSession | ChdbClient | FileSystem.FileSystem | Path.Path

// eslint-disable-next-line local/no-module-durable-cache -- Cucumber run-scoped harness runtime; product durability still lives in S2.
let runtime: ManagedRuntime.ManagedRuntime<HarnessServices, unknown> | undefined
// eslint-disable-next-line local/no-module-durable-cache -- Cucumber run-scoped trace exporter handle for per-scenario forceFlush.
let processor: BatchSpanProcessor | undefined
const states = new WeakMap<IWorld, ScenarioState>()

const stateFor = (world: IWorld): ScenarioState => {
  const state = states.get(world)
  if (state === undefined) {
    throw new Error("scenario state is not initialized")
  }
  return state
}

export const addTraceProof = (world: IWorld, sql: string): void => {
  stateFor(world).proofs.push({ sql: normalizeProofSql(sql) })
}

export const scenarioKey = (world: IWorld, key: string): string =>
  `${stateFor(world).scenarioId.replace(/[^A-Za-z0-9_.-]/g, "-")}-${key}`

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

const scenarioSpans = `
(
  SELECT *
  FROM otel_traces
  WHERE ${scenarioTraceWhere}
)
`

const normalizeProofSql = (sql: string): string => {
  const trimmed = sql.trim().replace(/;+\s*$/, "")
  if (!/^(select|with)\b/i.test(trimmed)) {
    throw new SqlProofError({ message: "trace proof SQL must be a SELECT or WITH query" })
  }
  if (trimmed.includes(";")) {
    throw new SqlProofError({ message: "trace proof SQL must contain a single read-only query" })
  }
  return trimmed.replace(/\bscenario_spans\b/g, scenarioSpans)
}

const proofFileFor = (featureUri: string): Effect.Effect<string, never, Path.Path> =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    return path.resolve(process.cwd(), featureUri.replace(/\.feature$/u, ".sql"))
  })

const parseNamedProofs = (file: string, content: string): Map<string, string> => {
  const blocks = new Map<string, string>()
  let name: string | undefined
  let lines: Array<string> = []
  const flush = (): void => {
    if (name === undefined) return
    blocks.set(name, normalizeProofSql(lines.join("\n")))
  }
  content.split(/\r?\n/u).forEach((line) => {
    const match = /^--\s*name:\s*([A-Za-z0-9_.:-]+)\s*$/u.exec(line)
    if (match === null) {
      lines.push(line)
      return
    }
    flush()
    name = match[1]
    lines = []
  })
  flush()
  return blocks
}

const sqlProofTagsFor = (scenario: ITestCaseHookParameter): ReadonlyArray<string> =>
  scenario.pickle.tags
    .map((tag) => tag.name)
    .filter((tag) => tag.startsWith("@sql:"))
    .map((tag) => tag.slice("@sql:".length))

const proofsFor = (
  scenario: ITestCaseHookParameter,
): Effect.Effect<Array<ProofBlock>, SqlProofError, FileSystem.FileSystem | Path.Path> => {
  const names = sqlProofTagsFor(scenario)
  if (names.length === 0) return Effect.succeed([])
  return Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const featureUri = scenario.pickle.uri
    const file = yield* proofFileFor(featureUri)
    const exists = yield* fs.exists(file).pipe(
      Effect.mapError((cause) => new SqlProofError({ message: `Unable to check SQL proof file for ${featureUri}: ${file}`, cause })),
    )
    if (exists === false) {
      return yield* new SqlProofError({ message: `SQL proof file not found for ${featureUri}: ${file}` })
    }
    const content = yield* fs.readFileString(file).pipe(
      Effect.mapError((cause) => new SqlProofError({ message: `Unable to read SQL proof file for ${featureUri}: ${file}`, cause })),
    )
    const proofs = yield* Effect.try({
      try: () => parseNamedProofs(file, content),
      catch: (cause) =>
        cause instanceof SqlProofError
          ? cause
          : new SqlProofError({ message: `Unable to parse SQL proof file for ${featureUri}: ${file}`, cause }),
    })
    return yield* Effect.forEach(names, (name) => {
      const sql = proofs.get(name)
      if (sql === undefined) {
        return Effect.fail(new SqlProofError({ message: `SQL proof ${name} not found in ${file}` }))
      }
      return Effect.succeed({
        name,
        source: `${file}#${name}`,
        sql,
      })
    })
  })
}

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

Before(async function(this: IWorld, scenario) {
  const activeRuntime = runtime
  if (activeRuntime === undefined) {
    throw new Error("spec runtime is not initialized")
  }
  states.set(this, {
    scenarioId: scenarioIdFor(scenario),
    proofs: await activeRuntime.runPromise(proofsFor(scenario)),
  })
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
  runtime = ManagedRuntime.make(
    Layer.mergeAll(OtelLive, S2LiteLive, NodeFileSystem.layer, NodePath.layer).pipe(Layer.provideMerge(ChdbLive)),
  )
  await runtime.runPromise(Effect.void)
  if (nextProcessor === undefined) {
    throw new Error("chDB span processor was not initialized")
  }
  processor = nextProcessor
})

After(async function(this: IWorld) {
  const activeRuntime = runtime
  const state = stateFor(this)
  try {
    if (activeRuntime === undefined) return
    if (processor !== undefined) {
      await processor.forceFlush()
    }
    const [spans, coverage] = await Promise.all([
      activeRuntime.runPromise(spanCounts(state.scenarioId)),
      activeRuntime.runPromise(traceCoverage(state.scenarioId)),
    ])
    recordScenarioReport({
      scenarioId: state.scenarioId,
      proofs: state.proofs.length,
      spans,
      coverage,
    })
    await Promise.all(state.proofs.map(async(proof) => {
      const result = await activeRuntime.runPromise(runTraceProofSql(proof.sql, state.scenarioId))
      if (result.ok === true) return
      const observed = await activeRuntime.runPromise(spanSummary(state.scenarioId))
      const label = proof.name === undefined ? "inline trace proof" : `trace proof ${proof.name}`
      const source = proof.source === undefined ? "" : `\nSource: ${proof.source}`
      throw new Error(`${label} failed: ${result.reason}${source}\n\n${proof.sql}\n\nObserved spans:\n${observed}`)
    }))
  } finally {
    states.delete(this)
  }
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
  world: IWorld,
  effect: Effect.Effect<A, E, R>,
): Promise<A> => {
  if (runtime === undefined) {
    throw new Error("spec runtime is not initialized")
  }
  const state = stateFor(world)
  return runtime.runPromise(
    effect.pipe(
      Effect.withSpan("firegrid.scenario", {
        attributes: {
          "firegrid.scenario.id": state.scenarioId,
        },
      }),
    ),
  )
}
