import { Data, Effect, FileSystem, Path } from "effect"
import { ChdbClient } from "@firegrid/observability"
import type { PreparedScenario } from "../durable/types.ts"
import type { World } from "../durable/support.ts"

/**
 * `@sql:` trace-proof loading + evaluation, ported from the legacy harness. A
 * proof is a single read-only chDB query over `scenario_spans` (the per-scenario
 * slice of `otel_traces`); it passes when the first column / `ok` is truthy.
 */

/** The scenario World handed to firegrid step bodies: the base World + its scenario id. */
export interface SpecWorld extends World {
  readonly scenarioId: string
}

/** Derive a stable, per-scenario key (idempotency keys, owner keys) from the World. */
export const scenarioKey = (world: { readonly scenarioId: string }, key: string): string =>
  `${world.scenarioId.replace(/[^A-Za-z0-9_.-]/g, "-")}-${key}`

export class SqlProofError extends Data.TaggedError("SqlProofError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export interface ProofBlock {
  readonly name: string
  readonly source: string
  readonly sql: string
}

export interface ProofResult {
  readonly scenarioId: string
  readonly name: string
  readonly ok: boolean
  readonly reason?: string
}

const escapeString = (value: string): string => value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

const bindTraceSql = (sql: string, scenarioId: string): string =>
  sql.replaceAll("{scenario_id:String}", `'${escapeString(scenarioId)}'`)

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
    throw new Error("trace proof SQL must be a SELECT or WITH query")
  }
  if (trimmed.includes(";")) {
    throw new Error("trace proof SQL must contain a single read-only query")
  }
  return trimmed.replace(/\bscenario_spans\b/g, scenarioSpans)
}

const parseNamedProofs = (content: string): Map<string, string> => {
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

const proofNames = (scenario: PreparedScenario): ReadonlyArray<string> =>
  scenario.tags.filter((tag) => tag.startsWith("@sql:")).map((tag) => tag.slice("@sql:".length))

/** Load the `@sql:`-named proofs for a scenario from the feature's sibling `.sql` file. */
export const loadProofs = (
  scenario: PreparedScenario,
): Effect.Effect<ReadonlyArray<ProofBlock>, SqlProofError, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function*() {
    const names = proofNames(scenario)
    if (names.length === 0) return []
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const file = path.resolve(process.cwd(), scenario.uri.replace(/\.feature$/u, ".sql"))
    const exists = yield* fs.exists(file).pipe(
      Effect.mapError((cause) => new SqlProofError({ message: `Unable to check SQL proof file ${file}`, cause })),
    )
    if (!exists) return yield* new SqlProofError({ message: `SQL proof file not found for ${scenario.uri}: ${file}` })
    const content = yield* fs.readFileString(file).pipe(
      Effect.mapError((cause) => new SqlProofError({ message: `Unable to read SQL proof file ${file}`, cause })),
    )
    const blocks = yield* Effect.try({
      try: () => parseNamedProofs(content),
      catch: (cause) => new SqlProofError({ message: `Unable to parse SQL proof file ${file}`, cause }),
    })
    return yield* Effect.forEach(names, (name) => {
      const sql = blocks.get(name)
      return sql === undefined
        ? Effect.fail(new SqlProofError({ message: `SQL proof ${name} not found in ${file}` }))
        : Effect.succeed({ name, source: `${file}#${name}`, sql })
    })
  })

const truthy = (value: unknown): boolean => {
  if (typeof value === "boolean") return value
  if (typeof value === "number") return value !== 0
  if (typeof value === "bigint") return value !== 0n
  if (typeof value === "string") return value !== "" && value !== "0" && value.toLowerCase() !== "false"
  return value != null
}

/** Run one proof against a scenario's spans; never fails — returns a pass/fail result. */
export const runProof = (
  proof: ProofBlock,
  scenarioId: string,
): Effect.Effect<ProofResult, never, ChdbClient> =>
  Effect.gen(function*() {
    const chdb = yield* ChdbClient
    const rows = yield* chdb.unsafe<Record<string, unknown>>(bindTraceSql(proof.sql, scenarioId))
    const row = rows[0]
    if (row === undefined) return { scenarioId, name: proof.name, ok: false, reason: "query returned no rows" }
    const value = "ok" in row ? row.ok : Object.values(row)[0]
    return truthy(value)
      ? { scenarioId, name: proof.name, ok: true }
      : { scenarioId, name: proof.name, ok: false, reason: "`ok` was false" }
  }).pipe(
    Effect.catch((cause) =>
      Effect.succeed({ scenarioId, name: proof.name, ok: false, reason: `proof query failed: ${String(cause)}` }),
    ),
  )
