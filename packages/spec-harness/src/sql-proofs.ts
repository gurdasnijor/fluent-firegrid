import { Data } from "effect"

export interface ProofBlock {
  readonly name?: string
  readonly source?: string
  readonly sql: string
}

export class SqlProofError extends Data.TaggedError("SqlProofError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export const scenarioTraceWhereSql = `
TraceId IN (
  SELECT TraceId
  FROM otel_traces
  WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
)
`

const scenarioSpansSql = `
(
  SELECT *
  FROM otel_traces
  WHERE ${scenarioTraceWhereSql}
)
`

export const truthy = (value: unknown): boolean => {
  if (typeof value === "boolean") return value
  if (typeof value === "number") return value !== 0
  if (typeof value === "bigint") return value !== 0n
  if (typeof value === "string") return value !== "" && value !== "0" && value.toLowerCase() !== "false"
  return value != null
}

export const normalizeProofSql = (sql: string): string => {
  const trimmed = sql.trim().replace(/;+\s*$/u, "")
  if (!/^(select|with)\b/iu.test(trimmed)) {
    throw new SqlProofError({ message: "trace proof SQL must be a SELECT or WITH query" })
  }
  if (trimmed.includes(";")) {
    throw new SqlProofError({ message: "trace proof SQL must contain a single read-only query" })
  }
  return trimmed.replace(/\bscenario_spans\b/gu, scenarioSpansSql)
}

export const parseNamedProofs = (file: string, content: string): Map<string, string> => {
  const blocks = new Map<string, string>()
  let name: string | undefined
  let lines: ReadonlyArray<string> = []
  const flush = (): void => {
    if (name === undefined) return
    if (blocks.has(name)) {
      throw new SqlProofError({ message: `Duplicate SQL proof ${name} in ${file}` })
    }
    blocks.set(name, normalizeProofSql(lines.join("\n")))
  }
  content.split(/\r?\n/u).forEach((line) => {
    const match = /^--\s*name:\s*([A-Za-z0-9_.:-]+)\s*$/u.exec(line)
    if (match === null) {
      lines = [...lines, line]
      return
    }
    flush()
    name = match[1]
    lines = []
  })
  flush()
  return blocks
}
