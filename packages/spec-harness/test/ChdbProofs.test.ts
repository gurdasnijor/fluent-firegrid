import { describe, expect, it } from "@effect/vitest"
import { normalizeProofSql, parseNamedProofs, SqlProofError, truthy } from "../src/sql-proofs.ts"

describe("normalizeProofSql", () => {
  it("accepts SELECT and WITH queries", () => {
    expect(normalizeProofSql("SELECT 1 AS ok")).toBe("SELECT 1 AS ok")
    expect(normalizeProofSql("WITH x AS (SELECT 1) SELECT * FROM x")).toBe("WITH x AS (SELECT 1) SELECT * FROM x")
  })

  it("strips trailing semicolons", () => {
    expect(normalizeProofSql("SELECT 1 AS ok;;;  ")).toBe("SELECT 1 AS ok")
  })

  it("rejects non-read and multi-statement SQL", () => {
    expect(() => normalizeProofSql("INSERT INTO x VALUES (1)")).toThrow(SqlProofError)
    expect(() => normalizeProofSql("SELECT 1; SELECT 2")).toThrow(SqlProofError)
  })

  it("expands the scenario_spans macro", () => {
    const sql = normalizeProofSql("SELECT count() AS ok FROM scenario_spans")
    expect(sql).toContain("FROM otel_traces")
    expect(sql).toContain("SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}")
    expect(sql).not.toContain("FROM scenario_spans")
  })
})

describe("parseNamedProofs", () => {
  it("parses named SQL blocks", () => {
    const proofs = parseNamedProofs("feature.sql", `
-- name: first
SELECT 1 AS ok
-- name: second.trace
WITH x AS (SELECT 2) SELECT * FROM x
`)
    expect(proofs.get("first")).toBe("SELECT 1 AS ok")
    expect(proofs.get("second.trace")).toBe("WITH x AS (SELECT 2) SELECT * FROM x")
  })

  it("returns no blocks when no names are present", () => {
    expect(parseNamedProofs("feature.sql", "SELECT 1 AS ok").size).toBe(0)
  })

  it("rejects duplicate names", () => {
    expect(() => parseNamedProofs("feature.sql", `
-- name: duplicate
SELECT 1 AS ok
-- name: duplicate
SELECT 2 AS ok
`)).toThrow(SqlProofError)
  })
})

describe("truthy", () => {
  it("matches the proof verdict matrix", () => {
    expect(truthy(1)).toBe(true)
    expect(truthy(true)).toBe(true)
    expect(truthy("yes")).toBe(true)
    expect(truthy(0)).toBe(false)
    expect(truthy("")).toBe(false)
    expect(truthy("0")).toBe(false)
    expect(truthy("false")).toBe(false)
    expect(truthy(null)).toBe(false)
    expect(truthy(undefined)).toBe(false)
  })
})
