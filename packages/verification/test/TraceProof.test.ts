import { layer as ChdbLayer } from "@firegrid/observability"
import { Effect } from "effect"
import { describe, expect, it } from "vitest"

import { bindTrialSql, normalizeProofSql, runTraceProof, traceSql, VerificationError } from "../src/index.ts"

describe("traceSql", () => {
  it("normalizes read-only proof SQL and expands trace views", () => {
    const proof = traceSql("has-spans", "SELECT count() > 0 AS ok FROM trial_spans;")

    expect(proof.sql).toContain("FROM otel_traces")
    expect(proof.sql).toContain("firegrid.trial.id")
    expect(proof.sql).not.toContain("trial_spans")
  })

  it("rejects non-read-only statements", () => {
    expect(() => normalizeProofSql("DROP TABLE otel_traces")).toThrow(VerificationError)
    expect(() => normalizeProofSql("SELECT 1; SELECT 2")).toThrow(VerificationError)
  })

  it("binds trial ids as string literals", () => {
    expect(bindTrialSql("SELECT {trial_id:String}", "trial-'1")).toBe("SELECT 'trial-\\'1'")
  })

  it("executes proofs through chDB", () =>
    Effect.gen(function*() {
      yield* runTraceProof(traceSql("constant", "SELECT 1 AS ok"), "trial-1")

      const exit = yield* Effect.exit(
        runTraceProof(traceSql("false", "SELECT 0 AS ok"), "trial-1")
      )

      expect(exit._tag).toBe("Failure")
    }).pipe(
      Effect.provide(ChdbLayer({})),
      Effect.scoped,
      Effect.runPromise
    ))
})
