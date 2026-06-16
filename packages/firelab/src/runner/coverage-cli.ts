/**
 * CLI handlers for the trace-coverage oracle:
 *   - `seams <id> [run-id]` re-judges a PAST run from disk with a validation's
 *     coverage spec (the same oracle the runner applies live). Iterate a spec
 *     against a stored trace without re-running the scenario.
 *   - `gaps [run-id]` prints the instrumentation map for a past run — every
 *     observed span classified evidence / edge / unknown, plus evidence spans
 *     that did not fire. Spec-independent (empty gates).
 * Both read normalized spans via runner/trace.ts; the verdict/gap report is
 * computed by runner/coverage.ts.
 */
import { FileSystem, Path } from "effect"
import { Console, Data, Effect } from "effect"
import { compileCoverage } from "../types.ts"
import { analyzeCoverage, printGaps, printSummary } from "./coverage.ts"
import type { CoverageObservations } from "./coverage.ts"
import { selectedValidation } from "./list.ts"
import { readTraceSpans, resolveRunDir, runsRoot } from "./trace.ts"

class NoRunForValidation extends Data.TaggedClass("NoRunForValidation")<{
  readonly validationId: string
  readonly runsRoot: string
}> {}

// The newest run directory for a specific validation. Run dir names are
// `<timestamp>__<id>`, so a descending lexicographic sort is chronological.
// Without this, `seams <id>` (no run-id) would resolve the GLOBAL latest run
// (latest.json) — judging a different validation's trace.
const latestRunDirForValidation = (validationId: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const root = yield* runsRoot
    const names = yield* fs.readDirectory(root).pipe(
      Effect.orElseSucceed(() => [] as ReadonlyArray<string>),
    )
    const match = [...names]
      .filter(name => name.endsWith(`__${validationId}`))
      .sort()
      .at(-1)
    if (match === undefined) {
      return yield* Effect.fail(new NoRunForValidation({ validationId, runsRoot: root }))
    }
    return path.join(root, match)
  })

const readObservations = (runDir: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const file = path.join(runDir, "observations.json")
    if (!(yield* fs.exists(file))) return {} satisfies CoverageObservations
    return JSON.parse(yield* fs.readFileString(file)) as CoverageObservations
  })

/** Re-judge a past run with a validation's coverage spec. Exit code gates on the
 *  computed verdict, exactly like `run`. */
export const seamsCoverage = (
  validationId: string,
  runId: string | undefined,
) =>
  Effect.gen(function*() {
    const validation = yield* selectedValidation(validationId)
    const coverage = compileCoverage(validation)
    if (coverage === undefined) {
      yield* Console.error(
        `validation "${validationId}" has no coverage spec; nothing to judge`,
      )
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
      return
    }
    // Explicit run-id resolves directly; otherwise the latest run FOR THIS validation
    // (not the global latest).
    const runDir = runId === undefined
      ? yield* latestRunDirForValidation(validationId)
      : yield* resolveRunDir(runId)
    const spans = yield* readTraceSpans(runDir)
    const observations = yield* readObservations(runDir)
    yield* Console.log(`seams: ${validationId}  ${runDir}  (${spans.length} spans)`)
    const report = analyzeCoverage(coverage, spans, observations)
    yield* printSummary(report)
    if (report.gatingFailing > 0) {
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
    }
  })

/** Print the instrumentation map for a past run (spec-independent). */
export const gapsReport = (runId: string | undefined) =>
  Effect.gen(function*() {
    const runDir = yield* resolveRunDir(runId)
    const spans = yield* readTraceSpans(runDir)
    yield* Console.log(`gaps: ${runDir}  (${spans.length} spans)`)
    const report = analyzeCoverage({ gates: [] }, spans)
    yield* printGaps(report.gaps)
  })
