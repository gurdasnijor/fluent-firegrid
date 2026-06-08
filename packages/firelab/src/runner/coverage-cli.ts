/**
 * CLI handlers for the trace-coverage oracle:
 *   - `seams <id> [run-id]` re-judges a PAST run from disk with a simulation's
 *     coverage spec (the same oracle the runner applies live). Iterate a spec
 *     against a stored trace without re-spawning the host.
 *   - `gaps [run-id]` prints the instrumentation map for a past run — every
 *     observed span classified host-substrate / edge / unknown, plus the
 *     host-substrate spans that did NOT fire. Spec-independent (empty gates).
 * Both read normalized spans via runner/trace.ts; the verdict/gap report is
 * computed by runner/coverage.ts.
 */
import { FileSystem, Path } from "@effect/platform"
import { Console, Data, Effect } from "effect"
import { analyzeCoverage, printGaps, printSummary } from "./coverage.ts"
import { selectedSimulation } from "./list.ts"
import { readTraceSpans, resolveRunDir, runsRoot } from "./trace.ts"

class NoRunForSimulation extends Data.TaggedClass("NoRunForSimulation")<{
  readonly simulationId: string
  readonly runsRoot: string
}> {}

// The newest run directory for a specific simulation. Run dir names are
// `<timestamp>__<id>`, so a descending lexicographic sort is chronological.
// Without this, `seams <id>` (no run-id) would resolve the GLOBAL latest run
// (latest.json) — judging a different sim's trace.
const latestRunDirForSim = (simulationId: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const root = yield* runsRoot
    const names = yield* fs.readDirectory(root).pipe(
      Effect.orElseSucceed(() => [] as ReadonlyArray<string>),
    )
    const match = [...names]
      .filter(name => name.endsWith(`__${simulationId}`))
      .sort()
      .at(-1)
    if (match === undefined) {
      return yield* Effect.fail(new NoRunForSimulation({ simulationId, runsRoot: root }))
    }
    return path.join(root, match)
  })

/** Re-judge a past run with a simulation's coverage spec. Exit code gates on the
 *  computed verdict, exactly like `run`. */
export const seamsCoverage = (
  simulationId: string,
  runId: string | undefined,
) =>
  Effect.gen(function*() {
    const simulation = yield* selectedSimulation(simulationId)
    if (simulation.coverage === undefined) {
      yield* Console.error(
        `simulation "${simulationId}" has no coverage spec; nothing to judge`,
      )
      yield* Effect.sync(() => {
        process.exitCode = 1
      })
      return
    }
    // Explicit run-id resolves directly; otherwise the latest run FOR THIS sim
    // (not the global latest).
    const runDir = runId === undefined
      ? yield* latestRunDirForSim(simulationId)
      : yield* resolveRunDir(runId)
    const spans = yield* readTraceSpans(runDir)
    yield* Console.log(`seams: ${simulationId}  ${runDir}  (${spans.length} spans)`)
    const report = analyzeCoverage(simulation.coverage, spans)
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
