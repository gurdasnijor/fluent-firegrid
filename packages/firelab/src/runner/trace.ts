import { FileSystem, Path } from "@effect/platform"
import { Array as Arr, Data, Effect, Option } from "effect"

// Package-relative `.simulate/` root, resolved off this module's URL (via the
// Path service inside an Effect) so it stays correct regardless of cwd — the
// runner may be invoked from anywhere in the monorepo via `pnpm --filter`.
const simulateRootUrl = new URL("../../.simulate/", import.meta.url)

// The `.simulate/` paths, resolved through the Path service. Returned together
// so a caller takes one yield and the Path service is requested once.
const simulatePaths = Effect.gen(function*() {
  const path = yield* Path.Path
  const root = yield* path.fromFileUrl(simulateRootUrl)
  return {
    runsRoot: path.join(root, "runs"),
    latestPath: path.join(root, "latest.json"),
  }
})

// `runsRoot` as an Effect (the runs directory). Exposed for consumers that list
// runs; replaces the former eagerly-resolved module-level string constant.
export const runsRoot = Effect.map(simulatePaths, paths => paths.runsRoot)

class NoRunsFound extends Data.TaggedClass("NoRunsFound")<{
  readonly runsRoot: string
}> {}

class RunNotFound extends Data.TaggedClass("RunNotFound")<{
  readonly runId: string
  readonly runsRoot: string
}> {}

class TraceFileMissing extends Data.TaggedClass("TraceFileMissing")<{
  readonly runDir: string
}> {}

export interface SpanRecord {
  // tf-9ia9: the observability file exporter can emit phase:start records for
  // in-flight spans (opt-in via FIREGRID_OTEL_FILE_PHASES=start-end). Trace
  // readers here consume completed spans only, so start records are filtered
  // out in readTraceSpans. Absent on legacy/end-only traces.
  readonly phase?: "start" | "end"
  readonly name: string
  readonly traceId: string
  readonly spanId: string
  readonly parentSpanId?: string
  readonly kind: number
  readonly startTime: readonly [number, number]
  readonly endTime: readonly [number, number]
  readonly duration: readonly [number, number]
  readonly status: { readonly code: number; readonly message?: string }
  readonly attributes: Record<string, unknown>
  readonly events?: ReadonlyArray<{
    readonly name: string
    readonly time: readonly [number, number]
    readonly attributes?: Record<string, unknown>
  }>
  readonly resource: Record<string, unknown>
}

export const nsFromHrTime = (time: readonly [number, number]): bigint =>
  BigInt(time[0]) * 1_000_000_000n + BigInt(time[1])

export const startNs = (span: SpanRecord): bigint =>
  nsFromHrTime(span.startTime)

export const endNs = (span: SpanRecord): bigint =>
  nsFromHrTime(span.endTime)

export const durationNs = (span: SpanRecord): bigint => {
  const fromField = nsFromHrTime(span.duration)
  return fromField >= 0n ? fromField : endNs(span) - startNs(span)
}

export const nsToMs = (ns: bigint): number =>
  Number(ns) / 1_000_000

export const compareNs = (a: bigint, b: bigint): number =>
  a < b ? -1 : a > b ? 1 : 0

export const isoFromNs = (ns: bigint): string =>
  new Date(Number(ns / 1_000_000n)).toISOString()

export const tracePathForRunDir = (path: Path.Path, runDir: string): string =>
  path.join(runDir, "trace.jsonl")

// True when `runDir` contains a readable `trace.jsonl`.
export const hasTraceJsonl = (runDir: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    return yield* fs.exists(tracePathForRunDir(path, runDir))
  })

export const resolveRunDir = (runId: string | undefined) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const { runsRoot, latestPath } = yield* simulatePaths

    // Explicit run id: resolve it directly, error if absent.
    if (runId !== undefined) {
      const candidate = path.join(runsRoot, runId)
      if (yield* fs.exists(candidate)) return candidate
      return yield* Effect.fail(new RunNotFound({ runId, runsRoot }))
    }

    // No id: prefer the latest-pointer if it points at a run with a trace.
    if (yield* fs.exists(latestPath)) {
      const latest = yield* fs.readFileString(latestPath).pipe(
        Effect.map(text => JSON.parse(text) as { readonly runDir?: string }),
        Effect.orElseSucceed(() => ({ runDir: undefined })),
      )
      if (
        latest.runDir !== undefined &&
        (yield* hasTraceJsonl(latest.runDir))
      ) {
        return latest.runDir
      }
    }

    // Otherwise fall back to the newest run directory that has a trace.
    const names = yield* fs.readDirectory(runsRoot).pipe(
      Effect.orElseSucceed(() => [] as ReadonlyArray<string>),
    )
    const newestWithTrace = yield* Effect.findFirst(
      [...names].sort().reverse(),
      name => hasTraceJsonl(path.join(runsRoot, name)),
    )
    return yield* Option.match(newestWithTrace, {
      onNone: () => Effect.fail(new NoRunsFound({ runsRoot })),
      onSome: name => Effect.succeed(path.join(runsRoot, name)),
    })
  })

export const readTraceSpans = (runDir: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const tracePath = tracePathForRunDir(path, runDir)
    if (!(yield* fs.exists(tracePath))) {
      return yield* Effect.fail(new TraceFileMissing({ runDir }))
    }
    const text = yield* fs.readFileString(tracePath)
    return Arr.filterMap(text.split("\n"), line =>
      line.length === 0
        ? Option.none()
        : Option.some(JSON.parse(line) as SpanRecord))
      // tf-9ia9: drop in-flight span-START records; readers here report on
      // completed spans (durations, self-time). End-only/legacy traces have no
      // `phase` field and are kept.
      .filter(span => span.phase !== "start")
  })
