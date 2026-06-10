import { FileSystem, Path } from "@effect/platform"
import { Console, Effect } from "effect"
import {
  compareNs,
  durationNs,
  hasTraceJsonl,
  readTraceSpans,
  resolveRunDir,
  runsRoot,
  startNs,
  type SpanRecord,
  nsToMs,
} from "./trace.ts"

// Spans as written by the file exporter in runner/telemetry.ts. We keep
// the parse permissive — missing optional fields are tolerated — so a
// partially-written trace (run still in progress, crash mid-batch) still
// renders something useful.
const durationMs = (s: SpanRecord): number =>
  nsToMs(durationNs(s))

// Span names embedded with IDs (the high-cardinality ones host-sdk emits
// today) are collapsed at the head so the tree stays readable. The id
// fragments survive in attributes; the viewer's job is to show
// *structure*, not every detail.
const collapseName = (name: string): string =>
  name
    .replace(/\bctx_ext_[A-Za-z0-9_-]+/g, "ctx_ext_…")
    .replace(/\binput_[A-Za-z0-9_-]+/g, "input_…")

const formatLine = (s: SpanRecord, depth: number): string => {
  const indent = "  ".repeat(depth)
  const ms = durationMs(s).toFixed(1)
  const sideAttr = s.attributes["firegrid.side"]
  const side = typeof sideAttr === "string" ? ` [${sideAttr}]` : ""
  const status =
    s.status.code === 2
      ? " ⚠"
      : s.status.code === 1
      ? ""
      : ""
  return `${indent}- ${collapseName(s.name)}${side}${status} (${ms}ms)`
}

const buildTree = (spans: ReadonlyArray<SpanRecord>): string => {
  // Mid-run / interrupted-run robustness: OTel exports a span only on
  // `end`, so a sim in flight will have thousands of completed
  // descendants whose parents (`firegrid.simulation.run`,
  // `firegrid.side.*`, workflow scopes) haven't ended yet and aren't on
  // disk. Treat any span whose `parentSpanId` is not present in this
  // file as a *visual* root so the tree still builds — otherwise the
  // user gets a blank tree against a 3000-span file and thinks the
  // viewer is broken.
  const spanIds = new Set(spans.map(s => s.spanId))
  const byParent = new Map<string | undefined, Array<SpanRecord>>()
  spans.forEach(span => {
    const parentKey =
      span.parentSpanId !== undefined && spanIds.has(span.parentSpanId)
        ? span.parentSpanId
        : undefined
    const arr = byParent.get(parentKey) ?? []
    arr.push(span)
    byParent.set(parentKey, arr)
  })
  byParent.forEach(arr => arr.sort((a, b) => compareNs(startNs(a), startNs(b))))
  const out: Array<string> = []
  const walk = (span: SpanRecord, depth: number): void => {
    out.push(formatLine(span, depth))
    const children = byParent.get(span.spanId) ?? []
    children.forEach(child => walk(child, depth + 1))
  }
  ;(byParent.get(undefined) ?? []).forEach(root => walk(root, 0))
  return out.join("\n")
}

const summary = (spans: ReadonlyArray<SpanRecord>): string => {
  const errored = spans.filter(s => s.status.code === 2).length
  const traceIds = new Set(spans.map(s => s.traceId)).size
  const sides = new Map<string, number>()
  spans.forEach(span => {
    const side = span.attributes["firegrid.side"]
    if (typeof side === "string") {
      sides.set(side, (sides.get(side) ?? 0) + 1)
    }
  })
  const sidesText = [...sides.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `${k}=${v}`)
    .join(" ")
  return [
    `spans: ${spans.length}`,
    `traces: ${traceIds}`,
    `errored: ${errored}`,
    sidesText.length > 0 ? `sides: ${sidesText}` : undefined,
  ]
    .filter((value): value is string => value !== undefined)
    .join("  ")
}

const formatShowOutput = (
  path: Path.Path,
  runDir: string,
  spans: ReadonlyArray<SpanRecord>,
): string =>
  [
    `run: ${path.basename(runDir)}`,
    `dir: ${runDir}`,
    summary(spans),
    "",
    buildTree(spans),
  ].join("\n")

export const showRun = (runId: string | undefined) =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    const runDir = yield* resolveRunDir(runId)
    const spans = yield* readTraceSpans(runDir)
    yield* Console.log(formatShowOutput(path, runDir, spans))
  })

// A "run" is a folder that contains `trace.jsonl`. Legacy folders left by
// the pre-#426 runner (run.json + trace.md + duckdb/) are filtered out
// silently — they're on disk for archival inspection but `simulate show`
// can't read them. Filtering here keeps the listing honest.
export const listRuns = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const runsDir = yield* runsRoot
  const names = yield* fs.readDirectory(runsDir).pipe(
    Effect.orElseSucceed(() => [] as ReadonlyArray<string>),
  )
  // A "run" is a folder containing `trace.jsonl`. Legacy folders left by the
  // pre-#426 runner (run.json + trace.md + duckdb/) lack one and are filtered
  // out silently — they remain on disk for archival inspection, but
  // `simulate show` can't read them, so listing them would be dishonest.
  const dirs = yield* Effect.filter([...names].sort(), name =>
    hasTraceJsonl(path.join(runsDir, name)))
  if (dirs.length === 0) {
    yield* Console.log(`(no runs in ${runsDir})`)
    return
  }
  yield* Effect.forEach(dirs, dir => Console.log(dir), { discard: true })
})
