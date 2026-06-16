import { Path } from "effect"
import { Console, Effect } from "effect"
import {
  compareNs,
  durationNs,
  endNs,
  isoFromNs,
  nsToMs,
  readTraceSpans,
  resolveRunDir,
  startNs,
  tracePathForRunDir,
  type SpanRecord,
} from "./trace.ts"

interface PerfOptions {
  readonly top: number
  readonly idleThresholdMs: number
  readonly findingDraft: boolean
  readonly findingThresholdMs: number
}

interface SpanTiming {
  readonly span: SpanRecord
  readonly startNs: bigint
  readonly endNs: bigint
  readonly durationNs: bigint
  readonly childNs: bigint
  readonly selfNs: bigint
}

interface HttpRoll {
  readonly key: string
  readonly count: number
  readonly totalNs: bigint
}

interface IdleGap {
  readonly startNs: bigint
  readonly endNs: bigint
  readonly durationNs: bigint
}

interface PerfReport {
  readonly runId: string
  readonly tracePath: string
  readonly spanCount: number
  readonly windowStartNs: bigint | undefined
  readonly windowEndNs: bigint | undefined
  readonly topSelf: ReadonlyArray<SpanTiming>
  readonly httpRolls: ReadonlyArray<HttpRoll>
  readonly idleGaps: ReadonlyArray<IdleGap>
  readonly findingGaps: ReadonlyArray<IdleGap>
}

const clamp = (value: bigint, min: bigint, max: bigint): bigint =>
  value < min ? min : value > max ? max : value

const sumUnionNs = (
  intervals: ReadonlyArray<readonly [bigint, bigint]>,
): bigint => {
  const sorted = intervals
    .filter(([start, end]) => end > start)
    .slice()
    .sort((a, b) => compareNs(a[0], b[0]))
  let total = 0n
  let current: readonly [bigint, bigint] | undefined
  sorted.forEach(interval => {
    if (current === undefined) {
      current = interval
      return
    }
    if (interval[0] <= current[1]) {
      current = [current[0], interval[1] > current[1] ? interval[1] : current[1]]
      return
    }
    total += current[1] - current[0]
    current = interval
  })
  if (current !== undefined) total += current[1] - current[0]
  return total
}

const analyzeSelfTime = (
  spans: ReadonlyArray<SpanRecord>,
): ReadonlyArray<SpanTiming> => {
  const childrenByParent = new Map<string, Array<SpanRecord>>()
  spans.forEach(span => {
    if (span.parentSpanId === undefined) return
    const children = childrenByParent.get(span.parentSpanId) ?? []
    children.push(span)
    childrenByParent.set(span.parentSpanId, children)
  })
  return spans.map(span => {
    const spanStart = startNs(span)
    const spanEnd = endNs(span)
    const total = durationNs(span)
    const childNs = sumUnionNs(
      (childrenByParent.get(span.spanId) ?? []).map(child => [
        clamp(startNs(child), spanStart, spanEnd),
        clamp(endNs(child), spanStart, spanEnd),
      ]),
    )
    const self = total > childNs ? total - childNs : 0n
    return { span, startNs: spanStart, endNs: spanEnd, durationNs: total, childNs, selfNs: self }
  })
}

const stringAttr = (
  attributes: Record<string, unknown>,
  names: ReadonlyArray<string>,
): string | undefined => {
  let found: string | undefined
  names.some(name => {
    const value = attributes[name]
    if (typeof value === "string" && value.length > 0) {
      found = value
      return true
    }
    return false
  })
  return found
}

const pathFromUrl = (value: string): string | undefined => {
  try {
    return new URL(value).pathname
  } catch {
    return undefined
  }
}

const normalizeRoute = (pathValue: string): string =>
  // Shapes observed in apps/factory and client-sdk traces; extend when new id-shapes leak into routes.
  pathValue
    .replace(/\bctx_ext_[A-Za-z0-9_-]+/g, "ctx_ext_:id")
    .replace(/\binput_[A-Za-z0-9_-]+/g, "input_:id")
    .split("/")
    .map(segment =>
      /^\d+$/.test(segment)
        || /^[0-9a-f]{8,}$/i.test(segment)
        || /^[0-9a-f]{8}-[0-9a-f-]{13,}$/i.test(segment)
        ? ":id"
        : segment,
    )
    .join("/")

const routeForSpan = (span: SpanRecord): string | undefined => {
  const route = stringAttr(span.attributes, ["http.route", "url.template"])
  if (route !== undefined) return route
  const pathValue = stringAttr(span.attributes, [
    "url.path",
    "http.target",
    "http.route.path",
  ])
  if (pathValue !== undefined) return normalizeRoute(pathValue)
  const urlValue = stringAttr(span.attributes, ["url.full", "http.url"])
  const parsed = urlValue === undefined ? undefined : pathFromUrl(urlValue)
  return parsed === undefined ? undefined : normalizeRoute(parsed)
}

const httpRolls = (spans: ReadonlyArray<SpanRecord>): ReadonlyArray<HttpRoll> => {
  const rolls = new Map<string, HttpRoll>()
  spans.forEach(span => {
    const route = routeForSpan(span)
    if (route === undefined) return
    const method = stringAttr(span.attributes, [
      "http.request.method",
      "http.method",
    ]) ?? "HTTP"
    const key = `${method.toUpperCase()} ${route}`
    const existing = rolls.get(key)
    rolls.set(key, {
      key,
      count: (existing?.count ?? 0) + 1,
      totalNs: (existing?.totalNs ?? 0n) + durationNs(span),
    })
  })
  return [...rolls.values()].sort((a, b) => {
    const byTotal = compareNs(b.totalNs, a.totalNs)
    return byTotal === 0 ? a.key.localeCompare(b.key) : byTotal
  })
}

const idleGaps = (
  spans: ReadonlyArray<SpanRecord>,
  thresholdMs: number,
): ReadonlyArray<IdleGap> => {
  const thresholdNs = BigInt(Math.max(0, thresholdMs)) * 1_000_000n
  const activity = spans
    .flatMap(span => [startNs(span), endNs(span)])
    .filter(time => time > 0n)
    .sort(compareNs)
  const gaps: Array<IdleGap> = []
  let previous: bigint | undefined
  activity.forEach(time => {
    if (previous === undefined) {
      previous = time
      return
    }
    const duration = time - previous
    if (duration > thresholdNs) {
      gaps.push({ startNs: previous, endNs: time, durationNs: duration })
    }
    previous = time
  })
  return gaps
}

const analyzePerf = (
  spans: ReadonlyArray<SpanRecord>,
  options: Pick<PerfOptions, "top" | "idleThresholdMs" | "findingThresholdMs">,
  runId: string,
  tracePath: string,
): PerfReport => {
  const topN = Math.max(1, options.top)
  const timings = analyzeSelfTime(spans)
  const sorted = timings.slice().sort((a, b) => {
    const bySelf = compareNs(b.selfNs, a.selfNs)
    return bySelf === 0 ? compareNs(b.durationNs, a.durationNs) : bySelf
  })
  const gaps = idleGaps(spans, options.idleThresholdMs)
  const findingThresholdNs = BigInt(Math.max(0, options.findingThresholdMs)) * 1_000_000n
  return {
    runId,
    tracePath,
    spanCount: spans.length,
    windowStartNs: timings.length === 0
      ? undefined
      : timings.reduce((min, span) => span.startNs < min ? span.startNs : min, timings[0]!.startNs),
    windowEndNs: timings.length === 0
      ? undefined
      : timings.reduce((max, span) => span.endNs > max ? span.endNs : max, timings[0]!.endNs),
    topSelf: sorted.slice(0, topN),
    httpRolls: httpRolls(spans),
    idleGaps: gaps,
    findingGaps: gaps.filter(gap => gap.durationNs > findingThresholdNs),
  }
}

const formatMs = (ns: bigint): string => {
  const ms = nsToMs(ns)
  return `${ms >= 1000 ? ms.toFixed(0) : ms.toFixed(1)}ms`
}

const sideLabel = (span: SpanRecord): string => {
  const side = span.attributes["firegrid.side"]
  return typeof side === "string" ? side : "-"
}

const renderReport = (report: PerfReport): string => {
  const window = report.windowStartNs === undefined || report.windowEndNs === undefined
    ? "empty"
    : `${isoFromNs(report.windowStartNs)} -> ${isoFromNs(report.windowEndNs)} (${formatMs(report.windowEndNs - report.windowStartNs)})`
  const lines = [
    `run: ${report.runId}`,
    `trace: ${report.tracePath}`,
    `spans: ${report.spanCount}`,
    `window: ${window}`,
    "",
    "top self-time spans:",
    ...report.topSelf.map((timing, index) =>
      `${index + 1}. ${formatMs(timing.selfNs)} self\t${formatMs(timing.durationNs)} total\t${sideLabel(timing.span)}\t${timing.span.name}`,
    ),
    "",
    "http rolls:",
    ...(report.httpRolls.length === 0
      ? ["(none)"]
      : report.httpRolls.map(roll =>
        `${formatMs(roll.totalNs)} total\t${roll.count} spans\t${roll.key}`,
      )),
    "",
    "idle gaps:",
    ...(report.idleGaps.length === 0
      ? ["(none above threshold)"]
      : report.idleGaps.map(gap =>
        `${formatMs(gap.durationNs)}\t${isoFromNs(gap.startNs)} -> ${isoFromNs(gap.endNs)}`,
      )),
  ]
  return lines.join("\n")
}

const renderFindingDraft = (report: PerfReport, thresholdMs: number): string => {
  const gaps = report.findingGaps
  if (gaps.length === 0) {
    return `firelab perf finding draft: no idle gaps exceeded ${thresholdMs}ms for ${report.runId}`
  }
  return [
    "## Finding Source: firelab perf idle gap regression",
    "",
    `Run: ${report.runId}`,
    `Trace: ${report.tracePath}`,
    `Threshold: ${thresholdMs}ms`,
    "",
    ...gaps.map((gap, index) =>
      [
        `${index + 1}. ${formatMs(gap.durationNs)}`,
        `   start: ${isoFromNs(gap.startNs)}`,
        `   end: ${isoFromNs(gap.endNs)}`,
      ].join("\n"),
    ),
  ].join("\n")
}

interface PerfOutput {
  readonly stdout: string
  readonly stderr: string | undefined
}

const formatPerfOutput = (
  report: PerfReport,
  options: Pick<PerfOptions, "findingDraft" | "findingThresholdMs">,
): PerfOutput => ({
  stdout: renderReport(report),
  stderr: options.findingDraft
    ? renderFindingDraft(report, options.findingThresholdMs)
    : undefined,
})

export const showPerf = (runId: string, options: PerfOptions) =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    const runDir = yield* resolveRunDir(runId)
    const spans = yield* readTraceSpans(runDir)
    // firegrid-observability.TINY_FIREGRID_SIMULATIONS.11
    const report = analyzePerf(spans, options, path.basename(runDir), tracePathForRunDir(path, runDir))
    const output = formatPerfOutput(report, options)
    yield* Console.log(output.stdout)
    // firegrid-observability.TINY_FIREGRID_SIMULATIONS.12
    if (output.stderr !== undefined) {
      yield* Console.error(output.stderr)
    }
  })
