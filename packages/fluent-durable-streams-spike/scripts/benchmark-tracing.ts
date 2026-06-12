import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import { appendFile, mkdir, readFile } from "node:fs/promises"
import { dirname } from "node:path"
import {
  SimpleSpanProcessor,
  type ReadableSpan,
  type SpanExporter,
} from "@opentelemetry/sdk-trace-base"
import type { Layer } from "effect"

type BackendName = "in-memory" | "s2-lite"

interface TraceSpanLine {
  readonly name: string
  readonly duration?: readonly [number, number]
  readonly attributes?: Record<string, unknown>
}

interface SpanStats {
  readonly count: number
  readonly totalMs: number
  readonly meanMs: number
  readonly p50Ms: number
  readonly p95Ms: number
  readonly maxMs: number
}

class JsonlSpanExporter implements SpanExporter {
  private readonly pending = new Set<Promise<void>>()
  private closed = false

  constructor(private readonly filePath: string) {}

  export(
    spans: ReadableSpan[],
    resultCallback: (result: { code: number; error?: Error }) => void,
  ): void {
    if (this.closed) {
      resultCallback({ code: 1, error: new Error("span exporter is closed") })
      return
    }

    const lines = spans.map((span) => {
      const context = span.spanContext()
      return JSON.stringify({
        name: span.name,
        traceId: context.traceId,
        spanId: context.spanId,
        parentSpanId: span.parentSpanContext?.spanId,
        kind: span.kind,
        startTime: span.startTime,
        endTime: span.endTime,
        duration: span.duration,
        status: span.status,
        attributes: span.attributes,
        resource: span.resource.attributes,
      }) + "\n"
    }).join("")

    const write = mkdir(dirname(this.filePath), { recursive: true }).then(() => appendFile(this.filePath, lines))
    this.pending.add(write)
    write.then(
      () => resultCallback({ code: 0 }),
      (cause: unknown) =>
        resultCallback({
          code: 1,
          error: cause instanceof Error ? cause : new Error(String(cause)),
        }),
    ).finally(() => {
      this.pending.delete(write)
    })
  }

  forceFlush(): Promise<void> {
    return Promise.all([...this.pending]).then(() => undefined)
  }

  shutdown(): Promise<void> {
    this.closed = true
    return this.forceFlush()
  }
}

export const benchmarkTraceLayer = (
  filePath: string,
  backend: BackendName,
): Layer.Layer<never, unknown> =>
  NodeSdk.layer(() => ({
    resource: {
      serviceName: "fluent-durable-streams-spike",
      attributes: {
        "benchmark.backend": backend,
      },
    },
    spanProcessor: [
      new SimpleSpanProcessor(new JsonlSpanExporter(filePath)),
    ],
  }))

const durationMs = (duration: readonly [number, number] | undefined): number =>
  duration === undefined ? 0 : duration[0] * 1000 + duration[1] / 1_000_000

const percentile = (values: readonly number[], p: number): number => {
  if (values.length === 0) {
    return 0
  }
  const sorted = [...values].sort((left, right) => left - right)
  const index = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, Math.min(index, sorted.length - 1))]!
}

const stats = (values: readonly number[]): SpanStats => ({
  count: values.length,
  totalMs: values.reduce((sum, value) => sum + value, 0),
  meanMs: values.reduce((sum, value) => sum + value, 0) / values.length,
  p50Ms: percentile(values, 50),
  p95Ms: percentile(values, 95),
  maxMs: Math.max(...values),
})

const format = (value: number): string =>
  value.toLocaleString("en-US", {
    maximumFractionDigits: 2,
    minimumFractionDigits: 2,
  })

export const printTraceSummary = async (filePath: string): Promise<void> => {
  const text = await readFile(filePath, "utf8")
  const byName = new Map<string, number[]>()
  for (const line of text.split("\n")) {
    if (line.trim().length === 0) {
      continue
    }
    const span = JSON.parse(line) as TraceSpanLine
    const values = byName.get(span.name) ?? []
    values.push(durationMs(span.duration))
    byName.set(span.name, values)
  }

  const rows = [...byName]
    .map(([name, values]) => ({ name, stats: stats(values) }))
    .sort((left, right) => right.stats.totalMs - left.stats.totalMs)

  console.log("\nTrace Span Summary")
  console.log("==================\n")
  console.log("| Span | Count | Total | Mean | p50 | p95 | Max |")
  console.log("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
  for (const row of rows.slice(0, 30)) {
    console.log(
      `| ${row.name} | ${row.stats.count} | ${format(row.stats.totalMs)} ms | ${format(row.stats.meanMs)} ms | ${format(row.stats.p50Ms)} ms | ${format(row.stats.p95Ms)} ms | ${format(row.stats.maxMs)} ms |`,
    )
  }
}
