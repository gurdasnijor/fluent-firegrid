/**
 * OTel `SpanExporter` that writes to chDB (embedded ClickHouse) using the
 * contrib clickhouseexporter `otel_traces` schema, VERBATIM (Map variant).
 *
 * Column names + types match the exporter's INSERT contract, so the same table
 * is readable by Grafana's ClickHouse OTel integration / ClickStack / HyperDX.
 * Wire it via `@effect/opentelemetry` NodeSdk + BatchSpanProcessor.
 *
 * Two deliberate choices:
 *  - Map variant (not JSON variant): your oracles do `SpanAttributes['k']`
 *    lookups, which the mapKeys/mapValues bloom indices serve. JSON variant is a
 *    drop-in if you set enable_json_type and switch the attr columns to JSON.
 *  - chDB's synchronous `Session.query` accepts `INSERT ... FORMAT JSONEachRow`
 *    directly. It does not accept the streaming `input(...)` form used by the
 *    network ClickHouse exporter, so timestamps are rendered as DateTime64(9)
 *    strings before insert.
 *
 * chDB `session.query` is synchronous and blocking — fine at oracle/test scale.
 * BatchSpanProcessor coalesces spans so each `export` is one multi-row INSERT.
 */
import { type Attributes, type AttributeValue, type HrTime, SpanKind, SpanStatusCode } from "@opentelemetry/api"
import { type ExportResult, ExportResultCode } from "@opentelemetry/core"
import type { ReadableSpan, SpanExporter } from "@opentelemetry/sdk-trace-base"
import type { Session } from "chdb"

export interface ChdbSpanExporterOptions {
  readonly session: Session
  /** Defaults to "otel_traces". */
  readonly table?: string
  /** Optional database; created with CREATE DATABASE IF NOT EXISTS when set. */
  readonly database?: string
}

// ── pdata-compatible string mappings (match SpanKind.String()/StatusCode.String()) ──

const SPAN_KIND: Record<number, string> = {
  [SpanKind.INTERNAL]: "Internal",
  [SpanKind.SERVER]: "Server",
  [SpanKind.CLIENT]: "Client",
  [SpanKind.PRODUCER]: "Producer",
  [SpanKind.CONSUMER]: "Consumer",
}

const STATUS_CODE: Record<number, string> = {
  [SpanStatusCode.UNSET]: "Unset",
  [SpanStatusCode.OK]: "Ok",
  [SpanStatusCode.ERROR]: "Error",
}

// ── value coercion ───────────────────────────────────────────────────────────

/** epoch nanoseconds as bigint (avoids JS-number precision loss on absolute ns). */
const hrNanos = ([seconds, nanos]: HrTime): bigint => BigInt(seconds) * 1_000_000_000n + BigInt(nanos)

/** Map(String, String) requires string values; stringify everything else. */
const attrValueToString = (v: AttributeValue | undefined): string =>
  v === undefined || v === null
    ? ""
    : Array.isArray(v)
    ? JSON.stringify(v)
    : typeof v === "string"
    ? v
    : String(v)

const attrsToObject = (attrs?: Attributes): Record<string, string> => {
  if (attrs === undefined) return {}
  return Object.fromEntries(Object.entries(attrs).map(([key, value]) => [key, attrValueToString(value)]))
}

// version-drift tolerant accessors (SDK renamed these across releases)
interface InstrumentationScopeLike {
  readonly name?: unknown
  readonly version?: unknown
}

interface LegacyReadableSpan {
  readonly instrumentationLibrary?: InstrumentationScopeLike
  readonly parentSpanId?: unknown
}

const stringOrEmpty = (value: unknown): string =>
  typeof value === "string" ? value : ""

const scopeOf = (span: ReadableSpan): { name: string; version: string } => {
  const scope = span.instrumentationScope ?? (span as LegacyReadableSpan).instrumentationLibrary
  return { name: stringOrEmpty(scope?.name), version: stringOrEmpty(scope?.version) }
}
const parentIdOf = (span: ReadableSpan): string =>
  span.parentSpanContext?.spanId ?? stringOrEmpty((span as LegacyReadableSpan).parentSpanId)

// ── SQL ──────────────────────────────────────────────────────────────────────

const createTableSql = (qualified: string): string =>
  `CREATE TABLE IF NOT EXISTS ${qualified} (
  Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
  TraceId String CODEC(ZSTD(1)),
  SpanId String CODEC(ZSTD(1)),
  ParentSpanId String CODEC(ZSTD(1)),
  TraceState String CODEC(ZSTD(1)),
  SpanName LowCardinality(String) CODEC(ZSTD(1)),
  SpanKind LowCardinality(String) CODEC(ZSTD(1)),
  ServiceName LowCardinality(String) CODEC(ZSTD(1)),
  ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  ScopeName String CODEC(ZSTD(1)),
  ScopeVersion String CODEC(ZSTD(1)),
  SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  Duration UInt64 CODEC(ZSTD(1)),
  StatusCode LowCardinality(String) CODEC(ZSTD(1)),
  StatusMessage String CODEC(ZSTD(1)),
  Events Nested (
    Timestamp DateTime64(9),
    Name LowCardinality(String),
    Attributes Map(LowCardinality(String), String)
  ) CODEC(ZSTD(1)),
  Links Nested (
    TraceId String,
    SpanId String,
    TraceState String,
    Attributes Map(LowCardinality(String), String)
  ) CODEC(ZSTD(1)),
  INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`

const insertHeader = (qualified: string): string =>
  `INSERT INTO ${qualified} FORMAT JSONEachRow`

const pad = (value: number, length: number): string => String(value).padStart(length, "0")

const nanosToDateTime64 = (value: bigint): string => {
  const millis = value / 1_000_000n
  const nanos = value % 1_000_000_000n
  const date = new Date(Number(millis))
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1, 2)}-${pad(date.getUTCDate(), 2)} `
    + `${pad(date.getUTCHours(), 2)}:${pad(date.getUTCMinutes(), 2)}:${pad(date.getUTCSeconds(), 2)}.${pad(Number(nanos), 9)}`
}

// ── exporter ─────────────────────────────────────────────────────────────────

export class ChdbSpanExporter implements SpanExporter {
  private readonly session: Session
  private readonly insertHeader: string

  constructor(options: ChdbSpanExporterOptions) {
    this.session = options.session
    const table = options.table ?? "otel_traces"
    const qualified = options.database ? `${options.database}.${table}` : table
    if (options.database) {
      this.session.query(`CREATE DATABASE IF NOT EXISTS ${options.database}`)
    }
    this.session.query(createTableSql(qualified))
    this.insertHeader = insertHeader(qualified)
  }

  export(spans: ReadableSpan[], resultCallback: (result: ExportResult) => void): void {
    if (spans.length === 0) {
      resultCallback({ code: ExportResultCode.SUCCESS })
      return
    }
    try {
      const ndjson = spans.map((span) => JSON.stringify(this.toRow(span))).join("\n")
      this.session.query(`${this.insertHeader}\n${ndjson}`) // synchronous
      resultCallback({ code: ExportResultCode.SUCCESS })
    } catch (e) {
      resultCallback({ code: ExportResultCode.FAILED, error: e instanceof Error ? e : new Error(String(e)) })
    }
  }

  async shutdown(): Promise<void> {}

  async forceFlush(): Promise<void> {}

  /** ReadableSpan -> JSONEachRow row matching the contrib column layout. */
  private toRow(span: ReadableSpan): Record<string, unknown> {
    const ctx = span.spanContext()
    const scope = scopeOf(span)
    const events = span.events ?? []
    const links = span.links ?? []
    return {
      Timestamp: nanosToDateTime64(hrNanos(span.startTime)),
      TraceId: ctx.traceId,
      SpanId: ctx.spanId,
      ParentSpanId: parentIdOf(span),
      TraceState: ctx.traceState?.serialize() ?? "",
      SpanName: span.name,
      SpanKind: SPAN_KIND[span.kind] ?? "Internal",
      ServiceName: attrValueToString(span.resource?.attributes?.["service.name"]) || "unknown_service",
      ResourceAttributes: attrsToObject(span.resource?.attributes),
      ScopeName: scope.name,
      ScopeVersion: scope.version,
      SpanAttributes: attrsToObject(span.attributes),
      // Duration is nanoseconds (UInt64); a span delta fits in a JS number safely.
      Duration: Number(hrNanos(span.endTime) - hrNanos(span.startTime)),
      StatusCode: STATUS_CODE[span.status.code] ?? "Unset",
      StatusMessage: span.status.message ?? "",
      "Events.Timestamp": events.map((e) => nanosToDateTime64(hrNanos(e.time))),
      "Events.Name": events.map((e) => e.name),
      "Events.Attributes": events.map((e) => attrsToObject(e.attributes)),
      "Links.TraceId": links.map((l) => l.context.traceId),
      "Links.SpanId": links.map((l) => l.context.spanId),
      "Links.TraceState": links.map((l) => l.context.traceState?.serialize() ?? ""),
      "Links.Attributes": links.map((l) => attrsToObject(l.attributes)),
    }
  }
}
