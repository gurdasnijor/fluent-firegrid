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
 *  - DateTime64(9) is fed as integer epoch-nanoseconds through `input()` +
 *    `fromUnixTimestamp64Nano`, NOT as a JSON datetime string. This is lossless
 *    and timezone-independent; JSONEachRow's own DateTime64 string parsing is
 *    neither under default settings.
 *
 * chDB `session.query` is synchronous and blocking — fine at oracle/test scale.
 * BatchSpanProcessor coalesces spans so each `export` is one multi-row INSERT.
 */
import { type Attributes, type AttributeValue, type HrTime, SpanKind, SpanStatusCode } from "@opentelemetry/api"
import { type ExportResult, ExportResultCode } from "@opentelemetry/core"
import type { ReadableSpan, SpanExporter } from "@opentelemetry/sdk-trace-base"

/** Minimal structural type for the chdb-node Session (`new Session(path)`). */
export interface ChdbSession {
  query(sql: string, format?: string): unknown
}

export interface ChdbSpanExporterOptions {
  readonly session: ChdbSession
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
  [SpanKind.CONSUMER]: "Consumer"
}

const STATUS_CODE: Record<number, string> = {
  [SpanStatusCode.UNSET]: "Unset",
  [SpanStatusCode.OK]: "Ok",
  [SpanStatusCode.ERROR]: "Error"
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
  const out: Record<string, string> = {}
  if (attrs) {
    for (const k of Object.keys(attrs)) out[k] = attrValueToString(attrs[k])
  }
  return out
}

// version-drift tolerant accessors (SDK renamed these across releases)
const scopeOf = (span: ReadableSpan): { name: string; version: string } => {
  const s = (span as any).instrumentationScope ?? (span as any).instrumentationLibrary ?? {}
  return { name: s.name ?? "", version: s.version ?? "" }
}
const parentIdOf = (span: ReadableSpan): string =>
  (span as any).parentSpanContext?.spanId ?? (span as any).parentSpanId ?? ""

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

/**
 * INSERT ... SELECT ... FROM input(...) FORMAT JSONEachRow
 * Rows (NDJSON) are appended after this header. `input()` receives Timestamps as
 * Int64 nanos and they're converted to DateTime64(9) in the SELECT; Map and
 * Array(Map) columns are read directly from JSON objects/arrays.
 */
const insertHeader = (qualified: string): string =>
  [
    `INSERT INTO ${qualified} (`,
    "  Timestamp, TraceId, SpanId, ParentSpanId, TraceState,",
    "  SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes,",
    "  Duration, StatusCode, StatusMessage,",
    "  `Events.Timestamp`, `Events.Name`, `Events.Attributes`,",
    "  `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`",
    ")",
    "SELECT",
    "  fromUnixTimestamp64Nano(Timestamp) AS Timestamp,",
    "  TraceId, SpanId, ParentSpanId, TraceState,",
    "  SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes,",
    "  Duration, StatusCode, StatusMessage,",
    "  arrayMap(x -> fromUnixTimestamp64Nano(x), `Events.Timestamp`) AS `Events.Timestamp`,",
    "  `Events.Name`, `Events.Attributes`,",
    "  `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`",
    "FROM input(",
    "  'Timestamp Int64, TraceId String, SpanId String, ParentSpanId String, TraceState String,",
    "   SpanName String, SpanKind String, ServiceName String,",
    "   ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, SpanAttributes Map(String, String),",
    "   Duration UInt64, StatusCode String, StatusMessage String,",
    "   `Events.Timestamp` Array(Int64), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)),",
    "   `Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), `Links.Attributes` Array(Map(String, String))'",
    ")",
    "FORMAT JSONEachRow"
  ].join("\n")

// ── exporter ─────────────────────────────────────────────────────────────────

export class ChdbSpanExporter implements SpanExporter {
  private readonly session: ChdbSession
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
      // Int64 epoch ns as a string (exceeds Number.MAX_SAFE_INTEGER); SELECT converts it.
      Timestamp: hrNanos(span.startTime).toString(),
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
      "Events.Timestamp": events.map((e) => hrNanos(e.time).toString()),
      "Events.Name": events.map((e) => e.name),
      "Events.Attributes": events.map((e) => attrsToObject(e.attributes)),
      "Links.TraceId": links.map((l) => l.context.traceId),
      "Links.SpanId": links.map((l) => l.context.spanId),
      "Links.TraceState": links.map((l) => l.context.traceState?.serialize() ?? ""),
      "Links.Attributes": links.map((l) => attrsToObject(l.attributes))
    }
  }
}