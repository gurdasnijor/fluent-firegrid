import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import {
  ConsoleSpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
  type SpanExporter,
  type SpanProcessor,
} from "@opentelemetry/sdk-trace-base"
import { Layer } from "effect"
// The OTel file span-exporter writes trace.jsonl through a node WriteStream
// (`createWriteStream`) — @effect/platform FileSystem has no OTel-exporter sink
// equivalent, so this is a genuine node-observability boundary. Documented
// escape-hatch from the local/no-raw-node-io ban (tf-636o); revisit if the
// exporter moves onto a platform Sink.
// eslint-disable-next-line local/no-raw-node-io
import {
  accessSync,
  constants as fsConstants,
  createWriteStream,
  existsSync,
  mkdirSync,
  type WriteStream,
} from "node:fs"
// eslint-disable-next-line local/no-raw-node-io
import path from "node:path"

export type {
  ReadableSpan,
  SpanProcessor,
} from "@opentelemetry/sdk-trace-base"

export type FiregridOtelAttributeValue =
  | string
  | number
  | boolean
  | Array<string>
  | Array<number>
  | Array<boolean>

export interface FiregridOtelResource {
  readonly serviceName: string
  readonly serviceVersion?: string
  readonly attributes?: Record<string, FiregridOtelAttributeValue>
}

export type FiregridOtelDestination =
  | { readonly _tag: "file"; readonly filePath: string }
  | { readonly _tag: "console" }

export interface FiregridOtelLayerOptions {
  readonly resource: FiregridOtelResource
  readonly destination: FiregridOtelDestination
  readonly spanProcessors?: ReadonlyArray<SpanProcessor>
}

export const FIREGRID_OTEL_OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"

const nonEmpty = (value: string | undefined): string | undefined =>
  value === undefined || value.length === 0 ? undefined : value

export const resolveFiregridOtelFileDestination = (
  options: {
    readonly filePath?: string
    readonly env?: NodeJS.ProcessEnv
    readonly envName?: string
    // tf-r1gz: base directory a RELATIVE filePath resolves against. Callers
    // pass the operator-supplied --cwd (the project root), so the trace lands
    // in the repo rather than wherever the runner process was launched (e.g.
    // Zed's cwd). When omitted, the raw path is returned unchanged. Keeping
    // the resolution here (a pure function of its inputs) instead of reading
    // process.cwd() inline makes it unit-testable.
    readonly baseDir?: string
  },
): FiregridOtelDestination | undefined => {
  // firegrid-observability.FILE_EXPORTERS.3
  const envName = options.envName ?? "FIREGRID_OTEL_FILE"
  const filePath = nonEmpty(options.filePath) ?? nonEmpty(options.env?.[envName])
  if (filePath === undefined) return undefined
  const baseDir = nonEmpty(options.baseDir)
  return {
    _tag: "file",
    filePath: baseDir === undefined ? filePath : path.resolve(baseDir, filePath),
  }
}

// tf-3718: typed result of a startup writability check for a file destination,
// so callers can fail loud with a clear error instead of letting
// `JsonlFileSpanExporter`'s constructor throw an opaque mkdir/createWriteStream
// defect when the OTel layer is built.
export type FiregridOtelFileWritability =
  | { readonly _tag: "writable" }
  | { readonly _tag: "unwritable"; readonly reason: string }

// firegrid-observability.FILE_EXPORTERS.3
// Mirrors what `JsonlFileSpanExporter`'s constructor does (mkdir the parent dir
// recursively, then open the file for append) as an up-front, idempotent check.
// Validates the parent directory is creatable + writable and, when the file
// already exists, that it is writable. Returns a typed result rather than
// throwing so the CLI can surface a typed usage error.
export const checkFiregridOtelFileWritable = (
  filePath: string,
): FiregridOtelFileWritability => {
  try {
    const resolved = path.resolve(filePath)
    const dir = path.dirname(resolved)
    mkdirSync(dir, { recursive: true })
    accessSync(dir, fsConstants.W_OK)
    if (existsSync(resolved)) {
      accessSync(resolved, fsConstants.W_OK)
    }
    return { _tag: "writable" }
  } catch (cause) {
    return {
      _tag: "unwritable",
      reason: cause instanceof Error ? cause.message : String(cause),
    }
  }
}

// tf-r1gz: the exporter that will actually run. This mirrors the precedence in
// `FiregridOtelLive` (OTLP wins over the file/console destination when
// OTEL_EXPORTER_OTLP_ENDPOINT is set) and the fact that the OTel layer is only
// installed once a destination is resolved. Callers use it to announce what
// will actually happen — a file announcement must NOT print when spans really
// go to OTLP, which would recreate the "trace file never appears" confusion.
export type FiregridOtelActiveExporter =
  | { readonly _tag: "otlp"; readonly endpoint: string }
  | { readonly _tag: "file"; readonly filePath: string }
  | { readonly _tag: "console" }
  | { readonly _tag: "none" }

export const resolveFiregridOtelActiveExporter = (
  options: {
    readonly destination: FiregridOtelDestination | undefined
    readonly env?: NodeJS.ProcessEnv
  },
): FiregridOtelActiveExporter => {
  if (options.destination === undefined) return { _tag: "none" }
  const endpoint = nonEmpty(options.env?.[FIREGRID_OTEL_OTLP_ENDPOINT_ENV])
  if (endpoint !== undefined) return { _tag: "otlp", endpoint }
  return options.destination
}

// tf-9ia9: every JSONL record carries a `phase` so a reader can tell an
// in-flight span START from a completed span END. The format stays mechanically
// parseable (one JSON object per line, self-describing via `phase`), and END
// records keep every field they had before — `phase` is purely additive, so
// existing end-span consumers that ignore unknown fields are unaffected.
export type FiregridOtelSpanPhase = "start" | "end"

const baseSpanFields = (span: ReadableSpan) => {
  const context = span.spanContext()
  return {
    name: span.name,
    traceId: context.traceId,
    spanId: context.spanId,
    parentSpanId: span.parentSpanContext?.spanId,
    kind: span.kind,
    startTime: span.startTime,
    attributes: span.attributes,
    links: span.links,
    resource: span.resource.attributes,
  }
}

export const spanToJsonLine = (span: ReadableSpan): string =>
  JSON.stringify({
    phase: "end" satisfies FiregridOtelSpanPhase,
    ...baseSpanFields(span),
    endTime: span.endTime,
    duration: span.duration,
    status: span.status,
    events: span.events,
  }) + "\n"

// tf-9ia9: a span-START record. endTime/duration/status are intentionally
// omitted — they are unset while the span is in flight, which is exactly the
// state this record exists to surface (e.g. an ACP `new_session` that started
// but has not ended). `attributes` carries the creation-time attributes the
// span was opened with (e.g. codec.sdk.call mcp_server_count); see
// JsonlFileStartSpanProcessor for why the write is deferred one microtask.
export const spanStartToJsonLine = (span: ReadableSpan): string =>
  JSON.stringify({
    phase: "start" satisfies FiregridOtelSpanPhase,
    ...baseSpanFields(span),
  }) + "\n"

export class JsonlFileSpanExporter implements SpanExporter {
  private readonly stream: WriteStream
  private closed = false

  constructor(filePath: string) {
    const resolvedPath = path.resolve(filePath)
    mkdirSync(path.dirname(resolvedPath), { recursive: true })
    this.stream = createWriteStream(resolvedPath, { flags: "a" })
  }

  export(
    spans: ReadableSpan[],
    resultCallback: (result: { code: number; error?: Error }) => void,
  ): void {
    if (this.closed) {
      resultCallback({ code: 1, error: new Error("exporter closed") })
      return
    }
    try {
      spans.forEach(span => this.stream.write(spanToJsonLine(span)))
      resultCallback({ code: 0 })
    } catch (cause) {
      resultCallback({ code: 1, error: cause as Error })
    }
  }

  // tf-9ia9: write a span-START record to the same file stream as the END
  // records (export()). Sharing one stream keeps the two phases interleaved in a
  // single mechanically-parseable artifact and avoids two writers racing on the
  // same path. Best-effort: a failed start write must never break tracing.
  writeStartRecord(span: ReadableSpan): void {
    if (this.closed) return
    try {
      this.stream.write(spanStartToJsonLine(span))
    } catch {
      // best-effort: start records are a debugging aid, not a correctness path.
    }
  }

  shutdown(): Promise<void> {
    if (this.closed) return Promise.resolve()
    this.closed = true
    return new Promise(resolve => this.stream.end(() => resolve()))
  }

  forceFlush(): Promise<void> {
    return Promise.resolve()
  }
}

export type FiregridOtelFlushMode = "immediate" | "batched"

// firegrid-observability.FILE_EXPORTERS.3
// tf-r1gz: the file flush strategy is a real tradeoff, so it is a knob with a
// safe default rather than a hardcode. `immediate` (SimpleSpanProcessor, the
// default — matching the console destination) writes each ended span as it
// completes, so a long-running ACP process populates the JSONL artifact
// continuously and an abrupt editor disconnect cannot discard a pending batch.
// `batched` (BatchSpanProcessor) restores 5s/512-span batching for high-span-
// rate non-interactive runs that prefer throughput over per-span latency.
export type FiregridOtelFilePhases = "end" | "start-end"

// tf-9ia9: emits a span-START record on span start through the shared file
// exporter. END records keep flowing through the (Simple|Batch) processor that
// wraps the SAME exporter, so there is one stream and one shutdown owner; this
// processor's lifecycle hooks are no-ops.
//
// The write is deferred one microtask because @effect/opentelemetry applies
// `Effect.withSpan({ attributes })` via setAttribute AFTER tracer.startSpan
// (which is what fires onStart) returns — so synchronously here the span has no
// attributes yet. A microtask runs after that synchronous attribute
// application but before the span body advances, capturing the creation-time
// attributes (e.g. codec.sdk.call mcp_server_count) without picking up later
// annotateCurrentSpan calls. Ordering vs the END record is therefore not
// guaranteed for spans that open and close within one tick; readers key on
// spanId + phase, not line order.
export class JsonlFileStartSpanProcessor implements SpanProcessor {
  constructor(private readonly exporter: JsonlFileSpanExporter) {}

  onStart(span: ReadableSpan): void {
    queueMicrotask(() => this.exporter.writeStartRecord(span))
  }

  onEnd(): void {}

  forceFlush(): Promise<void> {
    return Promise.resolve()
  }

  shutdown(): Promise<void> {
    return Promise.resolve()
  }
}

const fileSpanProcessors = (
  filePath: string,
  phases: FiregridOtelFilePhases,
): ReadonlyArray<SpanProcessor> => {
  const exporter = new JsonlFileSpanExporter(filePath)
  return phases === "start-end"
    ? [new JsonlFileStartSpanProcessor(exporter), new SimpleSpanProcessor(exporter)]
    : [new SimpleSpanProcessor(exporter)]
}

const fileTelemetryLive = (
  options: FiregridOtelLayerOptions & {
    readonly destination: { readonly _tag: "file"; readonly filePath: string }
    readonly flushMode: FiregridOtelFlushMode
    readonly phases: FiregridOtelFilePhases
  },
) =>
  NodeSdk.layer(() => ({
    resource: options.resource,
    spanProcessor: [
      ...fileSpanProcessors(options.destination.filePath, options.phases),
      ...(options.spanProcessors ?? []),
    ],
  }))

const consoleTelemetryLive = (
  options: FiregridOtelLayerOptions & {
    readonly destination: { readonly _tag: "console" }
  },
) =>
  NodeSdk.layer(() => ({
    resource: options.resource,
    spanProcessor: [
      new SimpleSpanProcessor(new ConsoleSpanExporter()),
      ...(options.spanProcessors ?? []),
    ],
  }))

export const FiregridOtelLive = (
  options: FiregridOtelLayerOptions,
): Layer.Layer<never, unknown> =>
  // firegrid-observability.FILE_EXPORTERS.1
  // firegrid-observability.FILE_EXPORTERS.2
  options.destination._tag === "console"
    ? consoleTelemetryLive({
      ...options,
      destination: options.destination,
    })
    : fileTelemetryLive({
      ...options,
      destination: options.destination,
      flushMode: "immediate",
      phases: "start-end",
    })
