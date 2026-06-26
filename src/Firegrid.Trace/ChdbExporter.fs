namespace Firegrid.Trace

open Fable.Core
open Fable.Core.JsInterop

/// OTel `SpanExporter` that writes to chDB (embedded ClickHouse) using the
/// contrib clickhouseexporter `otel_traces` schema, VERBATIM (Map variant).
///
/// Faithful port of `ChdbExporter.ts`. OTel value types (ReadableSpan, HrTime,
/// Attributes, SpanContext) are external JS objects accessed structurally via
/// `TraceSdk` accessors; epoch-nanosecond math is done over BigInt (carried as
/// `obj`) to avoid Number precision loss on absolute ns.
[<RequireQualifiedAccess>]
module ChdbExporter =

    /// A row as a JS object literal ready for `JSON.stringify` -> JSONEachRow.
    /// (`ChdbSpanRow = Record<string, unknown>` in TS.)
    type ChdbSpanRow = obj

    /// Options for the chDB span exporter / table helpers.
    type ChdbSpanExporterOptions =
        { /// Defaults to "otel_traces".
          Table: string option
          /// Optional database; created with CREATE DATABASE IF NOT EXISTS when set.
          Database: string option }

        static member Empty = { Table = None; Database = None }

    // ── pdata-compatible string mappings (SpanKind.String()/StatusCode.String()) ──
    // OTel numeric enum values: SpanKind INTERNAL=0 SERVER=1 CLIENT=2 PRODUCER=3
    // CONSUMER=4; SpanStatusCode UNSET=0 OK=1 ERROR=2.

    let private SPAN_KIND: Map<int, string> =
        Map.ofList [ 0, "Internal"; 1, "Server"; 2, "Client"; 3, "Producer"; 4, "Consumer" ]

    let private STATUS_CODE: Map<int, string> =
        Map.ofList [ 0, "Unset"; 1, "Ok"; 2, "Error" ]

    // ── value coercion ─────────────────────────────────────────────────────────

    /// epoch nanoseconds as bigint from an HrTime `[seconds, nanos]` tuple
    /// (`BigInt(seconds) * BigInt(1e9) + BigInt(nanos)`). Returns the BigInt as obj.
    let hrNanos (hrTime: obj) : obj =
        let seconds = TraceSdk.prop<obj> hrTime "0"
        let nanos = TraceSdk.prop<obj> hrTime "1"
        TraceSdk.hrNanosBig seconds nanos

    /// Map(String, String) requires string values; stringify everything else.
    /// `undefined|null -> "" ; Array -> JSON.stringify ; string -> v ; else String(v)`.
    let attrValueToString (v: obj) : string =
        if TraceSdk.isNullish v then ""
        elif TraceSdk.isArray v then TraceSdk.stringify v
        elif TraceSdk.typeOf v = "string" then unbox<string> v
        else TraceSdk.stringValue v

    /// `Object.fromEntries(Object.entries(attrs).map(([k,v]) => [k, str(v)]))`.
    /// Returns `{}` for `undefined`. Result is a plain JS object.
    let attrsToObject (attrs: obj) : obj =
        if TraceSdk.isNullish attrs then
            createObj []
        else
            TraceSdk.objectEntries attrs
            |> Array.map (fun entry ->
                let key = TraceSdk.prop<obj> entry "0"
                let value = TraceSdk.prop<obj> entry "1"
                TraceSdk.pair key (box (attrValueToString value)))
            |> TraceSdk.objectFromEntries

    let private stringOrEmpty (value: obj) : string =
        if TraceSdk.typeOf value = "string" then unbox<string> value else ""

    /// version-drift tolerant scope accessor: `span.instrumentationScope ??
    /// span.instrumentationLibrary`, reading name/version as strings.
    let scopeOf (span: obj) : {| Name: string; Version: string |} =
        let scope =
            let s = TraceSdk.prop<obj> span "instrumentationScope"
            if TraceSdk.isNullish s then TraceSdk.prop<obj> span "instrumentationLibrary" else s

        let name = if TraceSdk.isNullish scope then box "" else TraceSdk.prop<obj> scope "name"
        let version = if TraceSdk.isNullish scope then box "" else TraceSdk.prop<obj> scope "version"
        {| Name = stringOrEmpty name; Version = stringOrEmpty version |}

    /// `span.parentSpanContext?.spanId ?? stringOrEmpty(span.parentSpanId)`.
    let parentIdOf (span: obj) : string =
        let parentCtx = TraceSdk.prop<obj> span "parentSpanContext"

        if not (TraceSdk.isNullish parentCtx) then
            let spanId = TraceSdk.prop<obj> parentCtx "spanId"
            if not (TraceSdk.isNullish spanId) then unbox<string> spanId
            else stringOrEmpty (TraceSdk.prop<obj> span "parentSpanId")
        else
            stringOrEmpty (TraceSdk.prop<obj> span "parentSpanId")

    // ── DateTime64(9) rendering ─────────────────────────────────────────────────

    let private pad (value: obj) (length: int) : string = TraceSdk.padStart value length "0"

    /// bigint epoch-nanos -> "YYYY-MM-DD HH:MM:SS.nnnnnnnnn" (UTC).
    let nanosToDateTime64 (value: obj) : string =
        let millis = TraceSdk.bigDivMillis value
        let nanos = TraceSdk.bigModNanos value
        let date = TraceSdk.dateFromMillis (TraceSdk.bigToMillis millis)

        sprintf
            "%d-%s-%s %s:%s:%s.%s"
            (TraceSdk.getUTCFullYear date)
            (pad (box (TraceSdk.getUTCMonth date + 1)) 2)
            (pad (box (TraceSdk.getUTCDate date)) 2)
            (pad (box (TraceSdk.getUTCHours date)) 2)
            (pad (box (TraceSdk.getUTCMinutes date)) 2)
            (pad (box (TraceSdk.getUTCSeconds date)) 2)
            (pad (box (TraceSdk.bigToNumber nanos)) 9)

    // ── ReadableSpan -> JSONEachRow row ─────────────────────────────────────────

    let private mapArray (value: obj) (f: obj -> 'b) : 'b[] =
        if TraceSdk.isNullish value then [||]
        else unbox<obj[]> value |> Array.map f

    /// ReadableSpan -> row matching the contrib column layout.
    let spanToChdbRow (span: obj) : ChdbSpanRow =
        let ctx: obj = TraceSdk.spanContext span
        let scope = scopeOf span
        let events = TraceSdk.prop<obj> span "events"
        let links = TraceSdk.prop<obj> span "links"

        let startNanos = hrNanos (TraceSdk.prop<obj> span "startTime")
        let endNanos = hrNanos (TraceSdk.prop<obj> span "endTime")

        let traceState =
            let ts = TraceSdk.prop<obj> ctx "traceState"
            if TraceSdk.isNullish ts then "" else TraceSdk.serialize ts

        let resourceAttrs =
            let res = TraceSdk.prop<obj> span "resource"
            if TraceSdk.isNullish res then TraceSdk.undefinedObj else TraceSdk.prop<obj> res "attributes"

        let serviceName =
            let res = TraceSdk.prop<obj> span "resource"

            let sv =
                if TraceSdk.isNullish res then ""
                else
                    let attrs = TraceSdk.prop<obj> res "attributes"
                    if TraceSdk.isNullish attrs then "" else attrValueToString (TraceSdk.prop<obj> attrs "service.name")

            if sv = "" then "unknown_service" else sv

        let spanKind =
            match SPAN_KIND.TryFind(int (TraceSdk.numberValue (TraceSdk.prop<obj> span "kind"))) with
            | Some k -> k
            | None -> "Internal"

        let statusCode =
            let status = TraceSdk.prop<obj> span "status"
            match STATUS_CODE.TryFind(int (TraceSdk.numberValue (TraceSdk.prop<obj> status "code"))) with
            | Some s -> s
            | None -> "Unset"

        let statusMessage =
            let status = TraceSdk.prop<obj> span "status"
            let m = TraceSdk.prop<obj> status "message"
            if TraceSdk.isNullish m then "" else unbox<string> m

        createObj
            [ "Timestamp" ==> nanosToDateTime64 startNanos
              "TraceId" ==> TraceSdk.prop<string> ctx "traceId"
              "SpanId" ==> TraceSdk.prop<string> ctx "spanId"
              "ParentSpanId" ==> parentIdOf span
              "TraceState" ==> traceState
              "SpanName" ==> TraceSdk.prop<string> span "name"
              "SpanKind" ==> spanKind
              "ServiceName" ==> serviceName
              "ResourceAttributes" ==> attrsToObject resourceAttrs
              "ScopeName" ==> scope.Name
              "ScopeVersion" ==> scope.Version
              "SpanAttributes" ==> attrsToObject (TraceSdk.prop<obj> span "attributes")
              // Duration is nanoseconds (UInt64); a span delta fits a JS number.
              "Duration" ==> TraceSdk.bigToNumber (TraceSdk.bigSub endNanos startNanos)
              "StatusCode" ==> statusCode
              "StatusMessage" ==> statusMessage
              "Events.Timestamp" ==> (mapArray events (fun e -> nanosToDateTime64 (hrNanos (TraceSdk.prop<obj> e "time"))))
              "Events.Name" ==> (mapArray events (fun e -> TraceSdk.prop<string> e "name"))
              "Events.Attributes" ==> (mapArray events (fun e -> attrsToObject (TraceSdk.prop<obj> e "attributes")))
              "Links.TraceId" ==> (mapArray links (fun l -> TraceSdk.prop<string> (TraceSdk.prop<obj> l "context") "traceId"))
              "Links.SpanId" ==> (mapArray links (fun l -> TraceSdk.prop<string> (TraceSdk.prop<obj> l "context") "spanId"))
              "Links.TraceState"
              ==> (mapArray links (fun l ->
                  let ts = TraceSdk.prop<obj> (TraceSdk.prop<obj> l "context") "traceState"
                  if TraceSdk.isNullish ts then "" else TraceSdk.serialize ts))
              "Links.Attributes" ==> (mapArray links (fun l -> attrsToObject (TraceSdk.prop<obj> l "attributes"))) ]

    // ── SQL ─────────────────────────────────────────────────────────────────────

    let createTableSql (qualified: string) : string =
        "CREATE TABLE IF NOT EXISTS " + qualified + " (\n"
        + "  Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),\n"
        + "  TraceId String CODEC(ZSTD(1)),\n"
        + "  SpanId String CODEC(ZSTD(1)),\n"
        + "  ParentSpanId String CODEC(ZSTD(1)),\n"
        + "  TraceState String CODEC(ZSTD(1)),\n"
        + "  SpanName LowCardinality(String) CODEC(ZSTD(1)),\n"
        + "  SpanKind LowCardinality(String) CODEC(ZSTD(1)),\n"
        + "  ServiceName LowCardinality(String) CODEC(ZSTD(1)),\n"
        + "  ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),\n"
        + "  ScopeName String CODEC(ZSTD(1)),\n"
        + "  ScopeVersion String CODEC(ZSTD(1)),\n"
        + "  SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),\n"
        + "  Duration UInt64 CODEC(ZSTD(1)),\n"
        + "  StatusCode LowCardinality(String) CODEC(ZSTD(1)),\n"
        + "  StatusMessage String CODEC(ZSTD(1)),\n"
        + "  Events Nested (\n"
        + "    Timestamp DateTime64(9),\n"
        + "    Name LowCardinality(String),\n"
        + "    Attributes Map(LowCardinality(String), String)\n"
        + "  ) CODEC(ZSTD(1)),\n"
        + "  Links Nested (\n"
        + "    TraceId String,\n"
        + "    SpanId String,\n"
        + "    TraceState String,\n"
        + "    Attributes Map(LowCardinality(String), String)\n"
        + "  ) CODEC(ZSTD(1)),\n"
        + "  INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,\n"
        + "  INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,\n"
        + "  INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,\n"
        + "  INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,\n"
        + "  INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,\n"
        + "  INDEX idx_duration Duration TYPE minmax GRANULARITY 1\n"
        + ") ENGINE = MergeTree\n"
        + "PARTITION BY toDate(Timestamp)\n"
        + "ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))\n"
        + "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"

    let insertHeader (qualified: string) : string =
        "INSERT INTO " + qualified + " FORMAT JSONEachRow"

    /// `database === undefined ? table ?? "otel_traces" : `${database}.${table ?? "otel_traces"}``
    let qualifiedTable (options: ChdbSpanExporterOptions) : string =
        match options.Database with
        | None -> options.Table |> Option.defaultValue "otel_traces"
        | Some db -> db + "." + (options.Table |> Option.defaultValue "otel_traces")

    /// CREATE DATABASE (if set) + CREATE TABLE; returns the qualified name.
    let ensureOtelTracesTable (session: TraceSdk.RawSession) (options: ChdbSpanExporterOptions) : string =
        let qualified = qualifiedTable options

        match options.Database with
        | Some db -> TraceSdk.query session ("CREATE DATABASE IF NOT EXISTS " + db) |> ignore
        | None -> ()

        TraceSdk.query session (createTableSql qualified) |> ignore
        qualified

    /// Insert rows as a single multi-row JSONEachRow INSERT. No-op for empty.
    let insertChdbSpanRows
        (session: TraceSdk.RawSession)
        (rows: ChdbSpanRow list)
        (options: ChdbSpanExporterOptions)
        : unit =
        if List.isEmpty rows then
            ()
        else
            let qualified = ensureOtelTracesTable session options
            let ndjson = rows |> List.map TraceSdk.stringify |> String.concat "\n"
            TraceSdk.query session (insertHeader qualified + "\n" + ndjson) |> ignore

    // ── ExportResult shape (mirrors @opentelemetry/core) ────────────────────────
    // ExportResultCode.SUCCESS = 0, FAILED = 1. The result callback receives a
    // plain object `{ code, error? }`.

    let private successResult: obj = createObj [ "code" ==> 0 ]

    let private failedResult (error: exn) : obj =
        createObj [ "code" ==> 1; "error" ==> error ]

    // ── exporters (OTel SpanExporter shape) ─────────────────────────────────────

    /// chDB-backed exporter. `export`/`shutdown`/`forceFlush` match the OTel
    /// `SpanExporter` interface. Bootstraps the table once in the constructor.
    type ChdbSpanExporter(session: TraceSdk.RawSession, opts: ChdbSpanExporterOptions) =
        let insertHeaderStr = insertHeader (ensureOtelTracesTable session opts)

        /// `export(spans, resultCallback)`.
        member _.export (spans: obj[]) (resultCallback: obj -> unit) : unit =
            if spans.Length = 0 then
                resultCallback successResult
            else
                try
                    let ndjson =
                        spans
                        |> Array.map (fun span -> TraceSdk.stringify (spanToChdbRow span))
                        |> String.concat "\n"

                    TraceSdk.query session (insertHeaderStr + "\n" + ndjson) |> ignore // synchronous
                    resultCallback successResult
                with e ->
                    resultCallback (failedResult (TraceSdk.toError (box e)))

        member _.shutdown() : JS.Promise<unit> = TraceSdk.resolvedUnit ()

        member _.forceFlush() : JS.Promise<unit> = TraceSdk.resolvedUnit ()

    /// Constructor convenience matching the TS `new ChdbSpanExporter(options)`
    /// single-object signature.
    let makeChdbSpanExporter (session: TraceSdk.RawSession) (options: ChdbSpanExporterOptions) : ChdbSpanExporter =
        ChdbSpanExporter(session, options)

    /// HTTP exporter that POSTs JSON-rows to a remote chDB-ingest endpoint.
    type RemoteChdbSpanExporter(endpoint: string) =

        member _.export (spans: obj[]) (resultCallback: obj -> unit) : unit =
            let body = TraceSdk.stringify (spans |> Array.map spanToChdbRow)

            let init: obj =
                createObj
                    [ "body" ==> body
                      "headers" ==> createObj [ "content-type" ==> "application/json" ]
                      "method" ==> "POST" ]

            let onFulfilled (response: obj) : obj =
                let ok = TraceSdk.prop<bool> response "ok"

                if ok then
                    resultCallback successResult
                else
                    let status = TraceSdk.prop<obj> response "status"
                    let error: exn = TraceSdk.toError (box ("remote span export failed with " + TraceSdk.stringValue status))
                    resultCallback (failedResult error)

                TraceSdk.undefinedObj

            let onRejected (cause: obj) : obj =
                resultCallback (failedResult (TraceSdk.toError (box cause)))
                TraceSdk.undefinedObj

            TraceSdk.promiseCatch (TraceSdk.promiseThen (TraceSdk.fetch endpoint init) onFulfilled) onRejected
            |> ignore

        member _.shutdown() : JS.Promise<unit> = TraceSdk.resolvedUnit ()

        member _.forceFlush() : JS.Promise<unit> = TraceSdk.resolvedUnit ()
