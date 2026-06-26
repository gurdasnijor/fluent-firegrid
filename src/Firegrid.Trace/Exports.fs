module Firegrid.Trace.Exports

open Effect
open Firegrid.Trace

// ── Ch: typed SQL-literal builders (pure) ────────────────────────────────────

let escapeString (s: string) : string = Ch.escapeString s
let defaultLiteral (value: obj) : string = Ch.defaultLiteral value

// ── error classification ─────────────────────────────────────────────────────

let codeFromMessage (cause: obj) : int option = Ch.codeFromMessage cause
let classifyError = Ch.classifyError

// ── exporter helpers (the high-value surface) ────────────────────────────────

let spanToChdbRow (span: obj) : ChdbExporter.ChdbSpanRow = ChdbExporter.spanToChdbRow span

let createTableSql (qualified: string) : string = ChdbExporter.createTableSql qualified

let insertHeader (qualified: string) : string = ChdbExporter.insertHeader qualified

let qualifiedTable (options: ChdbExporter.ChdbSpanExporterOptions) : string =
    ChdbExporter.qualifiedTable options

let nanosToDateTime64 (value: obj) : string = ChdbExporter.nanosToDateTime64 value

let ensureOtelTracesTable
    (session: TraceSdk.RawSession)
    (options: ChdbExporter.ChdbSpanExporterOptions)
    : string =
    ChdbExporter.ensureOtelTracesTable session options

let insertChdbSpanRows
    (session: TraceSdk.RawSession)
    (rows: ChdbExporter.ChdbSpanRow list)
    (options: ChdbExporter.ChdbSpanExporterOptions)
    : unit =
    ChdbExporter.insertChdbSpanRows session rows options

/// `new ChdbSpanExporter({ session, ...options })`.
let makeChdbSpanExporter
    (session: TraceSdk.RawSession)
    (options: ChdbExporter.ChdbSpanExporterOptions)
    : ChdbExporter.ChdbSpanExporter =
    ChdbExporter.makeChdbSpanExporter session options

/// `new RemoteChdbSpanExporter(endpoint)`.
let makeRemoteChdbSpanExporter (endpoint: string) : ChdbExporter.RemoteChdbSpanExporter =
    ChdbExporter.RemoteChdbSpanExporter endpoint

// ── reduced ChdbClient surface ───────────────────────────────────────────────

let openSession (options: ChdbClient.ChdbClientConfig) : Effect<TraceSdk.RawSession, Ch.SqlError, unit> =
    ChdbClient.openSession options

let bootstrapSession
    (session: TraceSdk.RawSession)
    (options: ChdbClient.ChdbClientConfig)
    : Effect<unit, Ch.SqlError, unit> =
    ChdbClient.bootstrapSession session options

let closeSession (session: TraceSdk.RawSession) : Effect<unit, Ch.SqlError, unit> =
    ChdbClient.closeSession session

let makeChdbClient (options: ChdbClient.ChdbClientConfig) : Effect<ChdbClient.ChdbClient, Ch.SqlError, unit> =
    ChdbClient.make options

let makeChdbClientWithSession
    (session: TraceSdk.RawSession)
    (options: ChdbClient.ChdbClientConfig)
    : Effect<ChdbClient.ChdbClient, Ch.SqlError, unit> =
    ChdbClient.makeWithSession session options

let param (ch: Ch<'A>) (value: 'A) : string = ChdbClient.param ch value

let insertQuery
    (session: TraceSdk.RawSession)
    (options: ChdbClient.ChdbInsertOptions)
    : Effect<int, Ch.SqlError, unit> =
    ChdbClient.insertQuery session options
