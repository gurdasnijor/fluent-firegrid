namespace Firegrid.Trace

open Effect
open Fable.Core
open Fable.Core.JsInterop

/// A pragmatic, REDUCED EffSharp chDB client.
///
/// `ChdbClient.ts` is built on `effect/unstable/sql` (SqlClient, the Statement
/// compiler, the Connection machinery) plus the `chdb` Node package. EffSharp has
/// no equivalent of effect/unstable/sql, so the Statement compiler / SqlClient
/// / Connection layer are intentionally NOT ported. What IS ported, faithfully:
///   - `ChdbClientConfig` and the session open/bootstrap lifecycle.
///   - The `ChdbNative` query surface (query / queryBind / queryAsync /
///     queryBindAsync / insert / queryStream) each returning an EffSharp
///     `Effect<_, SqlError, _>` and classifying thrown chDB errors via
///     `Ch.classifyError`.
///   - `param` / `insertQuery` helpers built on `Ch` and JSON.
/// `SqlError` is the reduced `Ch.SqlError` record (kind + cause + message +
/// operation), since effect/unstable/sql's `SqlError` is unavailable.
[<RequireQualifiedAccess>]
module ChdbClient =

    type SqlError = Ch.SqlError

    /// Session directory / database / sticky settings (faithful to the TS config,
    /// minus the SqlClient-only `transformResultNames`/`transformQueryNames` hooks
    /// which belonged to the omitted compiler layer; retained as inert options).
    type ChdbClientConfig =
        { /// Session directory. A temp dir is created and removed on close if omitted.
          Path: string option
          Database: string option
          /// Session-scoped settings applied via SET (sticky for the session).
          Settings: (string * obj) list
          SpanAttributes: (string * obj) list }

        static member Empty =
            { Path = None
              Database = None
              Settings = []
              SpanAttributes = [] }

    // ── try/classify bridge ──────────────────────────────────────────────────

    /// Run a synchronous chDB thunk; on throw classify the error and fail the
    /// Effect. Mirrors TS `Effect.try({ try, catch })`.
    let private trySync
        (thunk: unit -> 'a)
        (message: string)
        (operation: string)
        (fallback: Ch.SqlErrorKind)
        : Effect<'a, SqlError, 'R> =
        let outcome = TraceSdk.tryRun thunk

        if TraceSdk.prop<bool> outcome "ok" then
            Effect.succeed (TraceSdk.prop<'a> outcome "value")
        else
            let cause = TraceSdk.prop<obj> outcome "error"
            Effect.fail (Ch.classifyError cause message operation fallback)

    /// Wrap a chDB promise; on rejection classify and fail. Mirrors TS
    /// `Effect.tryPromise`.
    let private tryPromise
        (promise: unit -> JS.Promise<'a>)
        (message: string)
        (operation: string)
        : Effect<'a, SqlError, 'R> =
        Effect.tryPromiseJS promise (fun cause -> Ch.classifyError (box cause) message operation Ch.UnknownError)

    // ── settings rendering ───────────────────────────────────────────────────

    /// `string -> '...' ; boolean -> 1|0 ; number -> String(v)`.
    let private renderSetting (v: obj) : string =
        match TraceSdk.typeOf v with
        | "string" -> "'" + Ch.escapeString (unbox<string> v) + "'"
        | "boolean" -> if unbox<bool> v then "1" else "0"
        | _ -> TraceSdk.stringValue v

    // ── construction / lifecycle ─────────────────────────────────────────────

    /// `new Session()` / `new Session(path)` wrapped in an Effect. The TS uses
    /// `Effect.acquireRelease`; EffSharp here exposes the open + an explicit
    /// `closeSession` (call on scope close). cleanup() drops the temp dir.
    let openSession (options: ChdbClientConfig) : Effect<TraceSdk.RawSession, SqlError, 'R> =
        trySync
            (fun () ->
                match options.Path with
                | None -> TraceSdk.openSession ()
                | Some path -> TraceSdk.openSessionWithPath path)
            "ChdbClient: failed to open session"
            "connect"
            Ch.ConnectionError

    /// `session.cleanup()` — idempotent release.
    let closeSession (session: TraceSdk.RawSession) : Effect<unit, SqlError, 'R> =
        Effect.sync (fun () -> TraceSdk.cleanup session)

    /// CREATE DATABASE + USE (if set), apply session SET settings, then SELECT 1.
    let bootstrapSession (session: TraceSdk.RawSession) (options: ChdbClientConfig) : Effect<unit, SqlError, 'R> =
        trySync
            (fun () ->
                match options.Database with
                | Some db -> TraceSdk.query session ("CREATE DATABASE IF NOT EXISTS " + db + "; USE " + db) |> ignore
                | None -> ()

                for (k, v) in options.Settings do
                    TraceSdk.query session ("SET " + k + " = " + renderSetting v) |> ignore

                TraceSdk.query session "SELECT 1" |> ignore)
            "ChdbClient: bootstrap failed"
            "connect"
            Ch.ConnectionError

    // ── native query surface (the portable part of ChdbClientApi) ────────────

    /// Mirrors `ChdbNative`. Results are returned as the raw chDB result coerced
    /// to a string (sync paths) or the native result object (async/insert paths).
    type ChdbNative =
        { Query: string -> string -> Effect<string, SqlError, unit>
          QueryBind: string -> obj -> string -> Effect<string, SqlError, unit>
          QueryAsync: string -> obj -> Effect<obj, SqlError, unit>
          QueryBindAsync: string -> obj -> obj -> Effect<obj, SqlError, unit>
          Insert: obj -> Effect<obj, SqlError, unit>
          QueryStream: string -> obj -> Effect<obj, SqlError, unit> }

    /// `String(session.query(sql, format) ?? "")`.
    let private queryString (session: TraceSdk.RawSession) (sql: string) (format: string) : string =
        let raw = TraceSdk.queryFormat session sql format
        if TraceSdk.isNullish raw then "" else TraceSdk.stringValue raw

    let private queryBindString (session: TraceSdk.RawSession) (sql: string) (args: obj) (format: string) : string =
        let raw = TraceSdk.queryBind session sql args format
        if TraceSdk.isNullish raw then "" else TraceSdk.stringValue raw

    /// Build the native surface over an open session. (chDB query default format
    /// is "CSV", matching the TS defaults.)
    let makeNative (session: TraceSdk.RawSession) : ChdbNative =
        { Query =
            fun sql format ->
                trySync (fun () -> queryString session sql format) "Failed to execute native query" "execute" Ch.UnknownError
          QueryBind =
            fun sql args format ->
                trySync
                    (fun () -> queryBindString session sql args format)
                    "Failed to execute native bound query"
                    "execute"
                    Ch.UnknownError
          QueryAsync =
            fun sql options ->
                tryPromise
                    (fun () -> TraceSdk.queryAsync session sql options)
                    "Failed to execute native async query"
                    "execute"
          QueryBindAsync =
            fun sql parameters options ->
                tryPromise
                    (fun () -> TraceSdk.queryBindAsync session sql parameters options)
                    "Failed to execute native async bound query"
                    "execute"
          Insert =
            fun parameters ->
                tryPromise (fun () -> TraceSdk.insert session parameters) "Failed to execute native insert" "insert"
          QueryStream =
            fun sql options ->
                trySync
                    (fun () -> TraceSdk.queryStream session sql options)
                    "Failed to open native query stream"
                    "execute"
                    Ch.UnknownError }

    // ── reduced client API ───────────────────────────────────────────────────

    /// Options for `insertQuery`. Mirrors the TS `insertQuery` opts, minus the
    /// `schema` codec (Schema encoding belonged to effect's Schema, not ported).
    type ChdbInsertOptions =
        { Table: string
          Values: obj list
          Format: string option }

    /// The reduced replacement for `ChdbClientApi`. The SqlClient/`sql` tag, the
    /// Schema-decoding `query`, the compiler-backed `Statement.Fragment` plumbing
    /// and `asCommand` are dropped (they belonged to effect/unstable/sql). What
    /// remains is the native surface; literal/insert helpers are module functions
    /// (`param`, `insertQuery`).
    type ChdbClient =
        { Config: ChdbClientConfig
          Session: TraceSdk.RawSession
          Native: ChdbNative
          /// INSERT row objects as JSONEachRow (or `format`); returns rows written.
          InsertQuery: ChdbInsertOptions -> Effect<int, SqlError, unit> }

    /// Render a typed parameter literal: `param Ch.Int64 5L`. (The TS produced a
    /// `Statement.Fragment`; without the compiler we expose the raw SQL literal.)
    let param (ch: Ch<'A>) (value: 'A) : string = ch.Lit value

    /// `INSERT INTO table FORMAT <format>\n<ndjson>`; returns the count written.
    let insertQuery (session: TraceSdk.RawSession) (options: ChdbInsertOptions) : Effect<int, SqlError, unit> =
        trySync
            (fun () ->
                let format = options.Format |> Option.defaultValue "JSONEachRow"
                let data = options.Values |> List.map TraceSdk.stringify |> String.concat "\n"
                TraceSdk.query session ("INSERT INTO " + options.Table + " FORMAT " + format + "\n" + data) |> ignore
                List.length options.Values)
            "Failed to insert data"
            "insert"
            Ch.UnknownError

    /// Build a reduced `ChdbClient` over an existing session (mirrors
    /// `makeWithSession`, minus the SqlClient assembly).
    let makeWithSession (session: TraceSdk.RawSession) (options: ChdbClientConfig) : Effect<ChdbClient, SqlError, 'R> =
        Effect.succeed
            { Config = options
              Session = session
              Native = makeNative session
              InsertQuery = insertQuery session }

    /// Open + bootstrap + build the reduced client (mirrors `make`). The caller is
    /// responsible for `closeSession` on teardown.
    let make (options: ChdbClientConfig) : Effect<ChdbClient, SqlError, 'R> =
        effect {
            let! session = openSession options
            do! bootstrapSession session options
            return! makeWithSession session options
        }
