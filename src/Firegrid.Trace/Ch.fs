namespace Firegrid.Trace

/// A typed SQL-literal builder: each value knows its ClickHouse type and how to
/// render a JS value as a one-way SQL literal (chDB has no bind channel).
///
/// `array`/`nullable`/`map` compose by function composition, so
/// `Ch.array (Ch.nullable Ch.Int64)` builds both the renderer and the type with
/// no regex and no stringly-typed dispatch. Faithful port of `Ch<A>` in
/// `ChdbClient.ts`. The renderer is untyped at the boundary (takes `obj`) since
/// the values come from JS runtime; callers pass the matching runtime value.
type Ch<'A> =
    { Type: string
      Lit: 'A -> string }

[<RequireQualifiedAccess>]
module Ch =

    /// `s.replace(/\\/g, "\\\\").replace(/'/g, "\\'")`
    let escapeString (s: string) : string =
        s.Replace("\\", "\\\\").Replace("'", "\\'")

    let private leaf (typ: string) (lit: 'A -> string) : Ch<'A> = { Type = typ; Lit = lit }

    // ── leaves ───────────────────────────────────────────────────────────────

    let String: Ch<string> = leaf "String" (fun v -> "'" + escapeString v + "'")

    /// Int64/UInt64 accept `number | bigint`; carried as `obj` since JS may pass
    /// either. `bigint -> v.toString()`, `number -> String(Math.trunc(v))`.
    let private intLit (v: obj) : string =
        if TraceSdk.typeOf v = "bigint" then TraceSdk.stringValue v
        else TraceSdk.stringValue (box (TraceSdk.trunc (TraceSdk.numberValue v)))

    let Int64: Ch<obj> = leaf "Int64" intLit

    let UInt64: Ch<obj> = leaf "UInt64" intLit

    let Float64: Ch<float> = leaf "Float64" (fun v -> TraceSdk.stringValue (box v))

    let Bool: Ch<bool> = leaf "Bool" (fun v -> if v then "true" else "false")

    /// Pass epoch nanoseconds (`number | bigint`); rendered losslessly and
    /// timezone-independently via `fromUnixTimestamp64Nano`.
    let DateTime64Nanos: Ch<obj> =
        // `fromUnixTimestamp64Nano(v.toString() | String(v))` — both branches in
        // the TS produce the same `String(v)` for our `obj` carrier.
        leaf "DateTime64(9)" (fun v -> "fromUnixTimestamp64Nano(" + TraceSdk.stringValue v + ")")

    let UUID: Ch<string> = leaf "UUID" (fun v -> "'" + escapeString v + "'")

    // ── combinators ──────────────────────────────────────────────────────────

    let array (inner: Ch<'A>) : Ch<'A list> =
        leaf
            ("Array(" + inner.Type + ")")
            (fun xs -> "[" + (xs |> List.map inner.Lit |> String.concat ", ") + "]")

    /// `Nullable(...)`: `None`/null/undefined render as the literal NULL.
    let nullable (inner: Ch<'A>) : Ch<'A option> =
        leaf
            ("Nullable(" + inner.Type + ")")
            (fun v ->
                match v with
                | None -> "NULL"
                | Some inner' -> inner.Lit inner')

    /// `Map(k, v)` from an F# (key,value) list (mirrors entries iteration).
    let map (k: Ch<'K>) (v: Ch<'V>) : Ch<('K * 'V) list> =
        leaf
            ("Map(" + k.Type + ", " + v.Type + ")")
            (fun entries ->
                let parts =
                    entries
                    |> List.collect (fun (mk, mv) -> [ k.Lit mk; v.Lit mv ])
                    |> String.concat ", "

                "map(" + parts + ")")

    // ── default literal (untyped `sql` interpolation) ─────────────────────────
    // Dispatch on the JS runtime value (not on a parsed type string). For
    // anything precise, use a typed `Ch` + `param`.

    let rec defaultLiteral (value: obj) : string =
        if TraceSdk.isNullish value then
            "NULL"
        elif TraceSdk.isArray value then
            let xs = unbox<obj[]> value
            "[" + (xs |> Array.map defaultLiteral |> String.concat ", ") + "]"
        else
            match TraceSdk.typeOf value with
            | "number" ->
                if TraceSdk.isInteger value then Int64.Lit value
                else Float64.Lit (TraceSdk.numberValue value)
            | "bigint" -> Int64.Lit value
            | "boolean" -> Bool.Lit (unbox<bool> value)
            | "string" -> String.Lit (TraceSdk.stringValue value)
            | "symbol" ->
                // value.description ?? value.toString()
                let desc = TraceSdk.prop<obj> value "description"

                let s =
                    if TraceSdk.isNullish desc then TraceSdk.stringValue value
                    else TraceSdk.stringValue desc

                String.Lit s
            | "function" -> String.Lit (TraceSdk.prop<string> value "name")
            | "undefined" -> "NULL"
            | "object" ->
                if TraceSdk.isDate value then
                    "fromUnixTimestamp64Milli(" + TraceSdk.stringValue (box (TraceSdk.dateGetTime value)) + ")"
                elif TraceSdk.isMap value then
                    let entries = TraceSdk.mapEntries value

                    let parts =
                        entries
                        |> Array.map (fun e ->
                            let key = TraceSdk.prop<obj> e "0"
                            let item = TraceSdk.prop<obj> e "1"
                            defaultLiteral key + ", " + defaultLiteral item)
                        |> String.concat ", "

                    "map(" + parts + ")"
                else
                    String.Lit (TraceSdk.stringify value)
            | _ -> "NULL"

    // ── error classification (chDB throws; the code is in the message) ────────

    /// `codeFromMessage`: extract the ClickHouse code from the cause message, or
    /// `None` when absent. (`-1` is the sentinel from the Sdk regex helper.)
    let codeFromMessage (cause: obj) : int option =
        let msg = TraceSdk.errorMessage cause
        let code = TraceSdk.codeFromText msg
        if code = -1 then None else Some code

    /// ClickHouse error codes that map to syntax errors.
    let SYNTAX_CODES: Set<int> = Set.ofList [ 36; 47; 60; 62; 81; 242 ]

    /// Classified SQL error kind (faithful to ChdbClient.ts' SqlError subtypes).
    type SqlErrorKind =
        | SqlSyntaxError
        | StatementTimeoutError
        | ConnectionError
        | UnknownError

    /// A reduced replacement for effect/unstable/sql's `SqlError`. The TS driver
    /// wraps a typed cause; here we carry the classified kind plus the original
    /// inputs (cause, message, operation).
    type SqlError =
        { Kind: SqlErrorKind
          Cause: obj
          Message: string
          Operation: string }

    /// Faithful port of `classifyError`. `fallback` is "unknown" (default) or
    /// "connection".
    let classifyError (cause: obj) (message: string) (operation: string) (fallback: SqlErrorKind) : SqlError =
        let kind =
            match codeFromMessage cause with
            | Some code when SYNTAX_CODES.Contains code -> SqlSyntaxError
            | Some 159
            | Some 160
            | Some 209 -> StatementTimeoutError
            | _ -> fallback

        { Kind = kind
          Cause = cause
          Message = message
          Operation = operation }
