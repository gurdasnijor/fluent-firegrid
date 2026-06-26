namespace Firegrid.Trace

open Fable.Core
open Fable.Core.JsInterop

/// Raw bindings to the external JS world: the `chdb` Node package (embedded
/// ClickHouse Session), the OpenTelemetry SDK value shapes (ReadableSpan etc.)
/// accessed structurally, plus JSON / Date / BigInt / string helpers.
///
/// Mirrors the `S2Sdk` style from `Firegrid.Log/S2/InternalSdk.fs`: every raw
/// JS-SDK or global binding lives here behind `[<Emit>]`/`[<Import>]`, with a
/// `#if FABLE_COMPILER` null fallback for imported values.
[<RequireQualifiedAccess>]
module internal TraceSdk =

    /// chDB Session handle (opaque).
    type RawSession = obj

#if FABLE_COMPILER
    [<Import("Session", "chdb")>]
    let private sessionConstructor: obj = jsNative
#else
    let private sessionConstructor: obj = null
#endif

    [<Emit("undefined")>]
    let undefinedObj: obj = jsNative

    // ── Session construction / lifecycle ─────────────────────────────────────

    [<Emit("new $0()")>]
    let private newSession (_ctor: obj) : RawSession = jsNative

    [<Emit("new $0($1)")>]
    let private newSessionWithPath (_ctor: obj) (_path: string) : RawSession = jsNative

    /// `new Session()` — chdb-node opens its own temp dir, removed on cleanup().
    let openSession () : RawSession = newSession sessionConstructor

    /// `new Session(path)` — an on-disk session that persists across opens.
    let openSessionWithPath (path: string) : RawSession =
        newSessionWithPath sessionConstructor path

    /// `session.cleanup()` — idempotent, never throws, drops the temp dir.
    [<Emit("$0.cleanup()")>]
    let cleanup (_session: RawSession) : unit = jsNative

    // ── Session query surface ────────────────────────────────────────────────

    /// `session.query(sql, format)` — synchronous, blocking. Returns the format
    /// string (or empty). The default chDB format is "CSV".
    [<Emit("$0.query($1, $2)")>]
    let queryFormat (_session: RawSession) (_sql: string) (_format: string) : obj = jsNative

    /// `session.query(sql)` — synchronous, default format.
    [<Emit("$0.query($1)")>]
    let query (_session: RawSession) (_sql: string) : obj = jsNative

    /// `session.queryBind(sql, args, format)` — synchronous bound query.
    [<Emit("$0.queryBind($1, $2, $3)")>]
    let queryBind (_session: RawSession) (_sql: string) (_args: obj) (_format: string) : obj = jsNative

    /// `session.queryAsync(sql, options)` — promise-returning async query.
    [<Emit("$0.queryAsync($1, $2)")>]
    let queryAsync (_session: RawSession) (_sql: string) (_options: obj) : JS.Promise<obj> = jsNative

    /// `session.queryBindAsync(sql, params, options)`.
    [<Emit("$0.queryBindAsync($1, $2, $3)")>]
    let queryBindAsync (_session: RawSession) (_sql: string) (_params: obj) (_options: obj) : JS.Promise<obj> = jsNative

    /// `session.insert(params)` — promise-returning native insert.
    [<Emit("$0.insert($1)")>]
    let insert (_session: RawSession) (_params: obj) : JS.Promise<obj> = jsNative

    /// `session.queryStream(sql, options)` — synchronous stream handle.
    [<Emit("$0.queryStream($1, $2)")>]
    let queryStream (_session: RawSession) (_sql: string) (_options: obj) : obj = jsNative

    // ── JSON ─────────────────────────────────────────────────────────────────

    [<Emit("JSON.stringify($0)")>]
    let stringify (_value: obj) : string = jsNative

    [<Emit("JSON.parse($0)")>]
    let parse<'A> (_value: string) : 'A = jsNative

    // ── generic value helpers ────────────────────────────────────────────────

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

    [<Emit("$0[$1]")>]
    let prop<'T> (_value: obj) (_key: string) : 'T = jsNative

    [<Emit("String($0)")>]
    let stringValue (_value: obj) : string = jsNative

    [<Emit("Number($0)")>]
    let numberValue (_value: obj) : float = jsNative

    [<Emit("Array.isArray($0)")>]
    let isArray (_value: obj) : bool = jsNative

    [<Emit("typeof $0")>]
    let typeOf (_value: obj) : string = jsNative

    [<Emit("$0 instanceof Date")>]
    let isDate (_value: obj) : bool = jsNative

    [<Emit("$0 instanceof Map")>]
    let isMap (_value: obj) : bool = jsNative

    [<Emit("Number.isInteger($0)")>]
    let isInteger (_value: obj) : bool = jsNative

    [<Emit("Math.trunc($0)")>]
    let trunc (_value: float) : float = jsNative

    /// `Object.entries(obj)` as an array of `[key, value]` pairs.
    [<Emit("Object.entries($0 || {})")>]
    let objectEntries (_value: obj) : obj[] = jsNative

    /// `Object.fromEntries(pairs)`.
    [<Emit("Object.fromEntries($0)")>]
    let objectFromEntries (_pairs: obj[]) : obj = jsNative

    /// `Array.from(map.entries())`.
    [<Emit("Array.from($0.entries())")>]
    let mapEntries (_map: obj) : obj[] = jsNative

    [<Emit("[$0, $1]")>]
    let pair (_a: obj) (_b: obj) : obj = jsNative

    /// `Array.prototype.join`.
    [<Emit("$0.join($1)")>]
    let join (_arr: obj) (_sep: string) : string = jsNative

    // ── error helpers ────────────────────────────────────────────────────────

    [<Emit("$0 instanceof Error ? $0.message : String($0)")>]
    let errorMessage (_cause: obj) : string = jsNative

    [<Emit("$0 instanceof Error ? $0 : new Error(String($0))")>]
    let toError (_cause: obj) : exn = jsNative

    /// Extract the ClickHouse error code from a message via `/Code:\s*(\d+)/`.
    /// Returns -1 when absent.
    [<Emit("(function(m){ var x = /Code:\\s*(\\d+)/.exec(m); return x !== null ? Number(x[1]) : -1; })($0)")>]
    let codeFromText (_message: string) : int = jsNative

    // ── Date / time ──────────────────────────────────────────────────────────

    [<Emit("Date.now()")>]
    let nowMillis () : float = jsNative

    [<Emit("new Date($0)")>]
    let dateFromMillis (_millis: float) : JS.Date = jsNative

    [<Emit("$0.getUTCFullYear()")>]
    let getUTCFullYear (_date: JS.Date) : int = jsNative

    [<Emit("$0.getUTCMonth()")>]
    let getUTCMonth (_date: JS.Date) : int = jsNative

    [<Emit("$0.getUTCDate()")>]
    let getUTCDate (_date: JS.Date) : int = jsNative

    [<Emit("$0.getUTCHours()")>]
    let getUTCHours (_date: JS.Date) : int = jsNative

    [<Emit("$0.getUTCMinutes()")>]
    let getUTCMinutes (_date: JS.Date) : int = jsNative

    [<Emit("$0.getUTCSeconds()")>]
    let getUTCSeconds (_date: JS.Date) : int = jsNative

    [<Emit("$0.getTime()")>]
    let dateGetTime (_date: obj) : float = jsNative

    // ── string ───────────────────────────────────────────────────────────────

    [<Emit("String($0).padStart($1, $2)")>]
    let padStart (_value: obj) (_length: int) (_fill: string) : string = jsNative

    // ── BigInt arithmetic (epoch-nanosecond math, precision-preserving) ───────
    // BigInt is opaque to F#; carried as `obj`.

    [<Emit("BigInt($0)")>]
    let bigInt (_value: obj) : obj = jsNative

    /// `BigInt(seconds) * BigInt(1000000000) + BigInt(nanos)`
    [<Emit("BigInt($0)*BigInt(1000000000)+BigInt($1)")>]
    let hrNanosBig (_seconds: obj) (_nanos: obj) : obj = jsNative

    /// `a - b` over BigInt.
    [<Emit("$0 - $1")>]
    let bigSub (_a: obj) (_b: obj) : obj = jsNative

    /// `value / BigInt(1000000)` (nanos -> millis, floored).
    [<Emit("$0 / BigInt(1000000)")>]
    let bigDivMillis (_value: obj) : obj = jsNative

    /// `value % BigInt(1000000000)` (sub-second nanos).
    [<Emit("$0 % BigInt(1000000000)")>]
    let bigModNanos (_value: obj) : obj = jsNative

    /// `Number(bigint)` — safe only for values within Number range (a span delta).
    [<Emit("Number($0)")>]
    let bigToNumber (_value: obj) : float = jsNative

    /// `Number(bigint)` for the millis quotient passed to `new Date(...)`.
    [<Emit("Number($0)")>]
    let bigToMillis (_value: obj) : float = jsNative

    // ── Promise helpers (faithful to the TS .then/.catch flow) ───────────────

    [<Emit("Promise.resolve()")>]
    let resolvedUnit () : JS.Promise<unit> = jsNative

    [<Emit("$0.then($1)")>]
    let promiseThen (_promise: JS.Promise<'A>) (_onFulfilled: 'A -> 'B) : JS.Promise<'B> = jsNative

    [<Emit("$0.catch($1)")>]
    let promiseCatch (_promise: JS.Promise<'A>) (_onRejected: obj -> 'A) : JS.Promise<'A> = jsNative

    /// `fetch(url, init)`.
    [<Emit("fetch($0, $1)")>]
    let fetch (_url: string) (_init: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.serialize()")>]
    let serialize (_value: obj) : string = jsNative

    /// `span.spanContext()`.
    [<Emit("$0.spanContext()")>]
    let spanContext (_span: obj) : obj = jsNative

    // ── try/catch bridge (chDB query methods throw synchronously) ────────────
    // Runs a thunk and reports success/failure as a tagged JS object
    // `{ ok: true, value } | { ok: false, error }` so F# can branch purely.

    [<Emit("(function(){ try { return { ok: true, value: $0() }; } catch (e) { return { ok: false, error: e }; } })()")>]
    let tryRun (_thunk: unit -> 'a) : obj = jsNative
