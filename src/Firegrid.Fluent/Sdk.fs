namespace Firegrid.Fluent

open Fable.Core
open Fable.Core.JsInterop

/// `FluentSdk` — all raw JS-SDK / global bindings used by the fluent port.
/// Mirrors the `InternalSdk` style of `Firegrid.Log`. Holds the
/// `@marcbachmann/cel-js` `Environment` binding, JSON, URL/URLSearchParams,
/// Headers/Request/Response/fetch, encode/decodeURIComponent, regex helpers,
/// `Object.*` reflection, `localeCompare` sort and property access.
[<RequireQualifiedAccess>]
module internal FluentSdk =

    // ── cel-js Environment ────────────────────────────────────────────
    // The TS uses `new Environment({ ... }).check(expr)` / `.evaluate(expr, ctx)`.

    type CelEnvironment = obj
    type CelCheckResult = obj

#if FABLE_COMPILER
    [<Import("Environment", "@marcbachmann/cel-js")>]
    let private environmentCtor: obj = jsNative
#else
    let private environmentCtor: obj = null
#endif

    [<Emit("new $0($1)")>]
    let private newEnvironment (_ctor: obj) (_options: obj) : CelEnvironment = jsNative

    /// `new Environment({ enableOptionalTypes: true, unlistedVariablesAreDyn: true })`.
    let createEnvironment () : CelEnvironment =
        newEnvironment
            environmentCtor
            (createObj
                [ "enableOptionalTypes" ==> true
                  "unlistedVariablesAreDyn" ==> true ])

    [<Emit("$0.check($1)")>]
    let check (_env: CelEnvironment) (_expression: string) : CelCheckResult = jsNative

    [<Emit("$0.evaluate($1, $2)")>]
    let evaluate (_env: CelEnvironment) (_expression: string) (_context: obj) : obj = jsNative

    // ── JSON ──────────────────────────────────────────────────────────

    [<Emit("JSON.stringify($0)")>]
    let jsonStringify (_value: obj) : string = jsNative

    [<Emit("JSON.parse($0)")>]
    let jsonParse<'A> (_value: string) : 'A = jsNative

    // ── nullish / type predicates ─────────────────────────────────────

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

    [<Emit("$0 === undefined")>]
    let isUndefined (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'string'")>]
    let isString (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'boolean'")>]
    let isBoolean (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'number'")>]
    let isNumber (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'function'")>]
    let isFunction (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'object' && $0 !== null")>]
    let isObject (_value: obj) : bool = jsNative

    [<Emit("Array.isArray($0)")>]
    let isArray (_value: obj) : bool = jsNative

    [<Emit("typeof $0")>]
    let typeOf (_value: obj) : string = jsNative

    [<Emit("$1 in $0")>]
    let hasKey (_value: obj) (_key: string) : bool = jsNative

    [<Emit("undefined")>]
    let undefinedValue: obj = jsNative

    // ── coercion ──────────────────────────────────────────────────────

    [<Emit("String($0)")>]
    let stringValue (_value: obj) : string = jsNative

    [<Emit("Number($0)")>]
    let numberValue (_value: obj) : float = jsNative

    // ── property access ───────────────────────────────────────────────

    [<Emit("$0[$1]")>]
    let prop<'T> (_value: obj) (_key: string) : 'T = jsNative

    [<Emit("$0[$1] = $2")>]
    let setProp (_value: obj) (_key: string) (_v: obj) : unit = jsNative

    [<Emit("$0 && $0.message ? String($0.message) : String($0)")>]
    let errorMessage (_error: obj) : string = jsNative

    [<Emit("$0 instanceof Error")>]
    let isError (_value: obj) : bool = jsNative

    // ── Object reflection ─────────────────────────────────────────────

    [<Emit("Object.keys($0)")>]
    let objectKeys (_value: obj) : string[] = jsNative

    [<Emit("Object.entries($0)")>]
    let objectEntries (_value: obj) : (string * obj)[] = jsNative

    [<Emit("Object.fromEntries($0)")>]
    let objectFromEntries (_entries: (string * obj)[]) : obj = jsNative

    [<Emit("Object.values($0)")>]
    let objectValues (_value: obj) : obj[] = jsNative

    [<Emit("({})")>]
    let emptyObject () : obj = jsNative

    [<Emit("Object.assign({}, $0, $1)")>]
    let assign2 (_a: obj) (_b: obj) : obj = jsNative

    /// `Object.defineProperty(obj, key, { enumerable: false, value })`.
    [<Emit("Object.defineProperty($0, $1, { enumerable: false, value: $2 })")>]
    let defineNonEnumerable (_target: obj) (_key: string) (_value: obj) : unit = jsNative

    // ── URI encode / decode ───────────────────────────────────────────

    [<Emit("encodeURIComponent($0)")>]
    let encodeURIComponent (_value: string) : string = jsNative

    [<Emit("decodeURIComponent($0)")>]
    let decodeURIComponent (_value: string) : string = jsNative

    // ── URL / URLSearchParams ─────────────────────────────────────────

    type Url = obj

    [<Emit("new URL($0)")>]
    let newUrl (_input: obj) : Url = jsNative

    [<Emit("$0.pathname")>]
    let urlPathname (_url: Url) : string = jsNative

    [<Emit("$0.pathname = $1")>]
    let setUrlPathname (_url: Url) (_value: string) : unit = jsNative

    [<Emit("$0.search = $1")>]
    let setUrlSearch (_url: Url) (_value: string) : unit = jsNative

    [<Emit("$0.searchParams.get($1)")>]
    let searchParamsGet (_url: Url) (_key: string) : obj = jsNative

    // ── string helpers ────────────────────────────────────────────────

    [<Emit("$0.startsWith($1)")>]
    let startsWith (_value: string) (_prefix: string) : bool = jsNative

    [<Emit("$0.endsWith($1)")>]
    let endsWith (_value: string) (_suffix: string) : bool = jsNative

    [<Emit("$0.slice($1)")>]
    let sliceFrom (_value: string) (_start: int) : string = jsNative

    [<Emit("$0.slice($1, $2)")>]
    let sliceRange (_value: string) (_start: int) (_stop: int) : string = jsNative

    [<Emit("$0.split($1)")>]
    let split (_value: string) (_sep: string) : string[] = jsNative

    [<Emit("$0.localeCompare($1)")>]
    let localeCompare (_a: string) (_b: string) : int = jsNative

    // ── regex helpers (faithful CEL field-ref scan + literal stripping) ─

    /// `expression.replace(/"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'/g, "")`.
    [<Emit("$0.replace(/\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'/g, \"\")")>]
    let stripCelStringLiterals (_expression: string) : string = jsNative

    /// Scan for `\b(row|old)\.([A-Za-z_][A-Za-z0-9_]*)` returning
    /// `[ scope, field ]` pairs in order. Implemented in raw JS to preserve
    /// the exact regex semantics of the TS.
    [<Emit("""(() => {
  const pattern = /\b(row|old)\.([A-Za-z_][A-Za-z0-9_]*)/g;
  const out = [];
  let match;
  while ((match = pattern.exec($0)) !== null) {
    out.push([match[1], match[2]]);
  }
  return out;
})()""")>]
    let scanStateFieldRefs (_stripped: string) : (string * string)[] = jsNative

    // ── Headers / Request / Response / fetch ──────────────────────────

    type Headers = obj
    type Request = obj
    type Response = obj

    [<Emit("new Headers($0)")>]
    let newHeaders (_init: obj) : Headers = jsNative

    [<Emit("$0.set($1, $2)")>]
    let headersSet (_headers: Headers) (_key: string) (_value: string) : unit = jsNative

    [<Emit("$0.get($1)")>]
    let headersGet (_headers: Headers) (_key: string) : obj = jsNative

    [<Emit("$0.forEach(($1))")>]
    let headersForEach (_headers: Headers) (_callback: string -> string -> unit) : unit = jsNative

    [<Emit("$0.method")>]
    let requestMethod (_request: Request) : string = jsNative

    [<Emit("$0.url")>]
    let requestUrl (_request: Request) : string = jsNative

    [<Emit("$0.headers")>]
    let requestHeaders (_request: Request) : Headers = jsNative

    [<Emit("$0.json()")>]
    let requestJson (_request: Request) : JS.Promise<obj> = jsNative

    [<Emit("$0.json()")>]
    let responseJson (_response: Response) : JS.Promise<obj> = jsNative

    [<Emit("$0.ok")>]
    let responseOk (_response: Response) : bool = jsNative

    [<Emit("$0.status")>]
    let responseStatus (_response: Response) : int = jsNative

    [<Emit("new Response($0, $1)")>]
    let newResponse (_body: obj) (_init: obj) : Response = jsNative

#if FABLE_COMPILER
    [<Emit("fetch($0, $1)")>]
    let globalFetch (_input: obj) (_init: obj) : JS.Promise<Response> = jsNative
#else
    let globalFetch (_input: obj) (_init: obj) : JS.Promise<Response> = jsNative
#endif

    [<Emit("$0($1, $2)")>]
    let callFetch (_fetch: obj) (_input: obj) (_init: obj) : JS.Promise<Response> = jsNative

    // ── misc globals ──────────────────────────────────────────────────

    [<Emit("Date.now()")>]
    let nowMillis () : float = jsNative

    [<Emit("Number.isFinite($0)")>]
    let isFinite (_value: float) : bool = jsNative
