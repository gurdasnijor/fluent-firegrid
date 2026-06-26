namespace Firegrid.Runtime

open Fable.Core
open Fable.Core.JsInterop

/// Raw JS-global / Emit bindings used across the Runtime port that are not
/// already exposed by `Firegrid.Core`'s `internal CoreSdk`. Mirrors the
/// `CoreSdk` style.
[<RequireQualifiedAccess>]
module internal RuntimeSdk =

    // ── Clock ────────────────────────────────────────────────────────
    [<Emit("Date.now()")>]
    let nowMillis () : float = jsNative

    // ── Number / Math ────────────────────────────────────────────────
    [<Emit("Number.isInteger($0)")>]
    let numberIsInteger (_value: obj) : bool = jsNative

    [<Emit("Number.isFinite($0)")>]
    let numberIsFinite (_value: obj) : bool = jsNative

    [<Emit("Number($0)")>]
    let numberValue (_value: obj) : float = jsNative

    [<Emit("Math.floor($0)")>]
    let mathFloor (_value: float) : float = jsNative

    [<Emit("parseInt($0, $1)")>]
    let parseInt (_value: string) (_radix: int) : float = jsNative

    // ── Object / property access ──────────────────────────────────────
    [<Emit("$0[$1]")>]
    let prop<'T> (_value: obj) (_key: string) : 'T = jsNative

    [<Emit("$0[$1] = $2")>]
    let setProp (_value: obj) (_key: string) (_v: obj) : unit = jsNative

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

    [<Emit("$0 === undefined")>]
    let isUndefined (_value: obj) : bool = jsNative

    [<Emit("undefined")>]
    let undefinedValue: obj = jsNative

    [<Emit("Object.entries($0)")>]
    let objectEntries (_value: obj) : (string * obj)[] = jsNative

    [<Emit("Object.values($0)")>]
    let objectValues<'T> (_value: obj) : 'T[] = jsNative

    [<Emit("$1 in $0")>]
    let hasKey (_value: obj) (_key: string) : bool = jsNative

    /// Shallow object spread clone (`{ ...value }`).
    [<Emit("{ ...$0 }")>]
    let shallowClone (_value: obj) : obj = jsNative

    [<Emit("typeof $0 === 'object'")>]
    let isTypeofObject (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'function'")>]
    let isTypeofFunction (_value: obj) : bool = jsNative

    // ── String ────────────────────────────────────────────────────────
    /// Split a string on a regular expression source (e.g. `\\s+`).
    [<Emit("$0.split(new RegExp($1))")>]
    let splitRegex (_value: string) (_pattern: string) : string[] = jsNative

    [<Emit("$0.trim()")>]
    let trim (_value: string) : string = jsNative

    [<Emit("$0.startsWith($1)")>]
    let startsWith (_value: string) (_prefix: string) : bool = jsNative

    // ── Date (UTC getters for cron) ───────────────────────────────────
    type JsDate = obj

    [<Emit("new Date($0)")>]
    let newDate (_ts: float) : JsDate = jsNative

    [<Emit("$0.getUTCMinutes()")>]
    let getUTCMinutes (_date: JsDate) : int = jsNative

    [<Emit("$0.getUTCHours()")>]
    let getUTCHours (_date: JsDate) : int = jsNative

    [<Emit("$0.getUTCDate()")>]
    let getUTCDate (_date: JsDate) : int = jsNative

    [<Emit("$0.getUTCDay()")>]
    let getUTCDay (_date: JsDate) : int = jsNative

    [<Emit("$0.getUTCMonth()")>]
    let getUTCMonth (_date: JsDate) : int = jsNative

    // ── Promise helpers ───────────────────────────────────────────────
    [<Emit("Promise.resolve($0)")>]
    let promiseResolve<'T> (_value: 'T) : JS.Promise<'T> = jsNative

    [<Emit("Promise.resolve()")>]
    let promiseResolveUnit () : JS.Promise<unit> = jsNative

    /// Drain a JS async-iterable of events, invoking `onEvent` per element.
    /// Used to faithfully reproduce `collectWorkflowEvents`'s `for await` loop
    /// (the per-event counting/truncation logic stays in F#).
    [<Emit("(async () => { for await (const e of $0) { $1(e); } })()")>]
    let forAwaitEach (_iterable: obj) (_onEvent: obj -> unit) : JS.Promise<unit> = jsNative
