namespace Firegrid.Core

open Fable.Core
open Fable.Core.JsInterop

/// Raw JS-global / Emit / Import bindings used across the Core port.
/// Mirrors the `S2Sdk` style in `Firegrid.Log/S2/InternalSdk.fs`, including the
/// `#if FABLE_COMPILER` ... `#else` null fallback for `[<Import>]` values.
[<RequireQualifiedAccess>]
module internal CoreSdk =

    // ── Clock / identity ─────────────────────────────────────────────
    [<Emit("Date.now()")>]
    let nowMillis () : float = jsNative

    [<Emit("globalThis.crypto.randomUUID()")>]
    let randomUuid () : string = jsNative

    [<Emit("Math.random()")>]
    let mathRandom () : float = jsNative

    // ── JSON / clone ─────────────────────────────────────────────────
    [<Emit("JSON.stringify($0)")>]
    let jsonStringify (_value: obj) : string = jsNative

    [<Emit("JSON.parse($0)")>]
    let jsonParse<'A> (_value: string) : 'A = jsNative

    [<Emit("structuredClone($0)")>]
    let structuredClone<'T> (_value: 'T) : 'T = jsNative

    // ── Math helpers ─────────────────────────────────────────────────
    [<Emit("Math.floor($0)")>]
    let mathFloor (_value: float) : float = jsNative

    [<Emit("Math.max($0, $1)")>]
    let mathMax (_a: float) (_b: float) : float = jsNative

    [<Emit("Math.trunc($0)")>]
    let mathTrunc (_value: float) : float = jsNative

    [<Emit("Math.pow($0, $1)")>]
    let mathPow (_a: float) (_b: float) : float = jsNative

    [<Emit("Number.isInteger($0)")>]
    let numberIsInteger (_value: obj) : bool = jsNative

    [<Emit("Number($0)")>]
    let numberValue (_value: obj) : float = jsNative

    [<Emit("String($0)")>]
    let stringValue (_value: obj) : string = jsNative

    // ── Object / property access ──────────────────────────────────────
    [<Emit("$0[$1]")>]
    let prop<'T> (_value: obj) (_key: string) : 'T = jsNative

    [<Emit("$0[$1] = $2")>]
    let setProp (_value: obj) (_key: string) (_v: obj) : unit = jsNative

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

    [<Emit("$0 === undefined")>]
    let isUndefined (_value: obj) : bool = jsNative

    [<Emit("Object.keys($0)")>]
    let objectKeys (_value: obj) : string[] = jsNative

    [<Emit("Object.entries($0)")>]
    let objectEntries (_value: obj) : (string * obj)[] = jsNative

    [<Emit("Object.assign($0, $1)")>]
    let objectAssign (_target: obj) (_source: obj) : obj = jsNative

    [<Emit("Object.prototype.hasOwnProperty.call($0, $1)")>]
    let hasOwnProperty (_value: obj) (_key: string) : bool = jsNative

    [<Emit("Object.is($0, $1)")>]
    let objectIs (_a: obj) (_b: obj) : bool = jsNative

    [<Emit("Array.isArray($0)")>]
    let isArray (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'object'")>]
    let isTypeofObject (_value: obj) : bool = jsNative

    [<Emit("typeof $0 === 'function'")>]
    let isTypeofFunction (_value: obj) : bool = jsNative

    [<Emit("$0.length")>]
    let arrayLength (_value: obj) : int = jsNative

    [<Emit("$0[$1]")>]
    let arrayItem<'T> (_value: obj) (_index: int) : 'T = jsNative

    [<Emit("$0.map($1)")>]
    let arrayMap<'A, 'B> (_value: 'A[]) (_f: 'A -> 'B) : 'B[] = jsNative

    [<Emit("undefined")>]
    let undefinedValue: obj = jsNative

    [<Emit("$1($0)")>]
    let callEmit (_arg: obj) (_f: obj) : unit = jsNative

    [<Emit("$0()")>]
    let callDone (_f: obj) : unit = jsNative

    // ── AbortController / AbortSignal ─────────────────────────────────
    type AbortController = obj
    type AbortSignal = obj

    [<Emit("new AbortController()")>]
    let newAbortController () : AbortController = jsNative

    [<Emit("$0.signal")>]
    let controllerSignal (_ctrl: AbortController) : AbortSignal = jsNative

    [<Emit("$0.abort()")>]
    let controllerAbort (_ctrl: AbortController) : unit = jsNative

    [<Emit("$0.aborted")>]
    let signalAborted (_signal: AbortSignal) : bool = jsNative

    [<Emit("$0.addEventListener($1, $2, $3)")>]
    let addEventListener (_target: obj) (_event: string) (_handler: unit -> unit) (_options: obj) : unit = jsNative

    [<Emit("$0.removeEventListener($1, $2)")>]
    let removeEventListener (_target: obj) (_event: string) (_handler: unit -> unit) : unit = jsNative

    let onceOptions: obj = createObj [ "once" ==> true ]

    // ── Timers ────────────────────────────────────────────────────────
    type TimeoutHandle = obj

    [<Emit("setTimeout($0, $1)")>]
    let setTimeout (_handler: unit -> unit) (_ms: float) : TimeoutHandle = jsNative

    [<Emit("clearTimeout($0)")>]
    let clearTimeout (_handle: TimeoutHandle) : unit = jsNative

    // ── Promise helpers ───────────────────────────────────────────────
    [<Emit("Promise.resolve($0)")>]
    let promiseResolve<'T> (_value: 'T) : JS.Promise<'T> = jsNative

    [<Emit("Promise.resolve()")>]
    let promiseResolveUnit () : JS.Promise<unit> = jsNative

    [<Emit("Promise.race($0)")>]
    let promiseRace<'T> (_promises: JS.Promise<'T>[]) : JS.Promise<'T> = jsNative

    [<Emit("new Promise($0)")>]
    let newPromise<'T> (_executor: (('T -> unit) -> (obj -> unit) -> unit)) : JS.Promise<'T> = jsNative

    [<Emit("$0 instanceof Promise")>]
    let isPromise (_value: obj) : bool = jsNative

    /// `promise.catch(() => undefined)` — swallow rejection, resolve to unit.
    [<Emit("Promise.resolve($0).catch(() => undefined)")>]
    let promiseCatchSwallow (_promise: JS.Promise<'T>) : JS.Promise<unit> = jsNative

    /// Fire-and-forget: ensure rejections don't surface as unhandled.
    [<Emit("void Promise.resolve($0).catch(() => undefined)")>]
    let promiseStartIgnore (_promise: JS.Promise<'T>) : unit = jsNative

    // ── Error introspection / construction ────────────────────────────
    [<Emit("$0 instanceof Error")>]
    let isError (_value: obj) : bool = jsNative

    [<Emit("$0 && $0.name != null ? String($0.name) : 'Error'")>]
    let errorName (_err: obj) : string = jsNative

    [<Emit("$0 && $0.message != null ? String($0.message) : ''")>]
    let errorMessage (_err: obj) : string = jsNative

    [<Emit("$0 && $0.stack != null ? String($0.stack) : undefined")>]
    let errorStack (_err: obj) : obj = jsNative

    [<Emit("(() => { const e = new Error($0); e.name = $1; if ($2 !== undefined) e.stack = $2; return e })()")>]
    let makeError (_message: string) (_name: string) (_stack: obj) : exn = jsNative

    // ── Standard Schema ───────────────────────────────────────────────
    /// Calls `schema["~standard"].validate(value)`.
    [<Emit("$0[\"~standard\"].validate($1)")>]
    let schemaValidate (_schema: obj) (_value: obj) : obj = jsNative

    // ── Request (server) ─────────────────────────────────────────────
    [<Emit("$0.json()")>]
    let requestJson (_request: obj) : JS.Promise<obj> = jsNative

    /// Build a JS async-iterable that yields events pushed through the returned
    /// `emit`, completes when `done()` is called, mirroring the queue + resolver
    /// handshake of the TS `runWorkflow` async generator. Returns
    /// `{ iterable, emit, done }`. `onEvent` (async) runs in-order before each
    /// yield (used for the best-effort `publish` fan-out).
    [<Emit("""(() => {
  const queue = [];
  let resolveWait = null;
  let executionDone = false;
  const emit = (event) => {
    queue.push(event);
    if (resolveWait) { resolveWait(); resolveWait = null; }
  };
  const done = () => {
    executionDone = true;
    if (resolveWait) { resolveWait(); resolveWait = null; }
  };
  const onEvent = $0;
  const iterable = {
    async *[Symbol.asyncIterator]() {
      for (;;) {
        while (queue.length > 0) {
          const event = queue.shift();
          if (onEvent) { await onEvent(event); }
          yield event;
        }
        if (executionDone) break;
        await new Promise((r) => { resolveWait = r; });
      }
    }
  };
  return { iterable, emit, done };
})()""")>]
    let makeEventChannel (_onEvent: obj) : obj = jsNative

    /// Drain a JS async-iterable of events into an array (the `for await` loop).
    [<Emit("(async () => { const out = []; for await (const e of $0) out.push(e); return out; })()")>]
    let collectAsyncIterable<'T> (_iterable: obj) : JS.Promise<'T[]> = jsNative

    /// Await `request.json()`, returning `{ ok: true, value }` on success or
    /// `{ ok: false, error }` on rejection — lets F# branch without relying on
    /// a specific `Promise.catch` API surface.
    [<Emit("$0.json().then(v => ({ ok: true, value: v }), e => ({ ok: false, error: e }))")>]
    let requestJsonResult (_request: obj) : JS.Promise<obj> = jsNative
