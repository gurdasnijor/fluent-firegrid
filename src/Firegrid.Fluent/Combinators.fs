namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop

// ============================================================
// combinators.ts — all / race / raceAll re-exports, orTimeout.
//
// REDUCTIONS:
//  * `all` / `race` / `raceAll` are re-exported from EffSharp's `Effect` module
//    where available (the TS re-exports them from `effect/Effect`). If a name is
//    not present on EffSharp's `Effect`, the wrapper here will not compile and
//    should be dropped by the integrator (documented in summary).
//  * `orTimeout` in the TS uses `Effect.timeout` + `Cause.isTimeoutError`.
//    EffSharp lacks `effect/Cause` and may lack `Effect.timeout`; this is a
//    best-effort implementation built on a JS `Promise.race` with a timer,
//    producing a `FluentTimeoutError` on expiry. See summary.
// ============================================================

/// `TimeoutDuration = DurationLike | Duration.Input`. EffSharp has no
/// `effect/Duration`; held as `obj` (a number-ms or a DurationLike object).
type TimeoutDuration = obj

/// `FluentTimeoutError` — `Data.TaggedError("FluentTimeoutError")`.
/// Implemented as a JS tagged Error so consumers can `_tag`-match it, matching
/// the other Fluent/Core errors.
[<RequireQualifiedAccess>]
module FluentTimeoutError =

    [<Emit("""(() => {
  const e = new Error($1);
  e.name = 'FluentTimeoutError';
  e._tag = 'FluentTimeoutError';
  e.duration = $0;
  if ($2 !== undefined) e.cause = $2;
  return e;
})()""")>]
    let private make (_duration: obj) (_message: string) (_cause: obj) : exn = jsNative

    let create (duration: TimeoutDuration) (message: string) : exn = make duration message FluentSdk.undefinedValue

    let createWithCause (duration: TimeoutDuration) (message: string) (cause: obj) : exn = make duration message cause

    [<Emit("$0 instanceof Error && $0._tag === 'FluentTimeoutError'")>]
    let is (_value: obj) : bool = jsNative

[<RequireQualifiedAccess>]
module Combinators =

    /// `isDurationLikeObject(input)` — object with any duration key.
    let private isDurationLikeObject (input: TimeoutDuration) : bool =
        FluentSdk.isObject input
        && not (FluentSdk.isArray input)
        && (FluentSdk.hasKey input "days"
            || FluentSdk.hasKey input "hours"
            || FluentSdk.hasKey input "milliseconds"
            || FluentSdk.hasKey input "minutes"
            || FluentSdk.hasKey input "seconds")

    /// `normalizeTimeoutDuration(input)` — DurationLike object → ms number.
    let private normalizeTimeoutDurationMs (input: TimeoutDuration) : float =
        if isDurationLikeObject input then
            Clients.duration input
        else
            FluentSdk.numberValue input

    // Best-effort timeout: race the effect's promise against a timer. EffSharp's
    // own `Effect.timeout`/`Cause` are not available here.
    [<Emit("""(() => {
  let timer;
  const timed = new Promise((_, reject) => { timer = setTimeout(() => reject('__fluent_timeout__'), $1); });
  return Promise.race([Promise.resolve($0).then((v) => { clearTimeout(timer); return v; }), timed]);
})()""")>]
    let private racePromiseWithTimeout (_promise: obj) (_ms: float) : JS.Promise<obj> = jsNative

    // Coerce an effect's requirements to `unit` so it can be run. The fluent
    // surface assumes the requirements are already satisfied at the call site.
    [<Emit("$0")>]
    let private asRunnable (_value: obj) : Effect<obj, exn, unit> = jsNative

    /// `orTimeout(input)(self)` — best-effort. Returns the value or fails with
    /// `FluentTimeoutError` once the duration elapses.
    let orTimeout (input: TimeoutDuration) (self: Effect<'A, exn, 'R>) : Effect<'A, exn, 'R> =
        let ms = normalizeTimeoutDurationMs input

        let durationText =
            if isDurationLikeObject input then
                sprintf "%gms" (Clients.duration input)
            else
                FluentSdk.stringValue input

        Effect.tryPromiseJS
            (fun () -> racePromiseWithTimeout (box (Effect.runPromise (asRunnable (box self)))) ms)
            (fun cause ->
                if FluentSdk.stringValue (box cause) = "__fluent_timeout__" then
                    FluentTimeoutError.create input (sprintf "operation timed out after %s" durationText)
                else
                    // Non-timeout failures are surfaced as-is (cast to exn).
                    unbox<exn> (box cause))
        |> Effect.map (fun v -> unbox<'A> v)
