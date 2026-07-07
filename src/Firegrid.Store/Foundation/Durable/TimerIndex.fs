namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log
open Firegrid.Store
open Firegrid.Foundation

/// MS-C3 (WP C2) — the **folded timer index** in front of the wake path. A
/// durable log of timer intents (`Armed`/`Fired`) whose pure fold materialises the
/// pending set; `fireDue now` posts a `TimerFired` wake for each due timer
/// **through the public `WakeShard` surface** (no deep imports) and records it
/// `Fired`. The C1 router then dispatches those wakes.
///
/// Firing is at-least-once with the router's idempotent drive (the approved
/// wake-path semantics): a `Fired` record dedups re-fires, and a re-post of a
/// still-due timer folds once at the router's cursor. `now` is passed in as data
/// (sans-IO core rule — no ambient clock). The timer log is open-append (anyone
/// may arm a timer, as anyone may post a wake) and never seals.
///
/// EffSharp-free: `Async` + `Result` + DU errors + `Codec` records.
[<RequireQualifiedAccess>]
module TimerIndex =

    /// A timer-log record. `Armed` registers a timer for a subject at `dueAt`;
    /// `Fired` retires it (so the fold drops it from the pending set).
    type TimerRecord =
        | Armed of subject: ActorAddress * timer: TimerId * dueAt: Timestamp
        | Fired of subject: ActorAddress * timer: TimerId

    let codec: SubjectHistory.Codec<TimerRecord> =
        { Encode =
            fun record ->
                match record with
                | Armed(target, TimerId timer, dueAt) ->
                    JsJson.stringify
                        {| kind = "armed"
                           subject = (target.Segments |> List.toArray)
                           timer = timer
                           dueAt = string dueAt |}
                | Fired(target, TimerId timer) ->
                    JsJson.stringify
                        {| kind = "fired"
                           subject = (target.Segments |> List.toArray)
                           timer = timer |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body

                    let target: ActorAddress =
                        { Segments = JsJson.prop<string[]> parsed "subject" |> Array.toList }

                    let timer = TimerId(JsJson.stringProp "timer" parsed)

                    match JsJson.stringProp "kind" parsed with
                    | "armed" -> Ok(Armed(target, timer, System.Int64.Parse(JsJson.stringProp "dueAt" parsed)))
                    | "fired" -> Ok(Fired(target, timer))
                    | other -> Error(sprintf "unknown timer kind '%s'" other)
                with error ->
                    Error error.Message }

    /// Derived (never random) timer-log subject: `"{ns}/timers"`.
    let subject (config: WakeShard.ShardConfig) : SubjectHistory.SubjectId =
        SubjectHistory.SubjectId(sprintf "%s/timers" config.Namespace)

    // ---- Pure fold core (sans-IO) ----------------------------------------

    /// The pending timer set — armed timers not yet fired. Abstract; consumers use
    /// `pending`/`due`.
    type Pending = private { Timers: Map<string, ActorAddress * TimerId * Timestamp> }

    let empty: Pending = { Timers = Map.empty }

    let private timerKey (target: ActorAddress) (TimerId timer) : string =
        (String.concat "/" target.Segments) + "#" + timer

    /// Fold one record into the pending set: `Armed` adds (last-arm wins on
    /// re-arm), `Fired` removes.
    let apply (state: Pending) (record: TimerRecord) : Pending =
        match record with
        | Armed(target, timer, dueAt) ->
            { Timers = state.Timers |> Map.add (timerKey target timer) (target, timer, dueAt) }
        | Fired(target, timer) ->
            { Timers = state.Timers |> Map.remove (timerKey target timer) }

    /// The currently-armed timers (pending, not yet fired).
    let pending (state: Pending) : (ActorAddress * TimerId * Timestamp) list =
        state.Timers |> Map.toList |> List.map snd

    /// The armed timers due at `now` (`dueAt <= now`).
    let due (now: Timestamp) (state: Pending) : (ActorAddress * TimerId * Timestamp) list =
        pending state |> List.filter (fun (_, _, dueAt) -> dueAt <= now)

    // ---- Shell -----------------------------------------------------------

    let private foldRecord (state: Pending) (stored: SubjectHistory.StoredRecord<TimerRecord>) : Pending =
        apply state stored.Body

    /// Rebuild the pending set from the timer log (fold from `Seq 0` to the tail).
    let load (basin: S2.Basin) (config: WakeShard.ShardConfig) : Async<Result<Pending, S2Errors.S2Failure>> =
        async {
            let subj = subject config
            let (SubjectHistory.SubjectId name) = subj

            try
                do! S2.ensureStream name basin
                let! tail = SubjectHistory.tail basin subj
                let! state, _ = SubjectHistory.foldTo basin codec subj (SubjectHistory.Seq 0L) tail empty foldRecord
                return Ok state
            with error ->
                return Error(S2Errors.classify error)
        }

    /// Arm a timer for `target` at `dueAt`: open-append an `Armed` record.
    let arm
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (target: ActorAddress)
        (timer: TimerId)
        (dueAt: Timestamp)
        : Async<Result<unit, S2Errors.S2Failure>> =
        async {
            let subj = subject config
            let (SubjectHistory.SubjectId name) = subj

            try
                do! S2.ensureStream name basin
                let! _ = SubjectHistory.append basin codec subj [ Armed(target, timer, dueAt) ]
                return Ok()
            with error ->
                return Error(S2Errors.classify error)
        }

    /// Fire every timer due at `now`: post a `TimerFired` wake for each through the
    /// public `WakeShard.post` (the router dispatches it), then record it `Fired`.
    /// Post-then-`Fired` is at-least-once with the router's idempotent drive; a
    /// not-yet-due timer (`dueAt > now`) is left armed. Returns the fired keys.
    let fireDue
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (now: Timestamp)
        : Async<Result<(ActorAddress * TimerId) list, S2Errors.S2Failure>> =
        async {
            match! load basin config with
            | Error failure -> return Error failure
            | Ok state ->
                let subj = subject config

                let rec fire acc remaining =
                    async {
                        match remaining with
                        | [] -> return Ok(List.rev acc)
                        | (target, timer, dueAt) :: rest ->
                            match! WakeShard.post basin config target (WakeReason.TimerFired(timer, dueAt)) with
                            | Error failure -> return Error failure
                            | Ok() ->
                                try
                                    let! _ = SubjectHistory.append basin codec subj [ Fired(target, timer) ]
                                    return! fire ((target, timer) :: acc) rest
                                with error ->
                                    return Error(S2Errors.classify error)
                    }

                return! fire [] (due now state)
        }
