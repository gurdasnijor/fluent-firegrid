/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — internal machinery (T1 green-making).
///
/// Everything here composes the L1 kernel (Firegrid.Store Foundation/Durable)
/// under the ratified L2 contract in Firegrid.Durable.fs. Nothing in this
/// file is public surface: the contract file's bodies are the only consumers.
///
/// Contract-type-agnostic by design: this file compiles BEFORE the contract,
/// so anything that needs a contract type (StepError, DurableCancelled, …)
/// is parameterized as a closure and supplied by the contract file's
/// `Wiring` module.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Internal

open System
open FSharp.Reflection
open Fable.Core
open Firegrid.Log
open Firegrid.Store.Foundation.Durable
open Firegrid.Store.Foundation.Durable.App

module KD = Firegrid.Store.Foundation.Durable.Durable
module KApp = Firegrid.Store.Foundation.Durable.App.DurableApp

// ── JS interop primitives ─────────────────────────────────────────────────

[<RequireQualifiedAccess>]
module internal Interop =
    [<Emit("Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10)")>]
    let entropy () : string = jsNative

    [<Emit("Math.random().toString(36).slice(2, 10)")>]
    let shortEntropy () : string = jsNative

    /// unref'd sleep: a dangling poll/worker loop must never keep the corpus
    /// process alive (the T0 ratchet requires prompt suite exit).
    [<Emit("new Promise(resolve => { const t = setTimeout(resolve, $0); if (t.unref) t.unref(); })")>]
    let private sleepPromise (_ms: int) : JS.Promise<unit> = jsNative

    let sleepUnref (ms: int) : Async<unit> = sleepPromise ms |> Async.AwaitPromise

    [<Emit("console.error($0)")>]
    let consoleError (_message: string) : unit = jsNative

    [<Emit("$0[$1]")>]
    let dynGet (_target: obj) (_key: string) : obj = jsNative

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

// ── Derived serialization: the type-directed codec walker ─────────────────
//
// Encode and decode are both driven by a System.Type captured (inline) at the
// contract's define sites. Wire shapes (pinned by the golden fixture):
//   records → {"Field":…} in declaration order
//   unions  → ["CaseName", …fields] (case-NAME-tagged; [<WireName>] pins)
//   lists   → arrays · tuples → arrays · unit → null
// Doctrine: int64/BigInt are NOT payload types — rejected at definition time.

[<RequireQualifiedAccess>]
module internal Codec =
    [<Emit("JSON.stringify($0)")>]
    let private stringifyRaw (_value: obj) : string = jsNative

    [<Emit("JSON.parse($0)")>]
    let private parseRaw (_text: string) : obj = jsNative

    [<Emit("$0[$1]")>]
    let private idx (_target: obj) (_index: int) : obj = jsNative

    [<Emit("$0[$1]")>]
    let private propGet (_target: obj) (_key: string) : obj = jsNative

    [<Emit("$0[$1] = $2")>]
    let private propSet (_target: obj) (_key: string) (_value: obj) : unit = jsNative

    [<Emit("{}")>]
    let private emptyObj () : obj = jsNative

    [<Emit("$0.length")>]
    let private rawLength (_target: obj) : int = jsNative

    [<Emit("String($0)")>]
    let private asString (_value: obj) : string = jsNative

    let isNullish (value: obj) : bool = Interop.isNullish value

    let private isPrimitive (name: string) =
        name = "System.String"
        || name = "System.Int32"
        || name = "System.Double"
        || name = "System.Boolean"

    let private listPrefix = "Microsoft.FSharp.Collections.FSharpList"
    let private unitName = "Microsoft.FSharp.Core.Unit"

    /// Payload doctrine, enforced at definition time: no int64/BigInt.
    let rec private checkSafe (seen: string list) (ty: Type) : unit =
        let name = ty.FullName

        if name = "System.Int64" || name = "System.UInt64" || name = "System.Decimal" then
            failwith (
                "Firegrid.Durable payload doctrine: int64/BigInt is not a payload type (use int or float): "
                + name
            )
        elif List.contains name seen then
            ()
        elif isPrimitive name || name = unitName then
            ()
        elif name.StartsWith listPrefix then
            checkSafe (name :: seen) (ty.GetGenericArguments().[0])
        elif FSharpType.IsTuple ty then
            FSharpType.GetTupleElements ty |> Array.iter (checkSafe (name :: seen))
        elif FSharpType.IsRecord ty then
            FSharpType.GetRecordFields ty
            |> Array.iter (fun field -> checkSafe (name :: seen) field.PropertyType)
        elif FSharpType.IsUnion ty then
            FSharpType.GetUnionCases ty
            |> Array.iter (fun unionCase ->
                unionCase.GetFields()
                |> Array.iter (fun field -> checkSafe (name :: seen) field.PropertyType))
        else
            ()

    let ensureWireSafe (ty: Type) : unit = checkSafe [] ty

    let rec private encodeRaw (ty: Type) (value: obj) : obj =
        let name = ty.FullName

        if name = unitName then
            null
        elif isNullish value then
            value
        elif isPrimitive name then
            value
        elif name.StartsWith listPrefix then
            let elementTy = ty.GetGenericArguments().[0]

            unbox<obj list> value
            |> List.map (encodeRaw elementTy)
            |> List.toArray
            |> box
        elif FSharpType.IsTuple ty then
            let elements = FSharpType.GetTupleElements ty
            let fields = FSharpValue.GetTupleFields value
            Array.map2 encodeRaw elements fields |> box
        elif FSharpType.IsRecord ty then
            let target = emptyObj ()

            for field in FSharpType.GetRecordFields ty do
                propSet target field.Name (encodeRaw field.PropertyType (propGet value field.Name))

            target
        elif FSharpType.IsUnion ty then
            let unionCase, fields = FSharpValue.GetUnionFields(value, ty)
            let fieldInfos = unionCase.GetFields()
            let parts = ResizeArray<obj>()
            parts.Add(box unionCase.Name)

            fields
            |> Array.iteri (fun index field -> parts.Add(encodeRaw fieldInfos.[index].PropertyType field))

            box (parts.ToArray())
        else
            value

    let encodeWith (ty: Type) (value: obj) : string =
        let text = stringifyRaw (encodeRaw ty value)
        if isNullish (box text) then "null" else text

    let rec private decodeRaw (ty: Type) (raw: obj) : obj =
        let name = ty.FullName

        if name = unitName then
            box ()
        elif isNullish raw then
            raw
        elif isPrimitive name then
            raw
        elif name.StartsWith listPrefix then
            let elementTy = ty.GetGenericArguments().[0]
            box [ for index in 0 .. rawLength raw - 1 -> decodeRaw elementTy (idx raw index) ]
        elif FSharpType.IsTuple ty then
            let elements = FSharpType.GetTupleElements ty

            FSharpValue.MakeTuple(
                elements |> Array.mapi (fun index elementTy -> decodeRaw elementTy (idx raw index)),
                ty
            )
        elif FSharpType.IsRecord ty then
            let values =
                FSharpType.GetRecordFields ty
                |> Array.map (fun field -> decodeRaw field.PropertyType (propGet raw field.Name))

            FSharpValue.MakeRecord(ty, values)
        elif FSharpType.IsUnion ty then
            let caseName = asString (idx raw 0)

            let unionCase =
                FSharpType.GetUnionCases ty
                |> Array.tryFind (fun candidate -> candidate.Name = caseName)
                |> Option.defaultWith (fun () ->
                    failwith ("derived codec: unknown union case '" + caseName + "' for " + name))

            let values =
                unionCase.GetFields()
                |> Array.mapi (fun index field -> decodeRaw field.PropertyType (idx raw (index + 1)))

            FSharpValue.MakeUnion(unionCase, values)
        else
            raw

    let decodeWith (ty: Type) (text: string) : obj = decodeRaw ty (parseRaw text)

// ── Step execution: journaled retry policy + failure-as-value ─────────────
//
// The kernel journals no ActivityFailed record and a throwing handler fails
// the whole host tick — so L2 step handlers NEVER throw: the wrapper applies
// the retry policy (journaled with the step, inside the activity input) and
// returns an encoded StepOutcome. The Call continuation turns failure
// outcomes back into the contract's DurableStepFailed exception.

type internal StepPolicy =
    { A: int // total attempts (>= 1)
      Ms: float // first delay between attempts
      F: float } // backoff factor

type internal StepEnvelope = { Pol: StepPolicy; P: string }

type internal StepOutcome =
    | SOk of string
    | SFail of string
    | STerm of string

/// Raised (via the contract's `Step.terminal`) inside a step implementation
/// to mark the failure terminal: one attempt, retries bypassed.
exception TerminalStepSignal of string

[<RequireQualifiedAccess>]
module internal StepWire =
    let private envelopeTy = typeof<StepEnvelope>
    let private outcomeTy = typeof<StepOutcome>

    let encodeEnvelope (envelope: StepEnvelope) : string =
        Codec.encodeWith envelopeTy (box envelope)

    let decodeEnvelope (text: string) : StepEnvelope =
        Codec.decodeWith envelopeTy text |> unbox

    let encodeOutcome (outcome: StepOutcome) : string = Codec.encodeWith outcomeTy (box outcome)

    let decodeOutcome (text: string) : StepOutcome = Codec.decodeWith outcomeTy text |> unbox

    /// Wrap a payload-typed handler as the registered activity handler:
    /// decode the envelope, run the attempts, never throw.
    let wrapHandler (run: string -> Async<string>) : string -> Async<string> =
        fun raw ->
            async {
                let envelope = decodeEnvelope raw
                let attempts = max 1 envelope.Pol.A

                let rec go attempt delay =
                    async {
                        let! result = Async.Catch(run envelope.P)

                        match result with
                        | Choice1Of2 encoded -> return SOk encoded
                        | Choice2Of2 error ->
                            match error with
                            | TerminalStepSignal message -> return STerm message
                            | _ ->
                                if attempt >= attempts then
                                    return SFail error.Message
                                else
                                    do! Interop.sleepUnref (int delay)
                                    return! go (attempt + 1) (delay * envelope.Pol.F)
                    }

                let! outcome = go 1 (max 0.0 envelope.Pol.Ms)
                return encodeOutcome outcome
            }

// ── Free-monad combinators over the kernel's Durable<'a> ──────────────────

[<RequireQualifiedAccess>]
module internal Programs =
    /// try/with over the free monad: exceptions raised while building the
    /// program or inside resumed continuations (pure workflow code, decoders,
    /// failure-outcome raises) route to the handler across suspension points.
    let rec catch (thunk: unit -> Durable<'a>) (handler: exn -> Durable<'a>) : Durable<'a> =
        let attempt =
            try
                Ok(thunk ())
            with error ->
                Error error

        match attempt with
        | Error error -> handler error
        | Ok program ->
            match program with
            | Return value -> Return value
            | Perform(activity, k) -> Perform(activity, (fun value -> catch (fun () -> k value) handler))
            | PerformAll(activities, k) -> PerformAll(activities, (fun values -> catch (fun () -> k values) handler))
            | Await(key, k) -> Await(key, (fun value -> catch (fun () -> k value) handler))
            | WhenAny(tasks, k) -> WhenAny(tasks, (fun winner -> catch (fun () -> k winner) handler))
            | CurrentTime k -> CurrentTime(fun timestamp -> catch (fun () -> k timestamp) handler)
            | Log(message, k) -> Log(message, (fun () -> catch (fun () -> k ()) handler))

    let rec whileLoop (guard: unit -> bool) (body: unit -> Durable<unit>) : Durable<unit> =
        if guard () then
            body () |> KD.bind (fun () -> whileLoop guard body)
        else
            Return()

/// Fan-out lowering: step calls are `Perform` nodes with pure decode
/// continuations, so parallel composition rewrites them into the kernel's
/// `PerformAll`. Anything else falls back to sequential bind (deterministic).
[<RequireQualifiedAccess>]
module internal Fanout =
    let private asPerform =
        function
        | Perform(activity, k) -> Some(activity, k)
        | _ -> None

    let pair (left: Durable<'a>) (right: Durable<'b>) : Durable<'a * 'b> =
        match asPerform left, asPerform right with
        | Some(leftActivity, leftK), Some(rightActivity, rightK) ->
            PerformAll(
                [ leftActivity; rightActivity ],
                function
                | [ leftValue; rightValue ] ->
                    leftK leftValue
                    |> KD.bind (fun a -> rightK rightValue |> KD.map (fun b -> a, b))
                | values -> failwith ("fanout pair: unexpected value count " + string (List.length values))
            )
        | _ -> left |> KD.bind (fun a -> right |> KD.map (fun b -> a, b))

    let all (programs: Durable<'o> list) : Durable<'o list> =
        let performs = programs |> List.map asPerform

        let sequential () =
            (Return [], programs)
            ||> List.fold (fun acc program ->
                acc
                |> KD.bind (fun values -> program |> KD.map (fun value -> value :: values)))
            |> KD.map List.rev

        if not (List.isEmpty performs) && performs |> List.forall Option.isSome then
            let pairs = performs |> List.choose id

            PerformAll(
                pairs |> List.map fst,
                fun values ->
                    (Return [], List.zip pairs values)
                    ||> List.fold (fun acc ((_, k), value) ->
                        acc
                        |> KD.bind (fun outputs -> k value |> KD.map (fun output -> output :: outputs)))
                    |> KD.map List.rev
            )
        else
            sequential ()

// ── The L2 program representation ──────────────────────────────────────────
//
// A Workflow<'a> wraps a WfNode<'a>. Primitive waits keep their race shape
// (WaitSpec) so `Workflow.select` can combine them into ONE kernel WhenAny;
// everything else is an opaque kernel program. Lowering a wait to the kernel
// weaves in the reserved cancellation signal: cancellation is delivered at
// wait boundaries as a catchable exception (supplied by the contract file).

type internal WaitSpec<'a> =
    { NeedsNow: bool
      Arity: int
      Tasks: int64 -> RaceTask list
      Project: int -> RaceResult -> Durable<'a> }

type internal WfNode<'a> =
    | NProg of Durable<'a>
    | NWait of WaitSpec<'a>

[<RequireQualifiedAccess>]
module internal WaitSpec =
    let map (f: 'a -> 'b) (spec: WaitSpec<'a>) : WaitSpec<'b> =
        { NeedsNow = spec.NeedsNow
          Arity = spec.Arity
          Tasks = spec.Tasks
          Project = fun baseIndex result -> spec.Project baseIndex result |> KD.map f }

    let combine (specs: WaitSpec<'a> list) : WaitSpec<'a> =
        { NeedsNow = specs |> List.exists (fun spec -> spec.NeedsNow)
          Arity = specs |> List.sumBy (fun spec -> spec.Arity)
          Tasks = fun now -> specs |> List.collect (fun spec -> spec.Tasks now)
          Project =
            fun baseIndex result ->
                let index =
                    match result with
                    | ActivityWon(i, _) -> i
                    | EventWon(i, _, _) -> i

                let rec pick offset remaining =
                    match remaining with
                    | [] -> failwith "select: race winner index out of range"
                    | (spec: WaitSpec<'a>) :: rest ->
                        if index < baseIndex + offset + spec.Arity then
                            spec.Project (baseIndex + offset) result
                        else
                            pick (offset + spec.Arity) rest

                pick 0 specs }

[<RequireQualifiedAccess>]
module internal Node =
    /// Reserved per-instance cancellation signal. `Run.Cancel` raises it; every
    /// lowered wait races it; winning raises the contract's DurableCancelled.
    let cancelSignalName = "firegrid/cancel"

    let currentTimeNode: WfNode<float> = NProg(CurrentTime(fun timestamp -> Return(float timestamp)))

    let lower (cancelled: unit -> exn) (node: WfNode<'a>) : Durable<'a> =
        match node with
        | NProg program -> program
        | NWait spec ->
            let race now =
                let tasks = spec.Tasks now

                WhenAny(
                    tasks @ [ RaceEvent(EventKey.Signal cancelSignalName) ],
                    fun result ->
                        let index =
                            match result with
                            | ActivityWon(i, _) -> i
                            | EventWon(i, _, _) -> i

                        if index = spec.Arity then
                            raise (cancelled ())
                        else
                            spec.Project 0 result
                )

            if spec.NeedsNow then
                CurrentTime(fun now -> race now)
            else
                race 0L

// ── Registration bag: what `reg …` folds into a worker ─────────────────────

/// String-typed entity runtime: the contract's Wiring closes the Decider's
/// generics into these closures at define time.
type internal EntityRuntimeSpec =
    { Name: string
      Initial: obj
      Evolve: obj -> string -> obj // state, encoded event → state
      Decide: string -> string -> obj -> string * string list } // key, encoded cmd, state → encoded reply, encoded events

type internal RegBag =
    { App: DurableApp
      Entities: EntityRuntimeSpec list }

[<RequireQualifiedAccess>]
module internal RegBag =
    let empty: RegBag = { App = KApp.empty; Entities = [] }

// ── Entity runtime: durable FIFO inbox admission + fenced commit ───────────
//
// Streams per key: journal `<entity>/<key>` (header-tagged records: t=e
// events, t=m decide markers), inbox `<entity>/<key>/in` (command
// envelopes). Reply + events commit as ONE fenced append; the marker carries
// the reply and the (source, seq) provenance for dedupe and caller pickup.

type internal CmdEnvelope = { Src: string; Seq: float; Cmd: string }

type internal EntityMarker =
    { Src: string
      Seq: float
      Cur: float // exclusive inbox cursor after this command
      Reply: string }

[<RequireQualifiedAccess>]
module internal EntityRun =
    let private cmdTy = typeof<CmdEnvelope>
    let private markerTy = typeof<EntityMarker>

    let journalName (entityName: string) (key: string) = entityName + "/" + key
    let inboxName (entityName: string) (key: string) = journalName entityName key + "/in"

    let private hasHeader (key: string) (value: string) (record: S2.ReadRecord) =
        record.Headers |> List.exists (fun (hk, hv) -> hk = key && hv = value)

    /// Full contents of a stream (empty when the stream has no records yet).
    /// checkTail-guarded (no 416 reads on empty streams) and PAGINATED — a
    /// single S2 read returns at most one batch (1000 records).
    let readAllRecords (stream: S2.Stream) : Async<S2.ReadRecord list> =
        async {
            try
                let! tail = stream |> S2.checkTail

                if tail.SeqNum <= 0L then
                    return []
                else
                    let rec page (from: int64) acc =
                        async {
                            if from >= tail.SeqNum then
                                return List.rev acc
                            else
                                let! records =
                                    stream
                                    |> S2.readWith
                                        { S2.ReadOptions.empty with
                                            Start = Some(S2.FromSeqNum from)
                                            Clamp = true
                                            IgnoreCommandRecords = true }

                                match List.rev records with
                                | [] -> return List.rev acc // only command records remain
                                | (last: S2.ReadRecord) :: _ ->
                                    let acc = (acc, records) ||> List.fold (fun state record -> record :: state)
                                    return! page (last.SeqNum + 1L) acc
                        }

                    return! page 0L []
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return []
                | _ -> return raise error
        }

    let private markersOf (records: S2.ReadRecord list) =
        records
        |> List.filter (hasHeader "t" "m")
        |> List.map (fun record -> Codec.decodeWith markerTy record.Body |> unbox<EntityMarker>)

    let submitCommand (basin: S2.Basin) (entityName: string) (key: string) (source: string) (seq: float) (encodedCmd: string) : Async<unit> =
        async {
            let envelope = { Src = source; Seq = seq; Cmd = encodedCmd }

            let! _ =
                basin
                |> S2.stream (inboxName entityName key)
                |> S2.append [ S2.Record.text (Codec.encodeWith cmdTy (box envelope)) ]

            return ()
        }

    let awaitReply (basin: S2.Basin) (entityName: string) (key: string) (source: string) (seq: float) : Async<string> =
        let journal = basin |> S2.stream (journalName entityName key)

        let rec wait () =
            async {
                let! records = readAllRecords journal

                let reply =
                    markersOf records
                    |> List.tryPick (fun marker ->
                        if marker.Src = source && marker.Seq = seq then
                            Some marker.Reply
                        else
                            None)

                match reply with
                | Some body -> return body
                | None ->
                    do! Interop.sleepUnref 80
                    return! wait ()
            }

        wait ()

    /// One admission pass for one key: fold the journal, read fresh commands
    /// from the inbox cursor, decide + commit each under a fresh fence.
    /// Returns true when the pass processed anything.
    let drive (spec: EntityRuntimeSpec) (basin: S2.Basin) (key: string) : Async<bool> =
        async {
            let journal = basin |> S2.stream (journalName spec.Name key)
            let inbox = basin |> S2.stream (inboxName spec.Name key)

            let! records = readAllRecords journal
            let markers = markersOf records
            let processed = markers |> List.map (fun marker -> marker.Src, marker.Seq) |> Set.ofList
            let cursor = markers |> List.fold (fun acc marker -> max acc (int64 marker.Cur)) 0L

            let initialState =
                records
                |> List.filter (hasHeader "t" "e")
                |> List.fold (fun state record -> spec.Evolve state record.Body) spec.Initial

            let! pending =
                async {
                    try
                        let! tail = inbox |> S2.checkTail

                        if cursor >= tail.SeqNum then
                            return []
                        else
                            return!
                                inbox
                                |> S2.readWith
                                    { S2.ReadOptions.empty with
                                        Start = Some(S2.FromSeqNum cursor)
                                        Clamp = true }
                    with error ->
                        match S2Errors.classify error with
                        | S2Errors.RangeNotSatisfiable _ -> return []
                        | _ -> return raise error
                }

            if List.isEmpty pending then
                return false
            else
                // Claim the key: a fresh fence on the journal. A deposed
                // holder computes but cannot commit.
                let fence = "l2e/" + Interop.entropy ()
                let! _ = journal |> S2.append [ S2.Record.fence fence ]
                let appendOptions = S2.AppendOptions.none |> S2.AppendOptions.fencingToken fence

                let mutable state = initialState
                let mutable processedNow = processed
                let mutable deposed = false

                for record in pending do
                    if not deposed then
                        let envelope = Codec.decodeWith cmdTy record.Body |> unbox<CmdEnvelope>

                        if not (processedNow.Contains(envelope.Src, envelope.Seq)) then
                            let replyBody, eventBodies = spec.Decide key envelope.Cmd state

                            let marker =
                                { Src = envelope.Src
                                  Seq = envelope.Seq
                                  Cur = float (record.SeqNum + 1L)
                                  Reply = replyBody }

                            let commitRecords =
                                [ for eventBody in eventBodies -> S2.Record.textWith [ "t", "e" ] eventBody ]
                                @ [ S2.Record.textWith [ "t", "m" ] (Codec.encodeWith markerTy (box marker)) ]

                            let! commit = journal |> S2.tryAppendWith appendOptions commitRecords

                            match commit with
                            | Ok _ ->
                                state <- eventBodies |> List.fold spec.Evolve state
                                processedNow <- processedNow.Add(envelope.Src, envelope.Seq)
                            | Error(S2Errors.FencingTokenMismatch _) -> deposed <- true
                            | Error failure ->
                                deposed <- true
                                Interop.consoleError ("Firegrid.Durable entity commit failed: " + string failure)

                return true
        }

// ── Outcome stream: `<instance>/out` ───────────────────────────────────────
//
// The kernel journals no terminal record; the L2 worker appends the encoded
// terminal outcome to a dedicated stream once the kernel host reports
// Completed. `Run.Result`/`Run.Status` (including handles reattached by id
// with no program in hand) read it from any process.

[<RequireQualifiedAccess>]
module internal OutStream =
    let private outName (key: string) = key + "/out"

    let ensure (basin: S2.Basin) (key: string) : Async<unit> = basin |> S2.ensureStream (outName key)

    let private readAttempt (basin: S2.Basin) (key: string) : Async<Result<string option, exn>> =
        async {
            try
                // checkTail first: polling an EMPTY stream must not issue a
                // read at all (a 416 per poll; under load the SDK has leaked
                // one as an unhandled rejection, killing the process).
                let stream = basin |> S2.stream (outName key)
                let! tail = stream |> S2.checkTail

                if tail.SeqNum <= 0L then
                    return Ok None
                else
                    let! records =
                        stream
                        |> S2.readWith
                            { S2.ReadOptions.empty with
                                Start = Some(S2.FromSeqNum 0L)
                                Count = Some 1
                                Clamp = true
                                IgnoreCommandRecords = true }

                    return Ok(records |> List.tryHead |> Option.map (fun record -> record.Body))
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return Ok None
                | _ -> return Error error
        }

    /// Read the terminal outcome, if written. Resilient to the stream not
    /// existing yet (attach-by-id may race the first worker pass).
    let readOutcome (basin: S2.Basin) (key: string) : Async<string option> =
        async {
            let! first = readAttempt basin key

            match first with
            | Ok value -> return value
            | Error _ ->
                do! ensure basin key
                let! second = readAttempt basin key

                match second with
                | Ok value -> return value
                | Error error -> return raise error
        }

    let awaitOutcome (basin: S2.Basin) (key: string) : Async<string> =
        let rec wait () =
            async {
                let! outcome = readOutcome basin key

                match outcome with
                | Some body -> return body
                | None ->
                    do! Interop.sleepUnref 100
                    return! wait ()
            }

        wait ()

    let writeOnce (basin: S2.Basin) (key: string) (payload: string) : Async<unit> =
        async {
            do! ensure basin key
            let! existing = readOutcome basin key

            match existing with
            | Some _ -> return ()
            | None ->
                let! _ = basin |> S2.stream (outName key) |> S2.append [ S2.Record.text payload ]
                return ()
        }

// ── The namespace worker loop ──────────────────────────────────────────────
//
// Discovers work (any `…/in` stream in the basin), drives workflow instances
// through the kernel host (mailbox admission → stepper → signal delivery →
// activity/timer adapters), drives entity keys through the admission pass,
// and journals terminal outcomes. Liveness: any live worker keeps everything
// moving; parked work costs one Waiting tick per pass.

[<RequireQualifiedAccess>]
module internal WorkerLoop =
    let start (basin: S2.Basin) (bag: RegBag) : unit -> Async<unit> =
        let hostId = "l2" + Interop.shortEntropy () // <= 15 chars (kernel limit)

        let worker =
            KApp.workerWith
                { Storage = DurableStorage.s2 basin
                  HostId = hostId
                  MaxRunUntilIdleTicks = Some 400 }
                bag.App

        let mutable stopRequested = false
        let mutable finished = false
        let written = System.Collections.Generic.HashSet<string>()

        // REAL workflow progress only. The kernel's own idle detection counts
        // adapter checkpoints as progress, and the activity/timer adapters
        // re-checkpoint whenever the OTHER adapter's checkpoint record lands
        // past their cursor — mutual ping-pong, so `runUntilIdle` never
        // idles: it spins its full tick budget on every parked instance and
        // bloats the journal with checkpoint records (observed: 52k records
        // for 55 loop iterations). We drive `runOnce` ourselves and continue
        // only on mailbox folds, step commits, signal deliveries, executed
        // activities, or fired timers.
        let realProgress (report: DurableHostTickReport<Payload>) =
            (match report.Inbox with
             | Some inbox -> inbox.Commit.IsSome
             | None -> false)
            || (match report.Step with
                | Some(DurableHostStatus.Committed _) -> true
                | _ -> false)
            || (match report.Signals with
                | Some signals -> signals.Delivered.IsSome
                | None -> false)
            || (match report.Activities with
                | Some activities -> not (List.isEmpty activities.Completed)
                | None -> false)
            || (match report.Timers with
                | Some timers -> not (List.isEmpty timers.Published)
                | None -> false)

        let driveWorkflow (key: string) : Async<bool> =
            async {
                let instanceId = InstanceId.create key
                let mutable keepTicking = true
                let mutable budget = 300
                let mutable active = false
                let mutable completion = None

                while keepTicking && budget > 0 && not stopRequested do
                    budget <- budget - 1
                    let! tick = worker.runOnce instanceId

                    match tick with
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed(value, _)) ->
                        completion <- Some value
                        // Already-recorded completions are idle, not progress:
                        // a finished instance must not hot-loop the worker.
                        if not (written.Contains key) then
                            active <- true

                        keepTicking <- false
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced report) ->
                        if realProgress report then
                            active <- true
                        else
                            keepTicking <- false
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Waiting _)
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Deposed _)
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Failed _)
                    | DurableWorkflowHostStatus.Deposed _
                    | DurableWorkflowHostStatus.Failed _ -> keepTicking <- false

                match completion with
                | Some payload when not (written.Contains key) ->
                    do! OutStream.writeOnce basin key payload
                    written.Add key |> ignore
                | _ -> ()

                return active
            }

        let pass () =
            async {
                let! streams = basin |> S2.listStreamsWith ""

                let inboxKeys =
                    streams
                    |> List.choose (fun stream ->
                        if stream.DeletedAt.IsNone && stream.Name.EndsWith "/in" then
                            Some(stream.Name.Substring(0, stream.Name.Length - 3))
                        else
                            None)
                    |> List.distinct

                let mutable active = false

                for key in inboxKeys do
                    if not stopRequested then
                        match bag.Entities |> List.tryFind (fun spec -> key.StartsWith(spec.Name + "/")) with
                        | Some spec ->
                            let entityKey = key.Substring(spec.Name.Length + 1)
                            let! entityActive = EntityRun.drive spec basin entityKey
                            if entityActive then active <- true
                        | None ->
                            let! workflowActive = driveWorkflow key
                            if workflowActive then active <- true

                return active
            }

        let rec loop (consecutiveErrors: int) =
            async {
                if stopRequested then
                    finished <- true
                else
                    let! outcome = Async.Catch(pass ())

                    match outcome with
                    | Choice2Of2 error when consecutiveErrors >= 2 ->
                        // Terminate on persistent infrastructure errors so a
                        // dangling worker can never wedge the corpus process;
                        // durable state is safe — another worker resumes.
                        Interop.consoleError ("Firegrid.Durable worker loop stopped: " + error.Message)
                        finished <- true
                    | Choice2Of2 error ->
                        // Transient hiccup (loaded s2, connection blip): retry.
                        Interop.consoleError ("Firegrid.Durable worker pass failed (retrying): " + error.Message)
                        do! Interop.sleepUnref 150
                        return! loop (consecutiveErrors + 1)
                    | Choice1Of2 active ->
                        do! Interop.sleepUnref (if active then 15 else 120)
                        return! loop 0
            }

        Async.StartAsPromise(loop 0) |> ignore

        fun () ->
            async {
                stopRequested <- true

                while not finished do
                    do! Interop.sleepUnref 25
            }
