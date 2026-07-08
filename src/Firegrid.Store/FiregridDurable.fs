// SPIKE S0 — throwaway experiment. `Firegrid.Durable` L2 surface slice, built
// to probe the ergonomics contract in docs/sdds/api-layering-sdd.md against
// the L1 kernel (Foundation/Durable). Nothing here ships; see SPIKE-FINDINGS.md.
namespace Firegrid.Durable

open Fable.Core
open Firegrid.Log
open Firegrid.Store
open Firegrid.Store.Foundation.Durable
open Firegrid.Store.Foundation.Durable.App

// Kernel module handles (the App namespace also exports `Workflow`/`Signal`/
// `Activity` modules; abbreviations keep this file unambiguous).
module KD = Firegrid.Store.Foundation.Durable.Durable
module KApp = Firegrid.Store.Foundation.Durable.App.DurableApp
module KActivity = Firegrid.Store.Foundation.Durable.App.Activity
module KWorkflow = Firegrid.Store.Foundation.Durable.App.Workflow

/// T3 — zero-codec derivation. Encoding: Fable's own `toJSON` shapes are
/// deterministic and name-tagged (records → `{ field: … }` in declaration
/// order; DUs → `["CaseName", …fields]`; lists → arrays), so JSON.stringify
/// IS the derived wire encoding. Decoding: JSON.parse yields plain JS data
/// that Fable pattern matching cannot consume (unions need instances, lists
/// need cons cells), so decode is TYPE-DIRECTED via Fable's reflection info
/// (`inline` + `typeof<'t>` resolves the type at each concrete call site).
/// FINDING: "zero codec parameters" is achievable, but not via naive
/// stringify/parse — it costs a reflection walker (or a Thoth.Json.Auto-style
/// dependency, or compile-time codegen) on the decode side.
[<RequireQualifiedAccess>]
module Codec =
    open FSharp.Reflection

    [<Emit("$0.length")>]
    let private rawLength (_raw: obj) : int = jsNative

    [<Emit("$0[$1]")>]
    let private rawIndex (_raw: obj) (_index: int) : obj = jsNative

    let encode (value: 't) : string =
        let text = JsJson.stringify (box value)
        if JsJson.isNullish (box text) then "null" else text

    let rec ofRaw (targetType: System.Type) (raw: obj) : obj =
        if JsJson.isNullish raw then
            raw
        elif
            targetType.FullName = "System.String"
            || targetType.FullName = "System.Int32"
            || targetType.FullName = "System.Double"
            || targetType.FullName = "System.Boolean"
        then
            raw
        elif targetType.FullName.StartsWith "Microsoft.FSharp.Collections.FSharpList" then
            let elementType = targetType.GetGenericArguments().[0]

            box
                [ for index in 0 .. rawLength raw - 1 do
                      yield ofRaw elementType (rawIndex raw index) ]
        elif FSharpType.IsRecord targetType then
            let values =
                FSharpType.GetRecordFields targetType
                |> Array.map (fun field -> ofRaw field.PropertyType (JsJson.prop raw field.Name))

            FSharpValue.MakeRecord(targetType, values)
        elif FSharpType.IsUnion targetType then
            let caseName = JsJson.stringValue (rawIndex raw 0)

            let case =
                FSharpType.GetUnionCases targetType
                |> Array.find (fun candidate -> candidate.Name = caseName)

            let values =
                case.GetFields()
                |> Array.mapi (fun index field -> ofRaw field.PropertyType (rawIndex raw (index + 1)))

            FSharpValue.MakeUnion(case, values)
        else
            raw

    let inline decode<'t> (text: string) : 't =
        ofRaw typeof<'t> (JsJson.parse<obj> text) |> unbox<'t>

type Id = Id of string

type Timestamp = float

type Duration = private DurationMs of float

module Duration =
    let millis (value: float) = DurationMs value
    let seconds (value: float) = DurationMs(value * 1000.0)
    let minutes (value: float) = DurationMs(value * 60_000.0)
    let hours (value: int) = DurationMs(float value * 3_600_000.0)
    let days (value: int) = DurationMs(float value * 86_400_000.0)
    let internal toMillis (DurationMs value) = int64 value

/// `Error Timeout` in signal awaits, per the SDD samples.
type SignalTimeout = Timeout

/// Fan-out lowering: step calls are `Perform` nodes with pure decode
/// continuations, so parallel composition can rewrite them into the kernel's
/// `PerformAll`. Anything else falls back to sequential bind (deterministic,
/// but not fanned out) — a documented spike limitation.
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

module internal Programs =
    /// try/with over the free monad: exceptions raised while building the
    /// program or inside resumed continuations (pure workflow code, decoders)
    /// are routed to the handler, across suspension points. Step-handler
    /// failures on the host are NOT routed here — the kernel has no
    /// ActivityFailed record (see findings).
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

/// Registration is data: `Worker.run basin "prod" [ reg reserve; reg checkout ]`.
type IRegistrable =
    abstract member Register: DurableApp -> DurableApp

type Signal<'t> =
    { Name: string
      Decode: string -> 't }

    /// Parks on the signal raced against a durable timer. Deadline is derived
    /// from journaled CurrentTime, so replay is deterministic.
    member signal.Await(timeout: Duration) : Durable<Result<'t, SignalTimeout>> =
        KD.currentTime
        |> KD.bind (fun now ->
            let deadline = now + Duration.toMillis timeout

            KD.whenAny [ RaceEvent(EventKey.Signal signal.Name); RaceEvent(EventKey.Timer deadline) ]
            |> KD.map (fun winner ->
                match winner with
                | EventWon(_, EventKey.Signal _, payload) -> Ok(signal.Decode payload)
                | EventWon(_, EventKey.Timer _, _) -> Error Timeout
                | ActivityWon _ -> failwith "signal await: unexpected activity winner"))

module Signal =
    let inline define<'t> (name: string) : Signal<'t> =
        { Name = name
          Decode = fun text -> Codec.decode<'t> text }

type Step<'i, 'o> =
    { Name: string
      Handler: 'i -> Async<'o>
      DecodeInput: string -> 'i
      DecodeOutput: string -> 'o }

    /// Journaled call; replay-served.
    member step.Call(input: 'i) : Durable<'o> =
        let activity: Activity =
            { Name = step.Name
              Input = Codec.encode input }

        Perform(activity, (fun value -> Return(step.DecodeOutput value)))

    /// Journaled, output discarded. DELTA vs SDD prose: the kernel has no
    /// fire-and-forget Perform, so Send still waits for handler completion
    /// before the next bind (see findings).
    member step.Send(input: 'i) : Durable<unit> =
        let activity: Activity =
            { Name = step.Name
              Input = Codec.encode input }

        Perform(activity, (fun _ -> Return()))

    interface IRegistrable with
        member step.Register app =
            app
            |> KApp.addActivity (
                KActivity.defineWith step.Name Codec.encode step.DecodeInput Codec.encode step.DecodeOutput step.Handler
            )

module Step =
    let inline define (name: string) (handler: 'i -> Async<'o>) : Step<'i, 'o> =
        { Name = name
          Handler = handler
          DecodeInput = fun text -> Codec.decode<'i> text
          DecodeOutput = fun text -> Codec.decode<'o> text }

type GridClient =
    { Basin: S2.Basin
      Source: string
      mutable NextSignalSeq: int64 }

module Client =
    [<Emit("Date.now().toString(36) + '-' + Math.random().toString(36).slice(2)")>]
    let private entropy () : string = jsNative

    let connect (basin: S2.Basin) : GridClient =
        { Basin = basin
          Source = "client:" + entropy ()
          NextSignalSeq = 0L }

/// Typed handle from a workflow descriptor (SDD: `Run<Receipt>`).
type Run<'o> =
    { Instance: InstanceId
      RaiseRaw: string -> string -> Async<unit>
      PollOnce: unit -> Async<'o option> }

    member run.Signal (signal: Signal<'t>) (payload: 't) : Async<unit> =
        run.RaiseRaw signal.Name (Codec.encode payload)

    /// Polls status until the journal replays to completion. Requires a
    /// worker ticking the instance (the wake/park mechanics stay in L1).
    member run.Result: Async<'o> =
        let rec wait attempts =
            async {
                let! value = run.PollOnce()

                match value with
                | Some output -> return output
                | None when attempts <= 0 -> return failwith "run.Result: timed out waiting for completion"
                | None ->
                    do! Async.Sleep 100
                    return! wait (attempts - 1)
            }

        wait 600

/// Share the contract, not the impl (SDD: declare/implement).
type WorkflowDecl<'i, 'o> =
    { DeclName: string
      DeclDecodeOutput: string -> 'o }

    /// DELTA: lowers to a journaled activity call named `<decl>/child`. The
    /// kernel has no child-workflow primitive, so execution of the child is a
    /// documented gap (see findings) — the call compiles and journals.
    member decl.CallChild(input: 'i) : Durable<'o> =
        let activity: Activity =
            { Name = decl.DeclName + "/child"
              Input = Codec.encode input }

        Perform(activity, (fun value -> Return(decl.DeclDecodeOutput value)))

type WorkflowDef<'i, 'o> =
    { Name: string
      Factory: 'i -> Durable<'o>
      DecodeInput: string -> 'i
      DecodeOutput: string -> 'o }

    member workflowDef.Start (client: GridClient) (input: 'i) (instance: Id) : Async<Run<'o>> =
        async {
            let (Id instanceName) = instance
            let instanceId = InstanceId.create instanceName

            let! result =
                DurableClient.startWith client.Basin instanceId (WorkflowName.create workflowDef.Name) (Codec.encode input)

            match result with
            | DurableClientStartStatus.Accepted _ -> ()
            | DurableClientStartStatus.Failed failure -> failwith (sprintf "workflow start failed: %A" failure)

            let raiseRaw name payload =
                async {
                    let seq = client.NextSignalSeq
                    client.NextSignalSeq <- seq + 1L

                    let! signalResult =
                        DurableClient.raiseSignalFrom client.Basin instanceId client.Source seq name payload

                    match signalResult with
                    | DurableClientSignalStatus.Accepted _ -> return ()
                    | DurableClientSignalStatus.Failed failure ->
                        return failwith (sprintf "workflow signal failed: %A" failure)
                }

            let pollOnce () =
                async {
                    let key = DurableClient.instanceKey instanceId
                    do! S2Substrate.ensureStreams client.Basin key
                    let pair = S2Substrate.streams client.Basin key

                    let owned =
                        { Key = key
                          Fence = FenceToken "surface:status"
                          Log = pair.Log
                          Inbox = pair.Inbox }

                    let! decoded = S2Substrate.readLogText StepRecordCodec.decode owned

                    let records =
                        decoded
                        |> List.choose (fun (_, entry) ->
                            match entry with
                            | Ok record -> Some record
                            | Error _ -> None)

                    let started =
                        records
                        |> List.tryPick (function
                            | Incoming(WorkflowStarted(_, startInput)) -> Some startInput
                            | _ -> None)

                    match started with
                    | None -> return None
                    | Some startInput ->
                        let history = DurableStepper.historyFromRecords records

                        match DurableStepper.plan 0L history (workflowDef.Factory(workflowDef.DecodeInput startInput)) with
                        | Complete value -> return Some value
                        | Commit _
                        | Waiting _ -> return None
                }

            return
                { Instance = instanceId
                  RaiseRaw = raiseRaw
                  PollOnce = pollOnce }
        }

    interface IRegistrable with
        member workflowDef.Register app =
            app
            |> KApp.addWorkflow (
                KWorkflow.defineWith
                    workflowDef.Name
                    Codec.encode
                    workflowDef.DecodeInput
                    Codec.encode
                    workflowDef.DecodeOutput
                    workflowDef.Factory
            )
            |> KApp.addActivity (
                KActivity.define (workflowDef.Name + "/child") (fun (_: string) ->
                    async {
                        return
                            failwith (
                                "spike gap: child workflows journal but cannot execute — the kernel has no child-workflow primitive ("
                                + workflowDef.Name
                                + ")"
                            )
                    })
            )

/// T5 — generation rollover value (SDD: ContinueAsNew eternal workflows).
type Rollover<'state, 'o> =
    | ContinueAsNew of 'state
    | Finish of 'o

module Workflow =
    let inline define (name: string) (factory: 'i -> Durable<'o>) : WorkflowDef<'i, 'o> =
        { Name = name
          Factory = factory
          DecodeInput = fun text -> Codec.decode<'i> text
          DecodeOutput = fun text -> Codec.decode<'o> text }

    let inline declare<'i, 'o> (name: string) : WorkflowDecl<'i, 'o> =
        { DeclName = name
          DeclDecodeOutput = fun text -> Codec.decode<'o> text }

    let sleepUntil (deadline: Timestamp) : Durable<unit> =
        Await(EventKey.Timer(int64 deadline), (fun _ -> Return()))

    let all (programs: Durable<'o> list) : Durable<'o list> = Fanout.all programs

module Worker =
    let inline implement (decl: WorkflowDecl<'i, 'o>) (factory: 'i -> Durable<'o>) : WorkflowDef<'i, 'o> =
        { Name = decl.DeclName
          Factory = factory
          DecodeInput = fun text -> Codec.decode<'i> text
          DecodeOutput = fun text -> Codec.decode<'o> text }

    let runWith (basin: S2.Basin) (hostId: string) (maxTicks: int) (registrations: IRegistrable list) : DurableAppWorker =
        let app =
            (KApp.empty, registrations)
            ||> List.fold (fun app registrable -> registrable.Register app)

        KApp.workerWith
            { Storage = DurableStorage.s2 basin
              HostId = hostId
              MaxRunUntilIdleTicks = Some maxTicks }
            app

    /// DELTA vs SDD sample: returns the worker handle; the caller drives the
    /// tick loop (`runForever`/`runUntilIdle`). The sample reads as if `run`
    /// also starts it.
    let run basin hostId registrations = runWith basin hostId 100 registrations

/// T1 — the `workflow { }` builder. Delay is GUARDED (returns the thunk; Run
/// forces one layer): recursive `return! drive (n + 1)` builds one suspended
/// iteration at a time, so replay unfolds iterations with a flat stack.
type WorkflowBuilder() =
    member _.Return(value: 'a) : Durable<'a> = Return value

    member _.ReturnFrom(program: Durable<'a>) : Durable<'a> = program

    member _.Bind(program: Durable<'a>, binder: 'a -> Durable<'b>) : Durable<'b> = KD.bind binder program

    member _.Zero() : Durable<unit> = Return()

    member _.Delay(thunk: unit -> Durable<'a>) : unit -> Durable<'a> = thunk

    member _.Run(thunk: unit -> Durable<'a>) : Durable<'a> = thunk ()

    member _.Combine(first: Durable<unit>, second: unit -> Durable<'a>) : Durable<'a> =
        first |> KD.bind (fun () -> second ())

    member _.While(guard: unit -> bool, body: unit -> Durable<unit>) : Durable<unit> = Programs.whileLoop guard body

    member _.TryWith(body: unit -> Durable<'a>, handler: exn -> Durable<'a>) : Durable<'a> = Programs.catch body handler

    member _.MergeSources(left: Durable<'a>, right: Durable<'b>) : Durable<'a * 'b> = Fanout.pair left right

[<AutoOpen>]
module Syntax =
    let workflow = WorkflowBuilder()

    let reg (registrable: #IRegistrable) : IRegistrable = registrable :> IRegistrable
