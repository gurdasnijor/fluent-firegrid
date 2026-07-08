// SPIKE S0 — throwaway fail-fast experiment for the Firegrid API-layering SDD
// ergonomics (docs/sdds/api-layering-sdd.md). Runs OUTSIDE the proof runner:
//   pnpm --filter @firegrid/foundation-proofs build && node dist/SpikeS0.js
// Evidence lands in SPIKE-FINDINGS.md at the repo root. Deliberately not
// house-proof style: this file is disposable.
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Log
open Firegrid.Store.Foundation.Durable
open Firegrid.Durable

/// Node interop for the spike driver.
module SpikeNode =
    type ChildProcess =
        abstract kill: signal: string -> bool

    [<Import("spawn", "node:child_process")>]
    let spawn (_command: string) (_args: string array) (_options: obj) : ChildProcess = jsNative

    let private fs: obj = importAll "node:fs"

    [<Emit("$0.readFileSync($1, 'utf8')")>]
    let private readFileWith (_fs: obj) (_path: string) : string = jsNative

    [<Emit("$0.existsSync($1)")>]
    let private existsWith (_fs: obj) (_path: string) : bool = jsNative

    let readFile path = readFileWith fs path
    let exists path = existsWith fs path

    [<Emit("process.argv.slice(2)")>]
    let argv () : string array = jsNative

    [<Emit("process.argv[1]")>]
    let scriptPath () : string = jsNative

    [<Emit("process.execPath")>]
    let nodePath () : string = jsNative

    [<Emit("process.cwd()")>]
    let cwd () : string = jsNative

    [<Emit("process.env[$0] || ''")>]
    let env (_name: string) : string = jsNative

    [<Emit("process.env[$0] = $1")>]
    let setEnv (_name: string) (_value: string) : unit = jsNative

    [<Emit("Object.assign({}, process.env, $0)")>]
    let withProcessEnv (_extra: obj) : obj = jsNative

    [<Emit("process.exitCode = $0")>]
    let setExitCode (_code: int) : unit = jsNative

    [<Emit("console.log($0)")>]
    let log (_message: string) : unit = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let sleepPromise (_millis: int) : JS.Promise<unit> = jsNative

    let sleep millis = sleepPromise millis |> Async.AwaitPromise

    [<Emit("fetch($0).then(() => true).catch(() => false)")>]
    let fetchReady (_url: string) : JS.Promise<bool> = jsNative

    [<Emit("20000 + Math.floor(Math.random() * 20000)")>]
    let randomPort () : int = jsNative

    [<Emit("process.env.S2_BIN || (process.env.HOME ? process.env.HOME + '/.s2/bin/s2' : 's2')")>]
    let s2Bin () : string = jsNative

/// T4 side channel: execution counts live in files named by env vars so they
/// survive a process kill and are shared with the killed child host process.
module SpikeSideChannel =
    open SpikeNode

    let private fileFor name = env ("SPIKE_COUNT_" + name)

    let read name =
        let file = fileFor name
        if file <> "" && exists file then int (readFile file) else 0

    let bump name =
        let file = fileFor name

        if file <> "" then
            let current = if exists file then int (readFile file) else 0
            Reports.write file (string (current + 1))

/// Drives a Durable program against the kernel Stepper over an in-memory
/// history — the pure replay/journal path with simulated activity + signal
/// responders. Used by T1 (semantics), T2 (flat stack) and T3 (golden bytes).
module SpikeInMemory =
    type DriveConfig =
        { Timestamp: int64
          SimulateActivity: Activity -> Value
          RespondSignal: string -> Value option
          FireTimers: bool
          CaptureJournal: bool }

    type DriveResult<'a> =
        { Value: 'a option
          History: History
          Journal: string list
          Plans: int }

    let drive (config: DriveConfig) (program: Durable<'a>) : DriveResult<'a> =
        let mutable history = History.empty
        let mutable finished: 'a option = None
        let mutable parked = false
        let mutable plans = 0
        let journal = ResizeArray<string>()

        let append (event: Event) =
            if config.CaptureJournal then
                journal.Add(StepRecordCodec.encode (Incoming(HistoryEvent event)))

            history <- History.append event history

        while finished.IsNone && not parked do
            plans <- plans + 1

            if plans > 1_000_000 then
                failwith "in-memory drive did not settle"

            match DurableStepper.plan config.Timestamp history program with
            | Complete value -> finished <- Some value
            | Commit records ->
                for record in records do
                    if config.CaptureJournal then
                        journal.Add(StepRecordCodec.encode record)

                    match record with
                    | Incoming(HistoryEvent event) -> history <- History.append event history
                    | _ -> ()
            | Waiting(opId, need) ->
                match need with
                | NeedsActivity activity -> append (ActivityCompleted(opId, config.SimulateActivity activity))
                | NeedsActivities pending ->
                    for id, activity in pending do
                        append (ActivityCompleted(id, config.SimulateActivity activity))
                | NeedsEvent(Timer _) ->
                    if config.FireTimers then
                        append (TimerFired opId)
                    else
                        parked <- true
                | NeedsEvent(Signal name) ->
                    match config.RespondSignal name with
                    | Some payload -> append (SignalReceived(opId, name, payload))
                    | None -> parked <- true
                | NeedsRace pending ->
                    let responded =
                        pending
                        |> List.tryPick (fun (id, task) ->
                            match task with
                            | RaceEvent(Signal name) ->
                                config.RespondSignal name
                                |> Option.map (fun payload -> id, name, payload)
                            | _ -> None)

                    match responded with
                    | Some(id, name, payload) -> append (SignalReceived(id, name, payload))
                    | None -> parked <- true
                | NeedsTimerCancellation _
                | NeedsCurrentTime
                | NeedsLog _ -> failwith "in-memory drive: planner should have committed this need"

        { Value = finished
          History = history
          Journal = List.ofSeq journal
          Plans = plans }

    /// Kernel replay outcome helper — defined before the sample DUs shadow
    /// the kernel's `Done` case name.
    let replayValue (history: History) (program: Durable<'a>) : 'a option =
        match Durable.replay history program with
        | Done value -> Some value
        | Blocked _ -> None

// ─────────────────────────────────────────────────────────────────────────────
// The SDD samples, as written (handlers filled in — the SDD elides them).
// Domain vocabulary is app-side by doctrine, so it is defined here, not in L2.
// ─────────────────────────────────────────────────────────────────────────────

module SpikeSamples =
    // ── checkout sample vocabulary (SDD "L2 — the public API") ─────────────
    type OrderId = OrderId of string

    module OrderId =
        let value (OrderId value) = value

    type Order = { Id: OrderId; Amount: float }
    type Decision = { Accepted: bool; Approver: string }
    type Reservation = { ReservationId: string }
    type Receipt = { Confirmed: bool; Reference: string }

    module Receipt =
        let confirmed (reservation: Reservation) =
            { Confirmed = true
              Reference = reservation.ReservationId }

        let rejected (OrderId id) = { Confirmed = false; Reference = id }

    // ── Define ──────────────────────────────────────────────────────────────
    let reserve =
        Step.define "orders/reserve" (fun (id: OrderId) ->
            async {
                SpikeSideChannel.bump "reserve"
                return { ReservationId = "res-" + OrderId.value id }
            })

    let notify =
        Step.define "orders/notify" (fun (id: OrderId) ->
            async {
                SpikeSideChannel.bump "notify"
                return "notified:" + OrderId.value id
            })

    let approved = Signal.define<Decision> "orders/approved"

    // ── Orchestrate (SDD sample, verbatim body) ─────────────────────────────
    let checkout =
        Workflow.define "orders/checkout" (fun (order: Order) ->
            workflow {
                let! reservation = reserve.Call order.Id
                do! notify.Send order.Id // journaled fire-and-forget

                match! approved.Await(Duration.hours 48) with // Ok decision | Error Timeout
                | Ok d when d.Accepted -> return Receipt.confirmed reservation
                | _ -> return Receipt.rejected order.Id
            })

    // ── firegrid drive-loop sample vocabulary (SDD "Worked example") ────────
    type ToolCall = { Tool: string; Args: string }
    type Request = { Reason: string }
    type Outcome = { Summary: string }
    type TurnInput = { Prompt: string }

    type FeedEvent =
        | ToolResult of string
        | Approved of Decision
        | ApprovalTimedOut
        | SelfPrompt of string
        | ChildResults of Outcome list

    type ModelState = { Transcript: FeedEvent list; Fuel: int }

    module Model =
        let start (prompt: string) =
            { Transcript = [ SelfPrompt prompt ]
              Fuel = 3 }

        let feed (model: ModelState) (event: FeedEvent) =
            { model with
                Transcript = event :: model.Transcript
                Fuel = model.Fuel - 1 }

    type ModelSays =
        | ToolUse of ToolCall
        | NeedsApproval of Request
        | FollowUpAt of Timestamp * prompt: string
        | Spawn of TurnInput list
        | Done of Outcome

    let callModel =
        Step.define "agent/model" (fun (m: ModelState) ->
            async {
                return
                    if m.Fuel <= 0 then
                        Done { Summary = "done:" + string (List.length m.Transcript) }
                    else
                        ToolUse { Tool = "echo"; Args = string m.Fuel }
            })

    let runTool =
        Step.define "agent/tool" (fun (t: ToolCall) -> async { return ToolResult(t.Tool + ":" + t.Args) })

    let approval = Signal.define<Decision> "agent/approval"
    let turnDecl = Workflow.declare<TurnInput, Outcome> "agent/turn" // for recursion

    // ── One turn (SDD sample, verbatim body) ────────────────────────────────
    let turn =
        Worker.implement turnDecl (fun input ->
            workflow {
                let rec drive model =
                    workflow {
                        match! callModel.Call model with
                        | Done outcome -> return outcome
                        | ToolUse call -> // execute()
                            let! result = runTool.Call call
                            return! drive (Model.feed model result)
                        | NeedsApproval req -> // wait_for(): human-in-the-loop
                            match! approval.Await(Duration.days 7) with
                            | Ok d -> return! drive (Model.feed model (Approved d))
                            | Error Timeout -> return! drive (Model.feed model ApprovalTimedOut)
                        | FollowUpAt(t, prompt) -> // wait_until(t, prompt): self-prompt
                            do! Workflow.sleepUntil t
                            return! drive (Model.feed model (SelfPrompt prompt))
                        | Spawn subtasks -> // spawn_all(): durable children
                            let! results = Workflow.all [ for s in subtasks -> turnDecl.CallChild s ]
                            return! drive (Model.feed model (ChildResults results))
                    }

                return! drive (Model.start input.Prompt)
            })

    // ── T5 eternal workflow: generation rollover value ──────────────────────
    type Ledger = { Remaining: int; Applied: int }

    let tick =
        Step.define "spike/tick" (fun (ledger: Ledger) ->
            async {
                SpikeSideChannel.bump "tick"
                return ledger.Applied + 1
            })

    let eternal =
        Workflow.define "spike/eternal" (fun (ledger: Ledger) ->
            workflow {
                let! applied = tick.Call ledger

                if ledger.Remaining <= 1 then
                    return Finish applied
                else
                    return
                        ContinueAsNew
                            { Remaining = ledger.Remaining - 1
                              Applied = applied }
            })

/// Deterministic in-memory stand-ins for the sample step handlers (the real
/// handlers run on hosts in T4/T5; these serve T1/T3 replay-only drives).
module SpikeSim =
    open SpikeSamples

    let checkoutActivities (activity: Activity) : Value =
        if activity.Name = "orders/reserve" then
            let id = Codec.decode<OrderId> activity.Input
            Codec.encode { ReservationId = "res-" + OrderId.value id }
        elif activity.Name = "orders/notify" then
            Codec.encode "notified"
        elif activity.Name = "spike/probe" then
            Codec.encode ("sim:" + Codec.decode<string> activity.Input)
        else
            failwith ("simulate: unexpected activity " + activity.Name)

    let driveActivities (activity: Activity) : Value =
        if activity.Name = "agent/model" then
            let m = Codec.decode<ModelState> activity.Input

            Codec.encode (
                if m.Fuel <= 0 then
                    Done { Summary = "done:" + string (List.length m.Transcript) }
                else
                    ToolUse { Tool = "echo"; Args = string m.Fuel }
            )
        elif activity.Name = "agent/tool" then
            let t = Codec.decode<ToolCall> activity.Input
            Codec.encode (ToolResult(t.Tool + ":" + t.Args))
        else
            failwith ("simulate: unexpected activity " + activity.Name)

type SpikeCheck =
    { Test: string
      Name: string
      Passed: bool
      Detail: string }

module SpikeCheck =
    let make test name passed detail =
        { Test = test
          Name = name
          Passed = passed
          Detail = detail }

// ─────────────────────────────────────────────────────────────────────────────
// T1 — CE feature set. The two SDD sample bodies compiling is the acceptance;
// these checks add semantic evidence per CE feature.
// ─────────────────────────────────────────────────────────────────────────────
module SpikeT1 =
    open SpikeSamples

    let private probe = Step.define "spike/probe" (fun (text: string) -> async { return "sim:" + text })

    // while (ref counter replays deterministically: the program rebuilds fresh
    // per replay) + try/with catching an exception thrown after a bind.
    let private quirks =
        workflow {
            let count = ref 0

            while count.Value < 3 do
                do! probe.Send(string count.Value)
                count.Value <- count.Value + 1

            try
                let! value = probe.Call "x"

                if value = "sim:x" then
                    failwith "kaboom"

                return "unreachable"
            with error ->
                return "caught:" + error.Message
        }

    // and! → MergeSources → kernel PerformAll fan-out.
    let private fanout =
        workflow {
            let! left = probe.Call "left"
            and! right = probe.Call "right"
            return left + "|" + right
        }

    let run () =
        let config: SpikeInMemory.DriveConfig =
            { Timestamp = 1_700_000_000_000L
              SimulateActivity = SpikeSim.checkoutActivities
              RespondSignal =
                fun name ->
                    if name = "orders/approved" then
                        Some(Codec.encode { Accepted = true; Approver = "human" })
                    else
                        None
              FireTimers = false
              CaptureJournal = false }

        let order = { Id = OrderId "ord-1"; Amount = 42.0 }
        let checkoutResult = SpikeInMemory.drive config (checkout.Factory order)

        let expectedReceipt =
            { Confirmed = true
              Reference = "res-ord-1" }

        let quirksResult = SpikeInMemory.drive config quirks
        let fanoutResult = SpikeInMemory.drive config fanout

        let fanoutIsPerformAll =
            match fanout with
            | PerformAll(activities, _) -> List.length activities = 2
            | _ -> false

        let driveConfig =
            { config with
                SimulateActivity = SpikeSim.driveActivities }

        let driveResult = SpikeInMemory.drive driveConfig (turn.Factory { Prompt = "go" })

        [ SpikeCheck.make
              "T1"
              "checkout sample compiles and completes (signal-approved path)"
              (checkoutResult.Value = Some expectedReceipt)
              (sprintf "%A" checkoutResult.Value)
          SpikeCheck.make
              "T1"
              "while + ref journals three sends; try/with catches continuation exception"
              (quirksResult.Value = Some "caught:kaboom")
              (sprintf "%A" quirksResult.Value)
          SpikeCheck.make
              "T1"
              "and! lowers to kernel PerformAll and joins both values"
              (fanoutIsPerformAll && fanoutResult.Value = Some "sim:left|sim:right")
              (sprintf "performAll=%b value=%A" fanoutIsPerformAll fanoutResult.Value)
          SpikeCheck.make
              "T1"
              "drive-loop sample compiles and completes via recursive return!"
              (driveResult.Value = Some { Summary = "done:4" })
              (sprintf "%A" driveResult.Value) ]

// ─────────────────────────────────────────────────────────────────────────────
// T2 — bounded-loop flat stack: recursive `return! drive (n + 1)` over N
// Step.calls, driven and then replayed from scratch, under Node/Fable.
// ─────────────────────────────────────────────────────────────────────────────
module SpikeT2 =
    let private step = Step.define "spike/t2" (fun (n: int) -> async { return n + 1 })

    let rec private driveLoop (iterations: int) (acc: int) (n: int) : Durable<int> =
        workflow {
            if n >= iterations then
                return acc
            else
                let! bumped = step.Call n
                return! driveLoop iterations (acc + bumped) (n + 1)
        }

    let run () =
        let iterations =
            match SpikeNode.env "SPIKE_T2_ITERS" with
            | "" -> 500
            | text -> int text

        let config: SpikeInMemory.DriveConfig =
            { Timestamp = 0L
              SimulateActivity = fun activity -> Codec.encode (Codec.decode<int> activity.Input + 1)
              RespondSignal = fun _ -> None
              FireTimers = false
              CaptureJournal = false }

        let expected = iterations * (iterations + 1) / 2

        let driveStart = Reports.nowMillis ()
        let result = SpikeInMemory.drive config (driveLoop iterations 0 0)
        let driveMillis = Reports.nowMillis () - driveStart

        let replayStart = Reports.nowMillis ()
        let replayed = SpikeInMemory.replayValue result.History (driveLoop iterations 0 0)
        let replayMillis = Reports.nowMillis () - replayStart

        let historyLength = List.length (History.toList result.History)

        [ SpikeCheck.make
              "T2"
              (sprintf "%d-iteration recursive loop drives to completion without stack overflow" iterations)
              (result.Value = Some expected)
              (sprintf "value=%A expected=%d plans=%d history=%d driveMs=%.0f" result.Value expected result.Plans historyLength driveMillis)
          SpikeCheck.make
              "T2"
              "single fresh replay over the full journal returns the same value (restart path)"
              (replayed = Some expected)
              (sprintf "replayed=%A replayMs=%.0f" replayed replayMillis) ]

// ─────────────────────────────────────────────────────────────────────────────
// T3 — zero-codec typed descriptors: derived JSON for records + DUs, and
// journal-byte stability (golden fixture).
// ─────────────────────────────────────────────────────────────────────────────
module SpikeT3 =
    open SpikeSamples

    let private goldenDir () =
        Reports.join [ SpikeNode.cwd (); "spike-fixtures" ]

    let private goldenPath () =
        Reports.join [ goldenDir (); "spike-s0-checkout-journal.golden.txt" ]

    /// Journal bytes for the checkout run up to the parked signal race —
    /// planner-committed records plus simulated completions, all through
    /// StepRecordCodec (the real wire encoding).
    let private capture () =
        let config: SpikeInMemory.DriveConfig =
            { Timestamp = 1_700_000_000_000L
              SimulateActivity = SpikeSim.checkoutActivities
              RespondSignal = fun _ -> None // park on the race — capture stops there
              FireTimers = false
              CaptureJournal = true }

        let order = { Id = OrderId "ord-1"; Amount = 42.0 }
        let result = SpikeInMemory.drive config (checkout.Factory order)
        String.concat "\n" result.Journal

    let run () =
        let first = capture ()
        let second = capture ()

        let existing =
            if SpikeNode.exists (goldenPath ()) then
                Some(SpikeNode.readFile (goldenPath ()))
            else
                None

        match existing with
        | Some _ -> ()
        | None ->
            Reports.ensureDir (goldenDir ())
            Reports.write (goldenPath ()) first

        let crossRunDetail =
            match existing with
            | Some golden when golden = first -> "matches committed golden fixture"
            | Some _ -> "MISMATCH against committed golden fixture"
            | None -> "golden fixture written on this run (commit it)"

        let crossRunStable =
            match existing with
            | Some golden -> golden = first
            | None -> true

        // DU round-trip through the derived codec (Fable tagged encoding).
        let says = FollowUpAt(123.5, "later")
        let saysJson = Codec.encode says
        let saysAgain = Codec.encode (FollowUpAt(123.5, "later"))

        let duRoundTrips =
            match Codec.decode<ModelSays> saysJson with
            | FollowUpAt(t, p) -> t = 123.5 && p = "later"
            | _ -> false

        let spawnJson = Codec.encode (Spawn [ { Prompt = "a" }; { Prompt = "b" } ])

        let listInDuRoundTrips =
            match Codec.decode<ModelSays> spawnJson with
            | Spawn [ a; b ] -> a.Prompt = "a" && b.Prompt = "b"
            | _ -> false

        let decision = { Accepted = true; Approver = "human" }
        let decisionBack = Codec.decode<Decision> (Codec.encode decision)

        [ SpikeCheck.make "T3" "journal bytes identical across two in-process runs" (first = second) (sprintf "%d bytes" first.Length)
          SpikeCheck.make "T3" "journal bytes stable against golden fixture" crossRunStable crossRunDetail
          SpikeCheck.make "T3" "DU payload round-trips through derived codec" duRoundTrips saysJson
          SpikeCheck.make "T3" "DU with record-list payload round-trips" listInDuRoundTrips spawnJson
          SpikeCheck.make
              "T3"
              "record payload round-trips through derived codec"
              (decisionBack.Accepted && decisionBack.Approver = "human")
              (Codec.encode decision)
          SpikeCheck.make
              "T3"
              "DU encoding deterministic within run"
              (saysJson = saysAgain)
              "" ]

// ─────────────────────────────────────────────────────────────────────────────
// s2 lite environment for T4/T5 (standalone; no TraceStore dependency).
// ─────────────────────────────────────────────────────────────────────────────
module SpikeS2Env =
    open SpikeNode

    let startLite (localRoot: string) : Async<string * ChildProcess> =
        async {
            Reports.ensureDir localRoot
            let port = randomPort ()
            let endpoint = sprintf "http://127.0.0.1:%d" port

            let proc =
                spawn
                    (s2Bin ())
                    [| "lite"; "--port"; string port; "--local-root"; localRoot |]
                    (createObj [ "stdio" ==> "ignore" ])

            let rec waitReady remaining =
                async {
                    let! ready = fetchReady endpoint |> Async.AwaitPromise

                    if ready then
                        return ()
                    elif remaining <= 0 then
                        return failwith ("s2 lite did not become ready at " + endpoint)
                    else
                        do! sleep 100
                        return! waitReady (remaining - 1)
                }

            do! waitReady 100
            return endpoint, proc
        }

    let connect (endpoint: string) =
        S2.connectWith
            { S2.ConnectOptions.create "spike-s0" with
                AccountEndpoint = Some endpoint
                BasinEndpoint = Some endpoint }

// ─────────────────────────────────────────────────────────────────────────────
// T4 — kill/replay through the NEW surface against the kernel host + s2 lite.
// A child host process executes step 1 (journaled), gets SIGKILLed, and a
// fresh host completes the workflow; a side-channel file counts executions.
// ─────────────────────────────────────────────────────────────────────────────
module SpikeT4 =
    open SpikeNode
    open SpikeSamples

    let private registrations () = [ reg reserve; reg notify; reg checkout ]

    let private order = { Id = OrderId "ord-1"; Amount = 42.0 }

    /// Child host: starts the checkout instance, ticks it until step 1 has
    /// executed and journaled, writes a marker file, then keeps ticking until
    /// the parent SIGKILLs it mid-flight.
    let childMain () : Async<int> =
        async {
            let s2 = SpikeS2Env.connect (env "SPIKE_S2_ENDPOINT")
            let basin = s2 |> S2.basin (env "SPIKE_S2_BASIN")
            let instanceName = env "SPIKE_INSTANCE"
            let marker = env "SPIKE_MARKER_FILE"
            let worker = Worker.run basin "spike-child" (registrations ())
            let client = Client.connect basin
            let! _run = checkout.Start client order (Id instanceName)
            let instanceId = InstanceId.create instanceName

            let rec loop (pass: int) : Async<int> =
                async {
                    let! _ = worker.runUntilIdle instanceId

                    if SpikeSideChannel.read "reserve" >= 1 && pass >= 2 && not (exists marker) then
                        Reports.write marker "step-1-journaled"

                    do! sleep 25
                    return! loop (pass + 1)
                }

            return! loop 0
        }

    let run (scratch: string) (endpoint: string) (basin: S2.Basin) (basinName: string) : Async<SpikeCheck list> =
        async {
            let countReserve = Reports.join [ scratch; "count-reserve.txt" ]
            let countNotify = Reports.join [ scratch; "count-notify.txt" ]
            let marker = Reports.join [ scratch; "t4-marker.txt" ]
            setEnv "SPIKE_COUNT_reserve" countReserve
            setEnv "SPIKE_COUNT_notify" countNotify
            let instanceName = "spike-checkout-1"

            let childEnv =
                createObj
                    [ "SPIKE_S2_ENDPOINT" ==> endpoint
                      "SPIKE_S2_BASIN" ==> basinName
                      "SPIKE_INSTANCE" ==> instanceName
                      "SPIKE_MARKER_FILE" ==> marker
                      "SPIKE_COUNT_reserve" ==> countReserve
                      "SPIKE_COUNT_notify" ==> countNotify ]

            let child =
                spawn
                    (nodePath ())
                    [| scriptPath (); "t4-child" |]
                    (createObj [ "stdio" ==> "inherit"; "env" ==> withProcessEnv childEnv ])

            let rec waitMarker remaining =
                async {
                    if exists marker then
                        return ()
                    elif remaining <= 0 then
                        return failwith "t4: child never journaled step 1"
                    else
                        do! sleep 100
                        return! waitMarker (remaining - 1)
                }

            do! waitMarker 600
            child.kill "SIGKILL" |> ignore
            do! sleep 300

            let executedInChild = SpikeSideChannel.read "reserve"

            // Restart: a fresh host (new fence) in THIS process, ticking in
            // the background while the SDD call-sample lines run against it.
            let worker = Worker.run basin "spike-b" (registrations ())
            let instanceId = InstanceId.create instanceName
            let stop = ref false

            let ticker =
                async {
                    while not stop.Value do
                        let! _ = worker.runUntilIdle instanceId
                        do! sleep 25
                }

            ticker |> Async.StartAsPromise |> ignore

            // ── Call: typed handles from the same descriptors (SDD sample) ──
            let client = Client.connect basin
            let! run = checkout.Start client order (Id instanceName)
            do! run.Signal approved { Accepted = true; Approver = "human" }
            let! receipt = run.Result
            stop.Value <- true

            let reserveTotal = SpikeSideChannel.read "reserve"
            let notifyTotal = SpikeSideChannel.read "notify"

            return
                [ SpikeCheck.make
                      "T4"
                      "step 1 executed exactly once in the killed child host"
                      (executedInChild = 1)
                      (sprintf "reserve executions at kill: %d" executedInChild)
                  SpikeCheck.make
                      "T4"
                      "after restart, step 1 is journal-served (never re-executed)"
                      (reserveTotal = 1)
                      (sprintf "reserve executions total: %d" reserveTotal)
                  SpikeCheck.make
                      "T4"
                      "notify executed exactly once across kill + restart"
                      (notifyTotal = 1)
                      (sprintf "notify executions total: %d" notifyTotal)
                  SpikeCheck.make
                      "T4"
                      "workflow completes correctly after kill + restart + signal"
                      (receipt = { Confirmed = true; Reference = "res-ord-1" })
                      (sprintf "%A" receipt) ]
        }

// ─────────────────────────────────────────────────────────────────────────────
// T5 — ContinueAsNew probe. The kernel has no rollover primitive; this
// prototypes the smallest viable mechanism ABOVE it: the workflow returns a
// Rollover value and the driver starts generation N+1 as a fresh instance
// (fresh stream), so the prior journal is not replayed.
// ─────────────────────────────────────────────────────────────────────────────
module SpikeT5 =
    open SpikeSamples

    let run (scratch: string) (basin: S2.Basin) : Async<SpikeCheck list> =
        async {
            SpikeNode.setEnv "SPIKE_COUNT_tick" (Reports.join [ scratch; "count-tick.txt" ])
            let worker = Worker.run basin "spike-t5" [ reg tick; reg eternal ]
            let client = Client.connect basin

            let countActivityCalls (instanceName: string) =
                async {
                    let key = StorageKey instanceName
                    let pair = S2Substrate.streams basin key

                    let owned =
                        { Key = key
                          Fence = FenceToken "spike:inspect"
                          Log = pair.Log
                          Inbox = pair.Inbox }

                    let! decoded = S2Substrate.readLogText StepRecordCodec.decode owned

                    return
                        decoded
                        |> List.sumBy (fun (_, entry) ->
                            match entry with
                            | Ok(Incoming(HistoryEvent(ActivityCalled _))) -> 1
                            | _ -> 0)
                }

            let rec generations gen (state: Ledger) (callsPerGeneration: int list) =
                async {
                    let instanceName = "spike-eternal-g" + string gen
                    let! run = eternal.Start client state (Id instanceName)
                    let instanceId = InstanceId.create instanceName

                    let rec driveTo remaining =
                        async {
                            if remaining <= 0 then
                                return failwith "t5: generation did not complete"
                            else
                                let! _ = worker.runUntilIdle instanceId
                                let! value = run.PollOnce()

                                match value with
                                | Some rollover -> return rollover
                                | None ->
                                    do! SpikeNode.sleep 20
                                    return! driveTo (remaining - 1)
                        }

                    let! rollover = driveTo 200
                    let! calls = countActivityCalls instanceName

                    match rollover with
                    | ContinueAsNew next -> return! generations (gen + 1) next (callsPerGeneration @ [ calls ])
                    | Finish total -> return total, gen + 1, callsPerGeneration @ [ calls ]
                }

            let! total, generationCount, callsPerGeneration = generations 0 { Remaining = 3; Applied = 0 } []
            let tickExecutions = SpikeSideChannel.read "tick"

            return
                [ SpikeCheck.make
                      "T5"
                      "rollover value chains three generations to completion with carried state"
                      (total = 3 && generationCount = 3)
                      (sprintf "total=%d generations=%d" total generationCount)
                  SpikeCheck.make
                      "T5"
                      "each generation journals exactly one step call (prior journal NOT replayed)"
                      (callsPerGeneration = [ 1; 1; 1 ])
                      (sprintf "%A" callsPerGeneration)
                  SpikeCheck.make
                      "T5"
                      "step executed once per generation (fresh execution, not journal-served from gen 0)"
                      (tickExecutions = 3)
                      (sprintf "tick executions: %d" tickExecutions) ]
        }

// ─────────────────────────────────────────────────────────────────────────────
// Entry point (Program.fs-style module-level do; only runs when this compiled
// module is the Node entry script — the proof runner never imports it).
// ─────────────────────────────────────────────────────────────────────────────
module SpikeS0 =
    open SpikeNode

    let private runAll () : Async<int> =
        async {
            let scratch = Reports.join [ cwd (); ".spike-s0" ]
            Reports.ensureDir scratch
            let checks = ResizeArray<SpikeCheck>()

            try
                checks.AddRange(SpikeT1.run ())
                checks.AddRange(SpikeT2.run ())
                checks.AddRange(SpikeT3.run ())

                let! endpoint, lite = SpikeS2Env.startLite (Reports.join [ scratch; "s2-lite" ])

                try
                    let s2 = SpikeS2Env.connect endpoint
                    let basinName = "spike-s0-" + string (int64 (Reports.nowMillis ()))
                    let! _ = s2 |> S2.createBasin basinName
                    let basin = s2 |> S2.basin basinName

                    let! t4 = SpikeT4.run scratch endpoint basin basinName
                    checks.AddRange t4

                    let! t5 = SpikeT5.run scratch basin
                    checks.AddRange t5
                finally
                    lite.kill "SIGTERM" |> ignore
            with error ->
                checks.Add(SpikeCheck.make "S0" "spike run aborted by exception" false error.Message)

            for check in checks do
                log (
                    sprintf
                        "[%s] %s — %s%s"
                        check.Test
                        (if check.Passed then "PASS" else "FAIL")
                        check.Name
                        (if check.Detail = "" then "" else " :: " + check.Detail)
                )

            let failed = checks |> Seq.filter (fun check -> not check.Passed) |> Seq.length
            log (sprintf "spike-s0: %d checks, %d failed" checks.Count failed)
            return if failed = 0 then 0 else 1
        }

    let private main () =
        async {
            match argv () |> Array.toList with
            | [ "t4-child" ] -> return! SpikeT4.childMain ()
            | _ -> return! runAll ()
        }

    main ()
    |> Async.StartAsPromise
    |> Promise.map (fun code -> setExitCode code)
    |> ignore
