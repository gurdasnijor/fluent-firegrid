/// ═══════════════════════════════════════════════════════════════════════
/// T1 corpus — stream-native read laws (Packet 0.2 re-expression): sealed
/// logs, graded projection reads, CEL predicate waits, and the golden wire
/// fixtures. Law ids, semantics, and ratchet status FROZEN; only ADDITIVE
/// strength (trace checks, negative controls).
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Firegrid.Durable

module StreamLawProofs =

    // ── t1.golden-wire-fixtures ───────────────────────────────────────────
    // Serialization is DERIVED from your types, and the derived formats are
    // load-bearing: this law pins them against a committed golden fixture
    // (records → fields in declaration order; unions → case-NAME-tagged
    // arrays; renames are wire-breaking unless pinned with [<WireName>]).
    // Observation path: a projection over the entity's journal address
    // receives the raw records; each fixture payload must appear, in command
    // order, within those records. (Assumes entity journal address =
    // [ entityName; key ] — flagged for ratification.)
    type GoldenSnap = { Total: int; Tags: string list }

    type GoldenCmd =
        | Note of string
        | Tally of int * string
        | Snapshot
        | Rename of float

    type GoldenEvt =
        | Noted of note: string
        | Tallied of count: int * label: string
        | Snapshotted of snap: GoldenSnap
        | [<WireName("legacy-name")>] Renamed of value: float

    type GoldenState = { Applied: int }

    type GoldenObs =
        { MissingInOrder: string list
          FixtureLineCount: int }

    /// dist/Main.js -> ../fixtures/golden-wire.fixture.jsonl, so the law
    /// resolves the committed fixture from the compiled entry location, not
    /// the caller's cwd (the ratchet runner invokes suites from the repo
    /// root).
    let private fixturePath () =
        Reports.join [ CorpusNode.entryScript (); ".."; ".."; "fixtures"; "golden-wire.fixture.jsonl" ]

    let private goldenWorkload (tamperExpectation: bool) (ctx: WorkloadContext) : Async<GoldenObs> =
        ProofOperation.run
            ctx
            "t1.golden-wire-fixtures"
            {| Tampered = tamperExpectation |}
            { ProofOperationOptions.empty with
                Key = Some "golden-wire" }
            (async {
                let path = fixturePath ()

                if not (CorpusNode.exists path) then
                    failwith ("golden fixture missing: " + path)

                let fixtureLines =
                    CorpusNode.readFile path
                    |> fun text -> text.Split('\n')
                    |> Array.toList
                    |> List.filter (fun line -> line <> "" && not (line.StartsWith "#"))

                let expectedLines =
                    if tamperExpectation then
                        // NEGATIVE CONTROL ONLY: a knowingly-wrong fixture
                        // line — the wire-pinning detector must catch it.
                        fixtureLines @ [ "[\"Bogus\",\"never-on-the-wire\"]" ]
                    else
                        fixtureLines

                let golden =
                    Entity.define
                        "corpus/golden"
                        { Initial = { Applied = 0 }
                          Evolve = fun state _event -> { Applied = state.Applied + 1 }
                          Decide =
                            fun (Key _) command state ->
                                let events =
                                    match command with
                                    | Note text -> [ Noted text ]
                                    | Tally(n, label) -> [ Tallied(n, label) ]
                                    | Snapshot -> [ Snapshotted { Total = 3; Tags = [ "a"; "b" ] } ]
                                    | Rename value -> [ Renamed value ]

                                state.Applied + 1, events }

                let! basin = CorpusSupport.workloadBasin ctx "t1-golden"
                let! worker = Worker.run basin "t1-golden" [ reg golden ]
                let client = Client.connect basin

                let! _ = golden.Call client "gold-1" (Note "hello wire")
                let! _ = golden.Call client "gold-1" (Tally(2, "twice"))
                let! _ = golden.Call client "gold-1" Snapshot
                let! _ = golden.Call client "gold-1" (Rename 1.5)

                // Raw record capture: fold the journal's records as opaque
                // strings.
                let raw =
                    Projection.define "corpus/golden-raw" [ "corpus/golden"; "gold-1" ] ([]: string list) (fun acc record ->
                        acc @ [ record ])

                let! view = client.Read raw Latest
                let joined = String.concat "\n" view.State

                // Every derived payload appears, in command order.
                let missing = ResizeArray<string>()
                let mutable searchFrom = 0

                for line in expectedLines do
                    let index = joined.IndexOf(line, searchFrom)

                    if index >= 0 then
                        searchFrom <- index + line.Length
                    else
                        missing.Add line

                do! worker.Stop()

                return
                    { MissingInOrder = List.ofSeq missing
                      FixtureLineCount = List.length fixtureLines }
            })

    let private goldenChecks (v: Verifiers<GoldenObs>) : Check<GoldenObs> list =
        [ LawCheck.equal
              "derived wire format pinned: every fixture line appears, in command order"
              (fun o -> o.MissingInOrder)
              []
          // The committed fixture is load-bearing: it must actually pin the
          // four command payloads (guards against an emptied fixture file
          // silently weakening the law).
          LawCheck.equal "the committed fixture pins 4 payload lines" (fun o -> o.FixtureLineCount) 4
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.golden-wire-fixtures") ]

    // Negative control (wire family): a knowingly-wrong fixture line must
    // fail the wire-pinning detector.
    let private goldenTamperedControl =
        negativeControl<GoldenObs> "knowingly-wrong fixture line fails the wire-pinning check" {
            workload (goldenWorkload true)

            verify
                [ LawCheck.equal
                      "derived wire format pinned: every fixture line appears, in command order"
                      (fun o -> o.MissingInOrder)
                      [] ]

            expectFailure "derived wire format pinned"
        }

    let goldenWireFixtures =
        let lawProperty =
            property "t1.golden-wire-fixtures" {
                s2Lite ""
                timeoutMs 120_000
                workload (goldenWorkload false)
                verify goldenChecks
                negativeControl goldenTamperedControl
                requiresNegativeControl
            }

        proof "t1.golden-wire-fixtures" {
            describedAs
                "Derived wire encodings are pinned to the committed golden fixture: every fixture payload appears in the entity's journal records, in command order."

            property lawProperty
        }
    // ── t1.log-attach-byte-faithful (RED) ─────────────────────────────────
    // One attach loop delivers the recorded prefix, then the live tail, then
    // the terminal — byte-faithful and in order, with no polling. The seal
    // is the ratified `DurableLog.Seal` amendment (architect ruling,
    // PR #117): sealing ends every attach with `Terminal reason` as the LAST
    // event.
    let private attachPrefix = [ "alpha"; "βeta-∆"; "line\nbreak"; "{\"json\":true}" ]
    let private attachTail = [ "tail-1"; "tail-2" ]

    type AttachObs =
        { Chunks: string list
          LastEventIsTerminal: bool
          TerminalReason: string option
          IterCompleted: bool }

    let private attachWorkload (ctx: WorkloadContext) : Async<AttachObs> =
        ProofOperation.run
            ctx
            "t1.log-attach-byte-faithful"
            {| Prefix = List.length attachPrefix
               Tail = List.length attachTail |}
            { ProofOperationOptions.empty with
                Key = Some "log-attach" }
            (async {
                let! basin = CorpusSupport.workloadBasin ctx "t1-attach"
                let client = Client.connect basin
                let log = client.Logs [ "corpus"; "logs"; "attach-1" ]

                // Recorded prefix, appended before anyone listens.
                for data in attachPrefix do
                    do! log.Append data |> Async.Ignore

                let events = ResizeArray<LogEvent>()
                let mutable iterCompleted = false

                // One loop: consume to the end in the background.
                async {
                    do! log.Attach() |> AsyncSeq.iter (fun ev -> events.Add ev)
                    iterCompleted <- true
                }
                |> Async.StartAsPromise
                |> ignore

                // Live tail, appended while attached.
                do! CorpusNode.sleep 250

                for data in attachTail do
                    do! log.Append data |> Async.Ignore

                let chunks () =
                    events
                    |> List.ofSeq
                    |> List.choose (fun ev ->
                        match ev with
                        | Chunk data -> Some data
                        | Terminal _ -> None)

                do!
                    CorpusSupport.until "attach delivered prefix + tail" 30_000 (fun () ->
                        async { return List.length (chunks ()) >= List.length attachPrefix + List.length attachTail })

                let chunksBeforeSeal = chunks ()

                // Seal — the terminal must END the attach loop.
                do! log.Seal "corpus-sealed"

                do!
                    CorpusSupport.until "attach delivered the terminal and completed" 30_000 (fun () ->
                        async {
                            let sawTerminal =
                                events
                                |> List.ofSeq
                                |> List.exists (fun ev ->
                                    match ev with
                                    | Terminal _ -> true
                                    | Chunk _ -> false)

                            return sawTerminal && iterCompleted
                        })

                let lastEvent = List.tryLast (List.ofSeq events)

                return
                    { Chunks = chunksBeforeSeal
                      LastEventIsTerminal =
                        (match lastEvent with
                         | Some(Terminal _) -> true
                         | _ -> false)
                      TerminalReason =
                        (match lastEvent with
                         | Some(Terminal reason) -> Some reason
                         | _ -> None)
                      IterCompleted = iterCompleted }
            })

    let private attachChecks (v: Verifiers<AttachObs>) : Check<AttachObs> list =
        [ LawCheck.equal
              "chunks are byte-faithful and ordered: prefix then tail"
              (fun o -> o.Chunks)
              (attachPrefix @ attachTail)
          LawCheck.holds
              "the terminal is the LAST delivered event and the loop completed"
              (fun o -> o.LastEventIsTerminal && o.IterCompleted)
              (fun o -> sprintf "lastIsTerminal=%b iterCompleted=%b" o.LastEventIsTerminal o.IterCompleted)
          LawCheck.equal "the terminal carries the seal reason" (fun o -> o.TerminalReason) (Some "corpus-sealed")
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.log-attach-byte-faithful") ]

    let logAttachByteFaithful =
        let lawProperty =
            property "t1.log-attach-byte-faithful" {
                s2Lite ""
                timeoutMs 120_000
                workload attachWorkload
                verify attachChecks
            }

        proof "t1.log-attach-byte-faithful" {
            describedAs
                "One attach delivers the recorded prefix, then the live tail, then the terminal — byte-faithful, ordered, no polling; sealing ends every attach with the terminal as the LAST event."

            property lawProperty
        }

    // ── t1.three-read-grades (RED) ────────────────────────────────────────
    // Eventual: a valid fold prefix with the lag exposed as data (Behind).
    // Latest: linearizable at the checked tail. Through v: read-your-writes
    // for a writer holding its own append ack (the ratified
    // `Append : Async<Version>` amendment), or any handed-off version
    // (e.g. the AsOf of a Latest view).
    type GradesObs =
        { LatestState: int
          LatestBehind: float
          EventualState: int
          EventualStatePlusBehind: int
          ViaAckState: int
          Latest2State: int
          ThroughHandedOffState: int
          ThroughOlderState: int }

    let private gradesWorkload (ctx: WorkloadContext) : Async<GradesObs> =
        ProofOperation.run
            ctx
            "t1.three-read-grades"
            {| Records = 7 |}
            { ProofOperationOptions.empty with
                Key = Some "read-grades" }
            (async {
                let counting =
                    Projection.define "corpus/grade-counts" [ "corpus"; "grades"; "g-1" ] 0 (fun n _record -> n + 1)

                let! basin = CorpusSupport.workloadBasin ctx "t1-grades"
                let client = Client.connect basin
                let log = client.Logs [ "corpus"; "grades"; "g-1" ]

                for i in 1..5 do
                    do! log.Append(sprintf "record-%d" i) |> Async.Ignore

                // Latest: linearizable — sees everything appended.
                let! latest = client.Read counting Latest

                // Eventual: any valid prefix, lag as data.
                let! eventual = client.Read counting Eventual

                // Through v — leg 1: the WRITER's own append ack.
                let! _ack6 = log.Append "record-6"
                let! ack7 = log.Append "record-7"
                let! viaAck = client.Read counting (Through ack7)

                // Through v — leg 2: a handed-off version (Latest's AsOf).
                let! latest2 = client.Read counting Latest
                let! through = client.Read counting (Through latest2.AsOf)
                let! throughOld = client.Read counting (Through latest.AsOf)

                return
                    { LatestState = latest.State
                      LatestBehind = latest.Behind
                      EventualState = eventual.State
                      EventualStatePlusBehind = eventual.State + int eventual.Behind
                      ViaAckState = viaAck.State
                      Latest2State = latest2.State
                      ThroughHandedOffState = through.State
                      ThroughOlderState = throughOld.State }
            })

    let private gradesChecks (v: Verifiers<GradesObs>) : Check<GradesObs> list =
        [ LawCheck.equal "Latest folds every record at the checked tail" (fun o -> o.LatestState) 5
          LawCheck.equal "Latest is not behind" (fun o -> o.LatestBehind) 0.0
          LawCheck.holds
              "Eventual state is a valid fold prefix"
              (fun o -> o.EventualState >= 0 && o.EventualState <= 5)
              (fun o -> sprintf "saw %d" o.EventualState)
          LawCheck.equal "Behind measures exactly the unfolded lag" (fun o -> o.EventualStatePlusBehind) 5
          LawCheck.equal
              "Through(own append ack) reads your write — no prior read needed"
              (fun o -> o.ViaAckState)
              7
          LawCheck.equal "Latest sees the new writes" (fun o -> o.Latest2State) 7
          LawCheck.equal "Through(handed-off AsOf) reads at that version" (fun o -> o.ThroughHandedOffState) 7
          LawCheck.holds
              "Through(older version) reads AT LEAST through it"
              (fun o -> o.ThroughOlderState >= 5)
              (fun o -> sprintf "saw %d" o.ThroughOlderState)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.three-read-grades") ]

    let threeReadGrades =
        let lawProperty =
            property "t1.three-read-grades" {
                s2Lite ""
                timeoutMs 120_000
                workload gradesWorkload
                verify gradesChecks
            }

        proof "t1.three-read-grades" {
            describedAs
                "Latest is linearizable at the checked tail; Eventual is a lagging valid prefix with the lag as data; Through(v) reads at least through v, including a writer's own append ack."

            property lawProperty
        }

    // ── t1.cel-wait (RED) ─────────────────────────────────────────────────
    // `Wait.state`: parks a workflow on a serializable CEL predicate over a
    // projection. Immediate if already true; resumes on a satisfying change;
    // does NOT resume on unrelated change; the recorded resolution is
    // replay-served (a later state change never rewrites history).
    type CelState = { Count: int }

    type CelObs =
        { ParkedAfterNoise: RunStatus
          SatisfiedResult: Result<int, RunFailure>
          ImmediateResult: Result<int, RunFailure>
          ReplayedResult: Result<int, RunFailure> }

    let private celWorkload (ctx: WorkloadContext) : Async<CelObs> =
        ProofOperation.run
            ctx
            "t1.cel-wait"
            {| Predicate = "state.Count >= 2" |}
            { ProofOperationOptions.empty with
                Key = Some "cel-wait" }
            (async {
                let bumps =
                    Projection.define
                        "corpus/cel-bumps"
                        [ "corpus"; "cel"; "c-1" ]
                        { Count = 0 }
                        (fun state record ->
                            if record = "bump" then
                                { Count = state.Count + 1 }
                            else
                                state)

                let waiter =
                    Workflow.define "corpus/cel-wait" (fun (_: string) ->
                        workflow {
                            match! Wait.state bumps (Cel "state.Count >= 2") (Duration.hours 1.0) with
                            | Ok state -> return state.Count
                            | Error Timeout -> return -1
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-celwait"
                let ns = "t1-cel"
                let! worker = Worker.run basin ns [ reg waiter ]
                let client = Client.connect basin
                let log = client.Logs [ "corpus"; "cel"; "c-1" ]

                // Predicate false (Count = 1): the workflow parks.
                do! log.Append "bump" |> Async.Ignore
                let! runA = waiter.Start client "go" (Id "cel-parked")

                do!
                    CorpusSupport.until "waiter parked (Running)" 60_000 (fun () ->
                        async {
                            let! status = runA.Status
                            return status = Running
                        })

                // Unrelated change: the projection folds it but the state is
                // unchanged — the waiter must NOT resume.
                do! log.Append "noise" |> Async.Ignore
                do! CorpusNode.sleep 1_500
                let! stillParked = runA.Status

                // Satisfying change (Count = 2): park → resume.
                do! log.Append "bump" |> Async.Ignore
                let! resultA = runA.Result

                // Immediate-if-true: a new instance sees Count >= 2 now.
                let! runB = waiter.Start client "go" (Id "cel-immediate")
                let! resultB = runB.Result

                // Replay-served: mutate the state PAST the recorded
                // resolution, restart the host, reattach — history does not
                // change.
                do! log.Append "bump" |> Async.Ignore
                do! worker.Stop()
                let! worker2 = Worker.run basin ns [ reg waiter ]
                let reattached = Client.attach<int> client (Id "cel-parked")
                let! replayed = reattached.Result
                do! worker2.Stop()

                return
                    { ParkedAfterNoise = stillParked
                      SatisfiedResult = resultA
                      ImmediateResult = resultB
                      ReplayedResult = replayed }
            })

    let private celChecks (v: Verifiers<CelObs>) : Check<CelObs> list =
        [ LawCheck.equal "unrelated change does not resume the wait" (fun o -> o.ParkedAfterNoise) Running
          LawCheck.equal "the wait resumes with the satisfying state" (fun o -> o.SatisfiedResult) (Ok 2)
          LawCheck.equal "an already-true predicate resolves immediately" (fun o -> o.ImmediateResult) (Ok 2)
          LawCheck.equal
              "replay serves the recorded resolution, not the current state"
              (fun o -> o.ReplayedResult)
              (Ok 2)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.cel-wait") ]

    let celWait =
        let lawProperty =
            property "t1.cel-wait" {
                s2Lite ""
                timeoutMs 180_000
                workload celWorkload
                verify celChecks
            }

        proof "t1.cel-wait" {
            describedAs
                "A CEL wait resolves immediately if already true, resumes only on a satisfying change (never on unrelated change), and the recorded resolution is replay-served."

            property lawProperty
        }
