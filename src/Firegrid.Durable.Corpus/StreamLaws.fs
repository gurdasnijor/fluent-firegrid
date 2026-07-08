/// ═══════════════════════════════════════════════════════════════════════
/// T1 red corpus — stream-native read laws: sealed logs, graded projection
/// reads, CEL predicate waits, and the golden wire fixtures.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Corpus

open Fable.Core
open Firegrid.Durable

module StreamLaws =

    // ── t1.log-attach-byte-faithful ───────────────────────────────────────
    // One attach loop delivers the recorded prefix, then the live tail, then
    // the terminal — byte-faithful and in order, with no polling. NOTE (for
    // the ratifier): the surface exposes no way to SEAL a log, so the
    // terminal leg of this law is unreachable until sealing semantics land —
    // reported as a surface gap in the T1 PR.
    let logAttachByteFaithful: Law =
        { Id = "t1.log-attach-byte-faithful"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let prefix = [ "alpha"; "βeta-∆"; "line\nbreak"; "{\"json\":true}" ]
                    let tail = [ "tail-1"; "tail-2" ]

                    do!
                        Harness.withEnv "log-attach" (fun env ->
                            async {
                                let client = Client.connect env.Basin
                                let log = client.Logs [ "corpus"; "logs"; "attach-1" ]

                                // Recorded prefix, appended before anyone listens.
                                for data in prefix do
                                    do! log.Append data

                                let events = ResizeArray<LogEvent>()
                                let mutable iterCompleted = false

                                // One loop: consume to the end in the background.
                                async {
                                    do! log.Attach () |> AsyncSeq.iter (fun ev -> events.Add ev)
                                    iterCompleted <- true
                                }
                                |> Async.StartAsPromise
                                |> ignore

                                // Live tail, appended while attached.
                                do! Node.sleep 250

                                for data in tail do
                                    do! log.Append data

                                let chunks () =
                                    events
                                    |> List.ofSeq
                                    |> List.choose (fun ev ->
                                        match ev with
                                        | Chunk data -> Some data
                                        | Terminal _ -> None)

                                do!
                                    Harness.until "attach delivered prefix + tail" 30_000 (fun () ->
                                        async { return List.length (chunks ()) >= List.length prefix + List.length tail })

                                Expect.equal
                                    "chunks are byte-faithful and ordered: prefix then tail"
                                    (prefix @ tail)
                                    (chunks ())

                                // The terminal: the attach loop must END with the
                                // log's terminal once the log is sealed.
                                do!
                                    Harness.until "attach delivered the terminal and completed" 30_000 (fun () ->
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

                                match List.tryLast (List.ofSeq events) with
                                | Some (Terminal _) -> ()
                                | other -> failwith (sprintf "the terminal must be the LAST delivered event, got %A" other)
                            })
                } }

    // ── t1.three-read-grades ──────────────────────────────────────────────
    // Eventual: a valid fold prefix with the lag exposed as data (Behind).
    // Latest: linearizable at the checked tail. Through v: read-your-writes
    // for a reader holding a version (here: the AsOf of a Latest view).
    let threeReadGrades: Law =
        { Id = "t1.three-read-grades"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let counting =
                        Projection.define "corpus/grade-counts" [ "corpus"; "grades"; "g-1" ] 0 (fun n _record -> n + 1)

                    do!
                        Harness.withEnv "read-grades" (fun env ->
                            async {
                                let client = Client.connect env.Basin
                                let log = client.Logs [ "corpus"; "grades"; "g-1" ]

                                for i in 1..5 do
                                    do! log.Append (sprintf "record-%d" i)

                                // Latest: linearizable — sees everything appended.
                                let! latest = client.Read counting Latest
                                Expect.equal "Latest folds every record at the checked tail" 5 latest.State
                                Expect.equal "Latest is not behind" 0.0 latest.Behind

                                // Eventual: any valid prefix, lag as data.
                                let! eventual = client.Read counting Eventual

                                Expect.isTrue
                                    (sprintf "Eventual state is a valid fold prefix (saw %d)" eventual.State)
                                    (eventual.State >= 0 && eventual.State <= 5)

                                Expect.equal
                                    "Behind measures exactly the unfolded lag"
                                    5
                                    (eventual.State + int eventual.Behind)

                                // Through v: read-your-writes via version handoff.
                                for i in 6..7 do
                                    do! log.Append (sprintf "record-%d" i)

                                let! latest2 = client.Read counting Latest
                                Expect.equal "Latest sees the new writes" 7 latest2.State

                                let! through = client.Read counting (Through latest2.AsOf)
                                Expect.equal "Through(ack) reads your writes at that version" 7 through.State

                                let! throughOld = client.Read counting (Through latest.AsOf)

                                Expect.isTrue
                                    (sprintf "Through(older version) reads AT LEAST through it (saw %d)" throughOld.State)
                                    (throughOld.State >= 5)
                            })
                } }

    // ── t1.cel-wait ───────────────────────────────────────────────────────
    // `Wait.state`: parks a workflow on a serializable CEL predicate over a
    // projection. Immediate if already true; resumes on a satisfying change;
    // does NOT resume on unrelated change; the recorded resolution is
    // replay-served (a later state change never rewrites history).
    type CelState = { Count: int }

    let celWait: Law =
        { Id = "t1.cel-wait"
          TimeoutMs = 180_000
          Run =
            fun () ->
                async {
                    let bumps =
                        Projection.define
                            "corpus/cel-bumps"
                            [ "corpus"; "cel"; "c-1" ]
                            { Count = 0 }
                            (fun state record -> if record = "bump" then { Count = state.Count + 1 } else state)

                    let waiter =
                        Workflow.define "corpus/cel-wait" (fun (_: string) ->
                            workflow {
                                match! Wait.state bumps (Cel "state.Count >= 2") (Duration.hours 1.0) with
                                | Ok state -> return state.Count
                                | Error Timeout -> return -1
                            })

                    do!
                        Harness.withEnv "cel-wait" (fun env ->
                            async {
                                let ns = "t1-cel"
                                let! worker = Worker.run env.Basin ns [ reg waiter ]
                                let client = Client.connect env.Basin
                                let log = client.Logs [ "corpus"; "cel"; "c-1" ]

                                // Predicate false (Count = 1): the workflow parks.
                                do! log.Append "bump"
                                let! runA = waiter.Start client "go" (Id "cel-parked")

                                do!
                                    Harness.until "waiter parked (Running)" 60_000 (fun () ->
                                        async {
                                            let! status = runA.Status
                                            return status = Running
                                        })

                                // Unrelated change: the projection folds it but the
                                // state is unchanged — the waiter must NOT resume.
                                do! log.Append "noise"
                                do! Node.sleep 1_500
                                let! stillParked = runA.Status
                                Expect.equal "unrelated change does not resume the wait" Running stillParked

                                // Satisfying change (Count = 2): park → resume.
                                do! log.Append "bump"
                                let! resultA = runA.Result
                                Expect.equal "the wait resumes with the satisfying state" (Ok 2) resultA

                                // Immediate-if-true: a new instance sees Count >= 2 now.
                                let! runB = waiter.Start client "go" (Id "cel-immediate")
                                let! resultB = runB.Result
                                Expect.equal "an already-true predicate resolves immediately" (Ok 2) resultB

                                // Replay-served: mutate the state PAST the recorded
                                // resolution, restart the host, reattach — history
                                // does not change.
                                do! log.Append "bump"
                                do! worker.Stop ()
                                let! worker2 = Worker.run env.Basin ns [ reg waiter ]
                                let reattached = Client.attach<int> client (Id "cel-parked")
                                let! replayed = reattached.Result
                                Expect.equal "replay serves the recorded resolution, not the current state" (Ok 2) replayed
                                do! worker2.Stop ()
                            })
                } }

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

    let goldenWireFixtures: Law =
        { Id = "t1.golden-wire-fixtures"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let fixturePath = Node.join [ Node.cwd (); "fixtures"; "golden-wire.fixture.jsonl" ]

                    if not (Node.exists fixturePath) then
                        failwith ("golden fixture missing: " + fixturePath)

                    let fixtureLines =
                        Node.readFile fixturePath
                        |> fun text -> text.Split('\n')
                        |> Array.toList
                        |> List.filter (fun line -> line <> "" && not (line.StartsWith "#"))

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
                                        | Tally (n, label) -> [ Tallied (n, label) ]
                                        | Snapshot -> [ Snapshotted { Total = 3; Tags = [ "a"; "b" ] } ]
                                        | Rename value -> [ Renamed value ]

                                    state.Applied + 1, events }

                    do!
                        Harness.withEnv "golden-wire" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-golden" [ reg golden ]
                                let client = Client.connect env.Basin

                                let! _ = golden.Call client "gold-1" (Note "hello wire")
                                let! _ = golden.Call client "gold-1" (Tally (2, "twice"))
                                let! _ = golden.Call client "gold-1" Snapshot
                                let! _ = golden.Call client "gold-1" (Rename 1.5)

                                // Raw record capture: fold the journal's records as
                                // opaque strings.
                                let raw =
                                    Projection.define
                                        "corpus/golden-raw"
                                        [ "corpus/golden"; "gold-1" ]
                                        ([]: string list)
                                        (fun acc record -> acc @ [ record ])

                                let! view = client.Read raw Latest
                                let joined = String.concat "\n" view.State

                                // Every derived payload appears, in command order.
                                let mutable searchFrom = 0

                                for line in fixtureLines do
                                    let index = joined.IndexOf(line, searchFrom)

                                    Expect.isTrue
                                        (sprintf "derived wire format pinned: %s (in order)" line)
                                        (index >= 0)

                                    searchFrom <- index + line.Length

                                do! worker.Stop ()
                            })
                } }
