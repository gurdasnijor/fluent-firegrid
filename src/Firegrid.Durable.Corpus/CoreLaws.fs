/// ═══════════════════════════════════════════════════════════════════════
/// T1 red corpus — core workflow laws.
///
/// Every law imports ONLY `Firegrid.Durable`'s public surface for the system
/// under test. These tests are the frozen specification: they FAIL while the
/// surface throws `notYet`, and greening them (without editing a body) is the
/// implementation gate. Surface values are constructed inside law bodies so
/// a red surface fails fast per law instead of at module import.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Corpus

open Firegrid.Durable

// ── t1.replay-determinism-across-kill ─────────────────────────────────────
// A step is executed exactly once even when the host executing it is
// SIGKILLed after the execution journaled; after restart the recorded result
// is journal-served (never re-executed) and the workflow completes correctly.
module ReplayScenario =
    type Order = { OrderId: string; Amount: float }
    type Decision = { Accepted: bool; By: string }
    type Receipt = { Confirmed: bool; Reference: string }

    let defs (scratch: string) =
        let reserve =
            Step.define "corpus/reserve" (fun (order: Order) ->
                async {
                    Counter.bump scratch "reserve"
                    return "res-" + order.OrderId
                })

        let notify =
            Step.define "corpus/notify" (fun (order: Order) ->
                async {
                    Counter.bump scratch "notify"
                    return ()
                })

        let approved = Signal.define<Decision> "corpus/approved"

        let checkout =
            Workflow.define "corpus/checkout" (fun (order: Order) ->
                workflow {
                    let! reservation = reserve.Call order
                    do! notify.Send order

                    match! approved.Await (Duration.hours 48.0) with
                    | Ok decision when decision.Accepted -> return { Confirmed = true; Reference = reservation }
                    | Ok _
                    | Error Timeout -> return { Confirmed = false; Reference = order.OrderId }
                })

        reserve, notify, approved, checkout

    /// Child host: registers the scenario and hosts it until the parent
    /// SIGKILLs this process mid-flight.
    let childHost () : Async<int> =
        async {
            let basin = Harness.childBasin ()
            let reserve, notify, _approved, checkout = defs (Node.env "T1C_SCRATCH")
            let! _worker = Worker.run basin (Node.env "T1C_NS") [ reg reserve; reg notify; reg checkout ]
            return! Harness.foreverChild ()
        }

module CoreLaws =
    open ReplayScenario

    let replayDeterminismAcrossKill: Law =
        { Id = "t1.replay-determinism-across-kill"
          TimeoutMs = 180_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "replay-kill"
                    // Surface first: while red this throws immediately.
                    let _reserve, _notify, approved, checkout = defs scratch

                    do!
                        Harness.withEnv "replay-kill" (fun env ->
                            async {
                                let ns = "t1-replay"
                                let child = Harness.spawnChildHost env "replay-host" ns scratch
                                let client = Client.connect env.Basin
                                let order = { OrderId = "ord-1"; Amount = 42.0 }

                                let! run = checkout.Start client order (Id "replay-kill-1")

                                // Step 1 executed + one-way dispatch executed in the child…
                                do! Harness.untilCount scratch "reserve" 1 60_000
                                do! Harness.untilCount scratch "notify" 1 60_000
                                // …then the child host dies, hard.
                                child.kill "SIGKILL" |> ignore
                                do! Node.sleep 300

                                Expect.equal "reserve executed exactly once before the kill" 1 (Counter.read scratch "reserve")

                                // Restart: a fresh host in THIS process.
                                let! worker = Worker.run env.Basin ns (let r, n, _, c = defs scratch in [ reg r; reg n; reg c ])
                                do! run.Signal approved { Accepted = true; By = "corpus" }
                                let! result = run.Result

                                Expect.equal
                                    "workflow completes correctly after kill + restart + signal"
                                    (Ok { Confirmed = true; Reference = "res-ord-1" })
                                    result

                                // Journal-served, never re-executed:
                                Expect.equal "reserve executed exactly once across kill + restart" 1 (Counter.read scratch "reserve")
                                Expect.equal "notify executed exactly once across kill + restart" 1 (Counter.read scratch "notify")
                                do! worker.Stop ()
                            })
                } }

    // ── t1.fanout-and-join ────────────────────────────────────────────────
    // `and!` fans out and joins both values; `Workflow.all` runs all branches,
    // waits for all, and yields results in LIST order (not completion order).
    let fanoutAndJoin: Law =
        { Id = "t1.fanout-and-join"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "fanout"

                    let slow =
                        Step.define "corpus/fan-slow" (fun (label: string) ->
                            async {
                                do! Node.sleep 400
                                Counter.bump scratch ("ran-" + label)
                                return "slow:" + label
                            })

                    let fast =
                        Step.define "corpus/fan-fast" (fun (label: string) ->
                            async {
                                Counter.bump scratch ("ran-" + label)
                                return "fast:" + label
                            })

                    let fanned =
                        Workflow.define "corpus/fanout" (fun (_: string) ->
                            workflow {
                                let! a = slow.Call "a"
                                and! b = fast.Call "b"
                                let! joined = Workflow.all [ slow.Call "c"; fast.Call "d"; fast.Call "e" ]
                                return a + "|" + b + "|" + String.concat "," joined
                            })

                    do!
                        Harness.withEnv "fanout" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-fanout" [ reg slow; reg fast; reg fanned ]
                                let client = Client.connect env.Basin
                                let! run = fanned.Start client "go" (Id "fanout-1")
                                let! result = run.Result

                                // Join yields every branch; `all` results arrive in list
                                // order even though "c" (slow) finishes last.
                                Expect.equal
                                    "and! joins both, all preserves list order"
                                    (Ok "slow:a|fast:b|slow:c,fast:d,fast:e")
                                    result

                                for label in [ "a"; "b"; "c"; "d"; "e" ] do
                                    Expect.equal (sprintf "branch %s executed exactly once" label) 1 (Counter.read scratch ("ran-" + label))

                                do! worker.Stop ()
                            })
                } }

    // ── t1.tagged-select-race ─────────────────────────────────────────────
    // `Workflow.select`: first branch to finish wins and the workflow matches
    // on the CALLER's union — signal beats a far deadline; a near deadline
    // beats signals that never arrive.
    type RaceOutcome =
        | ApprovedBy of string
        | Deadline
        | Withdrawn

    let taggedSelectRace: Law =
        { Id = "t1.tagged-select-race"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let decision = Signal.define<string> "corpus/select-decision"
                    let withdrawn = Signal.define<unit> "corpus/select-withdrawn"

                    let race =
                        Workflow.define "corpus/select" (fun (deadlineSecs: float) ->
                            workflow {
                                let! winner =
                                    Workflow.select
                                        [ ApprovedBy ^| decision.Await ()
                                          (fun () -> Deadline) ^| Workflow.sleep (Duration.seconds deadlineSecs)
                                          (fun () -> Withdrawn) ^| withdrawn.Await () ]

                                return winner
                            })

                    do!
                        Harness.withEnv "select" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-select" [ reg race ]
                                let client = Client.connect env.Basin

                                // Instance A: far deadline; the signal wins.
                                let! runA = race.Start client 3600.0 (Id "select-signal")
                                do! runA.Signal decision "alice"
                                let! resultA = runA.Result
                                Expect.equal "signal branch wins and carries its payload" (Ok (ApprovedBy "alice")) resultA

                                // Instance B: near deadline; no signal ever arrives.
                                let! runB = race.Start client 0.5 (Id "select-deadline")
                                let! resultB = runB.Result
                                Expect.equal "timer branch wins when no signal arrives" (Ok Deadline) resultB

                                do! worker.Stop ()
                            })
                } }

    // ── t1.signal-to-parked-across-restart ────────────────────────────────
    // A parked wait is journal state: it pins no process. The signal is sent
    // while NO worker exists anywhere; a later worker resumes the workflow
    // and delivers the payload.
    let signalToParkedAcrossRestart: Law =
        { Id = "t1.signal-to-parked-across-restart"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let go = Signal.define<string> "corpus/park-go"

                    let park =
                        Workflow.define "corpus/park" (fun (_: string) ->
                            workflow {
                                let! payload = go.Await ()
                                return "woke:" + payload
                            })

                    do!
                        Harness.withEnv "park" (fun env ->
                            async {
                                let ns = "t1-park"
                                let! worker1 = Worker.run env.Basin ns [ reg park ]
                                let client = Client.connect env.Basin
                                let! run = park.Start client "input" (Id "park-1")

                                do!
                                    Harness.until "instance parked (Running)" 60_000 (fun () ->
                                        async {
                                            let! status = run.Status
                                            return status = Running
                                        })

                                do! worker1.Stop ()

                                // No worker alive anywhere. Sending still works —
                                // signals address the journal, not a process.
                                do! run.Signal go "wake-up"

                                let! worker2 = Worker.run env.Basin ns [ reg park ]
                                let! result = run.Result
                                Expect.equal "parked workflow resumed by a later host" (Ok "woke:wake-up") result
                                do! worker2.Stop ()
                            })
                } }

    // ── t1.timer-across-restart ───────────────────────────────────────────
    // A durable timer fires at-or-after its deadline even when every worker
    // was stopped across the deadline: the wait is journal state.
    let timerAcrossRestart: Law =
        { Id = "t1.timer-across-restart"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let timed =
                        Workflow.define "corpus/timer" (fun (secs: float) ->
                            workflow {
                                let! before = Workflow.currentTime
                                do! Workflow.sleep (Duration.seconds secs)
                                let! after = Workflow.currentTime
                                return (before, after)
                            })

                    do!
                        Harness.withEnv "timer" (fun env ->
                            async {
                                let ns = "t1-timer"
                                let! worker1 = Worker.run env.Basin ns [ reg timed ]
                                let client = Client.connect env.Basin
                                let! run = timed.Start client 3.0 (Id "timer-1")

                                do!
                                    Harness.until "instance running (timer journaled)" 60_000 (fun () ->
                                        async {
                                            let! status = run.Status
                                            return status = Running
                                        })

                                do! worker1.Stop ()
                                // The deadline lapses while nothing hosts the timer.
                                do! Node.sleep 3_500

                                let! worker2 = Worker.run env.Basin ns [ reg timed ]
                                let! result = run.Result

                                match result with
                                | Ok (before, after) ->
                                    Expect.isTrue
                                        (sprintf "timer fired at-or-after its deadline (before=%f after=%f)" before after)
                                        (after >= before + 3_000.0)
                                | other -> failwith (sprintf "timer workflow did not complete: %A" other)

                                do! worker2.Stop ()
                            })
                } }

    // ── t1.typed-step-failure ─────────────────────────────────────────────
    // Retry exhaustion surfaces as a VALUE: `DurableStepFailed` carrying
    // `StepError.Failed` is catchable in-workflow (compensate, return); an
    // uncaught step failure terminates the run as `Failed(StepError)` in
    // both `Status` and `Result`. Retry policies bound handler attempts.
    let typedStepFailure: Law =
        { Id = "t1.typed-step-failure"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "step-failure"

                    let flaky =
                        Step.define "corpus/flaky" (fun (tag: string) ->
                            async {
                                Counter.bump scratch ("flaky-" + tag)
                                return (failwith "boom" : string)
                            })

                    let caught =
                        Workflow.define "corpus/failure-caught" (fun (_: string) ->
                            workflow {
                                try
                                    let! value = flaky.CallWith (Fixed (3, Duration.seconds 0.2)) "caught"
                                    return "unreachable:" + value
                                with DurableStepFailed (StepError.Failed _) ->
                                    return "caught-failed"
                            })

                    let uncaught =
                        Workflow.define "corpus/failure-uncaught" (fun (_: string) ->
                            workflow {
                                let! value = flaky.CallWith NoRetry "uncaught"
                                return value
                            })

                    do!
                        Harness.withEnv "step-failure" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-failure" [ reg flaky; reg caught; reg uncaught ]
                                let client = Client.connect env.Basin

                                let! runCaught = caught.Start client "go" (Id "failure-caught-1")
                                let! resultCaught = runCaught.Result
                                Expect.equal "exhausted retries are catchable as StepError.Failed" (Ok "caught-failed") resultCaught
                                let! statusCaught = runCaught.Status
                                Expect.equal "a workflow that handled the failure completes" Completed statusCaught
                                Expect.equal "Fixed(3, …) bounds the handler to 3 attempts" 3 (Counter.read scratch "flaky-caught")

                                let! runUncaught = uncaught.Start client "go" (Id "failure-uncaught-1")
                                let! resultUncaught = runUncaught.Result

                                match resultUncaught with
                                | Error (FailedRun (StepError.Failed _)) -> ()
                                | other -> failwith (sprintf "uncaught step failure must surface as FailedRun(Failed _): %A" other)

                                let! statusUncaught = runUncaught.Status

                                match statusUncaught with
                                | RunStatus.Failed (StepError.Failed _) -> ()
                                | other -> failwith (sprintf "uncaught step failure must surface in Status: %A" other)

                                Expect.equal "NoRetry runs the handler exactly once" 1 (Counter.read scratch "flaky-uncaught")
                                do! worker.Stop ()
                            })
                } }

    // ── t1.deterministic-currentTime ──────────────────────────────────────
    // `Workflow.currentTime` is captured once and journal-served on replay:
    // after a restart the program observes the SAME instant it recorded the
    // first time, not a fresh clock read.
    let deterministicCurrentTime: Law =
        { Id = "t1.deterministic-currentTime"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "current-time"

                    let observe =
                        Step.define "corpus/observe-time" (fun (t: float) ->
                            async {
                                Counter.bump scratch "observe"
                                Counter.writeOnce scratch "first-time" (string t)
                                return t
                            })

                    let clock =
                        Workflow.define "corpus/clock" (fun (_: string) ->
                            workflow {
                                let! t1 = Workflow.currentTime
                                let! echoed = observe.Call t1
                                do! Workflow.sleep (Duration.seconds 2.0)
                                let! t2 = Workflow.currentTime
                                return (echoed, t2)
                            })

                    do!
                        Harness.withEnv "current-time" (fun env ->
                            async {
                                let ns = "t1-clock"
                                let! worker1 = Worker.run env.Basin ns [ reg observe; reg clock ]
                                let client = Client.connect env.Basin
                                let! run = clock.Start client "go" (Id "clock-1")

                                // First execution recorded t1; restart lands mid-timer,
                                // forcing a replay of the currentTime capture.
                                do! Harness.untilCount scratch "observe" 1 60_000
                                do! worker1.Stop ()
                                do! Node.sleep 2_500

                                let! worker2 = Worker.run env.Basin ns [ reg observe; reg clock ]
                                let! result = run.Result

                                let recorded =
                                    match Counter.readValue scratch "first-time" with
                                    | Some text -> float text
                                    | None -> failwith "side channel never recorded the first time"

                                match result with
                                | Ok (t1, t2) ->
                                    Expect.equal "replay observes the SAME captured instant" recorded t1
                                    Expect.isTrue "second capture is at-or-after the timer deadline" (t2 >= t1 + 2_000.0)
                                | other -> failwith (sprintf "clock workflow did not complete: %A" other)

                                Expect.equal "the observing step never re-executed on replay" 1 (Counter.read scratch "observe")
                                do! worker2.Stop ()
                            })
                } }

    // ── t1.status-and-result-query ────────────────────────────────────────
    // A run is observable from anywhere: `Client.attach` reconstructs the
    // handle by id in another client; Status transitions Running → Completed
    // and Result is queryable (and stable) after completion.
    let statusAndResultQuery: Law =
        { Id = "t1.status-and-result-query"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let finish = Signal.define<string> "corpus/status-finish"

                    let flow =
                        Workflow.define "corpus/status" (fun (_: string) ->
                            workflow {
                                let! value = finish.Await ()
                                return "done:" + value
                            })

                    do!
                        Harness.withEnv "status" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-status" [ reg flow ]
                                let clientA = Client.connect env.Basin
                                let clientB = Client.connect env.Basin

                                let! run = flow.Start clientA "go" (Id "status-1")

                                do!
                                    Harness.until "run reports Running while parked" 60_000 (fun () ->
                                        async {
                                            let! status = run.Status
                                            return status = Running
                                        })

                                // Reattach BY ID from a different client.
                                let attached = Client.attach<string> clientB (Id "status-1")
                                let! attachedStatus = attached.Status
                                Expect.equal "an attached handle sees the same status" Running attachedStatus

                                do! attached.Signal finish "x"

                                let! result = run.Result
                                Expect.equal "the original handle observes the result" (Ok "done:x") result
                                let! attachedResult = attached.Result
                                Expect.equal "the attached handle observes the same result" (Ok "done:x") attachedResult

                                let! finalStatus = attached.Status
                                Expect.equal "status settles at Completed" Completed finalStatus
                                do! worker.Stop ()
                            })
                } }

    // ── t1.andbang-teaching ───────────────────────────────────────────────
    // The teaching law: `let!` SEQUENCES (the second step starts only after
    // the first completes); `and!` FANS OUT (a fast branch finishes before a
    // slow one that was written first). Pinned via handler completion order.
    let andbangTeaching: Law =
        { Id = "t1.andbang-teaching"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "andbang"

                    let mark =
                        Step.define "corpus/teach-mark" (fun (input: string * string) ->
                            async {
                                let group, label = input

                                if label = "slow" then
                                    do! Node.sleep 500

                                Counter.appendLine scratch ("order-" + group) label
                                return label
                            })

                    let sequential =
                        Workflow.define "corpus/teach-seq" (fun (_: string) ->
                            workflow {
                                let! a = mark.Call ("seq", "slow")
                                let! b = mark.Call ("seq", "fast")
                                return a + "," + b
                            })

                    let fannedOut =
                        Workflow.define "corpus/teach-par" (fun (_: string) ->
                            workflow {
                                let! a = mark.Call ("par", "slow")
                                and! b = mark.Call ("par", "fast")
                                return a + "," + b
                            })

                    do!
                        Harness.withEnv "andbang" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-teach" [ reg mark; reg sequential; reg fannedOut ]
                                let client = Client.connect env.Basin

                                let! runSeq = sequential.Start client "go" (Id "teach-seq-1")
                                let! resultSeq = runSeq.Result
                                Expect.equal "let! yields in program order" (Ok "slow,fast") resultSeq

                                Expect.equal
                                    "let! sequences: the slow step COMPLETES before the fast one starts"
                                    [ "slow"; "fast" ]
                                    (Counter.readLines scratch "order-seq")

                                let! runPar = fannedOut.Start client "go" (Id "teach-par-1")
                                let! resultPar = runPar.Result
                                Expect.equal "and! still binds values in program order" (Ok "slow,fast") resultPar

                                Expect.equal
                                    "and! fans out: the fast branch finishes FIRST despite being written second"
                                    [ "fast"; "slow" ]
                                    (Counter.readLines scratch "order-par")

                                do! worker.Stop ()
                            })
                } }

    // ── t1.bounded-loop-flat-stack ────────────────────────────────────────
    // Bounded loops recurse in-instance with a FLAT stack: ≥500 guarded
    // `return!` iterations (each journaling a step) complete under Node
    // without stack overflow, and replay converges to the same value.
    let boundedLoopFlatStack: Law =
        { Id = "t1.bounded-loop-flat-stack"
          TimeoutMs = 600_000
          Run =
            fun () ->
                async {
                    let inc = Step.define "corpus/inc" (fun (n: int) -> async { return n + 1 })

                    let looped =
                        Workflow.define "corpus/loop" (fun (iterations: int) ->
                            workflow {
                                let rec drive (acc: int) (n: int) =
                                    workflow {
                                        if n >= iterations then
                                            return acc
                                        else
                                            let! bumped = inc.Call n
                                            return! drive (acc + bumped) (n + 1)
                                    }

                                return! drive 0 0
                            })

                    do!
                        Harness.withEnv "loop" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-loop" [ reg inc; reg looped ]
                                let client = Client.connect env.Basin
                                let! run = looped.Start client 500 (Id "loop-1")
                                let! result = run.Result
                                // sum of 1..500
                                Expect.equal "500 recursive iterations complete with a flat stack" (Ok 125_250) result
                                do! worker.Stop ()
                            })
                } }
