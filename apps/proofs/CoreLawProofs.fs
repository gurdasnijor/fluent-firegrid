/// ═══════════════════════════════════════════════════════════════════════
/// T1 corpus — core workflow laws, re-expressed as harness properties
/// (Packet 0.2). Law ids, semantic content, and ratchet status are FROZEN;
/// re-expression only ADDS strength (fault-controller kills with report
/// evidence, trace-backed operation checks, negative controls). Every law
/// imports ONLY `Firegrid.Durable`'s public surface for the system under
/// test; surface values are constructed inside workloads so a red surface
/// fails fast per law instead of at module import.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Firegrid.Durable

// ── t1.replay-determinism-across-kill scenario ────────────────────────────
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
            Step.define "corpus/notify" (fun (_order: Order) ->
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

                    match! approved.Await(Duration.hours 48.0) with
                    | Ok decision when decision.Accepted ->
                        return
                            { Confirmed = true
                              Reference = reservation }
                    | Ok _
                    | Error Timeout ->
                        return
                            { Confirmed = false
                              Reference = order.OrderId }
                })

        reserve, notify, approved, checkout

    let basinName = "t1-replay-kill"
    let ns = "t1-replay"

    /// Child host: registers the scenario and hosts it until the runner's
    /// fault controller SIGKILLs this process mid-flight.
    let childHost () : Async<int> =
        async {
            let! basin = CorpusSupport.childBasin ()
            let scratch = CorpusSupport.childScratch ()
            let reserve, notify, _approved, checkout = defs scratch

            let! _worker =
                Worker.run basin (CorpusSupport.childNamespace ()) [ reg reserve; reg notify; reg checkout ]

            return! CorpusSupport.foreverChild ()
        }

module CoreLawProofs =
    open ReplayScenario

    // ── t1.replay-determinism-across-kill ─────────────────────────────────
    type ReplayKillObs =
        { ReserveAtKill: int
          Outcome: Result<Receipt, RunFailure>
          ReserveAfter: int
          NotifyAfter: int }

    let private replayWorkload (forgeDoubleExecution: bool) (ctx: WorkloadContext) : Async<ReplayKillObs> =
        ProofOperation.run
            ctx
            "t1.replay-determinism-across-kill"
            {| Forged = forgeDoubleExecution |}
            { ProofOperationOptions.empty with
                Key = Some "replay-kill" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                // Surface first: while red this throws immediately.
                let _reserve, _notify, approved, checkout = defs scratch
                let! basin = CorpusSupport.workloadBasin ctx basinName
                let client = Client.connect basin
                let order = { OrderId = "ord-1"; Amount = 42.0 }

                let! run = checkout.Start client order (Id "replay-kill-1")

                // Step 1 executed + one-way dispatch executed in the child…
                do! CorpusSupport.untilCount scratch "reserve" 1 60_000
                do! CorpusSupport.untilCount scratch "notify" 1 60_000
                // …then the child host dies, hard — through the fault
                // controller (report-level fault event + kill span).
                do! WorkloadContext.killHost "replay-host" ctx
                do! CorpusNode.sleep 300

                let reserveAtKill = Counter.read scratch "reserve"

                if forgeDoubleExecution then
                    // NEGATIVE CONTROL ONLY: forge a duplicate handler
                    // execution in the side channel — the exactly-once
                    // detector must catch exactly this.
                    Counter.bump scratch "reserve"

                // Restart: a fresh host in THIS process.
                let! worker =
                    Worker.run
                        basin
                        ns
                        (let r, n, _, c = defs scratch in
                         [ reg r; reg n; reg c ])

                do! run.Signal approved { Accepted = true; By = "corpus" }
                let! result = run.Result

                let observed =
                    { ReserveAtKill = reserveAtKill
                      Outcome = result
                      ReserveAfter = Counter.read scratch "reserve"
                      NotifyAfter = Counter.read scratch "notify" }

                do! worker.Stop()
                return observed
            })

    let private replayChecks (v: Verifiers<ReplayKillObs>) : Check<ReplayKillObs> list =
        [ LawCheck.equal "reserve executed exactly once before the kill" (fun o -> o.ReserveAtKill) 1
          LawCheck.equal
              "workflow completes correctly after kill + restart + signal"
              (fun o -> o.Outcome)
              (Ok
                  { Confirmed = true
                    Reference = "res-ord-1" })
          // Journal-served, never re-executed:
          LawCheck.equal "reserve executed exactly once across kill + restart" (fun o -> o.ReserveAfter) 1
          LawCheck.equal "notify executed exactly once across kill + restart" (fun o -> o.NotifyAfter) 1
          v.Host.Started "replay-host"
          v.Fault.HostKillReported "replay-host"
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.replay-determinism-across-kill")
          v.Trace.Sql "kill span carries signal and accepted flag" (CorpusSupport.killSpanSql "replay-host") ]

    // Negative control (core-replay family): a forged duplicate execution
    // must fail the exactly-once detector.
    let private replayForgedControl =
        negativeControl<ReplayKillObs> "forged duplicate execution fails the exactly-once check" {
            workload (replayWorkload true)

            verify
                [ LawCheck.equal "reserve executed exactly once across kill + restart" (fun o -> o.ReserveAfter) 1 ]

            expectFailure "reserve executed exactly once across kill + restart"
        }

    let replayDeterminismAcrossKill =
        let lawProperty =
            property "t1.replay-determinism-across-kill" {
                s2Lite ""
                processHost (CorpusSupport.childHostSpec "replay-host" "replay-host" basinName ns)
                timeoutMs 180_000
                workload (replayWorkload false)
                verify replayChecks
                negativeControl replayForgedControl
                requiresNegativeControl
            }

        proof "t1.replay-determinism-across-kill" {
            describedAs
                "A step executes exactly once across a SIGKILL of its host; the journal serves the recorded result on restart and the workflow completes correctly."

            property lawProperty
        }

    // ── t1.fanout-and-join ────────────────────────────────────────────────
    // `and!` fans out and joins both values; `Workflow.all` runs all
    // branches, waits for all, and yields results in LIST order (not
    // completion order).
    type FanoutObs =
        { Outcome: Result<string, RunFailure>
          BranchCounts: (string * int) list }

    let private fanoutWorkload (ctx: WorkloadContext) : Async<FanoutObs> =
        ProofOperation.run
            ctx
            "t1.fanout-and-join"
            {| Input = "go" |}
            { ProofOperationOptions.empty with
                Key = Some "fanout" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx

                let slow =
                    Step.define "corpus/fan-slow" (fun (label: string) ->
                        async {
                            do! CorpusNode.sleep 400
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

                let! basin = CorpusSupport.workloadBasin ctx "t1-fanout"
                let! worker = Worker.run basin "t1-fanout" [ reg slow; reg fast; reg fanned ]
                let client = Client.connect basin
                let! run = fanned.Start client "go" (Id "fanout-1")
                let! result = run.Result

                let observed =
                    { Outcome = result
                      BranchCounts =
                        [ for label in [ "a"; "b"; "c"; "d"; "e" ] -> label, Counter.read scratch ("ran-" + label) ] }

                do! worker.Stop()
                return observed
            })

    let private fanoutChecks (v: Verifiers<FanoutObs>) : Check<FanoutObs> list =
        [ // Join yields every branch; `all` results arrive in list order
          // even though "c" (slow) finishes last.
          LawCheck.equal
              "and! joins both, all preserves list order"
              (fun o -> o.Outcome)
              (Ok "slow:a|fast:b|slow:c,fast:d,fast:e")
          LawCheck.equal
              "every branch executed exactly once"
              (fun o -> o.BranchCounts)
              [ "a", 1; "b", 1; "c", 1; "d", 1; "e", 1 ]
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.fanout-and-join") ]

    let fanoutAndJoin =
        let lawProperty =
            property "t1.fanout-and-join" {
                s2Lite ""
                timeoutMs 120_000
                workload fanoutWorkload
                verify fanoutChecks
            }

        proof "t1.fanout-and-join" {
            describedAs
                "`and!` fans out and joins both values; `Workflow.all` runs all branches, waits for all, and yields results in declaration order."

            property lawProperty
        }

    // ── t1.tagged-select-race ─────────────────────────────────────────────
    // `Workflow.select`: first branch to finish wins and the workflow
    // matches on the CALLER's union — signal beats a far deadline; a near
    // deadline beats signals that never arrive.
    type RaceOutcome =
        | ApprovedBy of string
        | Deadline
        | Withdrawn

    type SelectObs =
        { SignalWon: Result<RaceOutcome, RunFailure>
          TimerWon: Result<RaceOutcome, RunFailure> }

    let private selectWorkload (ctx: WorkloadContext) : Async<SelectObs> =
        ProofOperation.run
            ctx
            "t1.tagged-select-race"
            {| Instances = 2 |}
            { ProofOperationOptions.empty with
                Key = Some "select" }
            (async {
                let decision = Signal.define<string> "corpus/select-decision"
                let withdrawn = Signal.define<unit> "corpus/select-withdrawn"

                let race =
                    Workflow.define "corpus/select" (fun (deadlineSecs: float) ->
                        workflow {
                            let! winner =
                                Workflow.select
                                    [ ApprovedBy ^| decision.Await()
                                      (fun () -> Deadline) ^| Workflow.sleep (Duration.seconds deadlineSecs)
                                      (fun () -> Withdrawn) ^| withdrawn.Await() ]

                            return winner
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-select"
                let! worker = Worker.run basin "t1-select" [ reg race ]
                let client = Client.connect basin

                // Instance A: far deadline; the signal wins.
                let! runA = race.Start client 3600.0 (Id "select-signal")
                do! runA.Signal decision "alice"
                let! resultA = runA.Result

                // Instance B: near deadline; no signal ever arrives.
                let! runB = race.Start client 0.5 (Id "select-deadline")
                let! resultB = runB.Result

                do! worker.Stop()

                return { SignalWon = resultA; TimerWon = resultB }
            })

    let private selectChecks (v: Verifiers<SelectObs>) : Check<SelectObs> list =
        [ LawCheck.equal "signal branch wins and carries its payload" (fun o -> o.SignalWon) (Ok(ApprovedBy "alice"))
          LawCheck.equal "timer branch wins when no signal arrives" (fun o -> o.TimerWon) (Ok Deadline)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.tagged-select-race") ]

    let taggedSelectRace =
        let lawProperty =
            property "t1.tagged-select-race" {
                s2Lite ""
                timeoutMs 120_000
                workload selectWorkload
                verify selectChecks
            }

        proof "t1.tagged-select-race" {
            describedAs
                "`Workflow.select` resolves to the first-finishing tagged branch with its payload: a signal beats a far deadline; a near deadline beats signals that never arrive."

            property lawProperty
        }

    // ── t1.signal-to-parked-across-restart ────────────────────────────────
    // A parked wait is journal state: it pins no process. The signal is sent
    // while NO worker exists anywhere; a later worker resumes the workflow
    // and delivers the payload.
    type ParkObs =
        { Resumed: Result<string, RunFailure> option }

    let private parkWorkload (sendTheSignal: bool) (ctx: WorkloadContext) : Async<ParkObs> =
        ProofOperation.run
            ctx
            "t1.signal-to-parked-across-restart"
            {| Sent = sendTheSignal |}
            { ProofOperationOptions.empty with
                Key = Some "park" }
            (async {
                let go = Signal.define<string> "corpus/park-go"

                let park =
                    Workflow.define "corpus/park" (fun (_: string) ->
                        workflow {
                            let! payload = go.Await()
                            return "woke:" + payload
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-parked"
                let ns = "t1-park"
                let! worker1 = Worker.run basin ns [ reg park ]
                let client = Client.connect basin
                let! run = park.Start client "input" (Id "park-1")

                do!
                    CorpusSupport.until "instance parked (Running)" 60_000 (fun () ->
                        async {
                            let! status = run.Status
                            return status = Running
                        })

                do! worker1.Stop()

                // No worker alive anywhere. Sending still works — signals
                // address the journal, not a process. (The negative control
                // SKIPS the send: nothing may resume the workflow.)
                if sendTheSignal then
                    do! run.Signal go "wake-up"

                let! worker2 = Worker.run basin ns [ reg park ]

                let! resumed =
                    if sendTheSignal then
                        async {
                            let! result = run.Result
                            return Some result
                        }
                    else
                        async {
                            // Bounded observation for the control: after a
                            // real grace period the un-signalled instance
                            // must still be parked. If it somehow resumed,
                            // surface whatever it produced — the control
                            // then rightly reports the system misbehaving.
                            do! CorpusNode.sleep 3_000
                            let! status = run.Status

                            if status = Running then
                                return None
                            else
                                let! result = run.Result
                                return Some result
                        }

                do! worker2.Stop()
                return { Resumed = resumed }
            })

    let private parkChecks (v: Verifiers<ParkObs>) : Check<ParkObs> list =
        [ LawCheck.equal "parked workflow resumed by a later host" (fun o -> o.Resumed) (Some(Ok "woke:wake-up"))
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.signal-to-parked-across-restart") ]

    // Negative control (delivery family): skipping the send must leave the
    // instance parked — the resumed-detector fails for exactly that reason.
    let private parkNoSignalControl =
        negativeControl<ParkObs> "no-signal variant fails the resumed check" {
            workload (parkWorkload false)

            verify
                [ LawCheck.equal "parked workflow resumed by a later host" (fun o -> o.Resumed) (Some(Ok "woke:wake-up")) ]

            expectFailure "parked workflow resumed by a later host"
        }

    let signalToParkedAcrossRestart =
        let lawProperty =
            property "t1.signal-to-parked-across-restart" {
                s2Lite ""
                timeoutMs 120_000
                workload (parkWorkload true)
                verify parkChecks
                negativeControl parkNoSignalControl
                requiresNegativeControl
            }

        proof "t1.signal-to-parked-across-restart" {
            describedAs
                "A signal sent while zero workers are alive is still delivered: the parked wait is journal state and a later worker resumes the workflow with the payload."

            property lawProperty
        }

    // ── t1.timer-across-restart ───────────────────────────────────────────
    // A durable timer fires at-or-after its deadline even when every worker
    // was stopped across the deadline: the wait is journal state.
    type TimerObs = { Before: float; After: float }

    let private timerWorkload (ctx: WorkloadContext) : Async<TimerObs> =
        ProofOperation.run
            ctx
            "t1.timer-across-restart"
            {| Seconds = 3.0 |}
            { ProofOperationOptions.empty with
                Key = Some "timer" }
            (async {
                let timed =
                    Workflow.define "corpus/timer" (fun (secs: float) ->
                        workflow {
                            let! before = Workflow.currentTime
                            do! Workflow.sleep (Duration.seconds secs)
                            let! after = Workflow.currentTime
                            return (before, after)
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-timer-law"
                let ns = "t1-timer"
                let! worker1 = Worker.run basin ns [ reg timed ]
                let client = Client.connect basin
                let! run = timed.Start client 3.0 (Id "timer-1")

                do!
                    CorpusSupport.until "instance running (timer journaled)" 60_000 (fun () ->
                        async {
                            let! status = run.Status
                            return status = Running
                        })

                do! worker1.Stop()
                // The deadline lapses while nothing hosts the timer.
                do! CorpusNode.sleep 3_500

                let! worker2 = Worker.run basin ns [ reg timed ]
                let! result = run.Result
                do! worker2.Stop()

                match result with
                | Ok(before, after) -> return { Before = before; After = after }
                | other -> return failwith (sprintf "timer workflow did not complete: %A" other)
            })

    let private timerChecks (v: Verifiers<TimerObs>) : Check<TimerObs> list =
        [ LawCheck.holds
              "timer fired at-or-after its deadline"
              (fun o -> o.After >= o.Before + 3_000.0)
              (fun o -> sprintf "before=%f after=%f" o.Before o.After)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.timer-across-restart") ]

    let timerAcrossRestart =
        let lawProperty =
            property "t1.timer-across-restart" {
                s2Lite ""
                timeoutMs 120_000
                workload timerWorkload
                verify timerChecks
            }

        proof "t1.timer-across-restart" {
            describedAs
                "A durable timer fires at-or-after its deadline across a full worker outage: the wait is journal state, not process state."

            property lawProperty
        }

    // ── t1.typed-step-failure ─────────────────────────────────────────────
    // Every step failure is a VALUE. Failed leg: retry exhaustion surfaces
    // as `DurableStepFailed(StepError.Failed _)` — catchable in-workflow;
    // uncaught it terminates the run as `Failed(StepError)` in both `Status`
    // and `Result`; retry policies bound handler attempts. Terminal leg
    // (ratified `Step.terminal` amendment): a handler that raises
    // `Step.terminal msg` surfaces as `StepError.Terminal` after exactly ONE
    // attempt — retries bypassed regardless of policy — equally catchable
    // and equally visible in Status/Result.
    type StepFailureObs =
        { CaughtResult: Result<string, RunFailure>
          CaughtStatus: RunStatus
          FlakyCaughtAttempts: int
          UncaughtResult: Result<string, RunFailure>
          UncaughtStatus: RunStatus
          FlakyUncaughtAttempts: int
          TerminalCaughtResult: Result<string, RunFailure>
          FatalCaughtAttempts: int
          TerminalUncaughtResult: Result<string, RunFailure>
          TerminalUncaughtStatus: RunStatus
          FatalUncaughtAttempts: int }

    let private stepFailureWorkload (ctx: WorkloadContext) : Async<StepFailureObs> =
        ProofOperation.run
            ctx
            "t1.typed-step-failure"
            {| Legs = 4 |}
            { ProofOperationOptions.empty with
                Key = Some "step-failure" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx

                let flaky =
                    Step.define "corpus/flaky" (fun (tag: string) ->
                        async {
                            Counter.bump scratch ("flaky-" + tag)
                            return (failwith "boom": string)
                        })

                let fatal =
                    Step.define "corpus/fatal" (fun (tag: string) ->
                        async {
                            Counter.bump scratch ("fatal-" + tag)
                            return (raise (Step.terminal "unrecoverable"): string)
                        })

                let caught =
                    Workflow.define "corpus/failure-caught" (fun (_: string) ->
                        workflow {
                            try
                                let! value = flaky.CallWith (Fixed(3, Duration.seconds 0.2)) "caught"
                                return "unreachable:" + value
                            with DurableStepFailed(StepError.Failed _) ->
                                return "caught-failed"
                        })

                let uncaught =
                    Workflow.define "corpus/failure-uncaught" (fun (_: string) ->
                        workflow {
                            let! value = flaky.CallWith NoRetry "uncaught"
                            return value
                        })

                let terminalCaught =
                    Workflow.define "corpus/terminal-caught" (fun (_: string) ->
                        workflow {
                            try
                                // A GENEROUS policy — Terminal must ignore it.
                                let! value = fatal.CallWith (Fixed(5, Duration.seconds 0.2)) "caught"
                                return "unreachable:" + value
                            with DurableStepFailed(StepError.Terminal _) ->
                                return "caught-terminal"
                        })

                let terminalUncaught =
                    Workflow.define "corpus/terminal-uncaught" (fun (_: string) ->
                        workflow {
                            let! value = fatal.CallWith (Fixed(5, Duration.seconds 0.2)) "uncaught"
                            return value
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-failure"

                let! worker =
                    Worker.run
                        basin
                        "t1-failure"
                        [ reg flaky
                          reg fatal
                          reg caught
                          reg uncaught
                          reg terminalCaught
                          reg terminalUncaught ]

                let client = Client.connect basin

                let! runCaught = caught.Start client "go" (Id "failure-caught-1")
                let! caughtResult = runCaught.Result
                let! caughtStatus = runCaught.Status

                let! runUncaught = uncaught.Start client "go" (Id "failure-uncaught-1")
                let! uncaughtResult = runUncaught.Result
                let! uncaughtStatus = runUncaught.Status

                let! runTermCaught = terminalCaught.Start client "go" (Id "terminal-caught-1")
                let! terminalCaughtResult = runTermCaught.Result

                let! runTermUncaught = terminalUncaught.Start client "go" (Id "terminal-uncaught-1")
                let! terminalUncaughtResult = runTermUncaught.Result
                let! terminalUncaughtStatus = runTermUncaught.Status

                let observed =
                    { CaughtResult = caughtResult
                      CaughtStatus = caughtStatus
                      FlakyCaughtAttempts = Counter.read scratch "flaky-caught"
                      UncaughtResult = uncaughtResult
                      UncaughtStatus = uncaughtStatus
                      FlakyUncaughtAttempts = Counter.read scratch "flaky-uncaught"
                      TerminalCaughtResult = terminalCaughtResult
                      FatalCaughtAttempts = Counter.read scratch "fatal-caught"
                      TerminalUncaughtResult = terminalUncaughtResult
                      TerminalUncaughtStatus = terminalUncaughtStatus
                      FatalUncaughtAttempts = Counter.read scratch "fatal-uncaught" }

                do! worker.Stop()
                return observed
            })

    let private stepFailureChecks (v: Verifiers<StepFailureObs>) : Check<StepFailureObs> list =
        [ LawCheck.equal "exhausted retries are catchable as StepError.Failed" (fun o -> o.CaughtResult) (Ok "caught-failed")
          LawCheck.equal "a workflow that handled the failure completes" (fun o -> o.CaughtStatus) Completed
          LawCheck.equal "Fixed(3, …) bounds the handler to 3 attempts" (fun o -> o.FlakyCaughtAttempts) 3
          LawCheck.holds
              "uncaught step failure surfaces as FailedRun(Failed _)"
              (fun o ->
                  match o.UncaughtResult with
                  | Error(FailedRun(StepError.Failed _)) -> true
                  | _ -> false)
              (fun o -> sprintf "got %A" o.UncaughtResult)
          LawCheck.holds
              "uncaught step failure surfaces in Status"
              (fun o ->
                  match o.UncaughtStatus with
                  | RunStatus.Failed(StepError.Failed _) -> true
                  | _ -> false)
              (fun o -> sprintf "got %A" o.UncaughtStatus)
          LawCheck.equal "NoRetry runs the handler exactly once" (fun o -> o.FlakyUncaughtAttempts) 1
          LawCheck.equal
              "Step.terminal is catchable as StepError.Terminal"
              (fun o -> o.TerminalCaughtResult)
              (Ok "caught-terminal")
          LawCheck.equal
              "Terminal bypasses the retry policy: exactly ONE attempt under Fixed(5, …)"
              (fun o -> o.FatalCaughtAttempts)
              1
          LawCheck.holds
              "uncaught terminal failure surfaces as FailedRun(Terminal _)"
              (fun o ->
                  match o.TerminalUncaughtResult with
                  | Error(FailedRun(StepError.Terminal _)) -> true
                  | _ -> false)
              (fun o -> sprintf "got %A" o.TerminalUncaughtResult)
          LawCheck.holds
              "uncaught terminal failure surfaces in Status"
              (fun o ->
                  match o.TerminalUncaughtStatus with
                  | RunStatus.Failed(StepError.Terminal _) -> true
                  | _ -> false)
              (fun o -> sprintf "got %A" o.TerminalUncaughtStatus)
          LawCheck.equal
              "uncaught terminal also ran exactly once despite Fixed(5, …)"
              (fun o -> o.FatalUncaughtAttempts)
              1
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.typed-step-failure") ]

    let typedStepFailure =
        let lawProperty =
            property "t1.typed-step-failure" {
                s2Lite ""
                timeoutMs 120_000
                workload stepFailureWorkload
                verify stepFailureChecks
            }

        proof "t1.typed-step-failure" {
            describedAs
                "Step failures are typed values in and out of the workflow: retries are policy-bounded, `Step.terminal` bypasses retries after exactly one attempt, and uncaught failures surface as typed terminals in Status and Result."

            property lawProperty
        }

    // ── t1.deterministic-currentTime ──────────────────────────────────────
    // `Workflow.currentTime` is captured once and journal-served on replay:
    // after a restart the program observes the SAME instant it recorded the
    // first time, not a fresh clock read.
    type CurrentTimeObs =
        { Recorded: float
          T1: float
          T2: float
          ObserveAttempts: int }

    let private currentTimeWorkload (ctx: WorkloadContext) : Async<CurrentTimeObs> =
        ProofOperation.run
            ctx
            "t1.deterministic-currentTime"
            {| SleepSeconds = 2.0 |}
            { ProofOperationOptions.empty with
                Key = Some "current-time" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx

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

                let! basin = CorpusSupport.workloadBasin ctx "t1-clock-law"
                let ns = "t1-clock"
                let! worker1 = Worker.run basin ns [ reg observe; reg clock ]
                let client = Client.connect basin
                let! run = clock.Start client "go" (Id "clock-1")

                // First execution recorded t1; restart lands mid-timer,
                // forcing a replay of the currentTime capture.
                do! CorpusSupport.untilCount scratch "observe" 1 60_000
                do! worker1.Stop()
                do! CorpusNode.sleep 2_500

                let! worker2 = Worker.run basin ns [ reg observe; reg clock ]
                let! result = run.Result
                do! worker2.Stop()

                let recorded =
                    match Counter.readValue scratch "first-time" with
                    | Some text -> float text
                    | None -> failwith "side channel never recorded the first time"

                match result with
                | Ok(t1, t2) ->
                    return
                        { Recorded = recorded
                          T1 = t1
                          T2 = t2
                          ObserveAttempts = Counter.read scratch "observe" }
                | other -> return failwith (sprintf "clock workflow did not complete: %A" other)
            })

    let private currentTimeChecks (v: Verifiers<CurrentTimeObs>) : Check<CurrentTimeObs> list =
        [ LawCheck.holds
              "replay observes the SAME captured instant"
              (fun o -> o.T1 = o.Recorded)
              (fun o -> sprintf "recorded=%f replayed=%f" o.Recorded o.T1)
          LawCheck.holds
              "second capture is at-or-after the timer deadline"
              (fun o -> o.T2 >= o.T1 + 2_000.0)
              (fun o -> sprintf "t1=%f t2=%f" o.T1 o.T2)
          LawCheck.equal "the observing step never re-executed on replay" (fun o -> o.ObserveAttempts) 1
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.deterministic-currentTime") ]

    let deterministicCurrentTime =
        let lawProperty =
            property "t1.deterministic-currentTime" {
                s2Lite ""
                timeoutMs 120_000
                workload currentTimeWorkload
                verify currentTimeChecks
            }

        proof "t1.deterministic-currentTime" {
            describedAs
                "`Workflow.currentTime` is captured once and journal-served on replay: after a restart the program observes the SAME instant, and the capturing step never re-executes."

            property lawProperty
        }

    // ── t1.status-and-result-query ────────────────────────────────────────
    // A run is observable from anywhere: `Client.attach` reconstructs the
    // handle by id in another client; Status transitions Running → Completed
    // and Result is queryable (and stable) after completion.
    type StatusObs =
        { AttachedStatus: RunStatus
          OriginalResult: Result<string, RunFailure>
          AttachedResult: Result<string, RunFailure>
          FinalStatus: RunStatus }

    let private statusWorkload (payload: string) (ctx: WorkloadContext) : Async<StatusObs> =
        ProofOperation.run
            ctx
            "t1.status-and-result-query"
            {| Payload = payload |}
            { ProofOperationOptions.empty with
                Key = Some "status" }
            (async {
                let finish = Signal.define<string> "corpus/status-finish"

                let flow =
                    Workflow.define "corpus/status" (fun (_: string) ->
                        workflow {
                            let! value = finish.Await()
                            return "done:" + value
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-status"
                let! worker = Worker.run basin "t1-status" [ reg flow ]
                let clientA = Client.connect basin
                let clientB = Client.connect basin

                let! run = flow.Start clientA "go" (Id "status-1")

                do!
                    CorpusSupport.until "run reports Running while parked" 60_000 (fun () ->
                        async {
                            let! status = run.Status
                            return status = Running
                        })

                // Reattach BY ID from a different client.
                let attached = Client.attach<string> clientB (Id "status-1")
                let! attachedStatus = attached.Status

                // The negative control signals a WRONG payload here: the
                // observed result must then differ from the pinned one.
                do! attached.Signal finish payload

                let! result = run.Result
                let! attachedResult = attached.Result
                let! finalStatus = attached.Status

                do! worker.Stop()

                return
                    { AttachedStatus = attachedStatus
                      OriginalResult = result
                      AttachedResult = attachedResult
                      FinalStatus = finalStatus }
            })

    let private statusChecks (v: Verifiers<StatusObs>) : Check<StatusObs> list =
        [ LawCheck.equal "an attached handle sees the same status" (fun o -> o.AttachedStatus) Running
          LawCheck.equal "the original handle observes the result" (fun o -> o.OriginalResult) (Ok "done:x")
          LawCheck.equal "the attached handle observes the same result" (fun o -> o.AttachedResult) (Ok "done:x")
          LawCheck.equal "status settles at Completed" (fun o -> o.FinalStatus) Completed
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.status-and-result-query") ]

    // Negative control (observation family): a run driven with the WRONG
    // input must fail the pinned-result detector.
    let private statusWrongPayloadControl =
        negativeControl<StatusObs> "wrong signal payload fails the pinned-result check" {
            workload (statusWorkload "y")

            verify [ LawCheck.equal "the original handle observes the result" (fun o -> o.OriginalResult) (Ok "done:x") ]

            expectFailure "the original handle observes the result"
        }

    let statusAndResultQuery =
        let lawProperty =
            property "t1.status-and-result-query" {
                s2Lite ""
                timeoutMs 120_000
                workload (statusWorkload "x")
                verify statusChecks
                negativeControl statusWrongPayloadControl
                requiresNegativeControl
            }

        proof "t1.status-and-result-query" {
            describedAs
                "Status and result are observable from any client by id: an attached handle sees the same status and result, and both are stable after completion."

            property lawProperty
        }

    // ── t1.andbang-teaching ───────────────────────────────────────────────
    // The teaching law: `let!` SEQUENCES (the second step starts only after
    // the first completes); `and!` FANS OUT (a fast branch finishes before a
    // slow one that was written first). Pinned via handler completion order.
    type AndbangObs =
        { SeqResult: Result<string, RunFailure>
          SeqOrder: string list
          ParResult: Result<string, RunFailure>
          ParOrder: string list }

    let private andbangWorkload (shamSequentialFanout: bool) (ctx: WorkloadContext) : Async<AndbangObs> =
        ProofOperation.run
            ctx
            "t1.andbang-teaching"
            {| Sham = shamSequentialFanout |}
            { ProofOperationOptions.empty with
                Key = Some "andbang" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx

                let mark =
                    Step.define "corpus/teach-mark" (fun (input: string * string) ->
                        async {
                            let group, label = input

                            if label = "slow" then
                                do! CorpusNode.sleep 500

                            Counter.appendLine scratch ("order-" + group) label
                            return label
                        })

                let sequential =
                    Workflow.define "corpus/teach-seq" (fun (_: string) ->
                        workflow {
                            let! a = mark.Call("seq", "slow")
                            let! b = mark.Call("seq", "fast")
                            return a + "," + b
                        })

                let fannedOut =
                    if shamSequentialFanout then
                        // NEGATIVE CONTROL ONLY: claims to fan out but
                        // sequences — the completion-order detector must
                        // catch it.
                        Workflow.define "corpus/teach-par" (fun (_: string) ->
                            workflow {
                                let! a = mark.Call("par", "slow")
                                let! b = mark.Call("par", "fast")
                                return a + "," + b
                            })
                    else
                        Workflow.define "corpus/teach-par" (fun (_: string) ->
                            workflow {
                                let! a = mark.Call("par", "slow")
                                and! b = mark.Call("par", "fast")
                                return a + "," + b
                            })

                let! basin = CorpusSupport.workloadBasin ctx "t1-teach"
                let! worker = Worker.run basin "t1-teach" [ reg mark; reg sequential; reg fannedOut ]
                let client = Client.connect basin

                let! runSeq = sequential.Start client "go" (Id "teach-seq-1")
                let! resultSeq = runSeq.Result

                let! runPar = fannedOut.Start client "go" (Id "teach-par-1")
                let! resultPar = runPar.Result

                let observed =
                    { SeqResult = resultSeq
                      SeqOrder = Counter.readLines scratch "order-seq"
                      ParResult = resultPar
                      ParOrder = Counter.readLines scratch "order-par" }

                do! worker.Stop()
                return observed
            })

    let private andbangChecks (v: Verifiers<AndbangObs>) : Check<AndbangObs> list =
        [ LawCheck.equal "let! yields in program order" (fun o -> o.SeqResult) (Ok "slow,fast")
          LawCheck.equal
              "let! sequences: the slow step COMPLETES before the fast one starts"
              (fun o -> o.SeqOrder)
              [ "slow"; "fast" ]
          LawCheck.equal "and! still binds values in program order" (fun o -> o.ParResult) (Ok "slow,fast")
          LawCheck.equal
              "and! fans out: the fast branch finishes FIRST despite being written second"
              (fun o -> o.ParOrder)
              [ "fast"; "slow" ]
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.andbang-teaching") ]

    // Negative control (composition family): a sequential implementation
    // masquerading as fan-out must fail the completion-order detector.
    let private andbangShamControl =
        negativeControl<AndbangObs> "sequential sham fan-out fails the completion-order check" {
            workload (andbangWorkload true)

            verify
                [ LawCheck.equal
                      "and! fans out: the fast branch finishes FIRST despite being written second"
                      (fun o -> o.ParOrder)
                      [ "fast"; "slow" ] ]

            expectFailure "and! fans out"
        }

    let andbangTeaching =
        let lawProperty =
            property "t1.andbang-teaching" {
                s2Lite ""
                timeoutMs 120_000
                workload (andbangWorkload false)
                verify andbangChecks
                negativeControl andbangShamControl
                requiresNegativeControl
            }

        proof "t1.andbang-teaching" {
            describedAs
                "`let!` sequences; `and!` fans out (a faster later branch finishes first) — pinned via handler completion order."

            property lawProperty
        }

    // ── t1.bounded-loop-flat-stack ────────────────────────────────────────
    // Bounded loops recurse in-instance with a FLAT stack: ≥500 guarded
    // `return!` iterations (each journaling a step) complete under Node
    // without stack overflow, and replay converges to the same value.
    let private loopWorkload (ctx: WorkloadContext) : Async<Result<int, RunFailure>> =
        ProofOperation.run
            ctx
            "t1.bounded-loop-flat-stack"
            {| Iterations = 500 |}
            { ProofOperationOptions.empty with
                Key = Some "loop" }
            (async {
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

                let! basin = CorpusSupport.workloadBasin ctx "t1-looped"
                let! worker = Worker.run basin "t1-loop" [ reg inc; reg looped ]
                let client = Client.connect basin
                let! run = looped.Start client 500 (Id "loop-1")
                let! result = run.Result
                do! worker.Stop()
                return result
            })

    let private loopChecks (v: Verifiers<Result<int, RunFailure>>) : Check<Result<int, RunFailure>> list =
        [ // sum of 1..500
          LawCheck.equal "500 recursive iterations complete with a flat stack" (fun o -> o) (Ok 125_250)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.bounded-loop-flat-stack") ]

    let boundedLoopFlatStack =
        let lawProperty =
            property "t1.bounded-loop-flat-stack" {
                s2Lite ""
                timeoutMs 600_000
                workload loopWorkload
                verify loopChecks
            }

        proof "t1.bounded-loop-flat-stack" {
            describedAs
                "≥500 guarded recursive `return!` iterations (each journaling a step) complete with a flat stack and replay-converge to the same value."

            property lawProperty
        }
