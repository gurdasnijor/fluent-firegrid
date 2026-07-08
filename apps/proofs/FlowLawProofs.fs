/// ═══════════════════════════════════════════════════════════════════════
/// T1 corpus — composition & lifecycle laws (Packet 0.2 re-expression):
/// sagas across kills, recoverable cancellation, contract sharing, durable
/// children, and eternal (ContinueAsNew) workflows. Law ids, semantics, and
/// ratchet status FROZEN; only ADDITIVE strength (fault evidence, trace
/// checks, negative controls).
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Firegrid.Durable

// ── t1.saga-compensation-across-kill scenario ─────────────────────────────
// Compensation is journaled work: after a mid-compensation SIGKILL the
// remaining compensations complete and none re-run.
module SagaScenario =
    let defs (scratch: string) =
        let bookCar =
            Step.define "corpus/book-car" (fun (_trip: string) ->
                async {
                    Counter.bump scratch "book-car"
                    return "car-ok"
                })

        let bookHotel =
            Step.define "corpus/book-hotel" (fun (_trip: string) ->
                async {
                    Counter.bump scratch "book-hotel"
                    return "hotel-ok"
                })

        let bookFlight =
            Step.define "corpus/book-flight" (fun (_trip: string) ->
                async {
                    Counter.bump scratch "book-flight"
                    return (failwith "no seats": string)
                })

        let cancelHotel =
            Step.define "corpus/cancel-hotel" (fun (_trip: string) ->
                async {
                    Counter.bump scratch "cancel-hotel"
                    return ()
                })

        let cancelCar =
            Step.define "corpus/cancel-car" (fun (_trip: string) ->
                async {
                    Counter.bump scratch "cancel-car"
                    return ()
                })

        let saga =
            Workflow.define "corpus/saga" (fun (trip: string) ->
                workflow {
                    try
                        let! car = bookCar.Call trip
                        let! hotel = bookHotel.Call trip
                        let! flight = bookFlight.CallWith NoRetry trip
                        return "booked:" + car + "," + hotel + "," + flight
                    with DurableStepFailed _ ->
                        // Compensations, journaled. The durable timer between
                        // them is the kill window: the host dies AFTER the
                        // first compensation committed, BEFORE the second ran.
                        do! cancelHotel.Call trip
                        do! Workflow.sleep (Duration.seconds 3.0)
                        do! cancelCar.Call trip
                        return "rolled-back"
                })

        bookCar, bookHotel, bookFlight, cancelHotel, cancelCar, saga

    let basinName = "t1-saga-kill"
    let ns = "t1-saga"

    let childHost () : Async<int> =
        async {
            let! basin = CorpusSupport.childBasin ()
            let scratch = CorpusSupport.childScratch ()
            let bookCar, bookHotel, bookFlight, cancelHotel, cancelCar, saga = defs scratch

            let! _worker =
                Worker.run
                    basin
                    (CorpusSupport.childNamespace ())
                    [ reg bookCar
                      reg bookHotel
                      reg bookFlight
                      reg cancelHotel
                      reg cancelCar
                      reg saga ]

            return! CorpusSupport.foreverChild ()
        }

module FlowLawProofs =
    open SagaScenario

    // ── t1.saga-compensation-across-kill ──────────────────────────────────
    type SagaObs =
        { CancelCarAtKill: int
          Outcome: Result<string, RunFailure>
          CancelHotelRuns: int
          CancelCarRuns: int
          BookCarRuns: int
          BookHotelRuns: int
          BookFlightRuns: int }

    let private sagaWorkload (forgeCompensationRerun: bool) (ctx: WorkloadContext) : Async<SagaObs> =
        ProofOperation.run
            ctx
            "t1.saga-compensation-across-kill"
            {| Forged = forgeCompensationRerun |}
            { ProofOperationOptions.empty with
                Key = Some "saga-kill" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                // Surface first: while red this throws immediately.
                let _, _, _, _, _, saga = defs scratch
                let! basin = CorpusSupport.workloadBasin ctx basinName
                let client = Client.connect basin

                let! run = saga.Start client "trip-7" (Id "saga-1")

                // The failure fired, the FIRST compensation committed…
                do! CorpusSupport.untilCount scratch "cancel-hotel" 1 60_000
                // …and the host dies mid-compensation (fault-controller kill:
                // report-level fault event + verification.host.kill span).
                do! WorkloadContext.killHost "saga-host" ctx
                do! CorpusNode.sleep 300

                let cancelCarAtKill = Counter.read scratch "cancel-car"

                if forgeCompensationRerun then
                    // NEGATIVE CONTROL ONLY: forge a duplicate compensation
                    // execution in the side channel — the un-repeated
                    // detector must catch exactly this.
                    Counter.bump scratch "cancel-hotel"

                // Restart: the remaining compensation completes.
                let! worker =
                    Worker.run
                        basin
                        ns
                        (let bc, bh, bf, ch, cc, sg = defs scratch in
                         [ reg bc; reg bh; reg bf; reg ch; reg cc; reg sg ])

                let! result = run.Result

                let observed =
                    { CancelCarAtKill = cancelCarAtKill
                      Outcome = result
                      CancelHotelRuns = Counter.read scratch "cancel-hotel"
                      CancelCarRuns = Counter.read scratch "cancel-car"
                      BookCarRuns = Counter.read scratch "book-car"
                      BookHotelRuns = Counter.read scratch "book-hotel"
                      BookFlightRuns = Counter.read scratch "book-flight" }

                do! worker.Stop()
                return observed
            })

    let private sagaChecks (v: Verifiers<SagaObs>) : Check<SagaObs> list =
        [ LawCheck.equal "second compensation had not run at the kill" (fun o -> o.CancelCarAtKill) 0
          LawCheck.equal "the saga settles rolled-back" (fun o -> o.Outcome) (Ok "rolled-back")
          LawCheck.equal "completed compensation did NOT re-run" (fun o -> o.CancelHotelRuns) 1
          LawCheck.equal "remaining compensation ran exactly once" (fun o -> o.CancelCarRuns) 1
          LawCheck.equal "forward step 1 ran exactly once" (fun o -> o.BookCarRuns) 1
          LawCheck.equal "forward step 2 ran exactly once" (fun o -> o.BookHotelRuns) 1
          LawCheck.equal "the failing step ran exactly once (NoRetry)" (fun o -> o.BookFlightRuns) 1
          v.Host.Started "saga-host"
          v.Fault.HostKillReported "saga-host"
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.saga-compensation-across-kill")
          v.Trace.Sql "kill span carries signal and accepted flag" (CorpusSupport.killSpanSql "saga-host") ]

    // Negative control (flow family): a forged re-run of the completed
    // compensation must fail the un-repeated detector.
    let private sagaForgedControl =
        negativeControl<SagaObs> "forged compensation re-run fails the un-repeated check" {
            workload (sagaWorkload true)
            verify [ LawCheck.equal "completed compensation did NOT re-run" (fun o -> o.CancelHotelRuns) 1 ]
            expectFailure "completed compensation did NOT re-run"
        }

    let sagaCompensationAcrossKill =
        let lawProperty =
            property "t1.saga-compensation-across-kill" {
                s2Lite ""
                processHost (CorpusSupport.childHostSpec "saga-host" "saga-host" basinName ns)
                timeoutMs 240_000
                workload (sagaWorkload false)
                verify sagaChecks
                negativeControl sagaForgedControl
                requiresNegativeControl
            }

        proof "t1.saga-compensation-across-kill" {
            describedAs
                "A mid-compensation SIGKILL loses nothing: completed compensations are not repeated, the remaining compensations complete, and every forward step ran exactly once."

            property lawProperty
        }

    // ── t1.recoverable-cancellation ───────────────────────────────────────
    // `run.Cancel` lands at the next bind boundary as catchable
    // `DurableCancelled`: caught → journaled compensation → ordinary return
    // (Completed). Uncaught → the run terminates as Cancelled, and Result is
    // `Error CancelledRun`.
    type CancelObs =
        { RecoverResult: Result<string, RunFailure>
          RecoverStatus: RunStatus
          CompensateRuns: int
          BareResult: Result<string, RunFailure>
          BareStatus: RunStatus }

    let private cancelWorkload (ctx: WorkloadContext) : Async<CancelObs> =
        ProofOperation.run
            ctx
            "t1.recoverable-cancellation"
            {| Instances = 2 |}
            { ProofOperationOptions.empty with
                Key = Some "cancel" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let never = Signal.define<string> "corpus/never"

                let compensate =
                    Step.define "corpus/compensate" (fun (_: string) ->
                        async {
                            Counter.bump scratch "compensate"
                            return ()
                        })

                let recovering =
                    Workflow.define "corpus/cancel-recover" (fun (_: string) ->
                        workflow {
                            try
                                let! value = never.Await()
                                return "signal:" + value
                            with DurableCancelled ->
                                // MORE journaled work is allowed after cancellation:
                                do! compensate.Call "c"
                                return "compensated"
                        })

                let bare =
                    Workflow.define "corpus/cancel-bare" (fun (_: string) ->
                        workflow {
                            let! value = never.Await()
                            return "signal:" + value
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-cancel"
                let! worker = Worker.run basin "t1-cancel" [ reg compensate; reg recovering; reg bare ]
                let client = Client.connect basin

                let! runA = recovering.Start client "go" (Id "cancel-recover-1")

                do!
                    CorpusSupport.until "recovering instance parked" 60_000 (fun () ->
                        async {
                            let! status = runA.Status
                            return status = Running
                        })

                do! runA.Cancel()
                let! resultA = runA.Result
                let! statusA = runA.Status

                let! runB = bare.Start client "go" (Id "cancel-bare-1")

                do!
                    CorpusSupport.until "bare instance parked" 60_000 (fun () ->
                        async {
                            let! status = runB.Status
                            return status = Running
                        })

                do! runB.Cancel()
                let! resultB = runB.Result
                let! statusB = runB.Status

                do! worker.Stop()

                return
                    { RecoverResult = resultA
                      RecoverStatus = statusA
                      CompensateRuns = Counter.read scratch "compensate"
                      BareResult = resultB
                      BareStatus = statusB }
            })

    let private cancelChecks (v: Verifiers<CancelObs>) : Check<CancelObs> list =
        [ LawCheck.equal "caught cancellation compensates and returns" (fun o -> o.RecoverResult) (Ok "compensated")
          LawCheck.equal "a workflow that caught the cancel COMPLETES" (fun o -> o.RecoverStatus) Completed
          LawCheck.equal "the compensation step ran exactly once" (fun o -> o.CompensateRuns) 1
          LawCheck.equal "uncaught cancellation is a typed terminal" (fun o -> o.BareResult) (Error CancelledRun)
          LawCheck.equal "status settles at Cancelled" (fun o -> o.BareStatus) Cancelled
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.recoverable-cancellation") ]

    let recoverableCancellation =
        let lawProperty =
            property "t1.recoverable-cancellation" {
                s2Lite ""
                timeoutMs 120_000
                workload cancelWorkload
                verify cancelChecks
            }

        proof "t1.recoverable-cancellation" {
            describedAs
                "Cancel lands at the next bind boundary as a catchable value: caught → journaled compensation → Completed; uncaught → typed Cancelled terminal in Status and Result."

            property lawProperty
        }

    // ── t1.declare-implement-roundtrip ────────────────────────────────────
    // The declaration is the contract: a caller holding only
    // `Workflow.declare`/`Step.declare` values (fresh instances — not the
    // implementer's) starts the workflow and collects its result; the worker
    // binds bodies with `Worker.implement`/`implementStep`.
    type DeclareObs =
        { Outcome: Result<float, RunFailure>
          EmbedRuns: int }

    let private declareWorkload (ctx: WorkloadContext) : Async<DeclareObs> =
        ProofOperation.run
            ctx
            "t1.declare-implement-roundtrip"
            {| Input = "hello" |}
            { ProofOperationOptions.empty with
                Key = Some "declare" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx

                // Shared contract (both "processes" construct their own copy):
                let declaredScore () = Workflow.declare<string, float> "corpus/score"
                let declaredEmbed () = Step.declare<string, int> "corpus/embed"

                // Worker process: binds the bodies.
                let embed =
                    Worker.implementStep (declaredEmbed ()) (fun (text: string) ->
                        async {
                            Counter.bump scratch "embed"
                            return String.length text
                        })

                let score =
                    Worker.implement (declaredScore ()) (fun text ->
                        workflow {
                            let! length = embed.Call text
                            return float length * 0.5
                        })

                let! basin = CorpusSupport.workloadBasin ctx "t1-declare"
                let! worker = Worker.run basin "t1-declare" [ reg embed; reg score ]

                // Caller process: a FRESH declaration value, no body.
                let client = Client.connect basin
                let callerHandle = declaredScore ()
                let! run = callerHandle.Start client "hello" (Id "score-1")
                let! result = run.Result

                do! worker.Stop()

                return
                    { Outcome = result
                      EmbedRuns = Counter.read scratch "embed" }
            })

    let private declareChecks (v: Verifiers<DeclareObs>) : Check<DeclareObs> list =
        [ LawCheck.equal
              "the declared workflow round-trips through its implementation"
              (fun o -> o.Outcome)
              (Ok 2.5)
          LawCheck.equal "the declared step's bound body executed once" (fun o -> o.EmbedRuns) 1
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t1.declare-implement-roundtrip") ]

    let declareImplementRoundtrip =
        let lawProperty =
            property "t1.declare-implement-roundtrip" {
                s2Lite ""
                timeoutMs 120_000
                workload declareWorkload
                verify declareChecks
            }

        proof "t1.declare-implement-roundtrip" {
            describedAs
                "A declared bodyless contract is callable by a caller holding only fresh declaration values; the implementation is bound independently by the worker."

            property lawProperty
        }
