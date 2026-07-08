/// ═══════════════════════════════════════════════════════════════════════
/// T1 red corpus — composition & lifecycle laws: sagas across kills,
/// recoverable cancellation, contract sharing, durable children, and
/// eternal (ContinueAsNew) workflows.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Corpus

open Firegrid.Durable

// ── t1.saga-compensation-across-kill ──────────────────────────────────────
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
                    return (failwith "no seats" : string)
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

    let childHost () : Async<int> =
        async {
            let basin = Harness.childBasin ()
            let bookCar, bookHotel, bookFlight, cancelHotel, cancelCar, saga = defs (Node.env "T1C_SCRATCH")

            let! _worker =
                Worker.run
                    basin
                    (Node.env "T1C_NS")
                    [ reg bookCar; reg bookHotel; reg bookFlight; reg cancelHotel; reg cancelCar; reg saga ]

            return! Harness.foreverChild ()
        }

module FlowLaws =

    let sagaCompensationAcrossKill: Law =
        { Id = "t1.saga-compensation-across-kill"
          TimeoutMs = 240_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "saga-kill"
                    let _, _, _, _, _, saga = SagaScenario.defs scratch

                    do!
                        Harness.withEnv "saga-kill" (fun env ->
                            async {
                                let ns = "t1-saga"
                                let child = Harness.spawnChildHost env "saga-host" ns scratch
                                let client = Client.connect env.Basin

                                let! run = saga.Start client "trip-7" (Id "saga-1")

                                // The failure fired, the FIRST compensation committed…
                                do! Harness.untilCount scratch "cancel-hotel" 1 60_000
                                // …and the host dies mid-compensation.
                                child.kill "SIGKILL" |> ignore
                                do! Node.sleep 300

                                Expect.equal "second compensation had not run at the kill" 0 (Counter.read scratch "cancel-car")

                                // Restart: the remaining compensation completes.
                                let! worker =
                                    Worker.run
                                        env.Basin
                                        ns
                                        (let bc, bh, bf, ch, cc, sg = SagaScenario.defs scratch in
                                         [ reg bc; reg bh; reg bf; reg ch; reg cc; reg sg ])

                                let! result = run.Result
                                Expect.equal "the saga settles rolled-back" (Ok "rolled-back") result

                                Expect.equal "completed compensation did NOT re-run" 1 (Counter.read scratch "cancel-hotel")
                                Expect.equal "remaining compensation ran exactly once" 1 (Counter.read scratch "cancel-car")
                                Expect.equal "forward step 1 ran exactly once" 1 (Counter.read scratch "book-car")
                                Expect.equal "forward step 2 ran exactly once" 1 (Counter.read scratch "book-hotel")
                                Expect.equal "the failing step ran exactly once (NoRetry)" 1 (Counter.read scratch "book-flight")
                                do! worker.Stop ()
                            })
                } }

    // ── t1.recoverable-cancellation ───────────────────────────────────────
    // `run.Cancel` lands at the next bind boundary as catchable
    // `DurableCancelled`: caught → journaled compensation → ordinary return
    // (Completed). Uncaught → the run terminates as Cancelled, and Result is
    // `Error CancelledRun`.
    let recoverableCancellation: Law =
        { Id = "t1.recoverable-cancellation"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "cancel"
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
                                    let! value = never.Await ()
                                    return "signal:" + value
                                with DurableCancelled ->
                                    // MORE journaled work is allowed after cancellation:
                                    do! compensate.Call "c"
                                    return "compensated"
                            })

                    let bare =
                        Workflow.define "corpus/cancel-bare" (fun (_: string) ->
                            workflow {
                                let! value = never.Await ()
                                return "signal:" + value
                            })

                    do!
                        Harness.withEnv "cancel" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-cancel" [ reg compensate; reg recovering; reg bare ]
                                let client = Client.connect env.Basin

                                let! runA = recovering.Start client "go" (Id "cancel-recover-1")

                                do!
                                    Harness.until "recovering instance parked" 60_000 (fun () ->
                                        async {
                                            let! status = runA.Status
                                            return status = Running
                                        })

                                do! runA.Cancel ()
                                let! resultA = runA.Result
                                Expect.equal "caught cancellation compensates and returns" (Ok "compensated") resultA
                                let! statusA = runA.Status
                                Expect.equal "a workflow that caught the cancel COMPLETES" Completed statusA
                                Expect.equal "the compensation step ran exactly once" 1 (Counter.read scratch "compensate")

                                let! runB = bare.Start client "go" (Id "cancel-bare-1")

                                do!
                                    Harness.until "bare instance parked" 60_000 (fun () ->
                                        async {
                                            let! status = runB.Status
                                            return status = Running
                                        })

                                do! runB.Cancel ()
                                let! resultB = runB.Result
                                Expect.equal "uncaught cancellation is a typed terminal" (Error CancelledRun) resultB
                                let! statusB = runB.Status
                                Expect.equal "status settles at Cancelled" Cancelled statusB
                                do! worker.Stop ()
                            })
                } }

    // ── t1.declare-implement-roundtrip ────────────────────────────────────
    // The declaration is the contract: a caller holding only
    // `Workflow.declare`/`Step.declare` values (fresh instances — not the
    // implementer's) starts the workflow and collects its result; the worker
    // binds bodies with `Worker.implement`/`implementStep`.
    let declareImplementRoundtrip: Law =
        { Id = "t1.declare-implement-roundtrip"
          TimeoutMs = 120_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "declare"

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

                    do!
                        Harness.withEnv "declare" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-declare" [ reg embed; reg score ]

                                // Caller process: a FRESH declaration value, no body.
                                let client = Client.connect env.Basin
                                let callerHandle = declaredScore ()
                                let! run = callerHandle.Start client "hello" (Id "score-1")
                                let! result = run.Result

                                Expect.equal "the declared workflow round-trips through its implementation" (Ok 2.5) result
                                Expect.equal "the declared step's bound body executed once" 1 (Counter.read scratch "embed")
                                do! worker.Stop ()
                            })
                } }

    // ── t1.child-spawn ────────────────────────────────────────────────────
    // Durable children: `CallChild` journals the parent/child link and waits
    // for the child's result; fan-out composes as `Workflow.all` over
    // children, results in order, each child executed exactly once.
    let childSpawn: Law =
        { Id = "t1.child-spawn"
          TimeoutMs = 180_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "child-spawn"

                    let childStep =
                        Step.define "corpus/child-step" (fun (tag: string) ->
                            async {
                                Counter.bump scratch ("child-" + tag)
                                return "child:" + tag
                            })

                    let childWf =
                        Workflow.define "corpus/child-wf" (fun (tag: string) ->
                            workflow {
                                let! value = childStep.Call tag
                                return value
                            })

                    let parentWf =
                        Workflow.define "corpus/parent-wf" (fun (_: string) ->
                            workflow {
                                let! one = childWf.CallChild "one"
                                let! fanned = Workflow.all [ for tag in [ "a"; "b"; "c" ] -> childWf.CallChild tag ]
                                return one + "|" + String.concat "," fanned
                            })

                    do!
                        Harness.withEnv "child-spawn" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-child" [ reg childStep; reg childWf; reg parentWf ]
                                let client = Client.connect env.Basin
                                let! run = parentWf.Start client "go" (Id "parent-1")
                                let! result = run.Result

                                Expect.equal
                                    "children ran and fan-out preserved list order"
                                    (Ok "child:one|child:a,child:b,child:c")
                                    result

                                for tag in [ "one"; "a"; "b"; "c" ] do
                                    Expect.equal (sprintf "child %s executed exactly once" tag) 1 (Counter.read scratch ("child-" + tag))

                                do! worker.Stop ()
                            })
                } }

    // ── t1.eternal-continueasnew ──────────────────────────────────────────
    // Eternal workflows roll generations: `ContinueAsNew state` carries the
    // state into a fresh generation (whose journal does not replay its
    // predecessors'), `Stop` terminates the chain, and the original handle's
    // Result follows the chain. Each generation observes the ROLLED state
    // and executes its step exactly once — pinned via the side-channel
    // progression. (Per-generation journal length is sub-public; the
    // bounded-history half of the law is enforced at the kernel row.)
    type GenState = { Remaining: int; Applied: int }

    let eternalContinueAsNew: Law =
        { Id = "t1.eternal-continueasnew"
          TimeoutMs = 180_000
          Run =
            fun () ->
                async {
                    let scratch = Harness.scratchFor "eternal"

                    let tick =
                        Step.define "corpus/gen-tick" (fun (gen: GenState) ->
                            async {
                                Counter.appendLine scratch "generations" (sprintf "%d:%d" gen.Remaining gen.Applied)
                                return gen.Applied + 1
                            })

                    let eternal =
                        Workflow.defineEternal "corpus/eternal" (fun (gen: GenState) ->
                            workflow {
                                let! applied = tick.Call gen

                                if gen.Remaining <= 1 then
                                    return Stop
                                else
                                    return ContinueAsNew { Remaining = gen.Remaining - 1; Applied = applied }
                            })

                    do!
                        Harness.withEnv "eternal" (fun env ->
                            async {
                                let! worker = Worker.run env.Basin "t1-eternal" [ reg tick; reg eternal ]
                                let client = Client.connect env.Basin

                                let! run = eternal.Start client { Remaining = 3; Applied = 0 } (Id "eternal-1")
                                let! result = run.Result
                                Expect.equal "the generation chain terminates on Stop" (Ok ()) result

                                Expect.equal
                                    "each generation ran once with the CARRIED state (no replayed predecessors)"
                                    [ "3:0"; "2:1"; "1:2" ]
                                    (Counter.readLines scratch "generations")

                                let! status = run.Status
                                Expect.equal "the original handle follows the chain to Completed" Completed status
                                do! worker.Stop ()
                            })
                } }
