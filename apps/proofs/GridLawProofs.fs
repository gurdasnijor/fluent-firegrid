/// ═══════════════════════════════════════════════════════════════════════
/// T2 corpus — the firegrid scenario laws (Phase C / C1), authored
/// BLACK-BOX against the `Firegrid.Grid` public surface: the eight ratified
/// scenarios of `src/Firegrid.Grid/GridExamples.fs` plus two laws pinning
/// known open semantics (reserved-segment admission at the Grid surface;
/// eager + concurrent spawn_all). ALL RED at authoring: every law runs and
/// fails as a law (pass:false, diagnostics, no crash) against the notYet
/// skeleton; green-making is the follow-on packets' work. Surface values
/// are constructed inside workloads so the red surface fails fast per law
/// instead of at module import. The model step is the scripted ModelSays
/// stub (GridCorpusSupport.fs) — deterministic data, not a model.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Firegrid
open Firegrid.Durable

// ── t2.converse-across-crashes scenario ──────────────────────────────────
// GridExamples §2 (Converse): one prompt starts a durable turn in a child
// host; the tool executes; every process dies; a fresh process resumes the
// turn — the journaled tool call is never re-executed, the turn completes,
// and an after-the-fact watch replays the whole recorded conversation.
module ConverseScenario =
    let basinName = "t2-converse"
    let ns = "t2-converse"

    let script: ModelSays.Script =
        { Agent = "assistant"
          Turns =
            [ [ ModelSays.Say "Investigating slow deploys."
                ModelSays.CallTool("search", "deploy timings last 24h")
                ModelSays.Say "Cold caches on the build fleet — fixed."
                ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let search =
            Tool.define "search" "Search the web" (fun (query: string) ->
                async {
                    Counter.bump scratch "search"
                    return "results: " + query
                })

        Agent.define
            "assistant"
            (ModelSays.harness script)
            { Instructions = "You are a careful infrastructure assistant."
              Tools = [ search ]
              TurnTimeout = Some(Duration.hours 8.0)
              RolloverEvery = 200 }

    /// Child host: serves the grid until the runner's fault controller
    /// SIGKILLs it mid-turn.
    let childHost () : Async<int> =
        async {
            let! basin = CorpusSupport.childBasin ()
            let scratch = CorpusSupport.childScratch ()
            let! _worker = Grid.serve basin (CorpusSupport.childNamespace ()) [ defs scratch ]
            return! CorpusSupport.foreverChild ()
        }

// ── t2.days-long-approval scenario ───────────────────────────────────────
// GridExamples §3 (Approval): a gated tool parks the turn on a human
// approval — one journal record, no process pinned (the only worker is
// STOPPED while parked). A different process approves later; the turn
// wakes and the gated tool runs exactly once, only after approval.
module ApprovalScenario =
    let basinName = "t2-approval"
    let ns = "t2-approval"
    let approvalPrompt = "Deploying to PRODUCTION — approve?"

    let script: ModelSays.Script =
        { Agent = "deployer"
          Turns =
            [ [ ModelSays.CallTool("deploy", "production")
                ModelSays.Say "Deployed."
                ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let deploy =
            Tool.define "deploy" "Deploy to production" (fun (env: string) ->
                async {
                    Counter.bump scratch "deploy"
                    return "deployed to " + env
                })

        Agent.define
            "deployer"
            (ModelSays.harness script)
            { Instructions = "Deploy when asked; production deploys are gated."
              Tools = [ Tool.gated approvalPrompt deploy ]
              TurnTimeout = None
              RolloverEvery = 200 }

// ── t2.researcher-writer-choreography scenario ───────────────────────────
// GridExamples §4 (Choreography): a writer session parks on a topic; a
// researcher session — which has never heard of the writer — publishes;
// the writer wakes with the findings AND its self-prompt as a NEW turn.
// Sessions never address each other; the topic log is the coordination.
module ChoreographyScenario =
    let basinName = "t2-choreo"
    let ns = "t2-choreo"
    let findingsPayload = "s2-perf: tail latency dominated by resends"
    let selfPrompt = "Findings arrived — draft the report."

    let researcherScript: ModelSays.Script =
        { Agent = "researcher"
          Turns =
            [ [ ModelSays.Say "Digging into S2 perf."
                ModelSays.Publish("findings", findingsPayload)
                ModelSays.EndTurn ] ] }

    let writerScript: ModelSays.Script =
        { Agent = "writer"
          Turns =
            [ [ ModelSays.WaitFor("findings", Some "topic == 's2-perf'", Some selfPrompt) ]
              [ ModelSays.RecordInput "record"; ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let record =
            Tool.define "record" "Record the drafting input" (fun (text: string) ->
                async {
                    Counter.appendLine scratch "writer-input" text
                    return "recorded"
                })

        let researcher =
            Agent.define
                "researcher"
                (ModelSays.harness researcherScript)
                { Instructions = "Research the topic; publish findings to 'findings' when confident."
                  Tools = []
                  TurnTimeout = None
                  RolloverEvery = 200 }

        let writer =
            Agent.define
                "writer"
                (ModelSays.harness writerScript)
                { Instructions = "wait_for findings on topics you're assigned; draft when they arrive."
                  Tools = [ record ]
                  TurnTimeout = None
                  RolloverEvery = 200 }

        researcher, writer

// ── t2.scheduled-self-prompts scenario ───────────────────────────────────
// GridExamples §5 (SelfPrompts): the model schedules its own future —
// wait_until(instant, prompt) is one durable record; the session sleeps
// through a full restart of the world (the instant passes while NOTHING
// runs) and wakes with the prompt as a new turn, exactly once.
module SelfPromptScenario =
    let basinName = "t2-selfprompt"
    let ns = "t2-selfprompt"
    let selfPrompt = "Check whether the build you kicked off finished; report status."

    let script: ModelSays.Script =
        { Agent = "builder"
          Turns =
            [ [ ModelSays.Say "Build kicked off; checking back later."
                ModelSays.WaitUntil(2_500.0, selfPrompt) ]
              [ ModelSays.RecordInput "record"; ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let record =
            Tool.define "record" "Record the wake prompt" (fun (text: string) ->
                async {
                    Counter.appendLine scratch "wake-input" text
                    return "recorded"
                })

        Agent.define
            "builder"
            (ModelSays.harness script)
            { Instructions = "Kick off builds; schedule your own follow-ups."
              Tools = [ record ]
              TurnTimeout = None
              RolloverEvery = 200 }

module GridLawProofs =

    // ── t2.converse-across-crashes ────────────────────────────────────────
    type ConverseObs =
        { SearchAtKill: int
          Outcome: Result<string, TurnFailure>
          SearchAfter: int
          Labels: string list }

    let private converseWorkload (ctx: WorkloadContext) : Async<ConverseObs> =
        ProofOperation.run
            ctx
            "t2.converse-across-crashes"
            {| Session = "gurdas/infra" |}
            { ProofOperationOptions.empty with
                Key = Some "converse" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                // Surface first: while red this throws immediately.
                let assistant = ConverseScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx ConverseScenario.basinName
                let grid = Grid.connect basin
                let session = grid.Session assistant "gurdas/infra"
                let! turn = session.Prompt "p-1" "Investigate why deploys are slow, then fix it."

                // The tool executed in the CHILD host…
                do! CorpusSupport.untilCount scratch "search" 1 60_000
                // …then every process dies, hard.
                do! WorkloadContext.killHost "grid-host" ctx
                do! CorpusNode.sleep 300
                let searchAtKill = Counter.read scratch "search"

                // "Restart tomorrow": a fresh host in THIS process; the turn
                // resumes exactly where it was.
                let! worker = Grid.serve basin ConverseScenario.ns [ ConverseScenario.defs scratch ]
                let! outcome = turn.Outcome
                let! labels = GridCorpus.watchLabels turn
                do! worker.Stop()

                return
                    { SearchAtKill = searchAtKill
                      Outcome = outcome
                      SearchAfter = Counter.read scratch "search"
                      Labels = labels }
            })

    let private converseChecks (v: Verifiers<ConverseObs>) : Check<ConverseObs> list =
        [ LawCheck.equal "the tool executed exactly once before the kill" (fun o -> o.SearchAtKill) 1
          LawCheck.holds
              "the turn completes after the crash (the journal resumes it; the model never re-decides)"
              (fun o ->
                  match o.Outcome with
                  | Ok _ -> true
                  | Error _ -> false)
              (fun o -> sprintf "expected a completed turn, got %A" o.Outcome)
          LawCheck.equal
              "the tool executed exactly once across kill + restart (journal-served on replay)"
              (fun o -> o.SearchAfter)
              1
          LawCheck.holds
              "an after-the-fact watch replays the recorded turn (tool call and completion visible)"
              (fun o -> List.contains "called:search" o.Labels && List.contains "ended:Completed" o.Labels)
              (fun o -> sprintf "expected called:search and ended:Completed in %A" o.Labels)
          v.Host.Started "grid-host"
          v.Fault.HostKillReported "grid-host"
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.converse-across-crashes") ]

    let converseAcrossCrashes =
        let lawProperty =
            property "t2.converse-across-crashes" {
                s2Lite ""

                processHost (
                    CorpusSupport.childHostSpec "grid-host" "grid-host" ConverseScenario.basinName ConverseScenario.ns
                )

                timeoutMs 180_000
                workload converseWorkload
                verify converseChecks
            }

        proof "t2.converse-across-crashes" {
            describedAs
                "A conversation survives anything: a turn started in one process resumes after that process is SIGKILLed — the journaled tool call executes exactly once, the turn completes, and any process can replay the whole conversation after the fact."

            property lawProperty
        }

    // ── t2.days-long-approval ─────────────────────────────────────────────
    type ApprovalObs =
        { ParkedText: string
          DeployWhileParked: int
          Outcome: Result<string, TurnFailure>
          DeployAfterApproval: int }

    let private approvalWorkload (ctx: WorkloadContext) : Async<ApprovalObs> =
        ProofOperation.run
            ctx
            "t2.days-long-approval"
            {| Session = "gurdas/infra" |}
            { ProofOperationOptions.empty with
                Key = Some "approval" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let deployer = ApprovalScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx ApprovalScenario.basinName
                let! worker = Grid.serve basin ApprovalScenario.ns [ deployer ]
                let grid = Grid.connect basin
                let session = grid.Session deployer "gurdas/infra"
                let! turn = session.Prompt "p-1" "Ship the release to production."

                // The gated tool parks the turn — visibly, in the trace,
                // with the token an operator needs.
                let! parkedText = GridCorpus.firstWaiting turn
                let deployWhileParked = Counter.read scratch "deploy"

                // Parked = one journal record; no process holds it. Prove
                // it: stop the ONLY worker while parked.
                do! worker.Stop()
                do! CorpusNode.sleep 500

                // "Four days later, from a phone": a fresh process hosts and
                // a separate connection approves.
                let! worker2 = Grid.serve basin ApprovalScenario.ns [ ApprovalScenario.defs scratch ]
                let approver = Grid.connect basin
                let approverSession = approver.Session (ApprovalScenario.defs scratch) "gurdas/infra"
                do! approverSession.Approve (GridCorpus.tokenOf parkedText) true

                let! outcome = turn.Outcome
                do! worker2.Stop()

                return
                    { ParkedText = parkedText
                      DeployWhileParked = deployWhileParked
                      Outcome = outcome
                      DeployAfterApproval = Counter.read scratch "deploy" }
            })

    let private approvalChecks (v: Verifiers<ApprovalObs>) : Check<ApprovalObs> list =
        [ LawCheck.holds
              "the park is visible in the trace and carries the approval prompt and a token"
              (fun o -> o.ParkedText.Contains ApprovalScenario.approvalPrompt && o.ParkedText.Contains "token=")
              (fun o -> "parked description: " + o.ParkedText)
          LawCheck.equal "the gated tool did NOT run while parked (the gate really gates)" (fun o -> o.DeployWhileParked) 0
          LawCheck.holds
              "the turn completes after approval"
              (fun o ->
                  match o.Outcome with
                  | Ok _ -> true
                  | Error _ -> false)
              (fun o -> sprintf "expected a completed turn, got %A" o.Outcome)
          LawCheck.equal
              "the gated tool ran exactly once, only after approval"
              (fun o -> o.DeployAfterApproval)
              1
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.days-long-approval") ]

    let daysLongApproval =
        let lawProperty =
            property "t2.days-long-approval" {
                s2Lite ""
                timeoutMs 120_000
                workload approvalWorkload
                verify approvalChecks
            }

        proof "t2.days-long-approval" {
            describedAs
                "Human-in-the-loop measured in days: a gated tool parks the turn as one journal record pinning no process (the only worker is stopped while parked); a later approval from a different connection wakes it, and the gated tool executes exactly once, only after approval."

            property lawProperty
        }

    // ── t2.researcher-writer-choreography ─────────────────────────────────
    type ChoreographyObs =
        { ParkedText: string
          Recorded: string list
          WriterTurns: int }

    let private choreographyWorkload (ctx: WorkloadContext) : Async<ChoreographyObs> =
        ProofOperation.run
            ctx
            "t2.researcher-writer-choreography"
            {| Writer = "team/writer-1"
               Researcher = "team/research-1" |}
            { ProofOperationOptions.empty with
                Key = Some "choreo" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let researcher, writer = ChoreographyScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx ChoreographyScenario.basinName
                let! worker = Grid.serve basin ChoreographyScenario.ns [ researcher; writer ]
                let grid = Grid.connect basin

                let writerSession = grid.Session writer "team/writer-1"
                let! writerTurn = writerSession.Prompt "p-1" "You cover s2-perf."
                // The writer parks on the topic — visibly.
                let! parkedText = GridCorpus.firstWaiting writerTurn

                // Hours later, in a session that has never heard of the
                // writer, the researcher publishes.
                let researcherSession = grid.Session researcher "team/research-1"
                let! _researcherTurn = researcherSession.Prompt "p-1" "Dig into S2 perf."

                // The writer wakes with the findings AND its self-prompt as
                // a NEW turn (observed through its recording tool).
                do!
                    CorpusSupport.until "the writer's wake turn recorded its input" 60_000 (fun () ->
                        async { return not (List.isEmpty (Counter.readLines scratch "writer-input")) })

                let recorded = Counter.readLines scratch "writer-input"
                let! history = writerSession.History Eventual
                do! worker.Stop()

                return
                    { ParkedText = parkedText
                      Recorded = recorded
                      WriterTurns = List.length history }
            })

    let private choreographyChecks (v: Verifiers<ChoreographyObs>) : Check<ChoreographyObs> list =
        [ LawCheck.holds
              "the writer's park names the topic it waits on (the trace is the coordination)"
              (fun o -> o.ParkedText.Contains "findings")
              (fun o -> "parked description: " + o.ParkedText)
          LawCheck.equal "the writer woke exactly once" (fun o -> List.length o.Recorded) 1
          LawCheck.holds
              "the wake turn carries the published findings"
              (fun o -> o.Recorded |> List.exists (fun line -> line.Contains ChoreographyScenario.findingsPayload))
              (fun o -> sprintf "recorded wake input: %A" o.Recorded)
          LawCheck.holds
              "the wake turn carries the writer's self-prompt"
              (fun o -> o.Recorded |> List.exists (fun line -> line.Contains ChoreographyScenario.selfPrompt))
              (fun o -> sprintf "recorded wake input: %A" o.Recorded)
          LawCheck.holds
              "the wake is a NEW turn in the writer's history"
              (fun o -> o.WriterTurns >= 2)
              (fun o -> sprintf "expected >= 2 writer turns, got %d" o.WriterTurns)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.researcher-writer-choreography") ]

    let researcherWriterChoreography =
        let lawProperty =
            property "t2.researcher-writer-choreography" {
                s2Lite ""
                timeoutMs 120_000
                workload choreographyWorkload
                verify choreographyChecks
            }

        proof "t2.researcher-writer-choreography" {
            describedAs
                "Choreography without addressing: a writer session parks on a topic; a researcher session that has never heard of the writer publishes findings; the writer wakes with the findings AND its self-prompt as a new turn — sessions coordinate only through published events."

            property lawProperty
        }

    // ── t2.scheduled-self-prompts ─────────────────────────────────────────
    type SelfPromptObs =
        { ParkedText: string
          WakesWhileParked: int
          Recorded: string list
          BuilderTurns: int }

    let private selfPromptWorkload (ctx: WorkloadContext) : Async<SelfPromptObs> =
        ProofOperation.run
            ctx
            "t2.scheduled-self-prompts"
            {| Session = "gurdas/builds" |}
            { ProofOperationOptions.empty with
                Key = Some "selfprompt" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let builder = SelfPromptScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx SelfPromptScenario.basinName
                let! worker = Grid.serve basin SelfPromptScenario.ns [ builder ]
                let grid = Grid.connect basin
                let session = grid.Session builder "gurdas/builds"
                let! turn = session.Prompt "p-1" "Kick off the nightly build."

                // wait_until parks the session — one durable record.
                let! parkedText = GridCorpus.firstWaiting turn
                let wakesWhileParked = Counter.readLines scratch "wake-input" |> List.length

                // The instant passes while NOTHING runs.
                do! worker.Stop()
                do! CorpusNode.sleep 3_000

                // A fresh process hosts; the due timer wakes the session
                // with the self-prompt as a new turn.
                let! worker2 = Grid.serve basin SelfPromptScenario.ns [ SelfPromptScenario.defs scratch ]

                do!
                    CorpusSupport.until "the scheduled self-prompt woke a new turn" 60_000 (fun () ->
                        async { return not (List.isEmpty (Counter.readLines scratch "wake-input")) })

                // Exactly-once window: no duplicate wake.
                do! CorpusNode.sleep 1_000
                let recorded = Counter.readLines scratch "wake-input"
                let! history = session.History Eventual
                do! worker2.Stop()

                return
                    { ParkedText = parkedText
                      WakesWhileParked = wakesWhileParked
                      Recorded = recorded
                      BuilderTurns = List.length history }
            })

    let private selfPromptChecks (v: Verifiers<SelfPromptObs>) : Check<SelfPromptObs> list =
        [ LawCheck.holds
              "the scheduled sleep is visible as a park in the trace"
              (fun o -> o.ParkedText <> "")
              (fun o -> "parked description: " + o.ParkedText)
          LawCheck.equal "no wake before the instant" (fun o -> o.WakesWhileParked) 0
          LawCheck.equal "the wake happened exactly once across the restart" (fun o -> List.length o.Recorded) 1
          LawCheck.holds
              "the wake turn carries the scheduled self-prompt"
              (fun o -> o.Recorded |> List.exists (fun line -> line.Contains SelfPromptScenario.selfPrompt))
              (fun o -> sprintf "recorded wake input: %A" o.Recorded)
          LawCheck.holds
              "the wake is a NEW turn in the session's history"
              (fun o -> o.BuilderTurns >= 2)
              (fun o -> sprintf "expected >= 2 builder turns, got %d" o.BuilderTurns)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.scheduled-self-prompts") ]

    let scheduledSelfPrompts =
        let lawProperty =
            property "t2.scheduled-self-prompts" {
                s2Lite ""
                timeoutMs 120_000
                workload selfPromptWorkload
                verify selfPromptChecks
            }

        proof "t2.scheduled-self-prompts" {
            describedAs
                "Time as a first-class move: wait_until(instant, prompt) is one durable record — the session sleeps through a full restart of the world (the instant passes while nothing runs) and wakes exactly once with the scheduled prompt as a new turn."

            property lawProperty
        }
