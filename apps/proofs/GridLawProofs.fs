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

// ── t2.spawn-all-fanout scenario ─────────────────────────────────────────
// GridExamples §6 (FanOut): the model fans three review assignments out to
// child agent turns with spawn_all, parks until all three outcomes are in,
// then wakes to synthesize them — in DECLARATION order.
module FanOutScenario =
    let basinName = "t2-fanout"
    let ns = "t2-fanout"

    let assignments =
        [ "reviewer", "review auth.fs"
          "reviewer", "review store.fs"
          "reviewer", "review kernel.fs" ]

    let expectedJoin = "review auth.fs|review store.fs|review kernel.fs"

    let leadScript: ModelSays.Script =
        { Agent = "lead"
          Turns =
            [ [ ModelSays.Say "Fanning the review out."
                ModelSays.SpawnAll(assignments, "synthesize")
                ModelSays.EndTurn ] ] }

    let reviewerScript: ModelSays.Script =
        { Agent = "reviewer"
          Turns = [ [ ModelSays.RecordInput "review"; ModelSays.EchoInput ] ] }

    let defs (scratch: string) =
        let review =
            Tool.define "review" "Record a review assignment" (fun (text: string) ->
                async {
                    Counter.appendLine scratch "reviews" text
                    return "reviewed"
                })

        let synthesize =
            Tool.define "synthesize" "Join the child outcomes" (fun (text: string) ->
                async {
                    Counter.appendLine scratch "synthesis" text
                    return "synthesized"
                })

        let reviewer =
            Agent.define
                "reviewer"
                (ModelSays.harness reviewerScript)
                { Instructions = "Review exactly what you are handed."
                  Tools = [ review ]
                  TurnTimeout = None
                  RolloverEvery = 200 }

        let lead =
            Agent.define
                "lead"
                (ModelSays.harness leadScript)
                { Instructions = "Fan reviews out to subagents; synthesize the results."
                  Tools = [ synthesize ]
                  TurnTimeout = None
                  RolloverEvery = 200 }

        reviewer, lead

// ── t2.spawn-all-eager-concurrent scenario ───────────────────────────────
// Open-semantics pin (G4 recorded deltas: an un-awaited spawn never
// starts; child fan-out is sequential). The ratified annotation promises
// durable children fanned out via Workflow.all: ALL children must START
// eagerly at the fan-out and EXECUTE concurrently — pinned by timestamp
// windows: every child's start precedes the first child's completion.
module EagerScenario =
    let basinName = "t2-eager"
    let ns = "t2-eager"

    let children =
        [ "sleeper", "a"
          "sleeper", "b"
          "sleeper", "c" ]

    let leadScript: ModelSays.Script =
        { Agent = "fan-lead"
          Turns = [ [ ModelSays.SpawnAll(children, "join"); ModelSays.EndTurn ] ] }

    let sleeperScript: ModelSays.Script =
        { Agent = "sleeper"
          Turns = [ [ ModelSays.RecordInput "sleepwork"; ModelSays.EchoInput ] ] }

    let defs (scratch: string) =
        let sleepwork =
            Tool.define "sleepwork" "Stamped sleepy work" (fun (label: string) ->
                async {
                    Counter.appendLine scratch "starts" (label + ":" + string (GridCorpus.nowMs ()))
                    do! CorpusNode.sleep 600
                    Counter.appendLine scratch "ends" (label + ":" + string (GridCorpus.nowMs ()))
                    return label
                })

        let join =
            Tool.define "join" "Join the child outcomes" (fun (text: string) ->
                async {
                    Counter.appendLine scratch "join" text
                    return "joined"
                })

        let sleeper =
            Agent.define
                "sleeper"
                (ModelSays.harness sleeperScript)
                { Instructions = "Do the stamped work you are handed."
                  Tools = [ sleepwork ]
                  TurnTimeout = None
                  RolloverEvery = 200 }

        let lead =
            Agent.define
                "fan-lead"
                (ModelSays.harness leadScript)
                { Instructions = "Fan the work out; join the results."
                  Tools = [ join ]
                  TurnTimeout = None
                  RolloverEvery = 200 }

        sleeper, lead

// ── t2.webhook-ingress scenario ──────────────────────────────────────────
// GridExamples §7 (Webhooks): any transport publishes an external event
// into the grid; the event is durable BEFORE the ack (proven: published
// while no worker runs anywhere); whichever agent wait_for's it wakes.
// Sender and agent never meet.
module WebhookScenario =
    let basinName = "t2-webhook"
    let ns = "t2-webhook"
    let payload = "{\"provider\":\"stripe\",\"amount\":4200}"
    let selfPrompt = "Payment arrived — reconcile."

    let listenerScript: ModelSays.Script =
        { Agent = "reconciler"
          Turns =
            [ [ ModelSays.WaitFor("payments", None, Some selfPrompt) ]
              [ ModelSays.RecordInput "reconcile"; ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let reconcile =
            Tool.define "reconcile" "Record the reconciliation input" (fun (text: string) ->
                async {
                    Counter.appendLine scratch "reconcile-input" text
                    return "reconciled"
                })

        Agent.define
            "reconciler"
            (ModelSays.harness listenerScript)
            { Instructions = "wait_for payments; reconcile them when they arrive."
              Tools = [ reconcile ]
              TurnTimeout = None
              RolloverEvery = 200 }

// ── t2.live-watch-ops scenario ───────────────────────────────────────────
// GridExamples §8 (Ops): the trace IS the schedule — the fleet view is a
// fold over the grid's journals (a parked session shows as parked, with
// what it waits on, while no process holds it), and per-session history
// carries REAL end causes (completed ≠ cancelled).
module OpsScenario =
    let basinName = "t2-ops"
    let ns = "t2-ops"

    let watcherScript: ModelSays.Script =
        { Agent = "ops-watcher"
          Turns = [ [ ModelSays.WaitFor("findings", Some "topic == 's2-perf'", None) ] ] }

    let assistantScript: ModelSays.Script =
        { Agent = "ops-assistant"
          Turns =
            [ [ ModelSays.Say "Quick job done."; ModelSays.EndTurn ]
              [ ModelSays.CallTool("slow-work", "runaway")
                ModelSays.Say "Should never be said."
                ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let slowWork =
            Tool.define "slow-work" "A runaway job" (fun (label: string) ->
                async {
                    Counter.bump scratch "slow-started"
                    do! CorpusNode.sleep 5_000
                    Counter.bump scratch "slow-finished"
                    return label
                })

        let watcher =
            Agent.define
                "ops-watcher"
                (ModelSays.harness watcherScript)
                { Instructions = "wait_for findings on s2-perf."
                  Tools = []
                  TurnTimeout = None
                  RolloverEvery = 200 }

        let assistant =
            Agent.define
                "ops-assistant"
                (ModelSays.harness assistantScript)
                { Instructions = "Do what you are asked."
                  Tools = [ slowWork ]
                  TurnTimeout = None
                  RolloverEvery = 200 }

        watcher, assistant

// ── t2.cancel-live-turn scenario ─────────────────────────────────────────
// The cancel scenario (GridExamples §8's runaway + the TurnHandle
// contract): a durable cancel is observed at the turn's next durable
// operation; later moves never run; the turn ends Cancelled — a cause
// distinct from failure or timeout — and the cancellation is visible in
// the watch stream.
module CancelScenario =
    let basinName = "t2-cancel"
    let ns = "t2-cancel"

    let script: ModelSays.Script =
        { Agent = "canceller"
          Turns =
            [ [ ModelSays.CallTool("slow-work", "long haul")
                ModelSays.CallTool("after-cancel", "must never run")
                ModelSays.Say "Should never be said."
                ModelSays.EndTurn ] ] }

    let defs (scratch: string) =
        let slowWork =
            Tool.define "slow-work" "A long-running job" (fun (label: string) ->
                async {
                    Counter.bump scratch "slow-started"
                    do! CorpusNode.sleep 4_000
                    return label
                })

        let afterCancel =
            Tool.define "after-cancel" "Evidence of a move after the cancel" (fun (label: string) ->
                async {
                    Counter.bump scratch "after-cancel"
                    return label
                })

        Agent.define
            "canceller"
            (ModelSays.harness script)
            { Instructions = "Do the long job, then the follow-up."
              Tools = [ slowWork; afterCancel ]
              TurnTimeout = None
              RolloverEvery = 200 }

// ── t2.reserved-segment-admission scenario ───────────────────────────────
// Open-semantics pin (architect ruling on PR #118, law-unpinned at the
// Grid surface until now): user-chosen session ids are identity; ids that
// embed the kernel's reserved path segments (`/gen/`, `/child/`) could
// alias another instance's journal and MUST be refused at admission with
// the typed rejection — while ids that merely CONTAIN the letters (e.g.
// "team/generalist") are admitted: the rule is segment-wise, not
// substring.
module ReservedScenario =
    let basinName = "t2-reserved"
    let ns = "t2-reserved"

    let script: ModelSays.Script =
        { Agent = "greeter"
          Turns = [ [ ModelSays.Say "Hello."; ModelSays.EndTurn ] ] }

    let defs (_scratch: string) =
        Agent.define
            "greeter"
            (ModelSays.harness script)
            { Instructions = "Say hello."
              Tools = []
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

    // ── t2.spawn-all-fanout ───────────────────────────────────────────────
    type FanOutObs =
        { Outcome: Result<string, TurnFailure>
          Reviews: string list
          Synthesis: string list
          SpawnedCount: int }

    let private fanoutWorkload (ctx: WorkloadContext) : Async<FanOutObs> =
        ProofOperation.run
            ctx
            "t2.spawn-all-fanout"
            {| Children = List.length FanOutScenario.assignments |}
            { ProofOperationOptions.empty with
                Key = Some "fanout" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let reviewer, lead = FanOutScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx FanOutScenario.basinName
                let! worker = Grid.serve basin FanOutScenario.ns [ reviewer; lead ]
                let grid = Grid.connect basin
                let session = grid.Session lead "team/lead-1"
                let! turn = session.Prompt "p-1" "Review the three modules."

                let! outcome = turn.Outcome
                let! labels = GridCorpus.watchLabels turn
                do! worker.Stop()

                return
                    { Outcome = outcome
                      Reviews = Counter.readLines scratch "reviews" |> List.sort
                      Synthesis = Counter.readLines scratch "synthesis"
                      SpawnedCount = labels |> List.filter (fun l -> l = "spawned:reviewer") |> List.length }
            })

    let private fanoutChecks (v: Verifiers<FanOutObs>) : Check<FanOutObs> list =
        [ LawCheck.holds
              "the parent turn completes after all children"
              (fun o ->
                  match o.Outcome with
                  | Ok _ -> true
                  | Error _ -> false)
              (fun o -> sprintf "expected a completed parent turn, got %A" o.Outcome)
          LawCheck.equal
              "every assignment ran in a child turn, exactly once"
              (fun o -> o.Reviews)
              (FanOutScenario.assignments |> List.map snd |> List.sort)
          LawCheck.equal
              "the parent synthesized ALL child outcomes, in declaration order"
              (fun o -> o.Synthesis)
              [ FanOutScenario.expectedJoin ]
          LawCheck.equal "the fan-out is visible in the watch (three spawned children)" (fun o -> o.SpawnedCount) 3
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.spawn-all-fanout") ]

    let spawnAllFanout =
        let lawProperty =
            property "t2.spawn-all-fanout" {
                s2Lite ""
                timeoutMs 120_000
                workload fanoutWorkload
                verify fanoutChecks
            }

        proof "t2.spawn-all-fanout" {
            describedAs
                "Fan-out as a durable fleet: spawn_all launches three child agent turns, the parent parks until all three outcomes are in, then wakes to synthesize them — every assignment runs exactly once and the outcomes join in declaration order."

            property lawProperty
        }

    // ── t2.spawn-all-eager-concurrent ─────────────────────────────────────
    type EagerObs =
        { StartCount: int
          EndCount: int
          Overlapping: bool
          Stamps: string
          Join: string list }

    let private eagerWorkload (ctx: WorkloadContext) : Async<EagerObs> =
        ProofOperation.run
            ctx
            "t2.spawn-all-eager-concurrent"
            {| Children = List.length EagerScenario.children |}
            { ProofOperationOptions.empty with
                Key = Some "eager" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let sleeper, lead = EagerScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx EagerScenario.basinName
                let! worker = Grid.serve basin EagerScenario.ns [ sleeper; lead ]
                let grid = Grid.connect basin
                let session = grid.Session lead "team/fan-lead-1"
                let! turn = session.Prompt "p-1" "Fan the work out."
                let! _outcome = turn.Outcome
                do! worker.Stop()

                let starts = GridCorpus.stampsOf (Counter.readLines scratch "starts")
                let ends = GridCorpus.stampsOf (Counter.readLines scratch "ends")

                let overlapping =
                    List.length starts = 3
                    && List.length ends = 3
                    && List.max starts < List.min ends

                return
                    { StartCount = List.length starts
                      EndCount = List.length ends
                      Overlapping = overlapping
                      Stamps = sprintf "starts=%A ends=%A" starts ends
                      Join = Counter.readLines scratch "join" }
            })

    let private eagerChecks (v: Verifiers<EagerObs>) : Check<EagerObs> list =
        [ LawCheck.equal "all three children started" (fun o -> o.StartCount) 3
          LawCheck.equal "all three children finished" (fun o -> o.EndCount) 3
          LawCheck.holds
              "children start eagerly and run concurrently: every start precedes the first completion (sequential fan-out would start child N+1 only after child N ends)"
              (fun o -> o.Overlapping)
              (fun o -> "execution windows do not overlap: " + o.Stamps)
          LawCheck.equal
              "the join carries all three outcomes in declaration order"
              (fun o -> o.Join)
              [ "a|b|c" ]
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.spawn-all-eager-concurrent") ]

    let spawnAllEagerConcurrent =
        let lawProperty =
            property "t2.spawn-all-eager-concurrent" {
                s2Lite ""
                timeoutMs 120_000
                workload eagerWorkload
                verify eagerChecks
            }

        proof "t2.spawn-all-eager-concurrent" {
            describedAs
                "spawn_all is eager and concurrent, as the ratified annotation promises: all three children START at the fan-out (not one-by-one as each is awaited) and their execution windows OVERLAP — pinned against G4's recorded platform deltas (deferred spawn; sequential child fan-out), so green-making forces the platform work."

            property lawProperty
        }

    // ── t2.webhook-ingress ────────────────────────────────────────────────
    type WebhookObs =
        { ParkedText: string
          AckedWhileDown: bool
          Recorded: string list
          ListenerTurns: int }

    let private webhookWorkload (ctx: WorkloadContext) : Async<WebhookObs> =
        ProofOperation.run
            ctx
            "t2.webhook-ingress"
            {| Topic = "payments" |}
            { ProofOperationOptions.empty with
                Key = Some "webhook" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let reconciler = WebhookScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx WebhookScenario.basinName
                let! worker = Grid.serve basin WebhookScenario.ns [ reconciler ]
                let grid = Grid.connect basin
                let session = grid.Session reconciler "finance/reconciler-1"
                let! turn = session.Prompt "p-1" "Watch for payments."
                let! parkedText = GridCorpus.firstWaiting turn

                // No process anywhere — and the webhook still lands:
                do! worker.Stop()
                do! CorpusNode.sleep 300
                let ingress = Grid.connect basin
                do! ingress.Publish { Topic = "payments"; Payload = WebhookScenario.payload }
                let ackedWhileDown = true

                // A worker returns; whoever waited, wakes.
                let! worker2 = Grid.serve basin WebhookScenario.ns [ WebhookScenario.defs scratch ]

                do!
                    CorpusSupport.until "the listener's wake turn recorded its input" 60_000 (fun () ->
                        async { return not (List.isEmpty (Counter.readLines scratch "reconcile-input")) })

                let recorded = Counter.readLines scratch "reconcile-input"
                let! history = session.History Eventual
                do! worker2.Stop()

                return
                    { ParkedText = parkedText
                      AckedWhileDown = ackedWhileDown
                      Recorded = recorded
                      ListenerTurns = List.length history }
            })

    let private webhookChecks (v: Verifiers<WebhookObs>) : Check<WebhookObs> list =
        [ LawCheck.holds
              "the listener's park names the topic"
              (fun o -> o.ParkedText.Contains "payments")
              (fun o -> "parked description: " + o.ParkedText)
          LawCheck.holds "the publish was acked while NO worker ran (durable before ack)" (fun o -> o.AckedWhileDown) (fun _ -> "publish did not ack while down")
          LawCheck.equal "the listener woke exactly once" (fun o -> List.length o.Recorded) 1
          LawCheck.holds
              "the wake turn carries the webhook payload"
              (fun o -> o.Recorded |> List.exists (fun line -> line.Contains WebhookScenario.payload))
              (fun o -> sprintf "recorded wake input: %A" o.Recorded)
          LawCheck.holds
              "the wake turn carries the listener's self-prompt"
              (fun o -> o.Recorded |> List.exists (fun line -> line.Contains WebhookScenario.selfPrompt))
              (fun o -> sprintf "recorded wake input: %A" o.Recorded)
          LawCheck.holds
              "the wake is a NEW turn in the listener's history"
              (fun o -> o.ListenerTurns >= 2)
              (fun o -> sprintf "expected >= 2 listener turns, got %d" o.ListenerTurns)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.webhook-ingress") ]

    let webhookIngress =
        let lawProperty =
            property "t2.webhook-ingress" {
                s2Lite ""
                timeoutMs 120_000
                workload webhookWorkload
                verify webhookChecks
            }

        proof "t2.webhook-ingress" {
            describedAs
                "The outside world becomes durable events: a transport publishes to a topic while NO worker runs anywhere and the ack means durable; when a worker returns, the agent that waited on the topic wakes exactly once with the payload and its self-prompt — sender and agent never meet."

            property lawProperty
        }

    // ── t2.live-watch-ops ─────────────────────────────────────────────────
    type OpsObs =
        { ParkedView: (string * string) list
          AwakeView: string list
          Causes: TurnEndCause option list }

    let private opsWorkload (ctx: WorkloadContext) : Async<OpsObs> =
        ProofOperation.run
            ctx
            "t2.live-watch-ops"
            {| Watcher = "team/writer-1"
               Assistant = "gurdas/infra" |}
            { ProofOperationOptions.empty with
                Key = Some "ops" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let watcher, assistant = OpsScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx OpsScenario.basinName
                let! worker = Grid.serve basin OpsScenario.ns [ watcher; assistant ]
                let grid = Grid.connect basin

                // One session parks (and stays parked):
                let watcherSession = grid.Session watcher "team/writer-1"
                let! watcherTurn = watcherSession.Prompt "p-1" "You cover s2-perf."
                let! _parked = GridCorpus.firstWaiting watcherTurn

                // One session completes a turn, then has a runaway cancelled:
                let opsSession = grid.Session assistant "gurdas/infra"
                let! quickTurn = opsSession.Prompt "p-1" "Do the quick job."
                let! _quick = quickTurn.Outcome
                let! runawayTurn = opsSession.Prompt "p-2" "Do the long job."
                do! CorpusSupport.untilCount scratch "slow-started" 1 60_000
                do! opsSession.CancelLiveTurn()
                let! _runaway = runawayTurn.Outcome

                // The trace IS the schedule:
                let! view = grid.Live Eventual
                let! turns = opsSession.History Eventual
                do! worker.Stop()

                return
                    { ParkedView = view.Parked
                      AwakeView = view.Awake
                      Causes =
                        turns
                        |> List.sortBy (fun t -> t.StartedAt)
                        |> List.map (fun t -> t.Cause) }
            })

    let private opsChecks (v: Verifiers<OpsObs>) : Check<OpsObs> list =
        [ LawCheck.holds
              "the fleet view shows the parked session, with what it waits on, while no process holds it"
              (fun o ->
                  o.ParkedView
                  |> List.exists (fun (id, what) -> id = "team/writer-1" && what.Contains "findings"))
              (fun o -> sprintf "parked view: %A" o.ParkedView)
          LawCheck.holds
              "the parked session is not counted awake"
              (fun o -> not (List.contains "team/writer-1" o.AwakeView))
              (fun o -> sprintf "awake view: %A" o.AwakeView)
          LawCheck.equal
              "per-session history carries REAL end causes (completed and cancelled are distinct)"
              (fun o -> o.Causes)
              [ Some TurnEndCause.Completed; Some TurnEndCause.Cancelled ]
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.live-watch-ops") ]

    let liveWatchOps =
        let lawProperty =
            property "t2.live-watch-ops" {
                s2Lite ""
                timeoutMs 120_000
                workload opsWorkload
                verify opsChecks
            }

        proof "t2.live-watch-ops" {
            describedAs
                "The trace is the schedule: the fleet view folded from the grid's journals shows a parked session (and what it waits on) that no process holds, and per-session history reports real, distinct end causes — a completed turn and an operator-cancelled runaway."

            property lawProperty
        }

    // ── t2.cancel-live-turn ───────────────────────────────────────────────
    type CancelObs =
        { Outcome: Result<string, TurnFailure>
          Labels: string list
          AfterCancelRuns: int }

    let private cancelWorkload (ctx: WorkloadContext) : Async<CancelObs> =
        ProofOperation.run
            ctx
            "t2.cancel-live-turn"
            {| Session = "gurdas/runaway" |}
            { ProofOperationOptions.empty with
                Key = Some "cancel" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let canceller = CancelScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx CancelScenario.basinName
                let! worker = Grid.serve basin CancelScenario.ns [ canceller ]
                let grid = Grid.connect basin
                let session = grid.Session canceller "gurdas/runaway"
                let! turn = session.Prompt "p-1" "Start the long haul."

                // Cancel while the first tool is mid-flight:
                do! CorpusSupport.untilCount scratch "slow-started" 1 60_000
                do! turn.Cancel()

                let! outcome = turn.Outcome
                let! labels = GridCorpus.watchLabels turn
                // Window for any (forbidden) post-cancel move to surface:
                do! CorpusNode.sleep 500
                do! worker.Stop()

                return
                    { Outcome = outcome
                      Labels = labels
                      AfterCancelRuns = Counter.read scratch "after-cancel" }
            })

    let private cancelChecks (v: Verifiers<CancelObs>) : Check<CancelObs> list =
        [ LawCheck.equal
              "the turn ends Cancelled — a cause distinct from failure"
              (fun o -> o.Outcome)
              (Error CancelledTurn)
          LawCheck.holds
              "the cancellation is visible in the watch stream"
              (fun o -> List.contains "ended:Cancelled" o.Labels)
              (fun o -> sprintf "expected ended:Cancelled in %A" o.Labels)
          LawCheck.equal
              "no move runs after the cancel lands (observed at the next durable operation)"
              (fun o -> o.AfterCancelRuns)
              0
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.cancel-live-turn") ]

    let cancelLiveTurn =
        let lawProperty =
            property "t2.cancel-live-turn" {
                s2Lite ""
                timeoutMs 120_000
                workload cancelWorkload
                verify cancelChecks
            }

        proof "t2.cancel-live-turn" {
            describedAs
                "Durable cancel: cancelling a live turn is observed at the turn's next durable operation — no later move ever runs, the turn ends Cancelled (distinct from failure), and the cancellation is visible in the watch stream."

            property lawProperty
        }

    // ── t2.reserved-segment-admission ─────────────────────────────────────
    type ReservedObs =
        { GenAttempt: string
          ChildAttempt: string
          GoodOutcome: Result<string, TurnFailure> }

    let private admissionAttempt (grid: Grid) (agent: AgentDef) (sessionId: string) : Async<string> =
        async {
            try
                let session = grid.Session agent sessionId
                let! _turn = session.Prompt "p-1" "hello"
                return "accepted"
            with
            | DurableReservedSegment(_id, segment) -> return "typed:" + segment
            | error -> return "untyped:" + error.Message
        }

    let private reservedWorkload (ctx: WorkloadContext) : Async<ReservedObs> =
        ProofOperation.run
            ctx
            "t2.reserved-segment-admission"
            {| Reserved = [ "gen"; "child" ] |}
            { ProofOperationOptions.empty with
                Key = Some "reserved" }
            (async {
                let scratch = CorpusSupport.scratchOf ctx
                let greeter = ReservedScenario.defs scratch
                let! basin = CorpusSupport.workloadBasin ctx ReservedScenario.basinName
                let! worker = Grid.serve basin ReservedScenario.ns [ greeter ]
                let grid = Grid.connect basin

                let! genAttempt = admissionAttempt grid greeter "team/gen/1"
                let! childAttempt = admissionAttempt grid greeter "team/child/1"

                // The rule is segment-wise, not substring: this id ADMITS.
                let goodSession = grid.Session greeter "team/generalist"
                let! goodTurn = goodSession.Prompt "p-1" "hello"
                let! goodOutcome = goodTurn.Outcome
                do! worker.Stop()

                return
                    { GenAttempt = genAttempt
                      ChildAttempt = childAttempt
                      GoodOutcome = goodOutcome }
            })

    let private reservedChecks (v: Verifiers<ReservedObs>) : Check<ReservedObs> list =
        [ LawCheck.equal
              "a session id embedding /gen/ is refused at admission with the typed rejection"
              (fun o -> o.GenAttempt)
              "typed:gen"
          LawCheck.equal
              "a session id embedding /child/ is refused at admission with the typed rejection"
              (fun o -> o.ChildAttempt)
              "typed:child"
          LawCheck.holds
              "an id that merely contains the letters (team/generalist) is admitted and completes"
              (fun o ->
                  match o.GoodOutcome with
                  | Ok _ -> true
                  | Error _ -> false)
              (fun o -> sprintf "expected a completed turn, got %A" o.GoodOutcome)
          v.Trace.Operation "law operation recorded ok" (CorpusSupport.lawOp "t2.reserved-segment-admission") ]

    let reservedSegmentAdmission =
        let lawProperty =
            property "t2.reserved-segment-admission" {
                s2Lite ""
                timeoutMs 120_000
                workload reservedWorkload
                verify reservedChecks
            }

        proof "t2.reserved-segment-admission" {
            describedAs
                "Session ids are user-chosen identity: ids embedding the kernel's reserved path segments (/gen/, /child/) are refused at Grid admission with the typed reserved-segment rejection — segment-wise, not substring, so team/generalist admits and completes."

            property lawProperty
        }
