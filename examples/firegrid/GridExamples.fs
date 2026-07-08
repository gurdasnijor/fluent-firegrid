/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid — end-user examples. THIS is the acceptance test for the whole
/// stack: if these read like the system we set out to build, the layering
/// works. Every capability shown lowers to Firegrid.Durable (annotations in
/// Firegrid.fs); nothing reaches around the platform.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.GridExamples

open Firegrid
open Firegrid.Durable   // only for Duration/ReadGrade value types

// ══ 1. Define an agent ════════════════════════════════════════════════════
module Define =

    let search  = Tool.define "search" "Search the web" (fun (q: string) -> async {
        return "…results…" })
    let deploy  = Tool.define "deploy" "Deploy to production" (fun (env: string) -> async {
        return "deployed" })

    let assistant =
        Agent.define "assistant" claudeHarness
            { Instructions = "You are a careful infrastructure assistant."
              Tools =
                [ search
                  Tool.gated "Deploying to PRODUCTION — approve?" deploy ]  // human gate
              TurnTimeout = Some (Duration.hours 8.0)
              RolloverEvery = 200 }

// ══ 2. A conversation that survives anything ══════════════════════════════
module Converse =
    open Define

    let demo basin = async {
        let grid = Grid.connect basin
        let session = grid.Session assistant "gurdas/infra"

        let! turn = session.Prompt "p-1" "Investigate why deploys are slow, then fix it."

        // Watch it live — from THIS process, or any other, mid-turn or later:
        do! turn.Watch () |> AsyncSeq.iter (function
            | Thinking t      -> printfn "  … %s" t
            | Said t          -> printfn "agent: %s" t
            | CalledTool (t, a) -> printfn "  ⚙ %s(%s)" t a
            | ToolReturned (t, _) -> printfn "  ✓ %s" t
            | WaitingFor what -> printfn "  ⏸ parked: %s (pinning nothing)" what
            | SpawnedChild a  -> printfn "  ↳ spawned %s" a
            | TurnEnded cause -> printfn "— ended: %A" cause)

        // Kill every process now. Restart tomorrow. The turn resumes exactly
        // where it was — completed tool calls served from the journal, the
        // model never re-decides what it already decided.
        let! outcome = turn.Outcome
        return outcome }

// ══ 3. Human-in-the-loop, measured in days ════════════════════════════════
module Approval =
    open Define

    // The agent calls the gated `deploy` tool → the turn parks on an
    // approval. Parked = one journal record. No process, no timer thread,
    // no cost. Four days later, from a phone, via whatever transport:
    let approveIt basin = async {
        let grid = Grid.connect basin
        let session = grid.Session assistant "gurdas/infra"
        do! session.Approve "appr-8f3" true }        // turn wakes, deploy runs

// ══ 4. Choreography — agents that never call each other ═══════════════════
// A researcher publishes findings. A writer, in another session — possibly
// defined by another team — wait_for's them. Neither knows the other exists.
// Plans emerge from published outputs; the topic log is the coordination.
module Choreography =

    let researcher =
        Agent.define "researcher" claudeHarness
            { Instructions = "Research the topic; publish findings to 'findings' when confident."
              Tools = [ (* search … *) ]
              TurnTimeout = None
              RolloverEvery = 200 }
        // model, mid-turn:  publish("findings", {topic: "s2-perf", …})

    let writer =
        Agent.define "writer" claudeHarness
            { Instructions = "wait_for findings on topics you're assigned; draft the report when they arrive."
              Tools = []
              TurnTimeout = None
              RolloverEvery = 200 }
        // model, mid-turn:  wait_for("findings", match: topic == "s2-perf",
        //                            self_prompt: "Findings arrived — draft the report.")

    let demo basin = async {
        let grid = Grid.connect basin
        let! _ = grid.Session researcher "team/research-1" |> fun s -> s.Prompt "p-1" "Dig into S2 perf."
        let! _ = grid.Session writer "team/writer-1" |> fun s -> s.Prompt "p-1" "You cover s2-perf."
        // writer parks on the topic; researcher publishes hours later;
        // writer wakes with the findings AND its self-prompt as a new turn.
        return () }

// ══ 5. Time as a first-class move ═════════════════════════════════════════
module SelfPrompts =
    // The model says: wait_until("tomorrow 09:00", "Check whether the build
    // I kicked off finished; report status."). One durable record. The
    // session sleeps through restarts and wakes with that prompt as a new
    // turn — time-based and event-based triggers are one family.
    ()

// ══ 6. Fan-out — a fleet of subagents, durably ════════════════════════════
module FanOut =
    // The model says: spawn_all([("reviewer", "review auth.fs"),
    //                            ("reviewer", "review store.fs"),
    //                            ("reviewer", "review kernel.fs")])
    // Three child turns run as durable children; the parent parks until all
    // three outcomes are in (kill anything meanwhile — nothing is lost),
    // then wakes to synthesize.
    ()

// ══ 7. Ingress — the outside world becomes durable events ═════════════════
module Webhooks =
    let onStripeWebhook basin (payload: string) = async {
        let grid = Grid.connect basin
        // Any transport (HTTP handler, queue consumer) just publishes; the
        // event is durable before we ack. Whatever agent wait_for's
        // payments, wakes. Sender and agent never meet.
        do! grid.Publish { Topic = "payments"; Payload = payload } }

// ══ 8. Operations — the trace IS the schedule ═════════════════════════════
module Ops =
    open Define

    let dashboard basin = async {
        let grid = Grid.connect basin

        // Fleet view from the journals — including sessions that have been
        // parked for a week with no process anywhere:
        let! view = grid.Live Eventual
        printfn "awake: %A" view.Awake
        printfn "parked: %A" view.Parked      // [("team/writer-1", "findings: topic == 's2-perf'"); …]

        // Per-session history with real end causes (timeout ≠ cancel):
        let session = grid.Session assistant "gurdas/infra"
        let! turns = session.History Eventual
        for t in turns do printfn "%s  %A" t.TurnId t.Cause

        // And when something runs away:
        do! session.CancelLiveTurn () }
