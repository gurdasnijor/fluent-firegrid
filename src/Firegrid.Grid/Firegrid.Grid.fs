/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid — the choreography-first agent substrate.
///
/// AN APPLICATION, NOT THE PLATFORM. This is L3 code built entirely on the
/// Firegrid.Durable contract (PR #115): every member's doc comment names
/// the platform operation it lowers to — [→ like this]. Zero new kernel
/// machinery. If something here could not lower, that would be a platform
/// gap; writing this file is how we find out.
///
/// The promise (from the original firegrid README): models own control flow
/// at runtime through durable primitives — every wait, spawn, and tool call
/// is durable, replayable, observable. Sessions never call each other; they
/// coordinate only through the durable core. Suspended work pins no process
/// and costs nothing. The trace IS the schedule.
///
/// The lowering, at a glance:
///   agent session   → entity (keyed, single-writer)      [→ Entity.define]
///   one turn        → workflow (journaled, replayed)     [→ Workflow.define]
///   tool call       → step (journal-served on replay)    [→ Step.define/.Call]
///   wait_for        → subscribe + typed signal           [→ Signal.Await]
///   wait_until      → durable timer + self-prompt        [→ Workflow.sleepUntil]
///   spawn/spawn_all → durable child workflows            [→ .CallChild / Workflow.all]
///   publish         → topic entity + signal fan-out      [→ Entity.Call + Run.Signal]
///   watch           → byte-faithful log attach           [→ client.Logs / Attach]
///   history         → checkpointed projection            [→ Projection / client.Read]
///
/// Implementation ceremony (C2 green-making; no public name, parameter
/// order, or member type shape changed — the same ratified T1 pattern):
///   • contract types gained INTERNAL backing fields (records with
///     `internal` fields; members unchanged) — a body cannot exist without
///     state (`ToolSpec`/`AgentSpec`/`GridTurnHandle` in InternalSessions.fs)
///   • `Tool.define` is `inline`: Fable erases generics, so the tool's
///     model-wire type capture (`typeof<…>`) must happen at the call site —
///     exactly as the platform's `define`/`declare`/`attach`
///   • `GridDerived` (below the `Tool` type) is public plumbing for the
///     inline capture site — F# requires inline-reachable code to be
///     public; it is NOT part of the ratified surface
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid

open Firegrid.Log // S2.Basin — the connection type in Grid.connect/serve
open Firegrid.Durable

[<AutoOpen>]
module private Impl =
    let notYet<'a> : 'a = failwith "Firegrid: surface only — not implemented"

// ── Tools — what an agent can do to the world ────────────────────────────

/// A tool the model may call. Durable: a tool call is journaled, so a crash
/// after execution never re-runs it — the model sees the recorded result.
/// [→ Step.define; calls lower to step.Call inside the turn workflow]
type Tool = internal Tool of ToolSpec

// Public ONLY because `Tool.define` is inline and F# requires
// inline-reachable code to be public (the platform's `DerivedInternal`
// ceremony, ratified with T1). NOT part of the ratified surface — never
// call directly.
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
module GridDerived =
    let mkTool<'args, 'result>
        (name: string)
        (description: string)
        (run: 'args -> Async<'result>)
        (argTy: System.Type)
        (resultTy: System.Type)
        : Tool =
        Tool(GridRuntime.mkToolSpec name description (GridRuntime.rawToolRun run argTy resultTy))

[<RequireQualifiedAccess>]
module Tool =
    /// Name + description (shown to the model) + an ordinary async function.
    let inline define (name: string) (description: string) (run: 'args -> Async<'result>) : Tool =
        GridDerived.mkTool name description run typeof<'args> typeof<'result>

    /// A tool that requires human approval before each execution.
    /// [→ the turn parks on a typed signal; see Session.Approve]
    let gated (approvalPrompt: string) (tool: Tool) : Tool = notYet

// ── Agents — a model, its tools, its policies ────────────────────────────

/// The harness that drives the model (Claude adapter, etc. — the TS-zone
/// adapter contract; here it is just a step the turn calls).
/// [→ Step.define "agent/model-turn"]
type Harness = internal Harness of unit

type AgentConfig =
    { Instructions: string
      Tools: Tool list
      /// Max wall-clock for one turn before it is timed out (durable — the
      /// deadline survives restarts). [→ tagged select vs Workflow.sleep]
      TurnTimeout: Duration option
      /// A long turn rolls its journal every N model iterations.
      /// [→ Workflow.defineEternal / ContinueAsNew]
      RolloverEvery: int }

type AgentDef = internal AgentDef of AgentSpec

[<RequireQualifiedAccess>]
module Agent =
    /// Define an agent. The model automatically receives the choreography
    /// affordances as tools — wait_for, wait_until, spawn, publish — so THE
    /// MODEL owns control flow; the substrate makes its choices durable.
    let define (name: string) (harness: Harness) (config: AgentConfig) : AgentDef =
        // The Harness VALUE carries nothing yet: the ratified surface has
        // no harness constructor (the adapter contract is T3), so the
        // "agent/model-turn" step binds by agent name — the corpus's
        // scripted model registers through GridScripted.
        ignore harness

        AgentDef
            { AgentName = name
              AgentTools = config.Tools |> List.map (fun (Tool spec) -> spec) }

// ── The choreography vocabulary (what the model sees as tools) ───────────
//
// These are not developer APIs — they are the tool-shaped affordances every
// agent gets. Documented here because they ARE the firegrid semantics:
//
//   wait_for  (topic, match?, selfPrompt?)
//     Park until a matching event is published. Costs nothing while parked;
//     pins no process; survives redeploys. On resume the model wakes with
//     the event (and the self-prompt, if given) as a new turn input.
//     [→ subscribe: topics.Call (Subscribe (runId, match)); park: Signal.Await]
//
//   wait_until (instant, selfPrompt)
//     Park until a time. "Tomorrow 9am, remind me to check the build" is
//     one durable record. [→ Workflow.sleepUntil; prompt rides turn state]
//
//   spawn (agent, prompt) / spawn_all ([...])
//     Durably launch child agent turns and await their outcomes; parent and
///    children all survive any crash. [→ turnDecl.CallChild / Workflow.all]
//
//   publish (topic, payload)
//     Append a durable event others can wait_for. Sessions NEVER address
//     each other — plans emerge from published outputs.
//     [→ Publish service: topic entity lists subscribers → Run.Signal each]
//
//   execute (tool, args)
//     Every tool call, journaled. [→ step.Call]

// ── Ingress & operations — the developer/operator surface ────────────────

/// An event on a topic: the coordination currency of the grid.
type Event = { Topic: string; Payload: string }

/// A live handle to one turn. Serializable; reattach from any process.
/// [→ Run<TurnOutcome> + the turn's output log]
type TurnHandle =
    internal { THSpec: GridTurnHandle }

    /// Watch this turn live: recorded prefix → live tail → terminal, from
    /// any process, mid-flight or after the fact. [→ client.Logs(...).Attach]
    member handle.Watch () : AsyncSeq<AgentEvent> =
        // Lowered through the ratified `AsyncSeq.map` amendment (architect
        // ruling on this PR). The log's seal reason IS the turn terminal
        // (one source of truth): chunks decode to the in-flight events; the
        // attach Terminal maps to TurnEnded; the sequence ends with the
        // source.
        handle.THSpec.HLog.Attach()
        |> AsyncSeq.map (fun logEvent ->
            match logEvent with
            | Chunk body ->
                match GridEventWire.decode body with
                | [ "th"; text ] -> Thinking text
                | [ "sa"; text ] -> Said text
                | [ "ct"; tool; args ] -> CalledTool(tool, args)
                | [ "tr"; tool; result ] -> ToolReturned(tool, result)
                | [ "wf"; what ] -> WaitingFor what
                | [ "sp"; agent ] -> SpawnedChild agent
                | _ -> failwith ("Firegrid: unrecognized turn event record: " + body)
            | Terminal reason ->
                match GridEventWire.decode reason with
                | [ "te"; "Completed"; _ ] -> TurnEnded TurnEndCause.Completed
                | [ "te"; "Cancelled"; _ ] -> TurnEnded TurnEndCause.Cancelled
                | [ "te"; "TimedOut"; _ ] -> TurnEnded TurnEndCause.TimedOut
                | [ "te"; "Failed"; detail ] -> TurnEnded(TurnEndCause.Failed detail)
                | _ -> failwith ("Firegrid: unrecognized turn terminal: " + reason))

    /// The turn's final outcome (waits durably). [→ run.Result]
    member handle.Outcome : Async<Result<string, TurnFailure>> =
        async {
            let! result = handle.THSpec.HRun.Result

            return
                match result with
                | Ok output -> Ok output
                | Error CancelledRun -> Error CancelledTurn
                | Error(FailedRun stepError) ->
                    match stepError with
                    | StepError.Failed message -> Error(FailedTurn message)
                    | StepError.Terminal message -> Error(FailedTurn message)
        }

    /// Durable cancel: the turn observes it at its next durable operation,
    /// may compensate, then ends Cancelled. [→ run.Cancel / DurableCancelled]
    member handle.Cancel () : Async<unit> = handle.THSpec.HRun.Cancel()

and AgentEvent =
    | Thinking of text: string
    | Said of text: string
    | CalledTool of tool: string * args: string
    | ToolReturned of tool: string * result: string
    | WaitingFor of what: string          // parked — visibly, in the trace
    | SpawnedChild of agent: string
    | TurnEnded of TurnEndCause

and TurnEndCause = Completed | Cancelled | TimedOut | Failed of string
and TurnFailure = FailedTurn of string | CancelledTurn

/// One agent session: a uniquely-addressed, durable conversation.
/// [→ the session entity (single live turn policy) + turn workflows]
type Session =
    internal
        { SClient: Client
          SAgent: AgentSpec
          SSessionId: string }

    /// Send a prompt; returns when the turn is durably accepted (not when it
    /// finishes — attach to the handle for that). Duplicate prompts (same
    /// promptId) are delivered once. [→ session entity .Call → turn .Start]
    member session.Prompt (promptId: string) (text: string) : Async<TurnHandle> =
        async {
            let! handle = GridRuntime.prompt session.SClient session.SAgent session.SSessionId promptId text
            return { THSpec = handle }
        }

    /// Deliver an external event into the grid (webhook, callback, sensor).
    /// Ingress becomes a durable event; whoever wait_for's it, wakes.
    /// [→ Publish service]
    member _.Deliver (event: Event) : Async<unit> = notYet

    /// Resolve a pending human-approval gate. [→ run.Signal on the approval signal]
    member _.Approve (token: string) (approved: bool) : Async<unit> = notYet

    /// Cancel the live turn, if any. [→ entity Call → run.Cancel]
    member session.CancelLiveTurn () : Async<unit> =
        GridRuntime.cancelLive session.SClient session.SAgent session.SSessionId

    /// This session's turn history — status, end causes, timings — at the
    /// staleness you choose. [→ Projection over the session's journal]
    member _.History (grade: ReadGrade) : Async<TurnSummary list> = notYet

and TurnSummary = { TurnId: string; Cause: TurnEndCause option; StartedAt: Timestamp }

/// The grid client.
type Grid =
    internal { GClient: Client }

    /// Address a session — creating it durably on first use (addressing is
    /// naming; activation is claiming). [→ entity key]
    member grid.Session (agent: AgentDef) (sessionId: string) : Session =
        let (AgentDef spec) = agent

        { SClient = grid.GClient
          SAgent = spec
          SSessionId = sessionId }

    /// Publish to a topic without a session (system-level ingress).
    member _.Publish (event: Event) : Async<unit> = notYet

    /// Fleet view: which sessions are awake, parked (and on what), or idle —
    /// a fold over the grid's journals; the trace IS the schedule.
    /// [→ Projection + client.Read]
    member _.Live (grade: ReadGrade) : Async<GridView> = notYet

and GridView = { Awake: string list; Parked: (string * string) list; Idle: int }

[<RequireQualifiedAccess>]
module Grid =
    /// Connect. [→ Client.connect]
    let connect (basin: S2.Basin) : Grid = { GClient = Client.connect basin }

    /// Host the grid: agents are definitions; registration is data.
    /// [→ Worker.run with the session entity, turn workflows, tool steps,
    ///    topic entity, and Publish service registered]
    let serve (basin: S2.Basin) (ns: string) (agents: AgentDef list) : Async<Worker> =
        GridRuntime.serve basin ns (agents |> List.map (fun (AgentDef spec) -> spec))
