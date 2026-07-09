/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Grid — session-spine machinery (Phase C / C2 green-making).
///
/// Implementation behind the ratified `Firegrid.Grid.fs` contract bodies:
/// sessions, turns, tools, and turn handles, lowered ONLY onto the public
/// Firegrid.Durable (L2) surface exactly as each member's [→ …] annotation
/// promises:
///
///   session          → entity "grid/session" (keyed, single-writer; the
///                      key is `<agent>/<sessionId>`, so the L2
///                      reserved-segment admission check — G2's typed
///                      `DurableReservedSegment` — applies to the
///                      user-chosen session id verbatim)
///   turn             → workflow "grid/turn" (instance id
///                      `turn/<agent>/<sessionId>/<promptId>`; the id is
///                      the idempotency key, so a duplicate promptId is
///                      delivered once)
///   model turn       → step "agent/model-turn" (the harness seam; T2
///                      binds the corpus's scripted ModelSays data through
///                      `GridScripted`, the adapter harness is T3)
///   tool call        → step "tool/<name>" (journal-served on replay:
///                      exactly-once across crashes)
///   agent events     → the turn's durable out-log `<turnId>/events`
///                      (DurableLog.Append per event via the journaled
///                      "turn/emit" step; DurableLog.Seal with the
///                      terminal cause via "turn/seal")
///   watch            → client.Logs(<turnId>/events).Attach, mapped to
///                      AgentEvents via the ratified `AsyncSeq.map`
///                      amendment (architect ruling on PR #130) — recorded
///                      prefix → live tail → terminal, where the log's
///                      SEAL REASON carries the turn's end cause (one
///                      source of truth for `TurnEnded`)
///   outcome / cancel → run.Result / run.Cancel on the turn's run
///
/// Nothing in this file reaches below the L2 contract.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid

open Firegrid.Log
open Firegrid.Durable

// ── The model-move vocabulary (product wire) ──────────────────────────────
//
// What one model iteration tells the turn workflow to do next: the
// choreography vocabulary of the ratified surface as plain data. PUBLIC
// only as plumbing for the T2 harness seam (`GridScripted.bind`) — the
// corpus's scripted ModelSays stub translates its moves into these; the T3
// adapter harness will produce them from real model output. NOT part of
// the ratified surface.
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type GridMove =
    | GSay of text: string
    | GThink of text: string
    | GCallTool of tool: string * args: string
    /// Corpus recording move: call the tool with THIS TURN'S INPUT text.
    | GCallToolWithInput of tool: string
    /// End the turn with this turn's input as its outcome.
    | GEchoInput
    | GWaitFor of topic: string * matchText: string option * selfPrompt: string option
    | GWaitUntil of afterMs: float * selfPrompt: string
    | GSpawnAll of children: (string * string) list * recordTool: string
    | GPublish of topic: string * payload: string
    | GEndTurn

/// The T2 harness seam: a process-global registry of scripted model turns,
/// keyed by agent name. The ratified surface exposes no `Harness`
/// constructor (the adapter contract is T3, and the corpus's stand-in
/// `Harness` value is empty), so the scripted model binds by agent name:
/// the corpus registers here, and the product's "agent/model-turn" step
/// serves each turn's moves from the registration. Deterministic data —
/// re-registration from restarts and child hosts is idempotent. PUBLIC
/// only as plumbing for the corpus; NOT part of the ratified surface.
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
[<RequireQualifiedAccess>]
module GridScripted =
    let private scripts = System.Collections.Generic.Dictionary<string, GridMove list list>()

    /// Bind the scripted model for an agent: one move list per turn; turn
    /// inputs consume the lists in order.
    let bind (agent: string) (turns: GridMove list list) : unit = scripts.[agent] <- turns

    let internal lookup (agent: string) : GridMove list list option =
        match scripts.TryGetValue agent with
        | true, turns -> Some turns
        | _ -> None

// ── Internal specs (backing fields of the contract types) ─────────────────

/// Backing of `Tool`: the journaled step plus the metadata the model sees.
type internal ToolSpec =
    { ToolName: string
      ToolDescription: string
      ToolStep: Step<string, string> }

/// Backing of `AgentDef`.
type internal AgentSpec =
    { AgentName: string
      AgentTools: ToolSpec list }

/// Backing of `TurnHandle`: the turn's run + its durable event out-log.
/// Both are reconstructable from (basin, turn id) — reattach from any
/// process.
type internal GridTurnHandle =
    { HRun: Run<string>
      HLog: DurableLog }

// ── Session-entity wire types ──────────────────────────────────────────────

type internal SessionCommand =
    | SPrompt of promptId: string * text: string
    | SCancelLive
    | STurnEnded of turnId: string * cause: string

type internal SessionEvent =
    | SEvAccepted of promptId: string * index: int * turnId: string
    | SEvEnded of turnId: string * cause: string

type internal SessionPromptRec =
    { RecPromptId: string
      RecIndex: int
      RecTurnId: string
      RecCause: string }

type internal SessionState =
    { Prompts: SessionPromptRec list
      LiveTurn: string }

type internal SessionReply =
    | SRAccepted of index: int
    | SRDuplicate of index: int
    | SRLive of turnId: string
    | SRAck

// ── Turn-workflow wire types ───────────────────────────────────────────────

/// Input of the "grid/turn" workflow (also the input of the
/// "agent/model-turn" step: the scripted model needs the turn's identity
/// and input to serve the right move list).
type internal TurnInput =
    { TAgent: string
      TSession: string
      TPromptId: string
      TTurnIndex: int
      TText: string }

/// Output of the "agent/model-turn" step.
type internal ModelReply = { RMoves: GridMove list }

type internal EmitIn = { EmStream: string; EmBody: string }
type internal SealIn = { SlStream: string; SlReason: string }

type internal NotifyIn =
    { NoKey: string
      NoTurnId: string
      NoCause: string }

// ── Agent-event wire (the `<turnId>/events` record format) ────────────────
//
// Grid-owned wire: unit-separator-joined fields, first field the kind tag.
// The platform's derived codecs cover step/entity payloads (and carry
// these records' ENVELOPES through the emit step's journal); the event
// records themselves are the turn log's own format, decoded by
// `TurnHandle.Watch`. Formalizing this wire is the T3 adapter packet's
// business.
module internal GridEventWire =
    let private us = "\u001F"

    let encode (parts: string list) : string = String.concat us parts
    let decode (body: string) : string list = body.Split('\u001F') |> Array.toList

// ── The session runtime ────────────────────────────────────────────────────

module internal GridRuntime =

    // ── Addressing (naming is addressing; creation on first use) ─────────

    let sessionKey (agent: string) (sessionId: string) : string = agent + "/" + sessionId

    let turnIdOf (agent: string) (sessionId: string) (promptId: string) : string =
        "turn/" + agent + "/" + sessionId + "/" + promptId

    let eventsStreamOf (turnId: string) : string = turnId + "/events"

    // ── The session entity: admission, dedup, live-turn tracking ─────────
    //
    // Decide is pure; the effectful turn is a workflow the PROMPT path
    // starts after admission (compose, don't fuse). Dedup is by promptId
    // in the decider (same promptId ⇒ same turn index ⇒ same turn id), and
    // the turn workflow's instance id makes the start idempotent below.

    let private sessionDecider: Decider<SessionCommand, SessionEvent, SessionState, SessionReply> =
        { Initial = { Prompts = []; LiveTurn = "" }
          Evolve =
            fun state event ->
                match event with
                | SEvAccepted(promptId, index, turnId) ->
                    { Prompts =
                        state.Prompts
                        @ [ { RecPromptId = promptId
                              RecIndex = index
                              RecTurnId = turnId
                              RecCause = "" } ]
                      LiveTurn = turnId }
                | SEvEnded(turnId, cause) ->
                    { Prompts =
                        state.Prompts
                        |> List.map (fun r ->
                            if r.RecTurnId = turnId && r.RecCause = "" then
                                { r with RecCause = cause }
                            else
                                r)
                      LiveTurn = if state.LiveTurn = turnId then "" else state.LiveTurn }
          Decide =
            fun (Key key) command state ->
                match command with
                | SPrompt(promptId, _text) ->
                    match state.Prompts |> List.tryFind (fun r -> r.RecPromptId = promptId) with
                    | Some existing -> SRDuplicate existing.RecIndex, []
                    | None ->
                        let index = List.length state.Prompts
                        let turnId = "turn/" + key + "/" + promptId
                        SRAccepted index, [ SEvAccepted(promptId, index, turnId) ]
                | SCancelLive -> SRLive state.LiveTurn, []
                | STurnEnded(turnId, cause) -> SRAck, [ SEvEnded(turnId, cause) ] }

    let sessionEntity: EntityDef<SessionCommand, SessionEvent, SessionState, SessionReply> =
        Entity.define "grid/session" sessionDecider

    // ── Tools ─────────────────────────────────────────────────────────────

    /// Adapt a typed tool body to the raw model wire. T2 scope: the model
    /// speaks text, and every ratified-corpus tool is string → string; a
    /// non-string payload is the T3 adapter's schema work and fails loud
    /// here rather than mis-serving silently.
    let rawToolRun<'args, 'result>
        (run: 'args -> Async<'result>)
        (argTy: System.Type)
        (resultTy: System.Type)
        : string -> Async<string> =
        fun raw ->
            async {
                if argTy.FullName <> "System.String" || resultTy.FullName <> "System.String" then
                    return
                        failwith
                            "Firegrid: non-string tool payloads are bound by the T3 adapter packet (model-wire schemas); T2 tools are string → string"
                else
                    let! result = run (unbox<'args> (box raw))
                    return unbox<string> (box result)
            }

    let mkToolSpec (name: string) (description: string) (run: string -> Async<string>) : ToolSpec =
        { ToolName = name
          ToolDescription = description
          ToolStep = Step.define ("tool/" + name) run }

    // ── The turn workflow ─────────────────────────────────────────────────

    let private describeMove (move: GridMove) : string =
        match move with
        | GSay _ -> "say"
        | GThink _ -> "think"
        | GCallTool _ -> "execute"
        | GCallToolWithInput _ -> "execute(turn-input)"
        | GEchoInput -> "echo-input"
        | GWaitFor _ -> "wait_for"
        | GWaitUntil _ -> "wait_until"
        | GSpawnAll _ -> "spawn_all"
        | GPublish _ -> "publish"
        | GEndTurn -> "end"

    let private stepErrorText (error: StepError) : string =
        match error with
        | StepError.Failed message -> message
        | StepError.Terminal message -> message

    /// The loop that drives one turn: model moves in order; every tool
    /// call a journaled step (exactly-once across crashes); every event a
    /// journaled append to the turn's out-log; the terminal seals it.
    let private turnProgram
        (agents: AgentSpec list)
        (modelStep: Step<TurnInput, ModelReply>)
        (emitStep: Step<EmitIn, unit>)
        (sealStep: Step<SealIn, unit>)
        (notifyStep: Step<NotifyIn, unit>)
        (input: TurnInput)
        : Workflow<string> =

        let turnId = turnIdOf input.TAgent input.TSession input.TPromptId
        let stream = eventsStreamOf turnId
        let agentSpec = agents |> List.tryFind (fun a -> a.AgentName = input.TAgent)

        let emit (parts: string list) : Workflow<unit> =
            emitStep.Call
                { EmStream = stream
                  EmBody = GridEventWire.encode parts }

        let terminal (message: string) : Workflow<'a> =
            workflow { return raise (DurableStepFailed(StepError.Terminal message)) }

        let callTool (tool: string) (args: string) : Workflow<string> =
            match agentSpec |> Option.bind (fun a -> a.AgentTools |> List.tryFind (fun t -> t.ToolName = tool)) with
            | Some spec -> spec.ToolStep.Call args
            | None -> terminal ("Firegrid: agent '" + input.TAgent + "' has no tool '" + tool + "'")

        // A durable cancel-observation point. The kernel delivers a durable
        // cancel at WAIT boundaries (every lowered wait races the reserved
        // cancel signal, and the host tick delivers a pending signal before
        // it runs the timer/activity adapters) — a moves-only turn is a
        // pure step chain and would otherwise never observe it. A short
        // durable sleep before each move makes "observed at the turn's
        // next durable operation; no later move ever runs" real: a pending
        // cancel wins the race and raises DurableCancelled; absent one,
        // the timer fires and the turn proceeds. The sleep must exceed a
        // host tick: a due-immediately timer is fired by the adapter pass
        // of the same tick that commits the race, resolving it before any
        // Waiting tick can deliver the pending cancel signal.
        let observeCancel: Workflow<unit> = Workflow.sleep (Duration.seconds 0.15)

        let rec runMoves (moves: GridMove list) (lastSaid: string) : Workflow<string> =
            match moves with
            | [] -> workflow { return lastSaid }
            | move :: rest ->
                workflow {
                    do! observeCancel
                    return! runMove move rest lastSaid
                }

        and runMove (move: GridMove) (rest: GridMove list) (lastSaid: string) : Workflow<string> =
            match move with
            | GEndTurn -> workflow { return lastSaid }
            | GSay text ->
                workflow {
                    do! emit [ "sa"; text ]
                    return! runMoves rest text
                }
            | GThink text ->
                workflow {
                    do! emit [ "th"; text ]
                    return! runMoves rest lastSaid
                }
            | GCallTool(tool, args) ->
                workflow {
                    do! emit [ "ct"; tool; args ]
                    let! result = callTool tool args
                    do! emit [ "tr"; tool; result ]
                    return! runMoves rest lastSaid
                }
            | _ ->
                // wait_for / wait_until / spawn_all / publish / the corpus
                // wake-recording moves: later packets' laws. A clean
                // terminal failure (never a hang): the turn seals Failed
                // and the law observing it stays red as a law.
                terminal ("Firegrid: choreography move not yet lowered in this packet: " + describeMove move)

        // The turn's terminal IS the log terminal: the seal reason carries
        // the encoded end cause, so `TurnEnded` has one source of truth —
        // Watch maps the attach Terminal to it, and no separate terminal
        // record can duplicate in a crash window.
        let finish (causeKind: string) (detail: string) (notifyCause: string) : Workflow<unit> =
            workflow {
                do!
                    sealStep.Call
                        { SlStream = stream
                          SlReason = GridEventWire.encode [ "te"; causeKind; detail ] }

                do!
                    notifyStep.Call
                        { NoKey = sessionKey input.TAgent input.TSession
                          NoTurnId = turnId
                          NoCause = notifyCause }
            }

        workflow {
            try
                let! reply = modelStep.Call input
                let! outcome = runMoves reply.RMoves ""
                do! finish "Completed" "" "completed"
                return outcome
            with
            | DurableCancelled ->
                // Durable cancel observed at a bind boundary: record the
                // cause in the watch stream, seal the out-log, then finish
                // Cancelled (more journaled work after the catch is the
                // contract's compensation allowance).
                do! finish "Cancelled" "" "cancelled"
                return raise DurableCancelled
            | DurableStepFailed stepError ->
                let message = stepErrorText stepError
                do! finish "Failed" message "failed"
                return raise (DurableStepFailed(StepError.Terminal message))
            | error ->
                do! finish "Failed" error.Message "failed"
                return raise (DurableStepFailed(StepError.Terminal error.Message))
        }

    // ── The model-turn step handler (the T2 harness seam) ─────────────────

    let private modelHandler (input: TurnInput) : Async<ModelReply> =
        async {
            match GridScripted.lookup input.TAgent with
            | None ->
                return
                    raise (
                        Step.terminal (
                            "Firegrid: no harness bound for agent '"
                            + input.TAgent
                            + "' — T2 binds the scripted model through GridScripted; the adapter harness is T3"
                        )
                    )
            | Some turns ->
                if input.TTurnIndex < List.length turns then
                    return { RMoves = List.item input.TTurnIndex turns }
                else
                    return
                        raise (
                            Step.terminal (
                                "Firegrid: scripted model for agent '"
                                + input.TAgent
                                + "' has no turn "
                                + string input.TTurnIndex
                            )
                        )
        }

    // ── Hosting ───────────────────────────────────────────────────────────

    let serve (basin: S2.Basin) (ns: string) (agents: AgentSpec list) : Async<Worker> =
        let client = Client.connect basin

        let emitStep =
            Step.define "turn/emit" (fun (e: EmitIn) ->
                async {
                    let! _version = (client.Logs [ e.EmStream ]).Append e.EmBody
                    return ()
                })

        let sealStep =
            Step.define "turn/seal" (fun (s: SealIn) ->
                async {
                    // Idempotent at the handler: a crash window between the
                    // seal's execution and its journal commit re-runs the
                    // step against an already-sealed log.
                    try
                        do! (client.Logs [ s.SlStream ]).Seal s.SlReason
                    with error ->
                        if error.Message.Contains "durable log is sealed" then
                            ()
                        else
                            raise error
                })

        let notifyStep =
            Step.define "session/turn-ended" (fun (n: NotifyIn) ->
                sessionEntity.Send client n.NoKey (STurnEnded(n.NoTurnId, n.NoCause)))

        let modelStep = Step.define "agent/model-turn" modelHandler

        let toolSteps =
            agents
            |> List.collect (fun a -> a.AgentTools)
            |> List.distinctBy (fun t -> t.ToolName)

        let turnDef =
            Workflow.define "grid/turn" (turnProgram agents modelStep emitStep sealStep notifyStep)

        let registrations =
            [ reg sessionEntity
              reg turnDef
              reg modelStep
              reg emitStep
              reg sealStep
              reg notifyStep ]
            @ (toolSteps |> List.map (fun t -> reg t.ToolStep))

        Worker.run basin ns registrations

    // ── The prompt path (admission → durable start → handle) ─────────────

    /// Session entity `.Call` (admission: dedup by promptId, single-live
    /// tracking; the L2 reserved-segment check applies to the key) → turn
    /// workflow `.Start` (declared def — the client side never needs the
    /// program; the instance id makes duplicates one run). Returns on
    /// durable acceptance.
    let prompt (client: Client) (agent: AgentSpec) (sessionId: string) (promptId: string) (text: string) : Async<GridTurnHandle> =
        async {
            let key = sessionKey agent.AgentName sessionId
            let! reply = sessionEntity.Call client key (SPrompt(promptId, text))

            let index =
                match reply with
                | SRAccepted index -> index
                | SRDuplicate index -> index
                | other -> failwith ("Firegrid: unexpected session admission reply: " + string other)

            let turnId = turnIdOf agent.AgentName sessionId promptId
            let turnDecl = Workflow.declare<TurnInput, string> "grid/turn"

            let input =
                { TAgent = agent.AgentName
                  TSession = sessionId
                  TPromptId = promptId
                  TTurnIndex = index
                  TText = text }

            let! run = turnDecl.Start client input (Id turnId)

            return
                { HRun = run
                  HLog = client.Logs [ eventsStreamOf turnId ] }
        }

    /// Cancel the live turn through the entity: the session resolves WHICH
    /// turn is live; the cancel itself is the run's durable cancel.
    let cancelLive (client: Client) (agent: AgentSpec) (sessionId: string) : Async<unit> =
        async {
            let key = sessionKey agent.AgentName sessionId
            let! reply = sessionEntity.Call client key SCancelLive

            match reply with
            | SRLive turnId when turnId <> "" ->
                let run = Client.attach<string> client (Id turnId)
                do! run.Cancel()
            | _ -> return ()
        }
