/// ═══════════════════════════════════════════════════════════════════════
/// Support code for the T2 firegrid corpus (Phase C / C1): the scripted
/// ModelSays model stub and grid trial helpers. Test INFRASTRUCTURE only —
/// the system under test is reached exclusively through Firegrid.Grid's
/// public surface (which itself consumes only the Firegrid.Durable L2
/// contract). The model step is SCRIPTED here because T2 pins substrate
/// semantics, not model behavior; the real Claude-adapter harness is T3,
/// out of scope, and the stub lives in the corpus, never in product code.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Firegrid
open Firegrid.Durable

/// The deterministic scripted model: "what the model says", per turn, as
/// plain data. Each scenario law declares one script per agent; the moves
/// are the choreography vocabulary the ratified surface gives every agent
/// (wait_for / wait_until / spawn / publish / execute), plus corpus-only
/// recording moves that turn substrate deliveries into observable
/// side-channel facts.
module ModelSays =
    type Move =
        /// The model says text (assistant output).
        | Say of text: string
        /// execute(tool, args) — a journaled tool call.
        | CallTool of tool: string * args: string
        /// wait_for(topic, match?, selfPrompt?) — park until a matching
        /// event is published; wake as a NEW turn carrying event + prompt.
        | WaitFor of topic: string * matchText: string option * selfPrompt: string option
        /// wait_until(now + afterMs, selfPrompt) — park until an instant;
        /// wake as a NEW turn carrying the self-prompt.
        | WaitUntil of afterMs: float * selfPrompt: string
        /// spawn_all([...]) — durably launch child agent turns, await ALL
        /// their outcomes, then execute recordTool with the outcomes joined
        /// by "|" in DECLARATION order (the corpus's observable join).
        | SpawnAll of children: (string * string) list * recordTool: string
        /// publish(topic, payload) — append a durable event others wait_for.
        | Publish of topic: string * payload: string
        /// Corpus recording move: execute the named tool with THIS TURN'S
        /// INPUT text as args — pins exactly what the substrate delivered
        /// to a waking/child turn (prompt, event payload, self-prompt).
        | RecordInput of tool: string
        /// End the turn with this turn's input as its outcome — lets a
        /// parent's spawn_all join observe what each child received.
        | EchoInput
        /// End the turn (outcome = the last Say, or "" if none).
        | EndTurn

    /// One agent's deterministic script: turn inputs consume Turns in order.
    type Script = { Agent: string; Turns: Move list list }

    /// Corpus-side script registry: the seam T2 green-making binds the
    /// scripted model through (keyed by agent name; deterministic, so
    /// re-registration from restarts and child hosts is idempotent).
    let private registry = System.Collections.Generic.Dictionary<string, Script>()

    let lookup (agent: string) : Script option =
        match registry.TryGetValue agent with
        | true, script -> Some script
        | _ -> None

    /// Translate a scripted move into the product's model-move vocabulary
    /// (the data the "agent/model-turn" step serves each turn from).
    let private gridMove (move: Move) : GridMove =
        match move with
        | Say text -> GSay text
        | CallTool(tool, args) -> GCallTool(tool, args)
        | WaitFor(topic, matchText, selfPrompt) -> GWaitFor(topic, matchText, selfPrompt)
        | WaitUntil(afterMs, selfPrompt) -> GWaitUntil(afterMs, selfPrompt)
        | SpawnAll(children, recordTool) -> GSpawnAll(children, recordTool)
        | Publish(topic, payload) -> GPublish(topic, payload)
        | RecordInput tool -> GCallToolWithInput tool
        | EchoInput -> GEchoInput
        | EndTurn -> GEndTurn

    /// A Harness driven by the script (the T2 model step). The ratified
    /// surface exposes no harness constructor (the adapter contract is
    /// T3), so this registers the script — through the product's
    /// `GridScripted` seam, which the "agent/model-turn" step serves each
    /// turn's moves from — and returns the same stand-in value the
    /// ratified GridExamples.fs itself uses (`ExampleHarness`).
    let harness (script: Script) : Harness =
        registry.[script.Agent] <- script
        GridScripted.bind script.Agent (script.Turns |> List.map (List.map gridMove))
        Unchecked.defaultof<Harness>

/// Grid trial helpers: observation over the PUBLIC Grid surface only.
module GridCorpus =
    [<Emit("Date.now()")>]
    let nowMs () : float = jsNative

    /// Stable label for an AgentEvent — trace assertions without pinning
    /// payload formats (which belong to the model/adapter, not the laws).
    let eventLabel (event: AgentEvent) : string =
        match event with
        | Thinking _ -> "thinking"
        | Said _ -> "said"
        | CalledTool(tool, _) -> "called:" + tool
        | ToolReturned(tool, _) -> "returned:" + tool
        | WaitingFor _ -> "waiting"
        | SpawnedChild agent -> "spawned:" + agent
        | TurnEnded cause -> "ended:" + string cause

    /// Collect every event label of a turn's watch stream (recorded prefix
    /// → live tail → terminal), from whatever process calls it.
    let watchLabels (turn: TurnHandle) : Async<string list> =
        async {
            let seen = ResizeArray<string>()
            do! turn.Watch() |> AsyncSeq.iter (fun event -> seen.Add(eventLabel event))
            return List.ofSeq seen
        }

    /// The first WaitingFor description on the turn's watch — how an
    /// operator SEES a park, and where approval tokens must surface.
    let firstWaiting (turn: TurnHandle) : Async<string> =
        turn.Watch()
        |> AsyncSeq.pick (fun event ->
            match event with
            | WaitingFor what -> Some what
            | _ -> None)

    /// Extract `token=<t>` from a parked description. Drafted pin (for the
    /// ratification read): the parked approval trace event must carry the
    /// token an operator needs to resolve the gate — the trace IS the
    /// schedule, so the trace must say what unblocks it.
    let tokenOf (waitingText: string) : string =
        let marker = "token="
        let index = waitingText.IndexOf marker

        if index < 0 then
            failwith ("no approval token in parked description: " + waitingText)
        else
            let rest = waitingText.Substring(index + marker.Length)

            match rest.Split(' ') |> Array.toList with
            | first :: _ when first <> "" -> first
            | _ -> failwith ("no approval token in parked description: " + waitingText)

    /// Parse "label:timestampMs" side-channel stamps (spawn concurrency
    /// observations).
    let stampsOf (lines: string list) : float list =
        lines
        |> List.map (fun line ->
            match line.Split(':') |> Array.toList with
            | [ _label; stamp ] -> float stamp
            | _ -> failwith ("malformed stamp line: " + line))
