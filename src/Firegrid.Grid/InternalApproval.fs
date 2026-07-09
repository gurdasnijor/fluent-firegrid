/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Grid — the human-approval gate (Phase C / C3 green-making).
///
/// Implementation behind `Tool.gated` and `Session.Approve`, lowered ONLY
/// onto the public Firegrid.Durable (L2) surface exactly as the ratified
/// annotations promise:
///
///   gate park    → a typed approval signal awaited inside the turn
///                  workflow [→ Signal.Await] — journal state: the park
///                  pins no process and survives restarts
///   gate trace   → the turn's WaitingFor event carrying the approval
///                  prompt and the ratified `token=` mechanic (the trace
///                  IS the schedule: the parked record says exactly what
///                  an operator needs to unblock it)
///   approve      → run.Signal on the gate's approval signal, resolved
///                  through the session entity's live turn — no
///                  in-process state; any fresh `Grid.connect` works
///   the tool     → the UNDERLYING journaled step, called only after an
///                  approved=true delivery — exactly-once across crashes,
///                  exactly as any ungated tool call
///
/// Nothing in this file reaches below the L2 contract.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid

open Firegrid.Durable

module internal GridApproval =

    /// The approval token: the deterministic identity of ONE gate — the
    /// turn that parks plus the position of the gated call in that turn's
    /// move sequence. Replay-stable (a pure function of the workflow's
    /// input and program position), distinct per gated call, and free of
    /// spaces so it survives the `token=<t>` trace mechanic verbatim.
    let token (turnId: string) (moveIndex: int) : string = turnId + "#" + string moveIndex

    /// The turn a token belongs to (the part before '#'). `Approve`
    /// verifies it against the session's LIVE turn so a stale token from
    /// an earlier turn fails loud instead of signalling the wrong run.
    let turnOfToken (tokenValue: string) : string =
        match tokenValue.Split('#') |> Array.toList with
        | turnId :: _ -> turnId
        | [] -> tokenValue

    /// The typed approval signal of one gate. Signals are addressed to a
    /// specific run's journal; the token-derived name keeps multiple gates
    /// of one turn distinct.
    let signalFor (tokenValue: string) : Signal<bool> =
        Signal.define<bool> ("grid/approval/" + tokenValue)

    /// The parked-approval trace text: the prompt an operator must answer
    /// plus the ratified `token=` mechanic.
    let parkText (prompt: string) (tokenValue: string) : string = prompt + " token=" + tokenValue

    /// One gated tool call inside the turn workflow: journal the WaitingFor
    /// park record, park on the typed approval signal, then — only on an
    /// approved=true delivery — run the underlying journaled step. On
    /// approved=false the step NEVER runs: the call resolves with a denial
    /// result the model sees as the tool's return and the turn continues
    /// (the frozen law pins only the approved=true path; a human saying
    /// "no" must gate the tool, not kill the conversation).
    let gatedCall
        (emit: string list -> Workflow<unit>)
        (turnId: string)
        (moveIndex: int)
        (prompt: string)
        (step: Step<string, string>)
        (args: string)
        : Workflow<string> =
        let tokenValue = token turnId moveIndex

        workflow {
            do! emit [ "wf"; parkText prompt tokenValue ]
            let! approved = (signalFor tokenValue).Await()

            if approved then
                return! step.Call args
            else
                return "denied: " + prompt
        }

    /// Deliver an approval decision to a parked turn from ANY process:
    /// attach the turn's run by id and raise the token's approval signal
    /// on it — the signal addresses the journal, not a process.
    let deliver (client: Client) (turnId: string) (tokenValue: string) (approved: bool) : Async<unit> =
        let run = Client.attach<string> client (Id turnId)
        run.Signal (signalFor tokenValue) approved
