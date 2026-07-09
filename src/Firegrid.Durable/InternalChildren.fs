/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — children + eternal internals (G4 green-making).
///
/// Machinery for the contract's durable-children and eternal (ContinueAsNew)
/// surfaces, composed over the K1 kernel primitives (PR #118):
///
///   • ChildFlow — CallChild/SpawnChild lowering onto the kernel
///     `PerformChild` free-monad node: the parent journals the child call
///     (ChildWorkflowCalled + CallChildWorkflow command), the ChildStart
///     adapter starts `<parent>/child/<opId>` with provenance dedupe, and
///     the parent parks until the child's terminal outcome is delivered
///     back (CompleteChild via the ChildResult adapter). Exactly-once per
///     child is the kernel's journal + mailbox-highwater dedupe; the parent
///     wake floor is the worker loop's inbox sweep (K1 recorded debt:
///     `WakeReason.ChildTerminal` is unwired — correctness first).
///   • RunFollow — generation-chain following for `Run.Result`/`Run.Status`
///     (including handles reattached by id): a rolled-over generation's out
///     stream carries a rollover marker pointing at `<base>/gen/<n+1>`
///     (written by the worker loop when it observes the terminal
///     ContinuedAsNew tick); following markers from any generation lands on
///     the chain's terminal outcome.
///
/// Contract-type-agnostic by design (same rule as Internal.fs): this file
/// compiles BEFORE the contract, so contract-typed glue (terminal-outcome
/// decode, Eternal<'state> lowering) lives in the contract file's own
/// Wiring bodies and is passed in as closures.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Internal

open Firegrid.Log
open Firegrid.Store.Foundation.Durable

[<RequireQualifiedAccess>]
module internal ChildFlow =
    /// The CallChild program node: kernel `PerformChild` with the caller's
    /// terminal-outcome decode woven into the continuation. The child is an
    /// L2 workflow, so its kernel-level output payload is the encoded L2
    /// terminal outcome — the continuation resumes with the decoded value
    /// or re-raises the child's failure as the contract's catchable
    /// exceptions (supplied by the contract file).
    let callNode (workflowName: string) (encodedInput: string) (decodeTerminal: string -> Durable<'o>) : WfNode<'o> =
        NProg(PerformChild(workflowName, encodedInput, decodeTerminal))

[<RequireQualifiedAccess>]
module internal RunFollow =
    /// Await the CHAIN terminal outcome: follow rollover markers across
    /// generations (`<base>` → `<base>/gen/1` → …) to the terminal. Waits
    /// durably — markers and outcomes are stream records, so following
    /// works from any process, including attach-by-id.
    let awaitTerminal (basin: S2.Basin) (key: string) : Async<string> =
        let rec follow key =
            async {
                let! body = OutStream.awaitOutcome basin key

                match OutStream.tryRolledTo body with
                | Some next -> return! follow next
                | None -> return body
            }

        follow key

    /// Read the chain's terminal outcome if one exists. None = the newest
    /// generation is still running (or not yet driven).
    let readTerminal (basin: S2.Basin) (key: string) : Async<string option> =
        let rec follow key =
            async {
                let! outcome = OutStream.readOutcome basin key

                match outcome with
                | None -> return None
                | Some body ->
                    match OutStream.tryRolledTo body with
                    | Some next -> return! follow next
                    | None -> return Some body
            }

        follow key
