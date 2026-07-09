/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — L2 Services machinery (Phase C / C4 green-making,
/// the authorized Services carve-out).
///
/// Implementation behind the contract's Services section (`Service.define`
/// / `.Call` / `.CallIdempotent`), exactly as the ratified doc comments
/// promise — a service call is a workflow execution with a derived
/// instance id ("semantically: a workflow with an auto-generated instance
/// id"):
///
///   durable admission    → `admit` journals the execution's StartWorkflow
///                          in the instance's mailbox (DurableClient
///                          startWith) BEFORE any worker is involved; the
///                          ack means the execution exists durably — a
///                          caller may die awaiting and the execution
///                          continues on whichever worker hosts the
///                          namespace
///   dedup (idempotency)  → the instance id IS the idempotency key:
///                          `Call` derives a fresh entropy id per call
///                          (each call its own execution);
///                          `CallIdempotent` derives
///                          `<service>/key/<key>`, and the kernel
///                          mailbox's provenance dedupe (source, seq)
///                          folds a second start of the same id ONCE —
///                          same key ⇒ same execution, one result
///   worker-hosted run    → registration lands the service's program in
///                          the worker RegBag as an ordinary workflow
///                          (contract layer, via `Wiring.mkService`); the
///                          worker loop discovers the instance's inbox
///                          and drives it — nothing executes on the caller
///   collect              → the terminal outcome journaled on the
///                          instance's out stream, generation-chain aware
///                          (`RunFollow`); the contract layer decodes it
///
/// Contract-type-agnostic by design (compiles before the contract file):
/// basins, ids, and encoded payloads only.
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Internal

open Firegrid.Log
open Firegrid.Store.Foundation.Durable

[<RequireQualifiedAccess>]
module internal ServiceExec =
    /// Auto-generated instance id: each `Call` is its own durable execution.
    let callId (service: string) : string =
        service + "/call/" + Interop.entropy ()

    /// Key-derived instance id: same key ⇒ same execution ⇒ one result.
    /// The user-chosen key is validated against K1's reserved identity
    /// segments at admission (G2 doctrine — exactly as workflow `Start`
    /// instance ids and entity keys).
    let idempotentId (service: string) (key: string) : string =
        Reserved.check key
        service + "/key/" + key

    /// Durable admission: journal the execution's start and ensure its out
    /// stream. Idempotent under the kernel mailbox's provenance dedupe — a
    /// duplicate start of the same instance id folds once (same source,
    /// same seq), so retries and same-key calls can never fork a second
    /// execution.
    let admit (basin: S2.Basin) (instanceId: string) (service: string) (encodedInput: string) : Async<unit> =
        async {
            let! result =
                DurableClient.startWith basin (InstanceId.create instanceId) (WorkflowName.create service) encodedInput

            match result with
            | DurableClientStartStatus.Accepted _ -> ()
            | DurableClientStartStatus.Failed failure -> failwith ("service call admission failed: " + string failure)

            do! OutStream.ensure basin instanceId
        }

    /// Await the execution's terminal outcome (follows ContinueAsNew
    /// generation chains). Returns the encoded outcome; the contract layer
    /// decodes success / failure / cancellation.
    let collect (basin: S2.Basin) (instanceId: string) : Async<string> =
        RunFollow.awaitTerminal basin instanceId
