namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store
open Firegrid.Store.Foundation.Durable

/// B3 — the MS-C5 turn-lifecycle-authority proof obligations, driven entirely
/// through the public `SessionLifecycle` surface (a session-actor POLICY over
/// B1's `Authority` (I5) and `DurableLog`/`Turn` (I1); no second authority). No
/// deep imports, no proof-only branches in production code. The turn stream is
/// observed through the I1 `DurableLog`/`Turn` reader, as any consumer would.
///
/// - `session.lifecycle-durable-cancel` — a `cancel` from a process that is not the
///   producer is a mailbox send (open-append to the session inbox); the holder's
///   `drive` admits it and seals the turn to a durable `TurnTerminal.Cancelled`. A
///   resend (same `(source, sourceSeq)`) folds once — no second terminal.
///
/// The `session.lifecycle-single-writer` and `session.lifecycle-deposed-producer`
/// obligations moved to the `foundation.fencing` invariant family
/// (FoundationFencingProof.fs) in Packet 0.3b.
module FoundationSessionLifecycleProof =

    // ---- Result records ---------------------------------------------------

    type DurableCancelProofResult =
        { CancelFromNonProducerDurable: bool
          DriveAdmittedAndSealedCancelled: bool
          ResendFoldsOnceNoSecondTerminal: bool
          TurnStreamTerminalCancelled: bool
          ReaderObservedWorkThenCancelled: bool }

    // ---- Helpers over the public surfaces ---------------------------------

    let private startLive
        (basin: S2.Basin)
        (session: Turn.SessionId)
        (turnId: Turn.TurnId)
        (holder: Authority.HolderId)
        (now: Timestamp)
        : Async<SessionLifecycle.LiveTurn> =
        async {
            match! SessionLifecycle.start basin SessionLifecycle.noTimeouts session turnId holder now with
            | Ok live -> return live
            | Error _ -> return failwith "session-lifecycle: start failed unexpectedly"
        }

    let private attachOk
        (basin: S2.Basin)
        (address: DurableLog.Address)
        : Async<DurableLog.Attachment<TurnChunk, TurnTerminal>> =
        async {
            match! DurableLog.attach basin Turn.codec address with
            | Ok attachment -> return attachment
            | Error _ -> return failwith "durable-log: attach failed unexpectedly"
        }

    /// Replay-from-zero + live-tail + terminal via the I1 reader. Blocks-with-wait
    /// on `next`; since each proof drains only after the turn is sealed, it
    /// terminates at the terminal (a hung reader would never return, failing the
    /// trial).
    let private drain
        (attachment: DurableLog.Attachment<TurnChunk, TurnTerminal>)
        : Async<TurnChunk list * TurnTerminal> =
        let chunks = ResizeArray<TurnChunk>()

        let rec loop () =
            async {
                match! DurableLog.next attachment with
                | Ok(Some chunk) ->
                    chunks.Add chunk
                    return! loop ()
                | Ok None ->
                    match! DurableLog.terminal attachment with
                    | Ok terminal -> return (List.ofSeq chunks, terminal)
                    | Error _ -> return failwith "durable-log: terminal read failed"
                | Error _ -> return failwith "durable-log: next read failed"
            }

        loop ()

    // ---- session.lifecycle-durable-cancel --------------------------------

    let private durableCancelWorkload ctx =
        ProofOperation.run
            ctx
            "session.lifecycle.durable-cancel"
            "session-lifecycle-durable-cancel"
            { ProofOperationOptions.empty with
                Key = Some "session-lifecycle-durable-cancel" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "lifecycle-cancel-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let session = Turn.SessionId("sess-" + suffix)
                let turn1 = Turn.TurnId "turn-1"

                // The producer host starts turn-1 and emits work.
                let! liveA = startLive basin session turn1 (Authority.HolderId "host-a") 1_000L
                let! _ = SessionLifecycle.append liveA (TurnChunk.Text "working")

                // A DIFFERENT process (not the producer, holding no session authority)
                // cancels durably — a mailbox send to the session inbox.
                let! cancelResult = SessionLifecycle.cancel basin session turn1 "api-pod-7" 0L

                // The holder observes the cancel on its next drive and seals the turn.
                let! firstDrive = SessionLifecycle.drive liveA WakeReason.MailboxReady 2_000L

                let sealedCancelled =
                    match firstDrive with
                    | Ok(SessionLifecycle.Progress.Ended SessionLifecycle.EndCause.Cancelled) -> true
                    | _ -> false

                // A resend — same (source, sourceSeq) — folds once: no second terminal.
                let! resend = SessionLifecycle.cancel basin session turn1 "api-pod-7" 0L
                let! secondDrive = SessionLifecycle.drive liveA WakeReason.MailboxReady 3_000L

                let resendFoldsOnce =
                    match resend, secondDrive with
                    | Ok(), Ok SessionLifecycle.Progress.Idle -> true
                    | _ -> false

                // The turn stream carries the work then the single Cancelled terminal.
                let! reader = attachOk basin (Turn.address session turn1)
                let! chunks, terminal = drain reader
                do! DurableLog.close reader

                let result =
                    { CancelFromNonProducerDurable = (cancelResult = Ok())
                      DriveAdmittedAndSealedCancelled = sealedCancelled
                      ResendFoldsOnceNoSecondTerminal = resendFoldsOnce
                      TurnStreamTerminalCancelled = (terminal = TurnTerminal.Cancelled)
                      ReaderObservedWorkThenCancelled =
                        (chunks = [ TurnChunk.Text "working" ] && terminal = TurnTerminal.Cancelled) }

                do!
                    ctx.EmitSpan
                        "proof.session.lifecycle-durable-cancel.completed"
                        [ "proof.property", "session.lifecycle-durable-cancel"
                          "lifecycle.sealed_cancelled", string result.DriveAdmittedAndSealedCancelled
                          "lifecycle.resend_folds_once", string result.ResendFoldsOnceNoSecondTerminal
                          "lifecycle.terminal_cancelled", string result.TurnStreamTerminalCancelled ]

                return result
            })

    let private durableCancelProperty =
        property "session.lifecycle-durable-cancel" {
            s2Lite ""
            workload durableCancelWorkload

            verify (fun v ->
                [ v.Expect.Workload "a cancel from a non-producer process is durable (a mailbox send)" (fun result ->
                      result.CancelFromNonProducerDurable)
                  v.Expect.Workload "the holder admits the cancel on its next drive and seals the turn Cancelled" (fun result ->
                      result.DriveAdmittedAndSealedCancelled)
                  v.Expect.Workload "a resend folds once — no second terminal" (fun result ->
                      result.ResendFoldsOnceNoSecondTerminal)
                  v.Expect.Workload "the turn stream's durable terminal is Cancelled" (fun result ->
                      result.TurnStreamTerminalCancelled)
                  v.Expect.Workload "a reader observes the work then the single Cancelled terminal" (fun result ->
                      result.ReaderObservedWorkThenCancelled)
                  v.Trace.SpanExists
                      "durable-cancel completion span emitted"
                      "proof.session.lifecycle-durable-cancel.completed"
                      [ "proof.property", "session.lifecycle-durable-cancel" ]
                  v.Trace.Operation
                      "durable-cancel operation recorded"
                      ({ TraceOperationMatch.named "session.lifecycle.durable-cancel" with
                          Status = Some "ok"
                          OutputContains = [ "DriveAdmittedAndSealedCancelled"; "ResendFoldsOnceNoSecondTerminal" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "session.lifecycle" {
            describedAs
                "MS-C5 turn lifecycle authority over the public SessionLifecycle surface: durable cancel as an idempotent mailbox send."

            property durableCancelProperty
        }
