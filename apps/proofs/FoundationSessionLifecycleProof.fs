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
/// - `session.lifecycle-single-writer` — two concurrent starts on one session
///   yield exactly one live turn: the session claim fences the loser (its stale
///   append fails `Deposed`) and the `AlreadyLive` policy rejects a start observing
///   a different live turn — never two producers on one session.
/// - `session.lifecycle-durable-cancel` — a `cancel` from a process that is not the
///   producer is a mailbox send (open-append to the session inbox); the holder's
///   `drive` admits it and seals the turn to a durable `TurnTerminal.Cancelled`. A
///   resend (same `(source, sourceSeq)`) folds once — no second terminal.
/// - `session.lifecycle-deposed-producer` — extends `store.object-live-fencing` /
///   `session.turn-crash-terminal` to lifecycle: after a takeover `start` rotates
///   the fence, the prior `LiveTurn`'s `append`/`complete` fails `Deposed` — it
///   computes but cannot commit. Recovery drives the turn to a durable terminal an
///   attached reader observes rather than hanging (two S2 clients over one `s2Lite`,
///   the "killed" producer kept live to model a still-computing owner).
module FoundationSessionLifecycleProof =

    // ---- Result records ---------------------------------------------------

    type SingleWriterProofResult =
        { FirstHostStartedLive: bool
          TakeoverReattachedSameStream: bool
          DeposedHostCannotAppend: bool
          TakeoverHostAppends: bool
          DifferentTurnRejectedAlreadyLive: bool
          OneStreamNeverForked: bool
          StaleWriteAbsent: bool
          ExactlyOneTerminalCompleted: bool }

    type DurableCancelProofResult =
        { CancelFromNonProducerDurable: bool
          DriveAdmittedAndSealedCancelled: bool
          ResendFoldsOnceNoSecondTerminal: bool
          TurnStreamTerminalCancelled: bool
          ReaderObservedWorkThenCancelled: bool }

    type DeposedProducerProofResult =
        { RecoveryHostTookOver: bool
          DeposedProducerCannotAppend: bool
          DeposedProducerCannotComplete: bool
          RecoveryReachedDurableTerminal: bool
          ReaderObservedTerminalNotHang: bool
          StaleWriteNeverEnteredLog: bool
          ReaderSawPreTakeoverThenRecovery: bool }

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

    // ---- session.lifecycle-single-writer ---------------------------------

    let private singleWriterWorkload ctx =
        ProofOperation.run
            ctx
            "session.lifecycle.single-writer"
            "session-lifecycle-single-writer"
            { ProofOperationOptions.empty with
                Key = Some "session-lifecycle-single-writer" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "lifecycle-single-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let session = Turn.SessionId("sess-" + suffix)
                let turn1 = Turn.TurnId "turn-1"
                let turn2 = Turn.TurnId "turn-2"

                // Host A starts turn-1 and produces one chunk (the live producer).
                let! startA = SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-a") 1_000L

                let liveA =
                    match startA with
                    | Ok live -> live
                    | Error _ -> failwith "host-a start failed unexpectedly"

                let! aEarly = SessionLifecycle.append liveA (TurnChunk.Text "from-a")

                // Host B races to start the SAME turn-1: a same-turnId re-attach /
                // takeover under a new epoch (recovery, never a fork).
                let! startB = SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-b") 1_000L

                let liveB =
                    match startB with
                    | Ok live -> live
                    | Error _ -> failwith "host-b takeover failed unexpectedly"

                // The fence resolves the race: host-a is deposed and cannot commit;
                // host-b is the sole live producer.
                let! aStale = SessionLifecycle.append liveA (TurnChunk.Text "from-a-stale")
                let! bAppend = SessionLifecycle.append liveB (TurnChunk.Text "from-b")

                // The AlreadyLive policy rejects a start observing a DIFFERENT live
                // turn — evidence that exactly one turn (turn-1) is live.
                let! startC = SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn2 (Authority.HolderId "host-c") 2_000L

                let differentTurnRejected =
                    match startC with
                    | Error(SessionLifecycle.StartError.AlreadyLive live) -> live = turn1
                    | _ -> false

                // Recovery seals the one turn stream; a reader sees no fork.
                let! _ = SessionLifecycle.complete liveB 3_000L
                let! reader = attachOk basin (Turn.address session turn1)
                let! chunks, terminal = drain reader
                do! DurableLog.close reader

                let result =
                    { FirstHostStartedLive = (aEarly = Ok())
                      TakeoverReattachedSameStream =
                        (match startB with
                         | Ok _ -> true
                         | Error _ -> false)
                      DeposedHostCannotAppend = (aStale = Error DurableLog.AppendError.Deposed)
                      TakeoverHostAppends = (bAppend = Ok())
                      DifferentTurnRejectedAlreadyLive = differentTurnRejected
                      OneStreamNeverForked = (chunks = [ TurnChunk.Text "from-a"; TurnChunk.Text "from-b" ])
                      StaleWriteAbsent = not (chunks |> List.contains (TurnChunk.Text "from-a-stale"))
                      ExactlyOneTerminalCompleted = (terminal = TurnTerminal.Completed) }

                do!
                    ctx.EmitSpan
                        "proof.session.lifecycle-single-writer.completed"
                        [ "proof.property", "session.lifecycle-single-writer"
                          "lifecycle.deposed_cannot_append", string result.DeposedHostCannotAppend
                          "lifecycle.different_turn_rejected", string result.DifferentTurnRejectedAlreadyLive
                          "lifecycle.one_stream", string result.OneStreamNeverForked ]

                return result
            })

    let private singleWriterProperty =
        property "session.lifecycle-single-writer" {
            s2Lite ""
            workload singleWriterWorkload

            verify (fun v ->
                [ v.Expect.Workload "host-a starts as the live producer" (fun result -> result.FirstHostStartedLive)
                  v.Expect.Workload "a same-session start re-attaches the one turn stream under a new epoch" (fun result ->
                      result.TakeoverReattachedSameStream)
                  v.Expect.Workload "the fenced-out host cannot append after the takeover" (fun result ->
                      result.DeposedHostCannotAppend)
                  v.Expect.Workload "the takeover host is the sole live producer" (fun result ->
                      result.TakeoverHostAppends)
                  v.Expect.Workload "a start observing a different live turn is rejected AlreadyLive" (fun result ->
                      result.DifferentTurnRejectedAlreadyLive)
                  v.Expect.Workload "one stream carries both hosts' committed chunks — never a fork" (fun result ->
                      result.OneStreamNeverForked)
                  v.Expect.Workload "the deposed host's stale write never entered the log" (fun result ->
                      result.StaleWriteAbsent)
                  v.Expect.Workload "the one turn seals a single Completed terminal" (fun result ->
                      result.ExactlyOneTerminalCompleted)
                  v.Trace.SpanExists
                      "single-writer completion span emitted"
                      "proof.session.lifecycle-single-writer.completed"
                      [ "proof.property", "session.lifecycle-single-writer" ]
                  v.Trace.Operation
                      "single-writer operation recorded"
                      ({ TraceOperationMatch.named "session.lifecycle.single-writer" with
                          Status = Some "ok"
                          OutputContains = [ "DeposedHostCannotAppend"; "DifferentTurnRejectedAlreadyLive" ]
                          Count = Some 1 }) ])
        }

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

    // ---- session.lifecycle-deposed-producer ------------------------------

    let private deposedProducerWorkload ctx =
        ProofOperation.run
            ctx
            "session.lifecycle.deposed-producer"
            "session-lifecycle-deposed-producer"
            { ProofOperationOptions.empty with
                Key = Some "session-lifecycle-deposed-producer" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let endpoint =
                    match s2.Endpoint with
                    | Some value -> value
                    | None -> failwith "deposed-producer requires an s2 endpoint (declare s2Lite)"

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "lifecycle-deposed-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                // Recovery host: a fresh S2 connection to the same durable store,
                // modelling a separate process that takes over the running session.
                let recoveryClient =
                    S2.connectWith
                        { S2.ConnectOptions.create "s2-lite-lifecycle-recovery" with
                            AccountEndpoint = Some endpoint
                            BasinEndpoint = Some endpoint }

                let recoveryBasin = recoveryClient |> S2.basin basinName

                let session = Turn.SessionId("sess-" + suffix)
                let turn1 = Turn.TurnId "turn-1"

                // Producer A drives turn-1 mid-flight, without sealing.
                let! liveA = startLive basin session turn1 (Authority.HolderId "host-a") 1_000L
                let! _ = SessionLifecycle.append liveA (TurnChunk.Text "a-0")
                let! _ = SessionLifecycle.append liveA (TurnChunk.Text "a-1")

                // A reader attaches mid-turn, before the takeover.
                let! reader = attachOk basin (Turn.address session turn1)

                // Recovery host B takes over the running session (same turn id, a new
                // epoch across a separate connection). A is kept live to model a
                // still-computing owner.
                let! startB = SessionLifecycle.start recoveryBasin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-b") 1_000L

                let recoveryTookOver =
                    match startB with
                    | Ok _ -> true
                    | Error _ -> false

                let liveB =
                    match startB with
                    | Ok live -> live
                    | Error _ -> failwith "recovery host failed to take over the running session"

                // The live deposed producer A computes but cannot commit — its next
                // emit and its completion both fail Deposed (D2's EmitError law).
                let! aStale = SessionLifecycle.append liveA (TurnChunk.Text "a-stale-post-takeover")
                let! aComplete = SessionLifecycle.complete liveA 2_000L

                // Recovery drives the interrupted turn to a durable terminal.
                let! _ = SessionLifecycle.append liveB (TurnChunk.Text "b-recovery")
                let! _ = SessionLifecycle.complete liveB 3_000L

                // The attached reader observes the terminal rather than hanging.
                let! chunks, terminal = drain reader
                do! DurableLog.close reader

                let result =
                    { RecoveryHostTookOver = recoveryTookOver
                      DeposedProducerCannotAppend = (aStale = Error DurableLog.AppendError.Deposed)
                      DeposedProducerCannotComplete = (aComplete = Error DurableLog.AppendError.Deposed)
                      RecoveryReachedDurableTerminal = (terminal = TurnTerminal.Completed)
                      ReaderObservedTerminalNotHang = (terminal = TurnTerminal.Completed)
                      StaleWriteNeverEnteredLog =
                        not (chunks |> List.contains (TurnChunk.Text "a-stale-post-takeover"))
                      ReaderSawPreTakeoverThenRecovery =
                        (chunks =
                            [ TurnChunk.Text "a-0"
                              TurnChunk.Text "a-1"
                              TurnChunk.Text "b-recovery" ]) }

                do!
                    ctx.EmitSpan
                        "proof.session.lifecycle-deposed-producer.completed"
                        [ "proof.property", "session.lifecycle-deposed-producer"
                          "lifecycle.deposed_cannot_append", string result.DeposedProducerCannotAppend
                          "lifecycle.recovery_terminal", string result.RecoveryReachedDurableTerminal
                          "lifecycle.stale_write_rejected", string result.StaleWriteNeverEnteredLog ]

                return result
            })

    let private deposedProducerProperty =
        property "session.lifecycle-deposed-producer" {
            s2Lite ""
            workload deposedProducerWorkload

            verify (fun v ->
                [ v.Expect.Workload "a recovery host takes over the running session under a new epoch" (fun result ->
                      result.RecoveryHostTookOver)
                  v.Expect.Workload "the deposed producer cannot append after the takeover" (fun result ->
                      result.DeposedProducerCannotAppend)
                  v.Expect.Workload "the deposed producer cannot complete the turn after the takeover" (fun result ->
                      result.DeposedProducerCannotComplete)
                  v.Expect.Workload "recovery drives the turn to a durable terminal" (fun result ->
                      result.RecoveryReachedDurableTerminal)
                  v.Expect.Workload "the attached reader observes the terminal rather than hanging" (fun result ->
                      result.ReaderObservedTerminalNotHang)
                  v.Expect.Workload "the deposed producer's stale write never entered the log" (fun result ->
                      result.StaleWriteNeverEnteredLog)
                  v.Expect.Workload "the reader observes the pre-takeover prefix then the recovery output" (fun result ->
                      result.ReaderSawPreTakeoverThenRecovery)
                  v.Trace.SpanExists
                      "deposed-producer completion span emitted"
                      "proof.session.lifecycle-deposed-producer.completed"
                      [ "proof.property", "session.lifecycle-deposed-producer" ]
                  v.Trace.Operation
                      "deposed-producer operation recorded"
                      ({ TraceOperationMatch.named "session.lifecycle.deposed-producer" with
                          Status = Some "ok"
                          OutputContains = [ "DeposedProducerCannotAppend"; "RecoveryReachedDurableTerminal" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "session.lifecycle" {
            describedAs
                "MS-C5 turn lifecycle authority over the public SessionLifecycle surface: single-writer start by fence + AlreadyLive policy, durable cancel as an idempotent mailbox send, and a deposed producer that computes but cannot commit while recovery reaches a durable terminal."

            property singleWriterProperty
            property durableCancelProperty
            property deposedProducerProperty
        }
