namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store
open Firegrid.Store.Foundation.Durable

/// A4 — the MS-C4 session-history proof obligations, driven end-to-end through
/// the public `SessionLifecycle` API (`start`/`cancel`/`complete`/`drive`
/// producing the REAL I6 lifecycle log) and read through the public
/// `SessionHistory` surface. No deep imports, no proof-only branches.
///
/// - `session.history-fold` — the L2 turn index folded from B3's session log
///   equals a fold-from-zero; checkpointing does not change the result, including
///   across a host restart. The `EndCause` distinction survives losslessly: an
///   idle-timeout lands as `Ended IdleTimeout`, never collapsed to `Cancelled`.
/// - `session.projection-lag-observable` — an eventual read's `AppliedTail`
///   exposes the projection's lag; a strong read is linearizable.
module FoundationSessionHistoryProof =
    module SH = SessionHistory
    module SL = SessionLifecycle

    type HistoryFoldProofResult =
        { FoldFromZeroHasThreeTurns: bool
          FirstTurnEndedDone: bool
          SecondTurnEndedCancelled: bool
          IdleTimeoutNotCollapsedToCancelled: bool
          CheckpointDoesNotChangeResult: bool
          RebuildWithSuffixHasFourTurns: bool
          RebuildAcrossRestartEquivalent: bool }

    type ProjectionLagProofResult =
        { ReaderSeededPastFenceReadsHistory: bool
          StrongReadObservesNewTurn: bool
          EventualIsPrefixNotPastTail: bool
          EventualNeverAheadOfStrong: bool
          LagExposedAsAppliedTail: bool }

    // ---- Helpers over the public surfaces ---------------------------------

    let private holder = Authority.HolderId "history-host"

    let private startOk basin session turnId timeouts now =
        async {
            match! SL.start basin timeouts session turnId holder now with
            | Ok live -> return live
            | Error _ -> return failwith "session-history proof: start failed unexpectedly"
        }

    let private completeOk (live: SL.LiveTurn) now =
        async {
            match! SL.complete live now with
            | Ok() -> return ()
            | Error _ -> return failwith "session-history proof: complete failed unexpectedly"
        }

    /// Idle-timer id derived from B3's public wake vocabulary (`TimerId of
    /// string`, `WakeReason.TimerFired`): the proof simulates the kernel (C1
    /// router) delivering a fired idle timer — B3 arms `idle:{turn}` (its arm
    /// intent lowers to the wake path, not wired in B3), so a `drive TimerFired`
    /// at/after the deadline seals `IdleTimeout`.
    let private idleTimerId (Turn.TurnId turn) : TimerId = TimerId("idle:" + turn)

    let private statusOf (history: SH.Turns.History) (turnKey: string) : SH.Turns.TurnStatus option =
        history.ByTurn.TryFind turnKey |> Option.map (fun entry -> entry.Status)

    let private turnCount (history: SH.Turns.History) : int = history.ByTurn.Count

    // Deterministic ids so `ByTurn` keys are known.
    let private t1 = Turn.TurnId "turn-1"
    let private t2 = Turn.TurnId "turn-2"
    let private t3 = Turn.TurnId "turn-3"
    let private t4 = Turn.TurnId "turn-4"

    /// Drive the real lifecycle log: turn-1 completes (Done), turn-2 is cancelled
    /// (Cancelled), turn-3 idle-times-out (IdleTimeout) — three distinct causes,
    /// one of which (`IdleTimeout`) collapses to `Cancelled` under `TurnTerminal`.
    let private driveThreeDistinctTurns basin session =
        async {
            // turn-1 — Done.
            let! live1 = startOk basin session t1 SL.noTimeouts 1_000L
            do! completeOk live1 1_100L

            // turn-2 — Cancelled (mailbox send + the holder's drive).
            let! live2 = startOk basin session t2 SL.noTimeouts 2_000L
            let! _ = SL.cancel basin session t2 "op" 1L
            let! cancelDrive = SL.drive live2 WakeReason.MailboxReady 2_100L

            // turn-3 — IdleTimeout (armed idle timer, fired via drive).
            let idleTimeouts: SL.Timeouts = { Idle = Some 100L; MaxDuration = None }
            let! live3 = startOk basin session t3 idleTimeouts 3_000L
            let! idleDrive = SL.drive live3 (WakeReason.TimerFired(idleTimerId t3, 3_100L)) 3_100L

            return cancelDrive, idleDrive
        }

    // ---- session.history-fold --------------------------------------------

    let private runHistoryFold ctx =
        ProofOperation.run
            ctx
            "foundation.session_history_fold"
            "foundation-session-history-fold"
            { ProofOperationOptions.empty with
                Key = Some "foundation-session-history-fold" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "hist-fold-" + suffix
                let session = Turn.SessionId("session-" + suffix)
                let logSubject = SL.logSubject session
                let (SubjectHistory.SubjectId logName) = logSubject
                let (SubjectHistory.SubjectId sidecarName) = Checkpoint.checkpointSubject logSubject

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream logName
                do! basin |> S2.createStream sidecarName

                let! _, idleDrive = driveThreeDistinctTurns basin session

                let idleSealedAsTimeout =
                    match idleDrive with
                    | Ok(SL.Progress.Ended SL.IdleTimeout) -> true
                    | _ -> false

                let projection = SH.Turns.make basin session

                // Fold-from-zero (sidecar empty): the reference truth. Checkpoint's
                // rebuild ignores the session log's fence command records (A2 fix),
                // so a fenced subject folds cleanly.
                let! fromZero, zeroVersion = SH.rebuild projection

                let foldFromZeroHasThreeTurns = turnCount fromZero = 3
                let firstTurnEndedDone = statusOf fromZero "turn-1" = Some(SH.Turns.Ended SL.Done)
                let secondTurnEndedCancelled = statusOf fromZero "turn-2" = Some(SH.Turns.Ended SL.Cancelled)

                // THE distinction: turn-3 is Ended IdleTimeout, not collapsed to Cancelled.
                let idleTimeoutNotCollapsed =
                    statusOf fromZero "turn-3" = Some(SH.Turns.Ended SL.IdleTimeout)
                    && idleSealedAsTimeout

                // Checkpoint, then a cold rebuild (fresh projection) with no suffix:
                // identical to the fold-from-zero.
                let! _ = SH.checkpoint projection
                let! viaCheckpoint, ckVersion = SH.rebuild (SH.Turns.make basin session)

                let checkpointDoesNotChangeResult =
                    viaCheckpoint = fromZero && ckVersion = zeroVersion

                // Append a suffix turn (turn-4, Done) past the checkpoint.
                let! live4 = startOk basin session t4 SL.noTimeouts 4_000L
                do! completeOk live4 4_100L

                // Cold rebuild = latest snapshot + suffix.
                let! withSuffix, suffixVersion = SH.rebuild (SH.Turns.make basin session)
                let rebuildWithSuffixHasFourTurns = turnCount withSuffix = 4

                // Across a host restart: a fresh client attaches to the same durable
                // log + sidecar and rebuilds identical state.
                let restartEndpoint =
                    match s2.Endpoint with
                    | Some endpoint -> endpoint
                    | None -> failwith "session.history-fold requires an s2 endpoint (declare s2Lite)"

                let restartClient =
                    S2.connectWith
                        { S2.ConnectOptions.create "s2-lite-history-restart" with
                            AccountEndpoint = Some restartEndpoint
                            BasinEndpoint = Some restartEndpoint }

                let restartProjection = SH.Turns.make (restartClient |> S2.basin basinName) session
                let! restartHistory, restartVersion = SH.rebuild restartProjection

                let rebuildAcrossRestartEquivalent =
                    restartHistory = withSuffix && restartVersion = suffixVersion

                let result =
                    { FoldFromZeroHasThreeTurns = foldFromZeroHasThreeTurns
                      FirstTurnEndedDone = firstTurnEndedDone
                      SecondTurnEndedCancelled = secondTurnEndedCancelled
                      IdleTimeoutNotCollapsedToCancelled = idleTimeoutNotCollapsed
                      CheckpointDoesNotChangeResult = checkpointDoesNotChangeResult
                      RebuildWithSuffixHasFourTurns = rebuildWithSuffixHasFourTurns
                      RebuildAcrossRestartEquivalent = rebuildAcrossRestartEquivalent }

                do!
                    ctx.EmitSpan
                        "proof.foundation.session_history_fold.completed"
                        [ "proof.property", "session.history-fold"
                          "history.fold_from_zero_three_turns", string result.FoldFromZeroHasThreeTurns
                          "history.idle_timeout_not_collapsed", string result.IdleTimeoutNotCollapsedToCancelled
                          "history.checkpoint_equivalent", string result.CheckpointDoesNotChangeResult
                          "history.restart_equivalent", string result.RebuildAcrossRestartEquivalent ]

                return result
            })

    // ---- session.projection-lag-observable -------------------------------

    let private runProjectionLag ctx =
        ProofOperation.run
            ctx
            "foundation.session_projection_lag"
            "foundation-session-projection-lag"
            { ProofOperationOptions.empty with
                Key = Some "foundation-session-projection-lag" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "proj-lag-" + suffix
                let session = Turn.SessionId("session-" + suffix)
                let logSubject = SL.logSubject session
                let (SubjectHistory.SubjectId logName) = logSubject
                let (SubjectHistory.SubjectId sidecarName) = Checkpoint.checkpointSubject logSubject

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream logName
                do! basin |> S2.createStream sidecarName

                // Populate the log with two completed turns.
                let! live1 = startOk basin session t1 SL.noTimeouts 1_000L
                do! completeOk live1 1_100L
                let! live2 = startOk basin session t2 SL.noTimeouts 2_000L
                do! completeOk live2 2_100L

                let projection = SH.Turns.make basin session

                // Checkpoint first: the resident reader seeds from the checkpoint
                // (`Checkpoint.resumeFrom`), so it starts PAST the session log's
                // seq-0 fence record — the A3 `StateView` pump is command-record
                // unaware, so the checkpoint seed is what keeps a reader off a fence.
                let! _ = SH.checkpoint projection

                let! reader = SH.startReader projection
                let! baseline = SH.readLatest reader
                let readerSeededPastFenceReadsHistory = baseline.State.ByTurn.ContainsKey "turn-1"

                // Append a third turn after the reader started.
                let! live3 = startOk basin session t3 SL.noTimeouts 3_000L
                do! completeOk live3 3_100L

                // Eventual: local applied prefix (may lag); Strong: linearizable.
                let! eventualAfter = SH.readEventual reader
                let! strongAfter = SH.readLatest reader

                let ver = SubjectHistory.versionNumber

                let strongReadObservesNewTurn = strongAfter.State.ByTurn.ContainsKey "turn-3"
                let eventualIsPrefixNotPastTail = ver eventualAfter.AppliedTail <= ver strongAfter.AppliedTail
                let eventualNeverAheadOfStrong = ver eventualAfter.AppliedTail <= ver strongAfter.AppliedTail

                // Lag is data: after a strong read forces catch-up, a fresh eventual
                // read has advanced (monotonic), and its AppliedTail is a readable
                // number the consumer can compare to the checked tail.
                let! caughtUp = SH.readEventual reader
                let lagExposedAsAppliedTail = ver caughtUp.AppliedTail >= ver eventualAfter.AppliedTail

                do! SH.stopReader reader

                let result =
                    { ReaderSeededPastFenceReadsHistory = readerSeededPastFenceReadsHistory
                      StrongReadObservesNewTurn = strongReadObservesNewTurn
                      EventualIsPrefixNotPastTail = eventualIsPrefixNotPastTail
                      EventualNeverAheadOfStrong = eventualNeverAheadOfStrong
                      LagExposedAsAppliedTail = lagExposedAsAppliedTail }

                do!
                    ctx.EmitSpan
                        "proof.foundation.session_projection_lag.completed"
                        [ "proof.property", "session.projection-lag-observable"
                          "lag.reader_reads_history", string result.ReaderSeededPastFenceReadsHistory
                          "lag.strong_observes_new", string result.StrongReadObservesNewTurn
                          "lag.eventual_is_prefix", string result.EventualIsPrefixNotPastTail ]

                return result
            })

    // ---- Properties -------------------------------------------------------

    let historyFoldProperty =
        property "session.history-fold" {
            s2Lite ""
            workload runHistoryFold

            verify (fun v ->
                [ v.Expect.Workload "fold-from-zero materializes the three driven turns" (fun r ->
                      r.FoldFromZeroHasThreeTurns)
                  v.Expect.Workload "a completed turn folds to Ended Done" (fun r -> r.FirstTurnEndedDone)
                  v.Expect.Workload "a cancelled turn folds to Ended Cancelled" (fun r -> r.SecondTurnEndedCancelled)
                  v.Expect.Workload "an idle-timeout folds to Ended IdleTimeout, never collapsed to Cancelled" (fun r ->
                      r.IdleTimeoutNotCollapsedToCancelled)
                  v.Expect.Workload "checkpointing does not change the fold result" (fun r ->
                      r.CheckpointDoesNotChangeResult)
                  v.Expect.Workload "rebuild = latest snapshot + suffix picks up the suffix turn" (fun r ->
                      r.RebuildWithSuffixHasFourTurns)
                  v.Expect.Workload "rebuild across a host restart is equivalent" (fun r ->
                      r.RebuildAcrossRestartEquivalent)
                  v.Trace.SpanExists
                      "session history-fold proof span emitted"
                      "proof.foundation.session_history_fold.completed"
                      [ "proof.property", "session.history-fold" ]
                  v.Trace.Operation
                      "session history-fold operation was recorded"
                      ({ TraceOperationMatch.named "foundation.session_history_fold" with
                          Status = Some "ok"
                          OutputContains =
                              [ "IdleTimeoutNotCollapsedToCancelled"
                                "CheckpointDoesNotChangeResult"
                                "RebuildAcrossRestartEquivalent" ]
                          Count = Some 1 }) ])
        }

    let projectionLagProperty =
        property "session.projection-lag-observable" {
            s2Lite ""
            workload runProjectionLag

            verify (fun v ->
                [ v.Expect.Workload "a reader seeded from the checkpoint reads history past the fence" (fun r ->
                      r.ReaderSeededPastFenceReadsHistory)
                  v.Expect.Workload "a strong read observes a turn appended after the reader started" (fun r ->
                      r.StrongReadObservesNewTurn)
                  v.Expect.Workload "an eventual read is a prefix, never past the checked tail" (fun r ->
                      r.EventualIsPrefixNotPastTail)
                  v.Expect.Workload "an eventual read is never ahead of a strong read" (fun r ->
                      r.EventualNeverAheadOfStrong)
                  v.Expect.Workload "projection lag is exposed as a monotonic AppliedTail" (fun r ->
                      r.LagExposedAsAppliedTail)
                  v.Trace.SpanExists
                      "session projection-lag proof span emitted"
                      "proof.foundation.session_projection_lag.completed"
                      [ "proof.property", "session.projection-lag-observable" ]
                  v.Trace.Operation
                      "session projection-lag operation was recorded"
                      ({ TraceOperationMatch.named "foundation.session_projection_lag" with
                          Status = Some "ok"
                          OutputContains = [ "ReaderSeededPastFenceReadsHistory"; "StrongReadObservesNewTurn" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "session.history" {
            describedAs "Session-history fold (L2 turn index, EndCause-lossless) + projection lag over B3's session log."
            property historyFoldProperty
            property projectionLagProperty
        }
