namespace Firegrid.Foundation.Proofs

open System
open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store
open Firegrid.Store.Foundation.Durable

/// Packet 0.3b — `foundation.rebuild-equivalence`: the fold/rebuild
/// equivalence invariant, stated ONCE (RebuildLaw.fs) and instantiated over
/// four foundation surfaces. Retires the five bespoke restatements:
///
///   1. checkpoint-trim   <- state.checkpoint-rebuild-equivalence + state.trim-safety (MERGED)
///   2. session-history   <- session.history-fold
///   3. state-view        <- foundation.state-view (incl. poisoned-decode)
///   4. kv-store          <- foundation.kv-store (incl. poisoned-apply)
///
/// Consolidation deletes RESTATEMENTS, never ASSERTIONS: every check of every
/// retired proof maps to a RebuildLaw core check or keeps its original name
/// as a fact check (see the PR correspondence table).
module FoundationRebuildProof =

    let private failsWith (expected: string) work =
        async {
            try
                let! _ = work
                return false
            with e ->
                return e.Message.Contains(expected)
        }

    // =======================================================================
    // 1. checkpoint-trim (merges state.checkpoint-rebuild-equivalence +
    //    state.trim-safety over the public Checkpoint surface)
    // =======================================================================

    type private RbDelta = RbDelta of int
    type private RbCounter = { Total: int; Applied: int }

    module private RbDelta =
        let encode (RbDelta value) = "delta|" + string value

        let decode (body: string) =
            match body.Split('|') |> Array.toList with
            | [ "delta"; value ] ->
                match Int32.TryParse value with
                | true, parsed -> Ok(RbDelta parsed)
                | false, _ -> Error("bad delta: " + value)
            | _ -> Error("unknown record body: " + body)

        let codec: SubjectHistory.Codec<RbDelta> = { Encode = encode; Decode = decode }

    module private RbCounter =
        let initial = { Total = 0; Applied = 0 }

        let apply (state: RbCounter) (record: SubjectHistory.StoredRecord<RbDelta>) =
            let (RbDelta value) = record.Body

            { Total = state.Total + value
              Applied = state.Applied + 1 }

        let codec: Checkpoint.StateCodec<RbCounter> =
            { Encode = fun state -> sprintf "%d,%d" state.Total state.Applied
              Decode =
                fun body ->
                    match body.Split(',') |> Array.toList with
                    | [ total; applied ] ->
                        match Int32.TryParse total, Int32.TryParse applied with
                        | (true, total), (true, applied) -> Ok { Total = total; Applied = applied }
                        | _ -> Error("bad counter state: " + body)
                    | _ -> Error("bad counter state: " + body) }

    let private rbMake basin source =
        Checkpoint.make basin RbDelta.codec RbCounter.codec source RbCounter.initial RbCounter.apply

    let private expectCheckpoint label result =
        match result with
        | Ok snapshot -> snapshot
        | Error _ -> failwithf "checkpoint %s failed unexpectedly (single-writer workload)" label

    // -- fact names: from state.checkpoint-rebuild-equivalence
    [<Literal>]
    let private ctFactNoSnapshotZero = "no snapshot resumes the fold from Seq 0"

    [<Literal>]
    let private ctFactFirstCheckpoint = "first checkpoint is as-of the source tail with folded state"

    [<Literal>]
    let private ctFactSecondCheckpoint = "second checkpoint advances AsOf and state"

    [<Literal>]
    let private ctFactLatestReflects = "latest returns the most recent committed snapshot"

    [<Literal>]
    let private ctFactResumeIsSuffix = "rebuild resumes from the latest AsOf suffix, not Seq 0"

    [<Literal>]
    let private ctFactColdRebuild = "cold rebuild equals fold-from-zero (checkpoint + suffix = full replay)"

    [<Literal>]
    let private ctFactRestartRebuild = "rebuild across a host restart equals fold-from-zero"

    [<Literal>]
    let private ctFactUncheckpointed = "an uncheckpointed source rebuilds as a fold-from-zero"

    // -- fact names: from state.trim-safety
    [<Literal>]
    let private ctFactGuardAhead = "trim past the latest committed AsOf is rejected as AheadOfCheckpoint"

    [<Literal>]
    let private ctFactGuardNoOp = "a rejected trim appends nothing to the source"

    [<Literal>]
    let private ctFactTrimAtFloor = "trim at the committed floor succeeds"

    [<Literal>]
    let private ctFactTrimBehindFloor = "trim behind the committed floor succeeds"

    [<Literal>]
    let private ctFactFloorRebuild = "a cold reader from the trim floor rebuilds equivalent state"

    [<Literal>]
    let private ctFactMarkerSkipped = "the trim marker advances the tail but is skipped by rebuild"

    [<Literal>]
    let private ctFactSecondTrimRebuild = "rebuild stays equivalent after a second trim"

    [<Literal>]
    let private ctFactNeverCkGuard = "a never-checkpointed source guards trim against Version 0"

    [<Literal>]
    let private ctFactNeverCkZeroOk = "trim at Version 0 on a never-checkpointed source is a no-op Ok"

    let private checkpointTrimSurface: RebuildLaw.RebuildSurface<_, _> =
        { Instance = "checkpoint-trim"
          OperationName = "foundation.rebuild-equivalence.checkpoint-trim"
          FactNames =
            [ ctFactNoSnapshotZero
              ctFactFirstCheckpoint
              ctFactSecondCheckpoint
              ctFactLatestReflects
              ctFactResumeIsSuffix
              ctFactColdRebuild
              ctFactRestartRebuild
              ctFactUncheckpointed
              ctFactGuardAhead
              ctFactGuardNoOp
              ctFactTrimAtFloor
              ctFactTrimBehindFloor
              ctFactFloorRebuild
              ctFactMarkerSkipped
              ctFactSecondTrimRebuild
              ctFactNeverCkGuard
              ctFactNeverCkZeroOk ]
          HasRestartVariant = true
          HasTrimPolicy = true
          WriterOps =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "rbld-" + suffix

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    // -- EQ scenario source (rebuild-equivalence): the consumer
                    // never names the sidecar; provisioning derives it.
                    let eqName = "chk-src-" + suffix
                    let eqSource = SubjectHistory.SubjectId eqName
                    let (SubjectHistory.SubjectId eqSidecarName) = Checkpoint.checkpointSubject eqSource
                    do! basin |> S2.createStream eqName
                    do! basin |> S2.createStream eqSidecarName

                    let eqFold = rbMake basin eqSource

                    // Pure sans-IO core: no snapshot resumes the fold from Seq 0.
                    let noSnapshotResumesFromZero =
                        Checkpoint.resumeFrom None RbCounter.initial = (SubjectHistory.Seq 0L, RbCounter.initial)

                    // Batch 1 -> source tail Version 5.
                    let! _ =
                        SubjectHistory.append basin RbDelta.codec eqSource [ RbDelta 1; RbDelta 2; RbDelta 3; RbDelta 4; RbDelta 5 ]

                    let! firstCheckpoint = Checkpoint.checkpoint eqFold
                    let snap1 = expectCheckpoint "first" firstCheckpoint

                    let firstCheckpointAtSourceTail =
                        snap1.AsOf = SubjectHistory.Version 5L
                        && snap1.State = { Total = 15; Applied = 5 }

                    // Batch 2 -> source tail Version 7.
                    let! _ = SubjectHistory.append basin RbDelta.codec eqSource [ RbDelta 10; RbDelta 20 ]

                    let! secondCheckpoint = Checkpoint.checkpoint eqFold
                    let snap2 = expectCheckpoint "second" secondCheckpoint

                    let secondCheckpointAdvances =
                        snap2.AsOf = SubjectHistory.Version 7L
                        && snap2.State = { Total = 45; Applied = 7 }

                    // Batch 3 (uncheckpointed suffix) -> source tail Version 8.
                    let! _ = SubjectHistory.append basin RbDelta.codec eqSource [ RbDelta 100 ]

                    // -- TRIM scenario source: seqs 0..4 ; checkpoint AsOf=5 ;
                    // seqs 5..7 ; checkpoint AsOf=8 ; uncheckpointed seqs 8..9.
                    let trimName = "trim-src-" + suffix
                    let trimSource = SubjectHistory.SubjectId trimName
                    let (SubjectHistory.SubjectId trimSidecarName) = Checkpoint.checkpointSubject trimSource
                    do! basin |> S2.createStream trimName
                    do! basin |> S2.createStream trimSidecarName

                    let trimFold = rbMake basin trimSource

                    let! _ =
                        SubjectHistory.append basin RbDelta.codec trimSource [ RbDelta 1; RbDelta 2; RbDelta 3; RbDelta 4; RbDelta 5 ]

                    let! trimFirst = Checkpoint.checkpoint trimFold
                    let _ = expectCheckpoint "trim-first" trimFirst
                    let! _ = SubjectHistory.append basin RbDelta.codec trimSource [ RbDelta 10; RbDelta 20; RbDelta 30 ]
                    let! trimSecond = Checkpoint.checkpoint trimFold
                    let trimSnap2 = expectCheckpoint "trim-second" trimSecond
                    // committed AsOf is now trimSnap2.AsOf = Version 8.

                    let! _ = SubjectHistory.append basin RbDelta.codec trimSource [ RbDelta 100; RbDelta 200 ]

                    // Reference: full rebuild BEFORE any trim.
                    let! refState, refVer = Checkpoint.rebuild trimFold

                    return
                        (s2,
                         basin,
                         (eqSource, eqName, eqSidecarName, eqFold, snap2, basinName),
                         (trimSource, trimName, trimSidecarName, trimFold, trimSnap2, refState, refVer),
                         (noSnapshotResumesFromZero, firstCheckpointAtSourceTail, secondCheckpointAdvances),
                         suffix)
                }
          Policy =
            fun _ctx world ->
                async {
                    let (_, basin, _, trimScenario, writerFacts, _) = world
                    let (trimSource, _, _, trimFold, trimSnap2, refState, refVer) = trimScenario

                    let (noSnapshotResumesFromZero, firstCheckpointAtSourceTail, secondCheckpointAdvances) =
                        writerFacts

                    // (A) Guard: trim past the latest committed AsOf is rejected.
                    let! ahead = Checkpoint.trim trimFold (SubjectHistory.Version 9L)

                    let guardRejectsAheadOfCheckpoint =
                        ahead = Error(Checkpoint.TrimFailure.AheadOfCheckpoint(SubjectHistory.Version 9L, trimSnap2.AsOf))

                    // A rejected trim appends nothing to the source.
                    let! tailAfterRejected = SubjectHistory.tail basin trimSource
                    let guardIsNoOpOnSource = tailAfterRejected = refVer

                    // (B) Trim at the committed floor succeeds.
                    let! atFloor = Checkpoint.trim trimFold trimSnap2.AsOf
                    let trimAtFloorOk = atFloor = Ok()

                    // (C) A cold reader from the trim floor rebuilds EQUIVALENT
                    // state.
                    let coldFold = rbMake basin trimSource
                    let! postState, postVer = Checkpoint.rebuild coldFold
                    let rebuildFromTrimFloorEquivalentState = postState = refState

                    // The trim marker advanced the source tail by one, yet
                    // rebuild skipped it (state unchanged) — the marker is on
                    // the log, not in the fold.
                    let trimMarkerAdvancesTailButIsSkipped =
                        SubjectHistory.versionNumber postVer = SubjectHistory.versionNumber refVer + 1L
                        && postState.Applied = refState.Applied

                    // (D) Trim behind the floor also succeeds and stays
                    // equivalent.
                    let! behind = Checkpoint.trim trimFold (SubjectHistory.Version 5L)
                    let trimBehindFloorOk = behind = Ok()
                    let! postState2, _ = Checkpoint.rebuild (rbMake basin trimSource)
                    let rebuildAfterSecondTrimEquivalent = postState2 = refState

                    // (E) Never-checkpointed source: committed floor is
                    // Version 0.
                    let (_, _, _, _, _, suffix) = world
                    let plainName = "trim-plain-" + suffix
                    let plain = SubjectHistory.SubjectId plainName
                    let (SubjectHistory.SubjectId plainSidecar) = Checkpoint.checkpointSubject plain
                    do! basin |> S2.createStream plainName
                    do! basin |> S2.createStream plainSidecar
                    let! _ = SubjectHistory.append basin RbDelta.codec plain [ RbDelta 7; RbDelta 8 ]
                    let plainFold = rbMake basin plain

                    let! plainAhead = Checkpoint.trim plainFold (SubjectHistory.Version 1L)

                    let neverCheckpointedGuardsAgainstZero =
                        plainAhead = Error(
                            Checkpoint.TrimFailure.AheadOfCheckpoint(SubjectHistory.Version 1L, SubjectHistory.Version 0L)
                        )

                    let! plainZero = Checkpoint.trim plainFold (SubjectHistory.Version 0L)
                    let neverCheckpointedTrimAtZeroOk = plainZero = Ok()

                    do! basin |> S2.deleteStream plainSidecar
                    do! basin |> S2.deleteStream plainName

                    let policySafe =
                        guardRejectsAheadOfCheckpoint
                        && guardIsNoOpOnSource
                        && trimAtFloorOk
                        && trimBehindFloorOk
                        && rebuildFromTrimFloorEquivalentState
                        && trimMarkerAdvancesTailButIsSkipped
                        && rebuildAfterSecondTrimEquivalent
                        && neverCheckpointedGuardsAgainstZero
                        && neverCheckpointedTrimAtZeroOk

                    let report: RebuildLaw.PolicyReport =
                        { PolicySafe = policySafe
                          Facts =
                            [ ctFactNoSnapshotZero, noSnapshotResumesFromZero
                              ctFactFirstCheckpoint, firstCheckpointAtSourceTail
                              ctFactSecondCheckpoint, secondCheckpointAdvances
                              ctFactGuardAhead, guardRejectsAheadOfCheckpoint
                              ctFactGuardNoOp, guardIsNoOpOnSource
                              ctFactTrimAtFloor, trimAtFloorOk
                              ctFactTrimBehindFloor, trimBehindFloorOk
                              ctFactFloorRebuild, rebuildFromTrimFloorEquivalentState
                              ctFactMarkerSkipped, trimMarkerAdvancesTailButIsSkipped
                              ctFactSecondTrimRebuild, rebuildAfterSecondTrimEquivalent
                              ctFactNeverCkGuard, neverCheckpointedGuardsAgainstZero
                              ctFactNeverCkZeroOk, neverCheckpointedTrimAtZeroOk ] }

                    return ((), report)
                }
          RebuildVsReference =
            fun _ctx world _policy ->
                async {
                    let (s2, basin, eqScenario, trimScenario, _, suffix) = world
                    let (eqSource, eqName, eqSidecarName, _, snap2, basinName) = eqScenario
                    let (_, trimName, trimSidecarName, _, _, _, _) = trimScenario

                    // Cold Fold: fresh value, no resident memory, same process.
                    let coldFold = rbMake basin eqSource

                    let! latestSnapshot = Checkpoint.latest coldFold
                    let latestReflectsLastCommit = latestSnapshot = Some snap2

                    // rebuild resumes from the latest checkpoint's AsOf, not Seq 0.
                    let resumeFromLatestIsSuffix =
                        Checkpoint.resumeFrom latestSnapshot RbCounter.initial = (SubjectHistory.Seq 7L, snap2.State)

                    let! rebuildState, rebuildVersion = Checkpoint.rebuild coldFold

                    // Reference: full replay from Seq 0 to the source tail.
                    let! sourceTail = SubjectHistory.tail basin eqSource

                    let! foldFromZeroState, foldFromZeroVersion =
                        SubjectHistory.foldTo
                            basin
                            RbDelta.codec
                            eqSource
                            (SubjectHistory.Seq 0L)
                            sourceTail
                            RbCounter.initial
                            RbCounter.apply

                    let rebuildEqualsFoldFromZero =
                        rebuildState = foldFromZeroState && rebuildVersion = foldFromZeroVersion

                    // Host restart: a fresh client attaches to the same durable
                    // S2, a fresh Fold rebuilds from latest snapshot + suffix.
                    let restartEndpoint =
                        match s2.Endpoint with
                        | Some endpoint -> endpoint
                        | None -> failwith "restart step requires an s2 endpoint (declare s2Lite)"

                    let restartClient =
                        S2.connectWith
                            { S2.ConnectOptions.create "s2-lite-proof-runner-restart" with
                                AccountEndpoint = Some restartEndpoint
                                BasinEndpoint = Some restartEndpoint }

                    let restartBasin = restartClient |> S2.basin basinName
                    let restartFold = rbMake restartBasin eqSource
                    let! restartState, restartVersion = Checkpoint.rebuild restartFold

                    let rebuildAcrossRestartEqualsFoldFromZero =
                        restartState = foldFromZeroState && restartVersion = foldFromZeroVersion

                    // A never-checkpointed source: rebuild is a fold-from-zero.
                    let plainName = "chk-plain-" + suffix
                    let plain = SubjectHistory.SubjectId plainName
                    let (SubjectHistory.SubjectId plainSidecar) = Checkpoint.checkpointSubject plain
                    do! basin |> S2.createStream plainName
                    do! basin |> S2.createStream plainSidecar

                    let! _ = SubjectHistory.append basin RbDelta.codec plain [ RbDelta 7; RbDelta 8; RbDelta 9 ]

                    let plainFold = rbMake basin plain
                    let! plainRebuildState, plainRebuildVersion = Checkpoint.rebuild plainFold
                    let! plainTail = SubjectHistory.tail basin plain

                    let! plainFoldState, plainFoldVersion =
                        SubjectHistory.foldTo
                            basin
                            RbDelta.codec
                            plain
                            (SubjectHistory.Seq 0L)
                            plainTail
                            RbCounter.initial
                            RbCounter.apply

                    let emptyRebuildIsFoldFromZero =
                        plainRebuildState = plainFoldState
                        && plainRebuildVersion = plainFoldVersion
                        && plainRebuildVersion = SubjectHistory.Version 3L

                    do! basin |> S2.deleteStream plainSidecar
                    do! basin |> S2.deleteStream plainName
                    do! basin |> S2.deleteStream eqSidecarName
                    do! basin |> S2.deleteStream eqName
                    do! basin |> S2.deleteStream trimSidecarName
                    do! basin |> S2.deleteStream trimName

                    return
                        { RebuildLaw.RebuildEqualsReference =
                            rebuildEqualsFoldFromZero && emptyRebuildIsFoldFromZero
                          RebuildLaw.AcrossRestartEquivalent = rebuildAcrossRestartEqualsFoldFromZero
                          RebuildLaw.Facts =
                            [ ctFactLatestReflects, latestReflectsLastCommit
                              ctFactResumeIsSuffix, resumeFromLatestIsSuffix
                              ctFactColdRebuild, rebuildEqualsFoldFromZero
                              ctFactRestartRebuild, rebuildAcrossRestartEqualsFoldFromZero
                              ctFactUncheckpointed, emptyRebuildIsFoldFromZero ] }
                }
          Poison = None }

    // =======================================================================
    // 2. session-history (from session.history-fold)
    // =======================================================================

    [<Literal>]
    let private shFactThreeTurns = "fold-from-zero materializes the three driven turns"

    [<Literal>]
    let private shFactDone = "a completed turn folds to Ended Done"

    [<Literal>]
    let private shFactCancelled = "a cancelled turn folds to Ended Cancelled"

    [<Literal>]
    let private shFactIdleTimeout = "an idle-timeout folds to Ended IdleTimeout, never collapsed to Cancelled"

    [<Literal>]
    let private shFactCheckpointNeutral = "checkpointing does not change the fold result"

    [<Literal>]
    let private shFactSuffixTurn = "rebuild = latest snapshot + suffix picks up the suffix turn"

    [<Literal>]
    let private shFactRestartEquivalent = "rebuild across a host restart is equivalent"

    let private sessionHistorySurface: RebuildLaw.RebuildSurface<_, _> =
        { Instance = "session-history"
          OperationName = "foundation.rebuild-equivalence.session-history"
          FactNames =
            [ shFactThreeTurns
              shFactDone
              shFactCancelled
              shFactIdleTimeout
              shFactCheckpointNeutral
              shFactSuffixTurn
              shFactRestartEquivalent ]
          HasRestartVariant = true
          HasTrimPolicy = false
          WriterOps =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx

                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "hist-fold-" + suffix
                    let session = Turn.SessionId("session-" + suffix)
                    let logSubject = SessionLifecycle.logSubject session
                    let (SubjectHistory.SubjectId logName) = logSubject
                    let (SubjectHistory.SubjectId sidecarName) = Checkpoint.checkpointSubject logSubject

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName
                    do! basin |> S2.createStream logName
                    do! basin |> S2.createStream sidecarName

                    let holder = Authority.HolderId "history-host"

                    let startOk turnId timeouts now =
                        async {
                            match! SessionLifecycle.start basin timeouts session turnId holder now with
                            | Ok live -> return live
                            | Error _ -> return failwith "session-history proof: start failed unexpectedly"
                        }

                    let completeOk (live: SessionLifecycle.LiveTurn) now =
                        async {
                            match! SessionLifecycle.complete live now with
                            | Ok() -> return ()
                            | Error _ -> return failwith "session-history proof: complete failed unexpectedly"
                        }

                    // Drive the real lifecycle log: turn-1 completes (Done),
                    // turn-2 is cancelled (Cancelled), turn-3 idle-times-out
                    // (IdleTimeout) — three distinct causes, one of which
                    // (`IdleTimeout`) collapses to `Cancelled` under
                    // `TurnTerminal`.
                    let t1 = Turn.TurnId "turn-1"
                    let t2 = Turn.TurnId "turn-2"
                    let t3 = Turn.TurnId "turn-3"

                    // turn-1 — Done.
                    let! live1 = startOk t1 SessionLifecycle.noTimeouts 1_000L
                    do! completeOk live1 1_100L

                    // turn-2 — Cancelled (mailbox send + the holder's drive).
                    let! live2 = startOk t2 SessionLifecycle.noTimeouts 2_000L
                    let! _ = SessionLifecycle.cancel basin session t2 "op" 1L
                    let! _ = SessionLifecycle.drive live2 WakeReason.MailboxReady 2_100L

                    // turn-3 — IdleTimeout (armed idle timer, fired via drive).
                    // Idle-timer id derived from B3's public wake vocabulary:
                    // B3 arms `idle:{turn}`, so a `drive TimerFired` at/after
                    // the deadline seals `IdleTimeout`.
                    let idleTimeouts: SessionLifecycle.Timeouts = { Idle = Some 100L; MaxDuration = None }
                    let! live3 = startOk t3 idleTimeouts 3_000L
                    let! idleDrive = SessionLifecycle.drive live3 (WakeReason.TimerFired(TimerId "idle:turn-3", 3_100L)) 3_100L

                    let idleSealedAsTimeout =
                        match idleDrive with
                        | Ok(SessionLifecycle.Progress.Ended SessionLifecycle.IdleTimeout) -> true
                        | _ -> false

                    let statusOf (history: SessionHistory.Turns.History) (turnKey: string) =
                        history.ByTurn.TryFind turnKey |> Option.map (fun entry -> entry.Status)

                    let projection = SessionHistory.Turns.make basin session

                    // Fold-from-zero (sidecar empty): the reference truth.
                    // Checkpoint's rebuild ignores the session log's fence
                    // command records, so a fenced subject folds cleanly.
                    let! fromZero, zeroVersion = SessionHistory.rebuild projection

                    let foldFromZeroHasThreeTurns = fromZero.ByTurn.Count = 3

                    let firstTurnEndedDone =
                        statusOf fromZero "turn-1" = Some(SessionHistory.Turns.Ended SessionLifecycle.Done)

                    let secondTurnEndedCancelled =
                        statusOf fromZero "turn-2" = Some(SessionHistory.Turns.Ended SessionLifecycle.Cancelled)

                    // THE distinction: turn-3 is Ended IdleTimeout, not
                    // collapsed to Cancelled.
                    let idleTimeoutNotCollapsed =
                        statusOf fromZero "turn-3" = Some(SessionHistory.Turns.Ended SessionLifecycle.IdleTimeout)
                        && idleSealedAsTimeout

                    return
                        (s2,
                         basin,
                         basinName,
                         session,
                         projection,
                         fromZero,
                         zeroVersion,
                         (foldFromZeroHasThreeTurns, firstTurnEndedDone, secondTurnEndedCancelled, idleTimeoutNotCollapsed))
                }
          Policy =
            fun _ctx world ->
                async {
                    let (_, basin, _, session, projection, fromZero, zeroVersion, _) = world

                    // Checkpoint, then a cold rebuild (fresh projection) with no
                    // suffix: identical to the fold-from-zero.
                    let! _ = SessionHistory.checkpoint projection
                    let! viaCheckpoint, ckVersion = SessionHistory.rebuild (SessionHistory.Turns.make basin session)

                    let checkpointDoesNotChangeResult =
                        viaCheckpoint = fromZero && ckVersion = zeroVersion

                    // Append a suffix turn (turn-4, Done) past the checkpoint —
                    // the writer keeps writing after the snapshot.
                    let holder = Authority.HolderId "history-host"

                    let! live4 =
                        async {
                            match!
                                SessionLifecycle.start basin SessionLifecycle.noTimeouts session (Turn.TurnId "turn-4") holder 4_000L
                            with
                            | Ok live -> return live
                            | Error _ -> return failwith "session-history proof: start failed unexpectedly"
                        }

                    match! SessionLifecycle.complete live4 4_100L with
                    | Ok() -> ()
                    | Error _ -> failwith "session-history proof: complete failed unexpectedly"

                    let report: RebuildLaw.PolicyReport = { PolicySafe = true; Facts = [] }
                    return (checkpointDoesNotChangeResult, report)
                }
          RebuildVsReference =
            fun _ctx world checkpointDoesNotChangeResult ->
                async {
                    let (s2, basin, basinName, session, _, _, _, writerFacts) = world

                    let (foldFromZeroHasThreeTurns, firstTurnEndedDone, secondTurnEndedCancelled, idleTimeoutNotCollapsed) =
                        writerFacts

                    // Cold rebuild = latest snapshot + suffix.
                    let! withSuffix, suffixVersion = SessionHistory.rebuild (SessionHistory.Turns.make basin session)
                    let rebuildWithSuffixHasFourTurns = withSuffix.ByTurn.Count = 4

                    // Across a host restart: a fresh client attaches to the same
                    // durable log + sidecar and rebuilds identical state.
                    let restartEndpoint =
                        match s2.Endpoint with
                        | Some endpoint -> endpoint
                        | None -> failwith "session-history requires an s2 endpoint (declare s2Lite)"

                    let restartClient =
                        S2.connectWith
                            { S2.ConnectOptions.create "s2-lite-history-restart" with
                                AccountEndpoint = Some restartEndpoint
                                BasinEndpoint = Some restartEndpoint }

                    let restartProjection = SessionHistory.Turns.make (restartClient |> S2.basin basinName) session
                    let! restartHistory, restartVersion = SessionHistory.rebuild restartProjection

                    let rebuildAcrossRestartEquivalent =
                        restartHistory = withSuffix && restartVersion = suffixVersion

                    return
                        { RebuildLaw.RebuildEqualsReference =
                            foldFromZeroHasThreeTurns
                            && firstTurnEndedDone
                            && secondTurnEndedCancelled
                            && idleTimeoutNotCollapsed
                            && checkpointDoesNotChangeResult
                            && rebuildWithSuffixHasFourTurns
                          RebuildLaw.AcrossRestartEquivalent = rebuildAcrossRestartEquivalent
                          RebuildLaw.Facts =
                            [ shFactThreeTurns, foldFromZeroHasThreeTurns
                              shFactDone, firstTurnEndedDone
                              shFactCancelled, secondTurnEndedCancelled
                              shFactIdleTimeout, idleTimeoutNotCollapsed
                              shFactCheckpointNeutral, checkpointDoesNotChangeResult
                              shFactSuffixTurn, rebuildWithSuffixHasFourTurns
                              shFactRestartEquivalent, rebuildAcrossRestartEquivalent ] }
                }
          Poison = None }

    // =======================================================================
    // 3. state-view (from foundation.state-view, incl. poisoned-decode)
    // =======================================================================

    type private SvRecord =
        | Add of amount: int
        | Mark of label: string

    type private SvState = { Total: int; Labels: string list }

    module private SvRecord =
        let encode record =
            match record with
            | Add amount -> "add|" + string amount
            | Mark label -> "mark|" + label

        let decode (body: string) =
            match body.Split('|') |> Array.toList with
            | [ "add"; amount ] ->
                match Int32.TryParse amount with
                | true, value -> Ok(Add value)
                | false, _ -> Error("bad add amount: " + amount)
            | [ "mark"; label ] -> Ok(Mark label)
            | _ -> Error("unknown record body: " + body)

        let codec: SubjectHistory.Codec<SvRecord> = { Encode = encode; Decode = decode }

        /// The known-bad negative-control codec: swallows undecodable records
        /// into a benign no-op instead of failing closed.
        let swallowingCodec: SubjectHistory.Codec<SvRecord> =
            { Encode = encode
              Decode =
                fun body ->
                    match decode body with
                    | Ok record -> Ok record
                    | Error _ -> Ok(Mark "swallowed") }

    module private SvRawRecord =
        let codec: SubjectHistory.Codec<string> = { Encode = id; Decode = Ok }

    module private SvState =
        let empty = { Total = 0; Labels = [] }

        let apply state (record: SubjectHistory.StoredRecord<SvRecord>) =
            match record.Body with
            | Add amount ->
                { state with
                    Total = state.Total + amount }
            | Mark label ->
                { state with
                    Labels = state.Labels @ [ label ] }

    [<Literal>]
    let private svFactSeededTail = "strong read catches up to seeded tail"

    [<Literal>]
    let private svFactSeededState = "strong read folds seeded state"

    [<Literal>]
    let private svFactEventualSnapshot = "eventual read returns local snapshot"

    [<Literal>]
    let private svFactFollowerCatchUp = "strong read catches follower append"

    [<Literal>]
    let private svFactFollowerState = "strong read returns follower state"

    [<Literal>]
    let private svFactStopCloses = "stop closes the StateView cursor"

    [<Literal>]
    let private svFactPumpErrorStrong = "pump error fails strong read"

    [<Literal>]
    let private svFactPumpErrorEventual = "pump error fails eventual read"

    [<Literal>]
    let private svFactStopAfterPumpError = "stop completes after pump error"

    /// `poisonSwallowed = false` is the law; `true` is the known-bad
    /// negative-control variant whose decoder silently swallows the poison —
    /// the core "poison fails closed" check must catch it.
    let private stateViewSurface (poisonSwallowed: bool) : RebuildLaw.RebuildSurface<_, _> =
        { Instance = "state-view"
          OperationName = "foundation.rebuild-equivalence.state-view"
          FactNames =
            [ svFactSeededTail
              svFactSeededState
              svFactEventualSnapshot
              svFactFollowerCatchUp
              svFactFollowerState
              svFactStopCloses
              svFactPumpErrorStrong
              svFactPumpErrorEventual
              svFactStopAfterPumpError ]
          HasRestartVariant = false
          HasTrimPolicy = false
          WriterOps =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx

                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "fnd-state-view-" + suffix
                    let subjectName = "subject-" + suffix
                    let failedSubjectName = "subject-failure-" + suffix
                    let subject = SubjectHistory.SubjectId subjectName
                    let failedSubject = SubjectHistory.SubjectId failedSubjectName

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName
                    do! basin |> S2.createStream subjectName
                    do! basin |> S2.createStream failedSubjectName

                    let! seeded = SubjectHistory.append basin SvRecord.codec subject [ Add 3; Mark "seeded" ]

                    return (basin, subject, subjectName, failedSubject, failedSubjectName, seeded)
                }
          Policy =
            fun _ctx _world ->
                async {
                    let report: RebuildLaw.PolicyReport = { PolicySafe = true; Facts = [] }
                    return ((), report)
                }
          RebuildVsReference =
            fun _ctx world _policy ->
                async {
                    let (basin, subject, _, _, _, seeded) = world

                    let! view =
                        StateView.start basin SvRecord.codec subject (SubjectHistory.Seq 0L) SvState.empty SvState.apply

                    let! seededStrong = StateView.read Strong view
                    let! eventual = StateView.read Eventual view

                    let! appended = SubjectHistory.append basin SvRecord.codec subject [ Add 4; Mark "after-start" ]

                    let! followerStrong = StateView.read Strong view

                    let! stopClosesCursor =
                        async {
                            try
                                do! StateView.stop view
                                return true
                            with _ ->
                                return false
                        }

                    let strongSeededTail = seededStrong.AppliedTail = seeded

                    let strongSeededState =
                        seededStrong.State.Total = 3 && seededStrong.State.Labels = [ "seeded" ]

                    let eventualSnapshot =
                        eventual.State.Total = 3
                        && eventual.State.Labels = [ "seeded" ]
                        && eventual.AppliedTail = SubjectHistory.Version 2L

                    let strongFollowerCatchUp = followerStrong.AppliedTail = appended

                    let strongFollowerState =
                        followerStrong.State.Total = 7
                        && followerStrong.State.Labels = [ "seeded"; "after-start" ]

                    return
                        { RebuildLaw.RebuildEqualsReference =
                            strongSeededTail
                            && strongSeededState
                            && eventualSnapshot
                            && strongFollowerCatchUp
                            && strongFollowerState
                          RebuildLaw.AcrossRestartEquivalent = true
                          RebuildLaw.Facts =
                            [ svFactSeededTail, strongSeededTail
                              svFactSeededState, strongSeededState
                              svFactEventualSnapshot, eventualSnapshot
                              svFactFollowerCatchUp, strongFollowerCatchUp
                              svFactFollowerState, strongFollowerState
                              svFactStopCloses, stopClosesCursor ] }
                }
          Poison =
            Some(fun _ctx world _policy ->
                async {
                    let (basin, _, subjectName, failedSubject, failedSubjectName, _) = world

                    let codec =
                        if poisonSwallowed then
                            SvRecord.swallowingCodec
                        else
                            SvRecord.codec

                    let! _ = SubjectHistory.append basin SvRawRecord.codec failedSubject [ "corrupt" ]

                    let! failedView =
                        StateView.start basin codec failedSubject (SubjectHistory.Seq 0L) SvState.empty SvState.apply

                    let! pumpErrorFailsStrongRead = StateView.read Strong failedView |> failsWith "decode failed"
                    let! pumpErrorFailsEventualRead = StateView.read Eventual failedView |> failsWith "decode failed"

                    let! stopAfterPumpError =
                        async {
                            try
                                do! StateView.stop failedView
                                return true
                            with _ ->
                                return false
                        }

                    do! basin |> S2.deleteStream failedSubjectName
                    do! basin |> S2.deleteStream subjectName

                    let report: RebuildLaw.PoisonReport =
                        { FailsClosed = pumpErrorFailsStrongRead && pumpErrorFailsEventualRead
                          Facts =
                            [ svFactPumpErrorStrong, pumpErrorFailsStrongRead
                              svFactPumpErrorEventual, pumpErrorFailsEventualRead
                              svFactStopAfterPumpError, stopAfterPumpError ] }

                    return report
                }) }

    // =======================================================================
    // 4. kv-store (from foundation.kv-store, incl. poisoned-apply)
    // =======================================================================

    type private TextKey(value: string, explode: bool) =
        member _.Value = value
        member _.Explode = explode

        interface IComparable with
            member this.CompareTo(other: obj) =
                match other with
                | :? TextKey as otherKey ->
                    if this.Explode || otherKey.Explode then
                        failwith "kv local apply comparison failed"
                    else
                        compare this.Value otherKey.Value
                | _ -> invalidArg "other" "expected TextKey"

        override this.Equals(other: obj) =
            match other with
            | :? TextKey as otherKey -> this.Value = otherKey.Value && this.Explode = otherKey.Explode
            | _ -> false

        override this.GetHashCode() = hash (this.Value, this.Explode)
        override this.ToString() = this.Value

    module private TextKeys =
        let normal value = TextKey(value, false)
        let explosive value = TextKey(value, true)

        let encode (key: TextKey) =
            if key.Explode then
                "explode:" + key.Value
            else
                "key:" + key.Value

        let decode (text: string) =
            if text.StartsWith("explode:", StringComparison.Ordinal) then
                Ok(explosive (text.Substring "explode:".Length))
            elif text.StartsWith("key:", StringComparison.Ordinal) then
                Ok(normal (text.Substring "key:".Length))
            else
                Error("bad key: " + text)

    module private KvCodec =
        let private field (value: string) = string value.Length + ":" + value

        let private readField (text: string) (index: int) =
            let colon = text.IndexOf(':', index)

            if colon < 0 then
                Error "missing field length separator"
            else
                let lengthText = text.Substring(index, colon - index)

                match Int32.TryParse lengthText with
                | false, _ -> Error("bad field length: " + lengthText)
                | true, length ->
                    let start = colon + 1
                    let finish = start + length

                    if finish > text.Length then
                        Error "field length exceeds record body"
                    else
                        Ok(text.Substring(start, length), finish)

        let encode event =
            match event with
            | Put(key, value) -> "put|" + field (TextKeys.encode key) + field (string value)
            | Delete key -> "delete|" + field (TextKeys.encode key)

        let decode (body: string) =
            if body.StartsWith("put|", StringComparison.Ordinal) then
                readField body 4
                |> Result.bind (fun (keyText, next) ->
                    readField body next
                    |> Result.bind (fun (valueText, finish) ->
                        if finish <> body.Length then
                            Error "trailing put data"
                        else
                            TextKeys.decode keyText
                            |> Result.bind (fun key ->
                                match Int32.TryParse valueText with
                                | true, value -> Ok(Put(key, value))
                                | false, _ -> Error("bad value: " + valueText))))
            elif body.StartsWith("delete|", StringComparison.Ordinal) then
                readField body 7
                |> Result.bind (fun (keyText, finish) ->
                    if finish <> body.Length then
                        Error "trailing delete data"
                    else
                        TextKeys.decode keyText |> Result.map Delete)
            else
                Error("unknown kv event body: " + body)

        let codec: SubjectHistory.Codec<KvEvent<TextKey, int>> = { Encode = encode; Decode = decode }

    [<Literal>]
    let private kvFactPutAck = "put returns durable append version"

    [<Literal>]
    let private kvFactStrongPut = "strong get observes put"

    [<Literal>]
    let private kvFactEventualPut = "eventual get observes put"

    [<Literal>]
    let private kvFactDeleteAck = "delete returns durable append version"

    [<Literal>]
    let private kvFactStrongDelete = "strong get observes delete"

    [<Literal>]
    let private kvFactFollowerCatchUp = "strong get catches up to another writer"

    [<Literal>]
    let private kvFactStableApplied = "precondition stable key is applied"

    [<Literal>]
    let private kvFactAckWindow = "put returns durable version before local apply succeeds"

    [<Literal>]
    let private kvFactStrongFails = "strong read fails after local apply failure"

    [<Literal>]
    let private kvFactEventualFails = "eventual read fails after local apply failure"

    let private kvStoreSurface: RebuildLaw.RebuildSurface<_, _> =
        { Instance = "kv-store"
          OperationName = "foundation.rebuild-equivalence.kv-store"
          FactNames =
            [ kvFactPutAck
              kvFactStrongPut
              kvFactEventualPut
              kvFactDeleteAck
              kvFactStrongDelete
              kvFactFollowerCatchUp
              kvFactStableApplied
              kvFactAckWindow
              kvFactStrongFails
              kvFactEventualFails ]
          HasRestartVariant = false
          HasTrimPolicy = false
          WriterOps =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx

                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "fnd-kv-store-" + suffix
                    let subjectName = "subject-" + suffix
                    let subject = SubjectHistory.SubjectId subjectName

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName
                    do! basin |> S2.createStream subjectName

                    let! store = KvStore.start basin KvCodec.codec subject (SubjectHistory.Seq 0L)
                    return (basin, subject, subjectName, store)
                }
          Policy =
            fun _ctx _world ->
                async {
                    let report: RebuildLaw.PolicyReport = { PolicySafe = true; Facts = [] }
                    return ((), report)
                }
          RebuildVsReference =
            fun _ctx world _policy ->
                async {
                    let (basin, subject, _, store) = world
                    let key name = TextKeys.normal name

                    let! putVersion = KvStore.put (key "alpha") 1 store
                    let! strongPutVersion, strongPutValue = KvStore.get Strong (key "alpha") store
                    let! eventualPutVersion, eventualPutValue = KvStore.get Eventual (key "alpha") store

                    let! deleteVersion = KvStore.delete (key "alpha") store
                    let! strongDeleteVersion, strongDeleteValue = KvStore.get Strong (key "alpha") store

                    let! externalVersion = SubjectHistory.append basin KvCodec.codec subject [ Put(key "beta", 2) ]
                    let! externalReadVersion, externalValue = KvStore.get Strong (key "beta") store

                    let putAck = putVersion = SubjectHistory.Version 1L
                    let strongGetAfterPut = strongPutVersion = putVersion && strongPutValue = Some 1
                    let eventualGetAfterPut = eventualPutVersion = putVersion && eventualPutValue = Some 1
                    let deleteAck = deleteVersion = SubjectHistory.Version 2L
                    let strongGetAfterDelete = strongDeleteVersion = deleteVersion && strongDeleteValue = None
                    let strongFollowerCatchUp = externalReadVersion = externalVersion && externalValue = Some 2

                    return
                        { RebuildLaw.RebuildEqualsReference =
                            putAck
                            && strongGetAfterPut
                            && eventualGetAfterPut
                            && deleteAck
                            && strongGetAfterDelete
                            && strongFollowerCatchUp
                          RebuildLaw.AcrossRestartEquivalent = true
                          RebuildLaw.Facts =
                            [ kvFactPutAck, putAck
                              kvFactStrongPut, strongGetAfterPut
                              kvFactEventualPut, eventualGetAfterPut
                              kvFactDeleteAck, deleteAck
                              kvFactStrongDelete, strongGetAfterDelete
                              kvFactFollowerCatchUp, strongFollowerCatchUp ] }
                }
          Poison =
            Some(fun _ctx world _policy ->
                async {
                    let (basin, _, subjectName, store) = world
                    let key name = TextKeys.normal name

                    let! normalVersion = KvStore.put (key "stable") 10 store
                    let! stableVersion, stableValue = KvStore.get Strong (key "stable") store

                    let! failingVersion = KvStore.put (TextKeys.explosive "bad") 99 store

                    let! strongFailure =
                        KvStore.get Strong (key "stable") store |> failsWith "kv local apply comparison failed"

                    let! eventualFailure =
                        KvStore.get Eventual (key "stable") store |> failsWith "kv local apply comparison failed"

                    do! KvStore.stop store
                    do! basin |> S2.deleteStream subjectName

                    let stableKeyAppliedBeforeFailure = stableVersion = normalVersion && stableValue = Some 10

                    let putReturnsBeforeLocalApply =
                        failingVersion = SubjectHistory.Version(SubjectHistory.versionNumber normalVersion + 1L)

                    let report: RebuildLaw.PoisonReport =
                        { FailsClosed = strongFailure && eventualFailure
                          Facts =
                            [ kvFactStableApplied, stableKeyAppliedBeforeFailure
                              kvFactAckWindow, putReturnsBeforeLocalApply
                              kvFactStrongFails, strongFailure
                              kvFactEventualFails, eventualFailure ] }

                    return report
                }) }

    // ---- properties + proof ------------------------------------------------

    /// Negative control (one per template, on this instantiation): the
    /// known-bad variant's decoder silently swallows the poison record
    /// instead of failing closed — the template's core checks must catch it
    /// for the expected reason (report.json shows failed-as-expected).
    let private poisonSwallowedControl: NegativeControlSpec<RebuildLaw.RebuildEvidence> =
        { Name = "poison-swallowing decoder: the poisoned decode is silently absorbed"
          Workload = Some(RebuildLaw.workload (stateViewSurface true))
          Verifiers = RebuildLaw.coreChecks false false true
          ExpectedFailure = Some "rebuild law: decode/apply poison fails closed permanently" }

    let private checkpointTrimProperty = RebuildLaw.makeProperty checkpointTrimSurface
    let private sessionHistoryProperty = RebuildLaw.makeProperty sessionHistorySurface

    let private stateViewProperty =
        RebuildLaw.makePropertyWith [ poisonSwallowedControl ] true (stateViewSurface false)

    let private kvStoreProperty = RebuildLaw.makeProperty kvStoreSurface

    let proof =
        proof "foundation.rebuild-equivalence" {
            describedAs
                "The fold/rebuild equivalence invariant stated once (RebuildLaw) and instantiated over four foundation surfaces: checkpoint-trim (rebuild = fold-from-zero incl. never-checkpointed + across-restart; trim never crosses a committed checkpoint, floor-rebuild equivalent), session-history (EndCause-lossless L2 turn index), state-view (fold + read grades + poisoned-decode fails closed), and kv-store (durable-commit-before-local-apply + poisoned-apply fails closed). Trace-op evidence per instantiation."

            property checkpointTrimProperty
            property sessionHistoryProperty
            property stateViewProperty
            property kvStoreProperty
        }
