namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store
open Firegrid.Store.Foundation.Durable

/// Packet 0.3b — `foundation.fencing`: the single-writer/fencing invariant,
/// stated ONCE (FencingLaw.fs) and instantiated over seven foundation
/// surfaces. Retires the seven bespoke restatements:
///
///   1. checkpoint-commit              <- state.checkpoint-race
///   2. turn-takeover                  <- session.turn-idempotent-create
///   3. turn-crash-terminal            <- session.turn-crash-terminal
///   4. lifecycle-single-writer        <- session.lifecycle-single-writer
///   5. lifecycle-deposed-producer     <- session.lifecycle-deposed-producer
///   6. resume-artifact                <- session.resume-artifact-fenced
///   7. wake-claim                     <- wake.single-claim
///
/// Consolidation deletes RESTATEMENTS, never ASSERTIONS: every check of every
/// retired proof maps to a FencingLaw core check or keeps its original name
/// as a named fact check (see the PR correspondence table). The scenario
/// bodies below are the retired proofs' workloads, recast into the template's
/// setup -> ownerAct -> supersede -> staleAttempt -> observe phases.
module FoundationFencingProof =

    // ---- shared helpers over the public DurableLog/Turn surface -----------
    // (from the retired turn-stream / session-lifecycle proofs)

    let private createOk
        (basin: S2.Basin)
        (address: DurableLog.Address)
        (holder: Authority.HolderId)
        : Async<DurableLog.Producer<TurnChunk, TurnTerminal>> =
        async {
            match! DurableLog.create basin Turn.codec address holder with
            | Ok producer -> return producer
            | Error _ -> return failwith "durable-log: create failed unexpectedly (single-writer workload)"
        }

    let private appendOk (producer: DurableLog.Producer<TurnChunk, TurnTerminal>) (chunk: TurnChunk) : Async<unit> =
        async {
            match! DurableLog.append producer chunk with
            | Ok() -> return ()
            | Error _ -> return failwith "durable-log: append by the live holder failed unexpectedly"
        }

    let private sealOk (producer: DurableLog.Producer<TurnChunk, TurnTerminal>) (terminal: TurnTerminal) : Async<unit> =
        async {
            match! DurableLog.seal producer terminal with
            | Ok() -> return ()
            | Error _ -> return failwith "durable-log: seal by the live holder failed unexpectedly"
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

    /// Replay-from-zero + live-tail + terminal. Blocks-with-wait on `next`;
    /// each scenario drains only after the log is sealed, so it terminates at
    /// the terminal (a hung reader would never return and fail the trial).
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

    let private secondClient (s2: S2Resource) (name: string) : S2.Client =
        let endpoint =
            match s2.Endpoint with
            | Some value -> value
            | None -> failwith "this fencing instantiation requires an s2 endpoint (declare s2Lite)"

        S2.connectWith
            { S2.ConnectOptions.create name with
                AccountEndpoint = Some endpoint
                BasinEndpoint = Some endpoint }

    // =======================================================================
    // 1. checkpoint-commit (from state.checkpoint-race)
    // =======================================================================

    type private CkDelta = CkDelta of int
    type private CkCounter = { Total: int; Applied: int }

    module private CkDelta =
        let encode (CkDelta value) = "delta|" + string value

        let decode (body: string) =
            match body.Split('|') |> Array.toList with
            | [ "delta"; value ] ->
                match System.Int32.TryParse value with
                | true, parsed -> Ok(CkDelta parsed)
                | false, _ -> Error("bad delta: " + value)
            | _ -> Error("unknown record body: " + body)

        let codec: SubjectHistory.Codec<CkDelta> = { Encode = encode; Decode = decode }

    module private CkCounter =
        let initial = { Total = 0; Applied = 0 }

        let apply (state: CkCounter) (record: SubjectHistory.StoredRecord<CkDelta>) =
            let (CkDelta value) = record.Body

            { Total = state.Total + value
              Applied = state.Applied + 1 }

        let codec: Checkpoint.StateCodec<CkCounter> =
            { Encode = fun state -> sprintf "%d,%d" state.Total state.Applied
              Decode =
                fun body ->
                    match body.Split(',') |> Array.toList with
                    | [ total; applied ] ->
                        match System.Int32.TryParse total, System.Int32.TryParse applied with
                        | (true, total), (true, applied) -> Ok { Total = total; Applied = applied }
                        | _ -> Error("bad counter state: " + body)
                    | _ -> Error("bad counter state: " + body) }

    let private ckMake basin source =
        Checkpoint.make basin CkDelta.codec CkCounter.codec source CkCounter.initial CkCounter.apply

    let private expectCheckpoint label result =
        match result with
        | Ok snapshot -> snapshot
        | Error _ -> failwithf "checkpoint %s failed unexpectedly (single-writer setup)" label

    let private ckIsOk result =
        match result with
        | Ok _ -> true
        | Error _ -> false

    let private ckIsRejection result =
        match result with
        | Error(Checkpoint.CommitFailure.Raced _)
        | Error(Checkpoint.CommitFailure.Regressed _) -> true
        | _ -> false

    let private ckClassify result =
        match result with
        | Ok version -> sprintf "ok:%d" (SubjectHistory.versionNumber version)
        | Error(Checkpoint.CommitFailure.Raced _) -> "raced"
        | Error(Checkpoint.CommitFailure.Regressed _) -> "regressed"
        | Error(Checkpoint.CommitFailure.Failed _) -> "failed"

    [<Literal>]
    let private ckFactOneSnapshot = "exactly one snapshot lands on the sidecar (not interleaved)"

    [<Literal>]
    let private ckFactLoserRejected = "the loser's snapshot is rejected, never interleaved"

    [<Literal>]
    let private ckFactLoserRaced = "the racing loser observes Raced (open-CAS Lost via Authority.admit)"

    [<Literal>]
    let private ckFactRegressedCarries = "Regressed carries the requested and latest AsOf"

    let private checkpointCommitSurface: FencingLaw.FencingSurface<_, _, _> =
        { Instance = "checkpoint-commit"
          OperationName = "foundation.fencing.checkpoint-commit"
          ExpectedFence = "Regressed"
          FactNames = [ ckFactOneSnapshot; ckFactLoserRejected; ckFactLoserRaced; ckFactRegressedCarries ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx

                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "race-" + suffix
                    let sourceName = "race-src-" + suffix
                    let source = SubjectHistory.SubjectId sourceName
                    let sidecar = Checkpoint.checkpointSubject source
                    let (SubjectHistory.SubjectId sidecarName) = sidecar

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName
                    do! basin |> S2.createStream sourceName
                    do! basin |> S2.createStream sidecarName

                    // Two checkpointers (two "processes") over one source + sidecar.
                    let checkpointerA = ckMake basin source
                    let checkpointerB = ckMake basin source

                    // Baseline snapshot at AsOf=5 (sidecar tail -> 1): both racers
                    // then share an identical read structure before the CAS.
                    let! _ =
                        SubjectHistory.append basin CkDelta.codec source [ CkDelta 1; CkDelta 2; CkDelta 3; CkDelta 4; CkDelta 5 ]

                    let! baseline = Checkpoint.checkpoint checkpointerA
                    let baselineSnap = expectCheckpoint "baseline" baseline

                    // More source records; both racers rebuild to the same tail (AsOf=8).
                    let! _ = SubjectHistory.append basin CkDelta.codec source [ CkDelta 10; CkDelta 20; CkDelta 30 ]
                    let! stateA, asOfA = Checkpoint.rebuild checkpointerA
                    let! stateB, asOfB = Checkpoint.rebuild checkpointerB
                    let snapA: Checkpoint.Snapshot<CkCounter> = { AsOf = asOfA; State = stateA }
                    let snapB: Checkpoint.Snapshot<CkCounter> = { AsOf = asOfB; State = stateB }

                    let! sidecarTailBefore = SubjectHistory.tail basin sidecar

                    return
                        (basin, source, sidecar, sourceName, sidecarName, checkpointerA, checkpointerB, baselineSnap, snapA, snapB, sidecarTailBefore)
                }
          OwnerAct =
            fun _ctx (_, _, _, _, _, checkpointerA, checkpointerB, _, snapA, snapB, _) ->
                async {
                    // RACE: both commit at the same observed sidecar tail. The CAS
                    // election is atomic — the winner's commit IS the supersession
                    // of the loser, so `Supersede` below only records the durable
                    // outcome of that election.
                    let! results =
                        Async.Parallel
                            [ Checkpoint.commit checkpointerA snapA
                              Checkpoint.commit checkpointerB snapB ]

                    return (results.[0], results.[1])
                }
          Supersede =
            fun _ctx (basin, _, sidecar, _, _, _, _, _, _, _, _) _owner ->
                async {
                    // The winner's snapshot is now the committed `latest`; capture
                    // the sidecar tail after the election for the observe phase.
                    let! sidecarTailAfterRace = SubjectHistory.tail basin sidecar
                    return sidecarTailAfterRace
                }
          StaleAttempt =
            fun _ctx (_, _, _, _, _, checkpointerA, _, baselineSnap, _, _, _) _owner _super ->
                async {
                    // Monotonic snapshots: a slow checkpointer with stale state
                    // (AsOf <= latest) is rejected `Regressed` — deterministically.
                    let staleSnap: Checkpoint.Snapshot<CkCounter> =
                        { AsOf = baselineSnap.AsOf; State = baselineSnap.State }

                    let! stale = Checkpoint.commit checkpointerA staleSnap

                    let regressedCarriesRequestedAndLatest =
                        match stale with
                        | Error(Checkpoint.CommitFailure.Regressed(requested, latest)) ->
                            requested = SubjectHistory.Version 5L && latest = SubjectHistory.Version 8L
                        | _ -> false

                    let outcome =
                        match stale with
                        | Error(Checkpoint.CommitFailure.Regressed _) -> FencingLaw.Fenced "Regressed"
                        | Error(Checkpoint.CommitFailure.Raced _) -> FencingLaw.Fenced "Raced"
                        | Error(Checkpoint.CommitFailure.Failed _) -> FencingLaw.Indeterminate "Failed"
                        | Ok version ->
                            FencingLaw.CommittedAnyway(sprintf "stale snapshot committed at %d" (SubjectHistory.versionNumber version))

                    return (outcome, [ ckFactRegressedCarries, regressedCarriesRequestedAndLatest ])
                }
          Observe =
            fun _ctx world (resultA, resultB) sidecarTailAfterRace _stale ->
                async {
                    let (basin, _, sidecar, sourceName, sidecarName, checkpointerA, _, _, _, _, sidecarTailBefore) =
                        world

                    let wins = [ resultA; resultB ] |> List.filter ckIsOk |> List.length

                    let loserRejectedNotInterleaved =
                        [ resultA; resultB ] |> List.filter (ckIsOk >> not) |> List.forall ckIsRejection

                    let concurrentLoserMode =
                        [ resultA; resultB ]
                        |> List.filter (ckIsOk >> not)
                        |> List.map ckClassify
                        |> String.concat "+"

                    // Both racers observed the same sidecar tail before either's
                    // CAS landed, so the loser is `Raced` (Authority.admit's
                    // open-CAS Lost), not merely monotonic-rejected.
                    let concurrentLoserIsRaced = concurrentLoserMode = "raced"

                    // Exactly one snapshot landed on the sidecar (not interleaved).
                    let committedCount =
                        SubjectHistory.versionNumber sidecarTailAfterRace
                        - SubjectHistory.versionNumber sidecarTailBefore

                    let exactlyOneSnapshotCommitted = committedCount = 1L

                    // The stale (Regressed) attempt appended nothing to the sidecar.
                    let! sidecarTailFinal = SubjectHistory.tail basin sidecar
                    let staleEffectVisible = sidecarTailFinal <> sidecarTailAfterRace

                    // `latest` reflects the single winner (AsOf=8, past baseline 5).
                    let! latestAfter = Checkpoint.latest checkpointerA

                    let latestReflectsWinner =
                        match latestAfter with
                        | Some snap ->
                            (snap.AsOf = SubjectHistory.Version 8L)
                            && (snap.State = { Total = 75; Applied = 8 })
                        | None -> false

                    do! basin |> S2.deleteStream sidecarName
                    do! basin |> S2.deleteStream sourceName

                    return
                        { FencingLaw.WinnerCommits = wins
                          FencingLaw.StaleEffectVisible = staleEffectVisible
                          FencingLaw.PostStateConsistent = latestReflectsWinner
                          FencingLaw.Facts =
                            [ ckFactOneSnapshot, exactlyOneSnapshotCommitted
                              ckFactLoserRejected, loserRejectedNotInterleaved
                              ckFactLoserRaced, concurrentLoserIsRaced ] }
                } }

    // =======================================================================
    // 2. turn-takeover (from session.turn-idempotent-create)
    // =======================================================================

    [<Literal>]
    let private ttFactRetryReattached = "a same-identity retry re-attaches to the live log"

    [<Literal>]
    let private ttFactRetryNotDeposed = "the same-identity retry does not depose the first producer"

    [<Literal>]
    let private ttFactTakeover = "a different identity takes over under a new epoch"

    [<Literal>]
    let private ttFactTerminalCompleted = "the takeover holder seals a Completed terminal"

    /// `superseded = true` is the law; `superseded = false` is the known-bad
    /// negative-control variant where the takeover never happens, so the
    /// "stale" writers are allowed to commit — the core checks must catch it.
    let private turnTakeoverSurface (superseded: bool) : FencingLaw.FencingSurface<_, _, _> =
        { Instance = "turn-takeover"
          OperationName = "foundation.fencing.turn-takeover"
          ExpectedFence = "Deposed"
          FactNames = [ ttFactRetryReattached; ttFactRetryNotDeposed; ttFactTakeover; ttFactTerminalCompleted ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "turn-create-" + suffix

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    let address = Turn.address (Turn.SessionId("sess-" + suffix)) (Turn.TurnId "turn-1")
                    return (basin, address)
                }
          OwnerAct =
            fun _ctx (basin, address) ->
                async {
                    // First create under holder-a, and one committed chunk.
                    let! producer1 = createOk basin address (Authority.HolderId "holder-a")
                    do! appendOk producer1 (TurnChunk.Text "from-p1")

                    // Same-identity retry: re-attaches to the live log (idempotent),
                    // never a second stream.
                    let! retryResult = DurableLog.create basin Turn.codec address (Authority.HolderId "holder-a")

                    let sameIdentityRetryReattached =
                        match retryResult with
                        | Ok _ -> true
                        | Error _ -> false

                    let producer2 =
                        match retryResult with
                        | Ok producer -> producer
                        | Error _ -> failwith "same-identity retry did not re-attach"

                    do! appendOk producer2 (TurnChunk.Text "from-p2")

                    // The retry did not depose the first producer (same epoch).
                    let! producer1StillLive = DurableLog.append producer1 (TurnChunk.Text "from-p1-again")
                    let sameIdentityNotDeposed = (producer1StillLive = Ok())

                    return
                        (producer1,
                         producer2,
                         [ ttFactRetryReattached, sameIdentityRetryReattached
                           ttFactRetryNotDeposed, sameIdentityNotDeposed ])
                }
          Supersede =
            fun _ctx (basin, address) _owner ->
                async {
                    if not superseded then
                        // Negative-control variant: the takeover never happens.
                        return (None, [ ttFactTakeover, false ])
                    else
                        // A different identity takes over under a new epoch.
                        let! takeoverResult = DurableLog.create basin Turn.codec address (Authority.HolderId "holder-b")

                        let differentIdentityTookOver =
                            match takeoverResult with
                            | Ok _ -> true
                            | Error _ -> false

                        let producer3 =
                            match takeoverResult with
                            | Ok producer -> Some producer
                            | Error _ -> failwith "different identity failed to take over"

                        return (producer3, [ ttFactTakeover, differentIdentityTookOver ])
                }
          StaleAttempt =
            fun _ctx _world (producer1, producer2, _) _super ->
                async {
                    // Both holder-a producers must now be deposed.
                    let! p1AfterTakeover = DurableLog.append producer1 (TurnChunk.Text "stale-1")
                    let! p2AfterTakeover = DurableLog.append producer2 (TurnChunk.Text "stale-2")

                    let outcome =
                        match p1AfterTakeover, p2AfterTakeover with
                        | Error DurableLog.AppendError.Deposed, Error DurableLog.AppendError.Deposed ->
                            FencingLaw.Fenced "Deposed"
                        | Ok(), _
                        | _, Ok() -> FencingLaw.CommittedAnyway "a prior producer appended after the takeover"
                        | _ -> FencingLaw.Indeterminate "prior producers failed, but not Deposed"

                    return (outcome, [])
                }
          Observe =
            fun _ctx (basin, address) (_, producer2, ownerFacts) (producer3, superFacts) _stale ->
                async {
                    // The live holder commits and seals the one stream. In the
                    // no-supersede variant the two holder-a producers are both
                    // still live writers — two "winners".
                    let! winnerCommits =
                        async {
                            match producer3 with
                            | Some producer ->
                                do! appendOk producer (TurnChunk.Text "from-p3")
                                do! sealOk producer TurnTerminal.Completed
                                return 1
                            | None ->
                                do! sealOk producer2 TurnTerminal.Completed
                                return 2
                        }

                    // One stream carries every committed chunk across both
                    // identities — no fork — and rejects the deposed producers'
                    // stale writes.
                    let! reader = attachOk basin address
                    let! chunks, terminal = drain reader
                    do! DurableLog.close reader

                    let expectedLog =
                        [ TurnChunk.Text "from-p1"
                          TurnChunk.Text "from-p2"
                          TurnChunk.Text "from-p1-again"
                          TurnChunk.Text "from-p3" ]

                    let staleWritesVisible =
                        chunks
                        |> List.exists (fun chunk -> chunk = TurnChunk.Text "stale-1" || chunk = TurnChunk.Text "stale-2")

                    return
                        { FencingLaw.WinnerCommits = winnerCommits
                          FencingLaw.StaleEffectVisible = staleWritesVisible
                          FencingLaw.PostStateConsistent = (chunks = expectedLog)
                          FencingLaw.Facts =
                            ownerFacts
                            @ superFacts
                            @ [ ttFactTerminalCompleted, (terminal = TurnTerminal.Completed) ] }
                } }

    // =======================================================================
    // 3. turn-crash-terminal (from session.turn-crash-terminal)
    // =======================================================================

    [<Literal>]
    let private tcFactRecoveryTookOver = "a recovery host takes over the crashed turn under a new epoch"

    [<Literal>]
    let private tcFactDurableTerminal = "recovery drives the turn to a durable terminal"

    [<Literal>]
    let private tcFactReaderTerminal = "the attached reader observes the terminal rather than hanging"

    let private turnCrashTerminalSurface: FencingLaw.FencingSurface<_, _, _> =
        { Instance = "turn-crash-terminal"
          OperationName = "foundation.fencing.turn-crash-terminal"
          ExpectedFence = "Deposed"
          FactNames = [ tcFactRecoveryTookOver; tcFactDurableTerminal; tcFactReaderTerminal ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "turn-crash-" + suffix

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    // Recovery host: a fresh S2 connection to the same durable
                    // store, modelling a separate process that takes over after
                    // the crash.
                    let recoveryClient = secondClient s2 "s2-lite-turn-crash-recovery"
                    let recoveryBasin = recoveryClient |> S2.basin basinName

                    let address = Turn.address (Turn.SessionId("sess-" + suffix)) (Turn.TurnId "turn-1")
                    return (basin, recoveryBasin, address)
                }
          OwnerAct =
            fun _ctx (basin, _, address) ->
                async {
                    // Producer A drives the turn mid-flight, without sealing.
                    let! producerA = createOk basin address (Authority.HolderId "producer-a")
                    do! appendOk producerA (TurnChunk.Text "a-chunk-0")
                    do! appendOk producerA (TurnChunk.Text "a-chunk-1")

                    // A reader attaches mid-turn, before the crash and recovery.
                    let! reader = attachOk basin address
                    return (producerA, reader)
                }
          Supersede =
            fun _ctx (_, recoveryBasin, address) _owner ->
                async {
                    // "kill -9": producer A is declared crashed. We keep its
                    // holder live to model a still-computing owner that will try
                    // to commit. Recovery process B claims the same turn (new
                    // epoch, takeover).
                    let! recoveryResult =
                        DurableLog.create recoveryBasin Turn.codec address (Authority.HolderId "recovery-b")

                    let recoveryHostTookOver =
                        match recoveryResult with
                        | Ok _ -> true
                        | Error _ -> false

                    let producerB =
                        match recoveryResult with
                        | Ok producer -> producer
                        | Error _ -> failwith "recovery host failed to take over the crashed turn"

                    return (producerB, [ tcFactRecoveryTookOver, recoveryHostTookOver ])
                }
          StaleAttempt =
            fun _ctx _world (producerA, _) _super ->
                async {
                    // The live deposed producer A computes but cannot commit.
                    let! staleResult = DurableLog.append producerA (TurnChunk.Text "a-stale-post-crash")

                    let outcome =
                        match staleResult with
                        | Error DurableLog.AppendError.Deposed -> FencingLaw.Fenced "Deposed"
                        | Ok() -> FencingLaw.CommittedAnyway "the deposed producer appended after the takeover"
                        | Error _ -> FencingLaw.Indeterminate "the deposed producer failed, but not Deposed"

                    return (outcome, [])
                }
          Observe =
            fun _ctx _world (_, reader) (producerB, superFacts) _stale ->
                async {
                    // Recovery drives the interrupted turn to a durable terminal.
                    do! appendOk producerB (TurnChunk.Text "b-recovery-note")
                    do! sealOk producerB TurnTerminal.Cancelled

                    // The attached reader observes the terminal rather than hanging.
                    let! chunks, terminal = drain reader
                    do! DurableLog.close reader

                    return
                        { FencingLaw.WinnerCommits = 1
                          FencingLaw.StaleEffectVisible =
                            chunks |> List.exists (fun chunk -> chunk = TurnChunk.Text "a-stale-post-crash")
                          FencingLaw.PostStateConsistent =
                            (chunks =
                                [ TurnChunk.Text "a-chunk-0"
                                  TurnChunk.Text "a-chunk-1"
                                  TurnChunk.Text "b-recovery-note" ])
                          FencingLaw.Facts =
                            superFacts
                            @ [ tcFactDurableTerminal, (terminal = TurnTerminal.Cancelled)
                                tcFactReaderTerminal, (terminal = TurnTerminal.Cancelled) ] }
                } }

    // =======================================================================
    // 4. lifecycle-single-writer (from session.lifecycle-single-writer)
    // =======================================================================

    [<Literal>]
    let private lswFactFirstHostLive = "host-a starts as the live producer"

    [<Literal>]
    let private lswFactTakeoverReattached = "a same-session start re-attaches the one turn stream under a new epoch"

    [<Literal>]
    let private lswFactAlreadyLive = "a start observing a different live turn is rejected AlreadyLive"

    [<Literal>]
    let private lswFactTerminalCompleted = "the one turn seals a single Completed terminal"

    let private lifecycleSingleWriterSurface: FencingLaw.FencingSurface<_, _, _> =
        { Instance = "lifecycle-single-writer"
          OperationName = "foundation.fencing.lifecycle-single-writer"
          ExpectedFence = "Deposed"
          FactNames = [ lswFactFirstHostLive; lswFactTakeoverReattached; lswFactAlreadyLive; lswFactTerminalCompleted ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "lifecycle-single-" + suffix

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    let session = Turn.SessionId("sess-" + suffix)
                    return (basin, session, Turn.TurnId "turn-1", Turn.TurnId "turn-2")
                }
          OwnerAct =
            fun _ctx (basin, session, turn1, _) ->
                async {
                    // Host A starts turn-1 and produces one chunk (the live producer).
                    let! startA =
                        SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-a") 1_000L

                    let liveA =
                        match startA with
                        | Ok live -> live
                        | Error _ -> failwith "host-a start failed unexpectedly"

                    let! aEarly = SessionLifecycle.append liveA (TurnChunk.Text "from-a")
                    return (liveA, [ lswFactFirstHostLive, (aEarly = Ok()) ])
                }
          Supersede =
            fun _ctx (basin, session, turn1, _) _owner ->
                async {
                    // Host B races to start the SAME turn-1: a same-turnId
                    // re-attach / takeover under a new epoch (recovery, never a
                    // fork).
                    let! startB =
                        SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-b") 1_000L

                    let liveB =
                        match startB with
                        | Ok live -> live
                        | Error _ -> failwith "host-b takeover failed unexpectedly"

                    let takeoverReattached =
                        match startB with
                        | Ok _ -> true
                        | Error _ -> false

                    return (liveB, [ lswFactTakeoverReattached, takeoverReattached ])
                }
          StaleAttempt =
            fun _ctx _world (liveA, _) _super ->
                async {
                    // The fence resolves the race: host-a is deposed and cannot
                    // commit.
                    let! aStale = SessionLifecycle.append liveA (TurnChunk.Text "from-a-stale")

                    let outcome =
                        match aStale with
                        | Error DurableLog.AppendError.Deposed -> FencingLaw.Fenced "Deposed"
                        | Ok() -> FencingLaw.CommittedAnyway "the fenced-out host appended after the takeover"
                        | Error _ -> FencingLaw.Indeterminate "the fenced-out host failed, but not Deposed"

                    return (outcome, [])
                }
          Observe =
            fun _ctx (basin, session, turn1, turn2) (_, ownerFacts) (liveB, superFacts) _stale ->
                async {
                    // Host B is the sole live producer.
                    let! bAppend = SessionLifecycle.append liveB (TurnChunk.Text "from-b")

                    // The AlreadyLive policy rejects a start observing a DIFFERENT
                    // live turn — evidence that exactly one turn (turn-1) is live.
                    let! startC =
                        SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn2 (Authority.HolderId "host-c") 2_000L

                    let differentTurnRejected =
                        match startC with
                        | Error(SessionLifecycle.StartError.AlreadyLive live) -> live = turn1
                        | _ -> false

                    // Recovery seals the one turn stream; a reader sees no fork.
                    let! _ = SessionLifecycle.complete liveB 3_000L
                    let! reader = attachOk basin (Turn.address session turn1)
                    let! chunks, terminal = drain reader
                    do! DurableLog.close reader

                    return
                        { FencingLaw.WinnerCommits = (if bAppend = Ok() then 1 else 0)
                          FencingLaw.StaleEffectVisible = chunks |> List.contains (TurnChunk.Text "from-a-stale")
                          FencingLaw.PostStateConsistent =
                            (chunks = [ TurnChunk.Text "from-a"; TurnChunk.Text "from-b" ])
                          FencingLaw.Facts =
                            ownerFacts
                            @ superFacts
                            @ [ lswFactAlreadyLive, differentTurnRejected
                                lswFactTerminalCompleted, (terminal = TurnTerminal.Completed) ] }
                } }

    // =======================================================================
    // 5. lifecycle-deposed-producer (from session.lifecycle-deposed-producer)
    // =======================================================================

    [<Literal>]
    let private ldpFactRecoveryTookOver = "a recovery host takes over the running session under a new epoch"

    [<Literal>]
    let private ldpFactCannotComplete = "the deposed producer cannot complete the turn after the takeover"

    [<Literal>]
    let private ldpFactDurableTerminal = "recovery drives the turn to a durable terminal"

    [<Literal>]
    let private ldpFactReaderTerminal = "the attached reader observes the terminal rather than hanging"

    let private lifecycleDeposedProducerSurface: FencingLaw.FencingSurface<_, _, _> =
        { Instance = "lifecycle-deposed-producer"
          OperationName = "foundation.fencing.lifecycle-deposed-producer"
          ExpectedFence = "Deposed"
          FactNames = [ ldpFactRecoveryTookOver; ldpFactCannotComplete; ldpFactDurableTerminal; ldpFactReaderTerminal ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "lifecycle-deposed-" + suffix

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    // Recovery host: a fresh S2 connection to the same durable
                    // store, modelling a separate process that takes over the
                    // running session.
                    let recoveryClient = secondClient s2 "s2-lite-lifecycle-recovery"
                    let recoveryBasin = recoveryClient |> S2.basin basinName

                    let session = Turn.SessionId("sess-" + suffix)
                    return (basin, recoveryBasin, session, Turn.TurnId "turn-1")
                }
          OwnerAct =
            fun _ctx (basin, _, session, turn1) ->
                async {
                    // Producer A drives turn-1 mid-flight, without sealing.
                    let! startA =
                        SessionLifecycle.start basin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-a") 1_000L

                    let liveA =
                        match startA with
                        | Ok live -> live
                        | Error _ -> failwith "session-lifecycle: start failed unexpectedly"

                    let! _ = SessionLifecycle.append liveA (TurnChunk.Text "a-0")
                    let! _ = SessionLifecycle.append liveA (TurnChunk.Text "a-1")

                    // A reader attaches mid-turn, before the takeover.
                    let! reader = attachOk basin (Turn.address session turn1)
                    return (liveA, reader)
                }
          Supersede =
            fun _ctx (_, recoveryBasin, session, turn1) _owner ->
                async {
                    // Recovery host B takes over the running session (same turn
                    // id, a new epoch across a separate connection). A is kept
                    // live to model a still-computing owner.
                    let! startB =
                        SessionLifecycle.start recoveryBasin SessionLifecycle.noTimeouts session turn1 (Authority.HolderId "host-b") 1_000L

                    let recoveryTookOver =
                        match startB with
                        | Ok _ -> true
                        | Error _ -> false

                    let liveB =
                        match startB with
                        | Ok live -> live
                        | Error _ -> failwith "recovery host failed to take over the running session"

                    return (liveB, [ ldpFactRecoveryTookOver, recoveryTookOver ])
                }
          StaleAttempt =
            fun _ctx _world (liveA, _) _super ->
                async {
                    // The live deposed producer A computes but cannot commit — its
                    // next emit and its completion both fail Deposed (D2's
                    // EmitError law).
                    let! aStale = SessionLifecycle.append liveA (TurnChunk.Text "a-stale-post-takeover")
                    let! aComplete = SessionLifecycle.complete liveA 2_000L

                    let outcome =
                        match aStale with
                        | Error DurableLog.AppendError.Deposed -> FencingLaw.Fenced "Deposed"
                        | Ok() -> FencingLaw.CommittedAnyway "the deposed producer appended after the takeover"
                        | Error _ -> FencingLaw.Indeterminate "the deposed producer failed, but not Deposed"

                    return (outcome, [ ldpFactCannotComplete, (aComplete = Error DurableLog.AppendError.Deposed) ])
                }
          Observe =
            fun _ctx _world (_, reader) (liveB, superFacts) _stale ->
                async {
                    // Recovery drives the interrupted turn to a durable terminal.
                    let! bAppend = SessionLifecycle.append liveB (TurnChunk.Text "b-recovery")
                    let! bComplete = SessionLifecycle.complete liveB 3_000L

                    // The attached reader observes the terminal rather than hanging.
                    let! chunks, terminal = drain reader
                    do! DurableLog.close reader

                    return
                        { FencingLaw.WinnerCommits = (if bAppend = Ok() && bComplete = Ok() then 1 else 0)
                          FencingLaw.StaleEffectVisible =
                            chunks |> List.contains (TurnChunk.Text "a-stale-post-takeover")
                          FencingLaw.PostStateConsistent =
                            (chunks = [ TurnChunk.Text "a-0"; TurnChunk.Text "a-1"; TurnChunk.Text "b-recovery" ])
                          FencingLaw.Facts =
                            superFacts
                            @ [ ldpFactDurableTerminal, (terminal = TurnTerminal.Completed)
                                ldpFactReaderTerminal, (terminal = TurnTerminal.Completed) ] }
                } }

    // =======================================================================
    // 6. resume-artifact (from session.resume-artifact-fenced)
    // =======================================================================

    [<Literal>]
    let private raFactWriterOpened = "host-a opens the fenced writer and stores its resume artifact"

    [<Literal>]
    let private raFactObserverRead = "an authority-free observer read returns the stored artifact"

    [<Literal>]
    let private raFactBareReadNoFence = "a bare read does not fence the writer — host-a stays live and can store again"

    [<Literal>]
    let private raFactClaimThenRead = "the resuming holder claims-then-reads, re-hydrating the last valid artifact"

    [<Literal>]
    let private raFactBareTakeover = "a bare takeover (claim, no store) does not shadow the current artifact"

    [<Literal>]
    let private raFactPayloadRoundTrips = "the artifact round-trips D2's harness/version/payload field-for-field"

    let private resumeArtifactSurface: FencingLaw.FencingSurface<_, _, _> =
        { Instance = "resume-artifact"
          OperationName = "foundation.fencing.resume-artifact"
          ExpectedFence = "Deposed"
          FactNames =
            [ raFactWriterOpened
              raFactObserverRead
              raFactBareReadNoFence
              raFactClaimThenRead
              raFactBareTakeover
              raFactPayloadRoundTrips ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "resume-artifact-" + suffix

                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    // Resuming host: a fresh S2 connection to the same durable
                    // store. The original writer host-a is kept live across
                    // `basin` to model a still-computing (stale) owner — the
                    // object-live-fencing lineage.
                    let resumeClient = secondClient s2 "s2-lite-resume-artifact-resume"
                    let resumeBasin = resumeClient |> S2.basin basinName

                    let session = Turn.SessionId("sess-" + suffix)
                    let turn1 = Turn.TurnId "turn-1"

                    let artifactA1 =
                        { Harness = "claude-agent-sdk"
                          Version = 1L
                          NativeType = Some "resume"
                          Payload = "claude-session-A1"
                          Turn = Some turn1 }

                    let artifactA2 = { artifactA1 with Payload = "claude-session-A2" }
                    let artifactStale = { artifactA1 with Payload = "claude-session-STALE-FORK" }

                    let artifactB =
                        { Harness = "claude-agent-sdk"
                          Version = 2L
                          NativeType = Some "resume"
                          Payload = "claude-session-B1"
                          Turn = Some turn1 }

                    return (basin, resumeBasin, session, artifactA1, artifactA2, artifactStale, artifactB)
                }
          OwnerAct =
            fun _ctx (basin, _, session, artifactA1, artifactA2, _, _) ->
                async {
                    // Host A becomes the live writer and stores its resume artifact.
                    let! writerA =
                        async {
                            match! ResumeArtifactStore.openWriter basin session (Authority.HolderId "host-a") with
                            | Ok writer -> return writer
                            | Error _ -> return failwith "resume-artifact: openWriter failed unexpectedly"
                        }

                    let! storeA1 = ResumeArtifactStore.store writerA artifactA1

                    // Observer/UI reads authority-free (no claim). This must NOT
                    // fence A.
                    let! obs1 =
                        async {
                            match! ResumeArtifactStore.read basin session with
                            | Ok artifact -> return artifact
                            | Error _ -> return failwith "resume-artifact: read failed unexpectedly"
                        }

                    // A bare read did not rotate the epoch: host A is still live
                    // and can store again. This is the "read-without-claim"
                    // branch — a read alone is not a fence.
                    let! storeA2 = ResumeArtifactStore.store writerA artifactA2

                    let! obs2 =
                        async {
                            match! ResumeArtifactStore.read basin session with
                            | Ok artifact -> return artifact
                            | Error _ -> return failwith "resume-artifact: read failed unexpectedly"
                        }

                    return
                        (writerA,
                         [ raFactWriterOpened, (storeA1 = Ok())
                           raFactObserverRead, (obs1 = Some artifactA1)
                           raFactBareReadNoFence, (storeA2 = Ok() && obs2 = Some artifactA2) ])
                }
          Supersede =
            fun _ctx (_, resumeBasin, session, _, artifactA2, _, _) _owner ->
                async {
                    // Resuming host B follows CLAIM-THEN-READ: `openWriter` FIRST
                    // — which rotates the register epoch and deposes A …
                    let! writerB =
                        async {
                            match! ResumeArtifactStore.openWriter resumeBasin session (Authority.HolderId "host-b") with
                            | Ok writer -> return writer
                            | Error _ -> return failwith "resume-artifact: openWriter failed unexpectedly"
                        }

                    // … THEN its re-hydration read (re-hydrates A's last valid
                    // artifact).
                    let! bRead =
                        async {
                            match! ResumeArtifactStore.read resumeBasin session with
                            | Ok artifact -> return artifact
                            | Error _ -> return failwith "resume-artifact: read failed unexpectedly"
                        }

                    return (writerB, [ raFactClaimThenRead, (bRead = Some artifactA2) ])
                }
          StaleAttempt =
            fun _ctx (_, _, _, _, _, artifactStale, _) (writerA, _) _super ->
                async {
                    // The interleaving the law pins: A is a still-live stale
                    // process; its LATE store lands AFTER B's claim-then-read. It
                    // is fenced `Deposed`, so it cannot fork the state B
                    // re-hydrated.
                    let! staleStore = ResumeArtifactStore.store writerA artifactStale

                    let outcome =
                        match staleStore with
                        | Error ResumeArtifactStore.StoreError.Deposed -> FencingLaw.Fenced "Deposed"
                        | Ok() -> FencingLaw.CommittedAnyway "the stale writer's late store landed after the takeover"
                        | Error _ -> FencingLaw.Indeterminate "the stale store failed, but not Deposed"

                    return (outcome, [])
                }
          Observe =
            fun _ctx world (_, ownerFacts) (writerB, superFacts) _stale ->
                async {
                    let (_, resumeBasin, session, _, artifactA2, _, artifactB) = world

                    let readOk () =
                        async {
                            match! ResumeArtifactStore.read resumeBasin session with
                            | Ok artifact -> return artifact
                            | Error _ -> return failwith "resume-artifact: read failed unexpectedly"
                        }

                    // The stale store cannot fork what the resuming holder
                    // re-hydrated.
                    let! afterStale = readOk ()

                    // last-store-under-fence-wins: B (the sole live holder)
                    // replaces the artifact.
                    let! storeB = ResumeArtifactStore.store writerB artifactB
                    let! bFinal = readOk ()

                    // A bare takeover (claim, no subsequent store) does not
                    // shadow the current artifact — the fence rotation is an S2
                    // command record that `read` skips, so the latest *artifact*
                    // still wins.
                    let! _writerC =
                        async {
                            match! ResumeArtifactStore.openWriter resumeBasin session (Authority.HolderId "host-c") with
                            | Ok writer -> return writer
                            | Error _ -> return failwith "resume-artifact: openWriter failed unexpectedly"
                        }

                    let! cRead = readOk ()

                    let payloadRoundTrips =
                        match bFinal with
                        | Some a ->
                            a.Harness = artifactB.Harness
                            && a.Version = artifactB.Version
                            && a.NativeType = artifactB.NativeType
                            && a.Payload = artifactB.Payload
                            && a.Turn = artifactB.Turn
                        | None -> false

                    return
                        { FencingLaw.WinnerCommits = (if storeB = Ok() && bFinal = Some artifactB then 1 else 0)
                          FencingLaw.StaleEffectVisible = (afterStale <> Some artifactA2)
                          FencingLaw.PostStateConsistent = (bFinal = Some artifactB)
                          FencingLaw.Facts =
                            ownerFacts
                            @ superFacts
                            @ [ raFactBareTakeover, (cRead = Some artifactB)
                                raFactPayloadRoundTrips, payloadRoundTrips ] }
                } }

    // =======================================================================
    // 7. wake-claim (from wake.single-claim)
    // =======================================================================

    [<Literal>]
    let private wcFactExactlyOneAdvanced = "exactly one router advances the cursor"

    [<Literal>]
    let private wcFactBothDrove = "both routers drove the same one wake (at-least-once dispatch)"

    [<Literal>]
    let private wcFactRedundantIdempotent = "the deposed holder's redundant re-drive is idempotent (drove once, no more)"

    [<Literal>]
    let private wcFactCursorOnce = "the durable cursor advanced exactly once"

    let private wakeClaimSurface: FencingLaw.FencingSurface<_, _, _> =
        { Instance = "wake-claim"
          OperationName = "foundation.fencing.wake-claim"
          ExpectedFence = "Deposed"
          FactNames = [ wcFactExactlyOneAdvanced; wcFactBothDrove; wcFactRedundantIdempotent; wcFactCursorOnce ]
          Setup =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "wake-claim-" + suffix
                    let! _ = s2.Client |> S2.createBasin basinName
                    let basinA = s2.Client |> S2.basin basinName

                    // A second client over the same durable store: the rival router.
                    let clientB = secondClient s2 "s2-lite-wake-single-claim-b"
                    let basinB = clientB |> S2.basin basinName

                    let config: WakeShard.ShardConfig = { Namespace = "claim-" + suffix; Count = 1 }
                    let subject: ActorAddress = { Segments = [ "sessions"; "claim-" + suffix ] }
                    let shard = WakeShard.shardOf config subject

                    // One wake both routers will see.
                    match! WakeShard.post basinA config subject WakeReason.MailboxReady with
                    | Error _ -> return failwith "wake-shard: post failed unexpectedly"
                    | Ok() -> return (basinA, basinB, config, subject, shard)
                }
          OwnerAct =
            fun _ctx (basinA, basinB, config, _, shard) ->
                async {
                    let aDispatched = ResizeArray<ActorAddress * WakeReason>()
                    let bDispatched = ResizeArray<ActorAddress * WakeReason>()
                    let mutable bResult: Result<WakeRouter.Cursor, WakeRouter.RouterError> option = None
                    let mutable bTriggered = false

                    let bDrive: WakeRouter.Drive =
                        fun s r ->
                            async {
                                bDispatched.Add(s, r)
                                return Ok()
                            }

                    // While router A drives the wake (before A commits its
                    // cursor), router B takes over: it claims (epoch+1), drains
                    // the same wake, drives it, and commits the cursor. A's own
                    // commit then fails `Deposed` — the mid-drive takeover IS the
                    // supersession, atomic with the owner's act, so `Supersede`
                    // below is a documented no-op.
                    let aDrive: WakeRouter.Drive =
                        fun s r ->
                            async {
                                aDispatched.Add(s, r)

                                if not bTriggered then
                                    bTriggered <- true
                                    let! b = WakeRouter.tick basinB config shard (Authority.HolderId "router-B") bDrive
                                    bResult <- Some b

                                return Ok()
                            }

                    let! aResult = WakeRouter.tick basinA config shard (Authority.HolderId "router-A") aDrive
                    return (aResult, bResult, List.ofSeq aDispatched, List.ofSeq bDispatched)
                }
          Supersede =
            // The rival router's claim happened mid-drive, inside OwnerAct —
            // the election is atomic with the owner's act.
            fun _ctx _world _owner -> async { return () }
          StaleAttempt =
            fun _ctx _world (aResult, _, _, _) _super ->
                async {
                    // A's fenced cursor commit is the stale attempt: it must fail
                    // typed `Deposed` after B's takeover.
                    let outcome =
                        match aResult with
                        | Error(WakeRouter.RouterError.Deposed _) -> FencingLaw.Fenced "Deposed"
                        | Ok cursor ->
                            FencingLaw.CommittedAnyway(
                                sprintf "the deposed router committed its cursor at %d" (SubjectHistory.seqNumber cursor.NextSeq)
                            )
                        | Error _ -> FencingLaw.Indeterminate "the deposed router failed, but not Deposed"

                    return (outcome, [])
                }
          Observe =
            fun _ctx (_, _, _, subject, _) (aResult, bResult, aDispatched, bDispatched) _super stale ->
                async {
                    let loserDeposed =
                        match stale with
                        | FencingLaw.Fenced "Deposed" -> true
                        | _ -> false

                    let winnerCursor =
                        match bResult with
                        | Some(Ok cursor) -> Some cursor
                        | _ -> None

                    let winnerAdvanced =
                        match winnerCursor with
                        | Some cursor -> SubjectHistory.seqNumber cursor.NextSeq = 1L
                        | None -> false

                    let staleAdvanced =
                        match aResult with
                        | Ok _ -> true
                        | Error _ -> false

                    return
                        { FencingLaw.WinnerCommits = (if winnerAdvanced then 1 else 0) + (if staleAdvanced then 1 else 0)
                          FencingLaw.StaleEffectVisible = staleAdvanced
                          // the durable authority advanced exactly once (winner to seq 1).
                          FencingLaw.PostStateConsistent = winnerAdvanced
                          FencingLaw.Facts =
                            [ // exactly one advanced the cursor: B ok, A deposed.
                              wcFactExactlyOneAdvanced, (winnerAdvanced && loserDeposed)
                              wcFactBothDrove,
                              (aDispatched.Length = 1
                               && bDispatched.Length = 1
                               && aDispatched.[0] = (subject, WakeReason.MailboxReady)
                               && bDispatched.[0] = (subject, WakeReason.MailboxReady))
                              // the redundant re-drive is idempotent: both drove once, no more.
                              wcFactRedundantIdempotent, (aDispatched.Length = 1 && bDispatched.Length = 1)
                              wcFactCursorOnce, winnerAdvanced ] }
                } }

    // ---- properties + proof ------------------------------------------------

    let private checkpointCommitProperty = FencingLaw.makeProperty checkpointCommitSurface
    let private turnTakeoverProperty = FencingLaw.makeProperty (turnTakeoverSurface true)
    let private turnCrashTerminalProperty = FencingLaw.makeProperty turnCrashTerminalSurface
    let private lifecycleSingleWriterProperty = FencingLaw.makeProperty lifecycleSingleWriterSurface
    let private lifecycleDeposedProducerProperty = FencingLaw.makeProperty lifecycleDeposedProducerSurface
    let private resumeArtifactProperty = FencingLaw.makeProperty resumeArtifactSurface
    let private wakeClaimProperty = FencingLaw.makeProperty wakeClaimSurface

    let proof =
        proof "foundation.fencing" {
            describedAs
                "The single-writer/fencing invariant stated once (FencingLaw) and instantiated over seven foundation surfaces: checkpoint-commit (CAS election + Regressed stale state), turn-takeover (idempotent create vs epoch takeover), turn-crash-terminal, lifecycle-single-writer (+ AlreadyLive), lifecycle-deposed-producer, resume-artifact (claim-then-read), and wake-claim (racing routers). Exactly one winner commits; the loser fails typed having committed nothing; the post-state is consistent; trace-op evidence per instantiation."

            property checkpointCommitProperty
            property turnTakeoverProperty
            property turnCrashTerminalProperty
            property lifecycleSingleWriterProperty
            property lifecycleDeposedProducerProperty
            property resumeArtifactProperty
            property wakeClaimProperty
        }
