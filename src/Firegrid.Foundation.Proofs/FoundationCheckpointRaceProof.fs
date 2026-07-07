namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation

/// A2 — `state.checkpoint-race`.
///
/// Proves the MS-C1 checkpoint-race + monotonic-snapshots laws through the
/// public `Checkpoint` surface. `Checkpoint.commit` routes its election through
/// `Authority.admit` (the I5 Open / bare-authority regime), so two racing
/// checkpointers at the same observed sidecar tail resolve to exactly one
/// committed snapshot; the loser's snapshot is rejected (`Raced` or, if the
/// winner already advanced `latest`, `Regressed`), never interleaved. A slow
/// checkpointer with stale state is rejected `Regressed`.
module FoundationCheckpointRaceProof =
    type Delta = Delta of int
    type Counter = { Total: int; Applied: int }

    module private Delta =
        let encode (Delta value) = "delta|" + string value

        let decode (body: string) =
            match body.Split('|') |> Array.toList with
            | [ "delta"; value ] ->
                match System.Int32.TryParse value with
                | true, parsed -> Ok(Delta parsed)
                | false, _ -> Error("bad delta: " + value)
            | _ -> Error("unknown record body: " + body)

        let codec: SubjectHistory.Codec<Delta> = { Encode = encode; Decode = decode }

    module private Counter =
        let initial = { Total = 0; Applied = 0 }

        let apply (state: Counter) (record: SubjectHistory.StoredRecord<Delta>) =
            let (Delta value) = record.Body

            { Total = state.Total + value
              Applied = state.Applied + 1 }

        let codec: Checkpoint.StateCodec<Counter> =
            { Encode = fun state -> sprintf "%d,%d" state.Total state.Applied
              Decode =
                fun body ->
                    match body.Split(',') |> Array.toList with
                    | [ total; applied ] ->
                        match System.Int32.TryParse total, System.Int32.TryParse applied with
                        | (true, total), (true, applied) -> Ok { Total = total; Applied = applied }
                        | _ -> Error("bad counter state: " + body)
                    | _ -> Error("bad counter state: " + body) }

    type CheckpointRaceProofResult =
        { ExactlyOneWon: bool
          ExactlyOneSnapshotCommitted: bool
          LoserRejectedNotInterleaved: bool
          ConcurrentLoserIsRaced: bool
          LatestReflectsWinner: bool
          ConcurrentLoserMode: string
          StaleCommitRegressed: bool
          RegressedCarriesRequestedAndLatest: bool }

    let private make basin source =
        Checkpoint.make basin Delta.codec Counter.codec source Counter.initial Counter.apply

    let private expectCheckpoint label result =
        match result with
        | Ok snapshot -> snapshot
        | Error _ -> failwithf "checkpoint %s failed unexpectedly (single-writer setup)" label

    let private classify result =
        match result with
        | Ok version -> sprintf "ok:%d" (SubjectHistory.versionNumber version)
        | Error (Checkpoint.CommitFailure.Raced _) -> "raced"
        | Error (Checkpoint.CommitFailure.Regressed _) -> "regressed"
        | Error (Checkpoint.CommitFailure.Failed _) -> "failed"

    let private isOk result =
        match result with
        | Ok _ -> true
        | Error _ -> false

    let private isRejection result =
        match result with
        | Error (Checkpoint.CommitFailure.Raced _)
        | Error (Checkpoint.CommitFailure.Regressed _) -> true
        | _ -> false

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "foundation.checkpoint_race"
            "foundation-checkpoint-race"
            { ProofOperationOptions.empty with
                Key = Some "foundation-checkpoint-race" }
            (async {
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

                // Two checkpointers (two "processes") over the same source + sidecar.
                let checkpointerA = make basin source
                let checkpointerB = make basin source

                // Baseline snapshot at AsOf=5 (sidecar tail -> 1): both racers then
                // share an identical read structure before the CAS.
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 1; Delta 2; Delta 3; Delta 4; Delta 5 ]
                let! baseline = Checkpoint.checkpoint checkpointerA
                let baselineSnap = expectCheckpoint "baseline" baseline

                // More source records; both racers rebuild to the same tail (AsOf=8).
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 10; Delta 20; Delta 30 ]
                let! stateA, asOfA = Checkpoint.rebuild checkpointerA
                let! stateB, asOfB = Checkpoint.rebuild checkpointerB
                let snapA: Checkpoint.Snapshot<Counter> = { AsOf = asOfA; State = stateA }
                let snapB: Checkpoint.Snapshot<Counter> = { AsOf = asOfB; State = stateB }

                let! sidecarTailBefore = SubjectHistory.tail basin sidecar

                // RACE: both commit at the same observed sidecar tail.
                let! results =
                    Async.Parallel [ Checkpoint.commit checkpointerA snapA
                                     Checkpoint.commit checkpointerB snapB ]

                let resultA = results.[0]
                let resultB = results.[1]

                let wins = [ resultA; resultB ] |> List.filter isOk |> List.length
                let exactlyOneWon = wins = 1

                let loserRejectedNotInterleaved =
                    [ resultA; resultB ] |> List.filter (isOk >> not) |> List.forall isRejection

                let concurrentLoserMode =
                    [ resultA; resultB ]
                    |> List.filter (isOk >> not)
                    |> List.map classify
                    |> String.concat "+"

                // Both racers observe the same sidecar tail before either's CAS
                // lands, so the loser is `Raced` (Authority.admit's open-CAS Lost),
                // not merely monotonic-rejected.
                let concurrentLoserIsRaced = concurrentLoserMode = "raced"

                // Exactly one snapshot landed on the sidecar (not interleaved).
                let! sidecarTailAfter = SubjectHistory.tail basin sidecar

                let committedCount =
                    SubjectHistory.versionNumber sidecarTailAfter
                    - SubjectHistory.versionNumber sidecarTailBefore

                let exactlyOneSnapshotCommitted = committedCount = 1L

                // `latest` reflects the single winner (AsOf=8, past the baseline 5).
                let! latestAfter = Checkpoint.latest checkpointerA

                let latestReflectsWinner =
                    match latestAfter with
                    | Some snap -> (snap.AsOf = SubjectHistory.Version 8L) && (snap.State = { Total = 75; Applied = 8 })
                    | None -> false

                // Monotonic snapshots: a slow checkpointer with stale state
                // (AsOf <= latest) is rejected `Regressed` — deterministically.
                let staleSnap: Checkpoint.Snapshot<Counter> =
                    { AsOf = baselineSnap.AsOf; State = baselineSnap.State }

                let! stale = Checkpoint.commit checkpointerA staleSnap
                let staleCommitRegressed = classify stale = "regressed"

                let regressedCarriesRequestedAndLatest =
                    match stale with
                    | Error (Checkpoint.CommitFailure.Regressed(requested, latest)) ->
                        requested = SubjectHistory.Version 5L && latest = SubjectHistory.Version 8L
                    | _ -> false

                do! basin |> S2.deleteStream sidecarName
                do! basin |> S2.deleteStream sourceName

                let result =
                    { ExactlyOneWon = exactlyOneWon
                      ExactlyOneSnapshotCommitted = exactlyOneSnapshotCommitted
                      LoserRejectedNotInterleaved = loserRejectedNotInterleaved
                      ConcurrentLoserIsRaced = concurrentLoserIsRaced
                      LatestReflectsWinner = latestReflectsWinner
                      ConcurrentLoserMode = concurrentLoserMode
                      StaleCommitRegressed = staleCommitRegressed
                      RegressedCarriesRequestedAndLatest = regressedCarriesRequestedAndLatest }

                do!
                    ctx.EmitSpan
                        "proof.foundation.checkpoint_race.completed"
                        [ "proof.property", "state.checkpoint-race"
                          "race.exactly_one_won", string result.ExactlyOneWon
                          "race.exactly_one_committed", string result.ExactlyOneSnapshotCommitted
                          "race.loser_mode", result.ConcurrentLoserMode
                          "race.stale_regressed", string result.StaleCommitRegressed ]

                return result
            })

    let checkpointRaceProperty =
        property "state.checkpoint-race" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "two racing commits: exactly one wins" (fun result -> result.ExactlyOneWon)
                  v.Expect.Workload "exactly one snapshot lands on the sidecar (not interleaved)" (fun result ->
                      result.ExactlyOneSnapshotCommitted)
                  v.Expect.Workload "the loser's snapshot is rejected, never interleaved" (fun result ->
                      result.LoserRejectedNotInterleaved)
                  v.Expect.Workload "the racing loser observes Raced (open-CAS Lost via Authority.admit)" (fun result ->
                      result.ConcurrentLoserIsRaced)
                  v.Expect.Workload "latest reflects the single winner" (fun result -> result.LatestReflectsWinner)
                  v.Expect.Workload "a stale-state commit is rejected Regressed" (fun result ->
                      result.StaleCommitRegressed)
                  v.Expect.Workload "Regressed carries the requested and latest AsOf" (fun result ->
                      result.RegressedCarriesRequestedAndLatest)
                  v.Trace.SpanExists
                      "checkpoint-race proof span emitted"
                      "proof.foundation.checkpoint_race.completed"
                      [ "proof.property", "state.checkpoint-race" ]
                  v.Trace.Operation
                      "checkpoint-race operation was recorded"
                      ({ TraceOperationMatch.named "foundation.checkpoint_race" with
                          Status = Some "ok"
                          OutputContains =
                              [ "ExactlyOneWon"
                                "ExactlyOneSnapshotCommitted"
                                "StaleCommitRegressed" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "state.checkpoint-race" {
            describedAs "Two racing checkpointers commit exactly one snapshot; the loser is rejected, never interleaved; stale state is Regressed."
            property checkpointRaceProperty
        }
