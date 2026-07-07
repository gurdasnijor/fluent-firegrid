namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation

/// A1 — `state.checkpoint-rebuild-equivalence`.
///
/// Proves the MS-C1 rebuild-equivalence law: for any source log and any
/// sequence of committed snapshots, `Checkpoint.rebuild` yields the same
/// `(state, Version)` as `SubjectHistory.foldTo` from `Seq 0` to the source
/// tail — *including across a host restart*. A cold `Fold` with no resident
/// memory reconstructs identical state from `latest snapshot + suffix`.
module FoundationCheckpointProof =
    /// Domain-free counter fold: each source record is a signed delta.
    type Delta = Delta of int

    /// Folded state: the running total and the number of records applied. Both
    /// are recomputed by replay, so `rebuild` and `foldTo` must agree exactly.
    type Counter = { Total: int; Applied: int }

    module private Delta =
        let encode (Delta value) = "delta|" + string value

        let decode (body: string) =
            let parts = body.Split('|') |> Array.toList

            match parts with
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

        // State codec for the snapshot sidecar. Uses `,` so the `Checkpoint`
        // sidecar framing (which splits on the first `|`) recovers it verbatim.
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

    type CheckpointProofResult =
        { NoSnapshotResumesFromZero: bool
          FirstCheckpointAtSourceTail: bool
          SecondCheckpointAdvances: bool
          LatestReflectsLastCommit: bool
          ResumeFromLatestIsSuffix: bool
          RebuildEqualsFoldFromZero: bool
          RebuildAcrossRestartEqualsFoldFromZero: bool
          EmptyRebuildIsFoldFromZero: bool
          RebuildState: Counter
          FoldFromZeroState: Counter }

    let private expectCheckpoint label result =
        match result with
        | Ok snapshot -> snapshot
        | Error _ -> failwithf "checkpoint %s failed unexpectedly (single-writer workload)" label

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "foundation.checkpoint"
            "foundation-checkpoint"
            { ProofOperationOptions.empty with
                Key = Some "foundation-checkpoint" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "chk-" + suffix
                let sourceName = "chk-src-" + suffix
                let source = SubjectHistory.SubjectId sourceName
                let (SubjectHistory.SubjectId sidecarName) = Checkpoint.checkpointSubject source

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                // The consumer never names the sidecar; provisioning derives it.
                do! basin |> S2.createStream sourceName
                do! basin |> S2.createStream sidecarName

                let fold =
                    Checkpoint.make basin Delta.codec Counter.codec source Counter.initial Counter.apply

                // Pure sans-IO core: no snapshot resumes the fold from Seq 0.
                let noSnapshotResumesFromZero =
                    Checkpoint.resumeFrom None Counter.initial = (SubjectHistory.Seq 0L, Counter.initial)

                // Batch 1 -> source tail Version 5.
                let! _ =
                    SubjectHistory.append basin Delta.codec source [ Delta 1; Delta 2; Delta 3; Delta 4; Delta 5 ]

                let! firstCheckpoint = Checkpoint.checkpoint fold
                let snap1 = expectCheckpoint "first" firstCheckpoint

                let firstCheckpointAtSourceTail =
                    snap1.AsOf = SubjectHistory.Version 5L
                    && snap1.State = { Total = 15; Applied = 5 }

                // Batch 2 -> source tail Version 7.
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 10; Delta 20 ]

                let! secondCheckpoint = Checkpoint.checkpoint fold
                let snap2 = expectCheckpoint "second" secondCheckpoint

                let secondCheckpointAdvances =
                    snap2.AsOf = SubjectHistory.Version 7L
                    && snap2.State = { Total = 45; Applied = 7 }

                // Batch 3 (uncheckpointed suffix) -> source tail Version 8.
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 100 ]

                // Cold Fold: fresh value, no resident memory, same process.
                let coldFold =
                    Checkpoint.make basin Delta.codec Counter.codec source Counter.initial Counter.apply

                let! latestSnapshot = Checkpoint.latest coldFold
                let latestReflectsLastCommit = latestSnapshot = Some snap2

                // rebuild resumes from the latest checkpoint's AsOf, not Seq 0.
                let resumeFromLatestIsSuffix =
                    Checkpoint.resumeFrom latestSnapshot Counter.initial = (SubjectHistory.Seq 7L, snap2.State)

                let! rebuildState, rebuildVersion = Checkpoint.rebuild coldFold

                // Reference: full replay from Seq 0 to the source tail.
                let! sourceTail = SubjectHistory.tail basin source

                let! foldFromZeroState, foldFromZeroVersion =
                    SubjectHistory.foldTo
                        basin
                        Delta.codec
                        source
                        (SubjectHistory.Seq 0L)
                        sourceTail
                        Counter.initial
                        Counter.apply

                let rebuildEqualsFoldFromZero =
                    rebuildState = foldFromZeroState && rebuildVersion = foldFromZeroVersion

                // Host restart: a fresh client attaches to the same durable S2,
                // a fresh Fold rebuilds from latest snapshot + suffix.
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

                let restartFold =
                    Checkpoint.make restartBasin Delta.codec Counter.codec source Counter.initial Counter.apply

                let! restartState, restartVersion = Checkpoint.rebuild restartFold

                let rebuildAcrossRestartEqualsFoldFromZero =
                    restartState = foldFromZeroState && restartVersion = foldFromZeroVersion

                // A never-checkpointed source: rebuild is a fold-from-zero.
                let plainName = "chk-plain-" + suffix
                let plain = SubjectHistory.SubjectId plainName
                let (SubjectHistory.SubjectId plainSidecar) = Checkpoint.checkpointSubject plain
                do! basin |> S2.createStream plainName
                do! basin |> S2.createStream plainSidecar

                let! _ = SubjectHistory.append basin Delta.codec plain [ Delta 7; Delta 8; Delta 9 ]

                let plainFold =
                    Checkpoint.make basin Delta.codec Counter.codec plain Counter.initial Counter.apply

                let! plainRebuildState, plainRebuildVersion = Checkpoint.rebuild plainFold
                let! plainTail = SubjectHistory.tail basin plain

                let! plainFoldState, plainFoldVersion =
                    SubjectHistory.foldTo
                        basin
                        Delta.codec
                        plain
                        (SubjectHistory.Seq 0L)
                        plainTail
                        Counter.initial
                        Counter.apply

                let emptyRebuildIsFoldFromZero =
                    plainRebuildState = plainFoldState
                    && plainRebuildVersion = plainFoldVersion
                    && plainRebuildVersion = SubjectHistory.Version 3L

                do! basin |> S2.deleteStream plainSidecar
                do! basin |> S2.deleteStream plainName
                do! basin |> S2.deleteStream sidecarName
                do! basin |> S2.deleteStream sourceName

                let result =
                    { NoSnapshotResumesFromZero = noSnapshotResumesFromZero
                      FirstCheckpointAtSourceTail = firstCheckpointAtSourceTail
                      SecondCheckpointAdvances = secondCheckpointAdvances
                      LatestReflectsLastCommit = latestReflectsLastCommit
                      ResumeFromLatestIsSuffix = resumeFromLatestIsSuffix
                      RebuildEqualsFoldFromZero = rebuildEqualsFoldFromZero
                      RebuildAcrossRestartEqualsFoldFromZero = rebuildAcrossRestartEqualsFoldFromZero
                      EmptyRebuildIsFoldFromZero = emptyRebuildIsFoldFromZero
                      RebuildState = rebuildState
                      FoldFromZeroState = foldFromZeroState }

                do!
                    ctx.EmitSpan
                        "proof.foundation.checkpoint.completed"
                        [ "proof.property", "foundation.checkpoint-rebuild-equivalence"
                          "checkpoint.rebuild_equals_fold", string result.RebuildEqualsFoldFromZero
                          "checkpoint.rebuild_across_restart", string result.RebuildAcrossRestartEqualsFoldFromZero
                          "checkpoint.latest_reflects_commit", string result.LatestReflectsLastCommit
                          "checkpoint.resume_is_suffix", string result.ResumeFromLatestIsSuffix
                          "checkpoint.total", string result.RebuildState.Total ]

                return result
            })

    let checkpointRebuildEquivalenceProperty =
        property "foundation.checkpoint-rebuild-equivalence" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "no snapshot resumes the fold from Seq 0" (fun result ->
                      result.NoSnapshotResumesFromZero)
                  v.Expect.Workload "first checkpoint is as-of the source tail with folded state" (fun result ->
                      result.FirstCheckpointAtSourceTail)
                  v.Expect.Workload "second checkpoint advances AsOf and state" (fun result ->
                      result.SecondCheckpointAdvances)
                  v.Expect.Workload "latest returns the most recent committed snapshot" (fun result ->
                      result.LatestReflectsLastCommit)
                  v.Expect.Workload "rebuild resumes from the latest AsOf suffix, not Seq 0" (fun result ->
                      result.ResumeFromLatestIsSuffix)
                  v.Expect.Workload "cold rebuild equals fold-from-zero (checkpoint + suffix = full replay)" (fun result ->
                      result.RebuildEqualsFoldFromZero)
                  v.Expect.Workload "rebuild across a host restart equals fold-from-zero" (fun result ->
                      result.RebuildAcrossRestartEqualsFoldFromZero)
                  v.Expect.Workload "an uncheckpointed source rebuilds as a fold-from-zero" (fun result ->
                      result.EmptyRebuildIsFoldFromZero)
                  v.Trace.SpanExists
                      "checkpoint proof span emitted"
                      "proof.foundation.checkpoint.completed"
                      [ "proof.property", "foundation.checkpoint-rebuild-equivalence" ]
                  v.Trace.Operation
                      "checkpoint operation was recorded"
                      ({ TraceOperationMatch.named "foundation.checkpoint" with
                          Status = Some "ok"
                          OutputContains =
                              [ "RebuildEqualsFoldFromZero"
                                "RebuildAcrossRestartEqualsFoldFromZero"
                                "EmptyRebuildIsFoldFromZero" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.checkpoint-rebuild-equivalence" {
            describedAs
                "Checkpoint rebuild equals fold-from-zero for the same source, including across a host restart."

            property checkpointRebuildEquivalenceProperty
        }
