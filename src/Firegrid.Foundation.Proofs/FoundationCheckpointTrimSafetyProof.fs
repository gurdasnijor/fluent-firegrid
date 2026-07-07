namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation

/// A2 — `state.trim-safety`.
///
/// Proves the MS-C1 trim-safety law through the public `Checkpoint` surface:
/// `trim upTo` never advances past the latest committed snapshot's `AsOf`
/// (`AheadOfCheckpoint` otherwise), and a reader starting at the trim floor
/// still rebuilds *equivalent state* — the snapshot covers the trimmed prefix,
/// and the trim marker left on the source log is skipped by rebuild, never
/// folded.
module FoundationCheckpointTrimSafetyProof =
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

    type TrimSafetyProofResult =
        { GuardRejectsAheadOfCheckpoint: bool
          GuardIsNoOpOnSource: bool
          TrimAtFloorOk: bool
          TrimBehindFloorOk: bool
          RebuildFromTrimFloorEquivalentState: bool
          TrimMarkerAdvancesTailButIsSkipped: bool
          RebuildAfterSecondTrimEquivalent: bool
          NeverCheckpointedGuardsAgainstZero: bool
          NeverCheckpointedTrimAtZeroOk: bool }

    let private make basin source =
        Checkpoint.make basin Delta.codec Counter.codec source Counter.initial Counter.apply

    let private expectCheckpoint label result =
        match result with
        | Ok snapshot -> snapshot
        | Error _ -> failwithf "checkpoint %s failed unexpectedly (single-writer workload)" label

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "foundation.checkpoint_trim_safety"
            "foundation-checkpoint-trim-safety"
            { ProofOperationOptions.empty with
                Key = Some "foundation-checkpoint-trim-safety" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "trim-" + suffix
                let sourceName = "trim-src-" + suffix
                let source = SubjectHistory.SubjectId sourceName
                let (SubjectHistory.SubjectId sidecarName) = Checkpoint.checkpointSubject source

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream sourceName
                do! basin |> S2.createStream sidecarName

                let fold = make basin source

                // seqs 0..4 ; checkpoint AsOf=5 ; seqs 5..7 ; checkpoint AsOf=8
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 1; Delta 2; Delta 3; Delta 4; Delta 5 ]
                let! firstCheckpoint = Checkpoint.checkpoint fold
                let _ = expectCheckpoint "first" firstCheckpoint
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 10; Delta 20; Delta 30 ]
                let! secondCheckpoint = Checkpoint.checkpoint fold
                let snap2 = expectCheckpoint "second" secondCheckpoint
                // committed AsOf is now snap2.AsOf = Version 8.

                // uncheckpointed suffix seqs 8..9
                let! _ = SubjectHistory.append basin Delta.codec source [ Delta 100; Delta 200 ]

                // Reference: full rebuild BEFORE any trim.
                let! refState, refVer = Checkpoint.rebuild fold

                // (A) Guard: trim past the latest committed AsOf is rejected.
                let! ahead = Checkpoint.trim fold (SubjectHistory.Version 9L)

                let guardRejectsAheadOfCheckpoint =
                    ahead = Error(Checkpoint.TrimFailure.AheadOfCheckpoint(SubjectHistory.Version 9L, snap2.AsOf))

                // A rejected trim appends nothing to the source.
                let! tailAfterRejected = SubjectHistory.tail basin source
                let guardIsNoOpOnSource = tailAfterRejected = refVer

                // (B) Trim at the committed floor succeeds.
                let! atFloor = Checkpoint.trim fold snap2.AsOf
                let trimAtFloorOk = atFloor = Ok()

                // (C) A cold reader from the trim floor rebuilds EQUIVALENT state.
                let coldFold = make basin source
                let! postState, postVer = Checkpoint.rebuild coldFold
                let rebuildFromTrimFloorEquivalentState = postState = refState

                // The trim marker advanced the source tail by one, yet rebuild
                // skipped it (state unchanged) — the marker is on the log, not
                // in the fold.
                let trimMarkerAdvancesTailButIsSkipped =
                    SubjectHistory.versionNumber postVer = SubjectHistory.versionNumber refVer + 1L
                    && postState.Applied = refState.Applied

                // (D) Trim behind the floor also succeeds and stays equivalent.
                let! behind = Checkpoint.trim fold (SubjectHistory.Version 5L)
                let trimBehindFloorOk = behind = Ok()
                let! postState2, _ = Checkpoint.rebuild (make basin source)
                let rebuildAfterSecondTrimEquivalent = postState2 = refState

                // (E) Never-checkpointed source: committed floor is Version 0.
                let plainName = "trim-plain-" + suffix
                let plain = SubjectHistory.SubjectId plainName
                let (SubjectHistory.SubjectId plainSidecar) = Checkpoint.checkpointSubject plain
                do! basin |> S2.createStream plainName
                do! basin |> S2.createStream plainSidecar
                let! _ = SubjectHistory.append basin Delta.codec plain [ Delta 7; Delta 8 ]
                let plainFold = make basin plain

                let! plainAhead = Checkpoint.trim plainFold (SubjectHistory.Version 1L)

                let neverCheckpointedGuardsAgainstZero =
                    plainAhead = Error(Checkpoint.TrimFailure.AheadOfCheckpoint(SubjectHistory.Version 1L, SubjectHistory.Version 0L))

                let! plainZero = Checkpoint.trim plainFold (SubjectHistory.Version 0L)
                let neverCheckpointedTrimAtZeroOk = plainZero = Ok()

                do! basin |> S2.deleteStream plainSidecar
                do! basin |> S2.deleteStream plainName
                do! basin |> S2.deleteStream sidecarName
                do! basin |> S2.deleteStream sourceName

                let result =
                    { GuardRejectsAheadOfCheckpoint = guardRejectsAheadOfCheckpoint
                      GuardIsNoOpOnSource = guardIsNoOpOnSource
                      TrimAtFloorOk = trimAtFloorOk
                      TrimBehindFloorOk = trimBehindFloorOk
                      RebuildFromTrimFloorEquivalentState = rebuildFromTrimFloorEquivalentState
                      TrimMarkerAdvancesTailButIsSkipped = trimMarkerAdvancesTailButIsSkipped
                      RebuildAfterSecondTrimEquivalent = rebuildAfterSecondTrimEquivalent
                      NeverCheckpointedGuardsAgainstZero = neverCheckpointedGuardsAgainstZero
                      NeverCheckpointedTrimAtZeroOk = neverCheckpointedTrimAtZeroOk }

                do!
                    ctx.EmitSpan
                        "proof.foundation.checkpoint_trim_safety.completed"
                        [ "proof.property", "foundation.checkpoint-trim-safety"
                          "trim.guard_rejects_ahead", string result.GuardRejectsAheadOfCheckpoint
                          "trim.rebuild_from_floor_equivalent", string result.RebuildFromTrimFloorEquivalentState
                          "trim.marker_skipped", string result.TrimMarkerAdvancesTailButIsSkipped ]

                return result
            })

    let checkpointTrimSafetyProperty =
        property "foundation.checkpoint-trim-safety" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "trim past the latest committed AsOf is rejected as AheadOfCheckpoint" (fun result ->
                      result.GuardRejectsAheadOfCheckpoint)
                  v.Expect.Workload "a rejected trim appends nothing to the source" (fun result ->
                      result.GuardIsNoOpOnSource)
                  v.Expect.Workload "trim at the committed floor succeeds" (fun result -> result.TrimAtFloorOk)
                  v.Expect.Workload "trim behind the committed floor succeeds" (fun result -> result.TrimBehindFloorOk)
                  v.Expect.Workload "a cold reader from the trim floor rebuilds equivalent state" (fun result ->
                      result.RebuildFromTrimFloorEquivalentState)
                  v.Expect.Workload "the trim marker advances the tail but is skipped by rebuild" (fun result ->
                      result.TrimMarkerAdvancesTailButIsSkipped)
                  v.Expect.Workload "rebuild stays equivalent after a second trim" (fun result ->
                      result.RebuildAfterSecondTrimEquivalent)
                  v.Expect.Workload "a never-checkpointed source guards trim against Version 0" (fun result ->
                      result.NeverCheckpointedGuardsAgainstZero)
                  v.Expect.Workload "trim at Version 0 on a never-checkpointed source is a no-op Ok" (fun result ->
                      result.NeverCheckpointedTrimAtZeroOk)
                  v.Trace.SpanExists
                      "trim-safety proof span emitted"
                      "proof.foundation.checkpoint_trim_safety.completed"
                      [ "proof.property", "foundation.checkpoint-trim-safety" ]
                  v.Trace.Operation
                      "trim-safety operation was recorded"
                      ({ TraceOperationMatch.named "foundation.checkpoint_trim_safety" with
                          Status = Some "ok"
                          OutputContains =
                              [ "GuardRejectsAheadOfCheckpoint"
                                "RebuildFromTrimFloorEquivalentState"
                                "TrimMarkerAdvancesTailButIsSkipped" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.checkpoint-trim-safety" {
            describedAs "Trim never passes the latest committed snapshot; a reader from the trim floor rebuilds equivalent state."
            property checkpointTrimSafetyProperty
        }
