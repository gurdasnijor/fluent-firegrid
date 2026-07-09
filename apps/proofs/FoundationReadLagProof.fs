namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store
open Firegrid.Store.Foundation.Durable

/// Packet 0.3b — `foundation.read-lag`: ONE property merging the two read-lag
/// restatements (the architect-designed merge without a template):
///
///   - `state.stateview-strong-read` — a strong read (`readLatest` /
///     `readThrough`) issued after a *second host's* acknowledged append
///     observes that append (linearizable); an eventual read is a monotonic
///     prefix that may lag — never ahead of a strong read — and catches up
///     once the fold has applied through the committed tail.
///   - `session.projection-lag-observable` — the same law over the
///     SessionHistory projection: an eventual read's `AppliedTail` exposes
///     the projection's lag as data; a strong read is linearizable.
///
/// Strong = linearizable; eventual = monotonic lagging prefix; the lag is
/// bounded by the strong read and observable. Consolidation deletes
/// RESTATEMENTS, never ASSERTIONS: both retired workloads run verbatim inside
/// this one property, and every original check (including both recorded
/// trace operations) survives under its original name.
module FoundationReadLagProof =
    module SH = SessionHistory
    module SL = SessionLifecycle

    // ---- state.stateview-strong-read scenario (verbatim) -------------------

    type CounterRecord =
        | Add of amount: int
        | Mark of label: string

    type CounterState = { Total: int; Labels: string list }

    module private CounterRecord =
        let encode record =
            match record with
            | Add amount -> "add|" + string amount
            | Mark label -> "mark|" + label

        let decode (body: string) =
            match body.Split('|') |> Array.toList with
            | [ "add"; amount ] ->
                match System.Int32.TryParse amount with
                | true, value -> Ok(Add value)
                | false, _ -> Error("bad add amount: " + amount)
            | [ "mark"; label ] -> Ok(Mark label)
            | _ -> Error("unknown record body: " + body)

        let codec: SubjectHistory.Codec<CounterRecord> = { Encode = encode; Decode = decode }

    module private CounterState =
        let empty = { Total = 0; Labels = [] }

        let apply state (record: SubjectHistory.StoredRecord<CounterRecord>) =
            match record.Body with
            | Add amount -> { state with Total = state.Total + amount }
            | Mark label -> { state with Labels = state.Labels @ [ label ] }

    type StateReadsPart =
        { BaselineStrongCorrect: bool
          StrongObservesSecondHostAppend: bool
          ReadThroughObservesSecondHostAppend: bool
          EventualIsPrefixNotAhead: bool
          EventualNeverAheadOfStrong: bool
          EventualCatchesUpAfterStrong: bool
          SecondHostVersion: int64
          EventualTailAfterSecondHost: int64 }

    type ProjectionLagPart =
        { ReaderSeededPastFenceReadsHistory: bool
          StrongReadObservesNewTurn: bool
          EventualIsPrefixNotPastTail: bool
          EventualNeverAheadOfStrong: bool
          LagExposedAsAppliedTail: bool }

    type ReadLagProofResult =
        { StateReads: StateReadsPart
          Projection: ProjectionLagPart }

    let private ver = SubjectHistory.versionNumber

    let private hasHostB (state: CounterState) =
        state.Total = 13 && (state.Labels |> List.contains "host-b")

    let private runStateReadsScenario ctx =
        ProofOperation.run
            ctx
            "foundation.stateview_strong_read"
            "foundation-stateview-strong-read"
            { ProofOperationOptions.empty with
                Key = Some "foundation-stateview-strong-read" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "sv-reads-" + suffix
                let subjectName = "subject-" + suffix
                let subject = SubjectHistory.SubjectId subjectName

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream subjectName

                // Host A seeds the subject, then starts a resident reader.
                let! seededTail = SubjectHistory.append basin CounterRecord.codec subject [ Add 3; Mark "seed" ]

                let! reader =
                    StateReads.start basin CounterRecord.codec subject (SubjectHistory.Seq 0L) CounterState.empty CounterState.apply

                let! baseline = StateReads.readLatest reader

                let baselineStrongCorrect =
                    baseline.AppliedTail = seededTail
                    && baseline.State.Total = 3
                    && baseline.State.Labels = [ "seed" ]

                // A SECOND HOST (a distinct S2 client over the same s2-lite)
                // appends to the same subject and gets an acknowledged version.
                let secondHostEndpoint =
                    match s2.Endpoint with
                    | Some endpoint -> endpoint
                    | None -> failwith "foundation.read-lag requires an s2 endpoint (declare s2Lite)"

                let secondHost =
                    S2.connectWith
                        { S2.ConnectOptions.create "s2-lite-second-host" with
                            AccountEndpoint = Some secondHostEndpoint
                            BasinEndpoint = Some secondHostEndpoint }

                let secondHostBasin = secondHost |> S2.basin basinName

                let! secondHostVersion =
                    SubjectHistory.append secondHostBasin CounterRecord.codec subject [ Add 10; Mark "host-b" ]

                // Eventual read right after the second host's ack: the local
                // applied snapshot — a prefix that need not yet include host-b.
                let! eventualAfterB = StateReads.readEventual reader

                // Strong read observes the second host's acknowledged append.
                let! strongAfterB = StateReads.readLatest reader

                let strongObservesSecondHostAppend =
                    strongAfterB.AppliedTail = secondHostVersion && hasHostB strongAfterB.State

                // readThrough the committed version also observes it (fast path
                // falls through to one Strong read when the fold has not caught up).
                let! throughVersion = StateReads.readThrough secondHostVersion reader
                let readThroughObservesSecondHostAppend = hasHostB throughVersion.State

                // Eventual is a valid prefix: never past the committed tail, and
                // never ahead of the strong read.
                let eventualIsPrefixNotAhead = ver eventualAfterB.AppliedTail <= ver secondHostVersion
                let eventualNeverAheadOfStrong = ver eventualAfterB.AppliedTail <= ver strongAfterB.AppliedTail

                // After a strong read forces the fold through the tail, a fresh
                // eventual read has caught up (monotonic) and reflects host-b.
                let! eventualCaughtUp = StateReads.readEventual reader

                let eventualCatchesUpAfterStrong =
                    ver eventualCaughtUp.AppliedTail >= ver secondHostVersion
                    && hasHostB eventualCaughtUp.State

                do! StateReads.stop reader
                do! basin |> S2.deleteStream subjectName

                let result =
                    { BaselineStrongCorrect = baselineStrongCorrect
                      StrongObservesSecondHostAppend = strongObservesSecondHostAppend
                      ReadThroughObservesSecondHostAppend = readThroughObservesSecondHostAppend
                      EventualIsPrefixNotAhead = eventualIsPrefixNotAhead
                      EventualNeverAheadOfStrong = eventualNeverAheadOfStrong
                      EventualCatchesUpAfterStrong = eventualCatchesUpAfterStrong
                      SecondHostVersion = ver secondHostVersion
                      EventualTailAfterSecondHost = ver eventualAfterB.AppliedTail }

                do!
                    ctx.EmitSpan
                        "proof.foundation.stateview_strong_read.completed"
                        [ "proof.property", "foundation.read-lag"
                          "stateview.strong_observes_second_host", string result.StrongObservesSecondHostAppend
                          "stateview.eventual_is_prefix", string result.EventualIsPrefixNotAhead
                          "stateview.eventual_catches_up", string result.EventualCatchesUpAfterStrong
                          "stateview.second_host_version", string result.SecondHostVersion
                          "stateview.eventual_tail_after_second_host", string result.EventualTailAfterSecondHost ]

                return result
            })

    // ---- session.projection-lag-observable scenario (verbatim) -------------

    let private t1 = Turn.TurnId "turn-1"
    let private t2 = Turn.TurnId "turn-2"
    let private t3 = Turn.TurnId "turn-3"

    let private holder = Authority.HolderId "history-host"

    let private startOk basin session turnId timeouts now =
        async {
            match! SL.start basin timeouts session turnId holder now with
            | Ok live -> return live
            | Error _ -> return failwith "read-lag proof: start failed unexpectedly"
        }

    let private completeOk (live: SL.LiveTurn) now =
        async {
            match! SL.complete live now with
            | Ok() -> return ()
            | Error _ -> return failwith "read-lag proof: complete failed unexpectedly"
        }

    let private runProjectionLagScenario ctx =
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
                        [ "proof.property", "foundation.read-lag"
                          "lag.reader_reads_history", string result.ReaderSeededPastFenceReadsHistory
                          "lag.strong_observes_new", string result.StrongReadObservesNewTurn
                          "lag.eventual_is_prefix", string result.EventualIsPrefixNotPastTail ]

                return result
            })

    // ---- the merged property ------------------------------------------------

    let private runWorkload ctx =
        async {
            let! stateReads = runStateReadsScenario ctx
            let! projection = runProjectionLagScenario ctx

            return
                { StateReads = stateReads
                  Projection = projection }
        }

    let private readLagProperty =
        property "foundation.read-lag" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ // from state.stateview-strong-read
                  v.Expect.Workload "baseline strong read folds the seeded state" (fun r ->
                      r.StateReads.BaselineStrongCorrect)
                  v.Expect.Workload "a strong read observes a second host's acknowledged append" (fun r ->
                      r.StateReads.StrongObservesSecondHostAppend)
                  v.Expect.Workload "readThrough the committed version observes the second host's append" (fun r ->
                      r.StateReads.ReadThroughObservesSecondHostAppend)
                  v.Expect.Workload "an eventual read is a prefix, never past the committed tail" (fun r ->
                      r.StateReads.EventualIsPrefixNotAhead)
                  v.Expect.Workload "an eventual read is never ahead of a strong read" (fun r ->
                      r.StateReads.EventualNeverAheadOfStrong && r.Projection.EventualNeverAheadOfStrong)
                  v.Expect.Workload "an eventual read catches up after a strong read (monotonic)" (fun r ->
                      r.StateReads.EventualCatchesUpAfterStrong)
                  // from session.projection-lag-observable
                  v.Expect.Workload "a reader seeded from the checkpoint reads history past the fence" (fun r ->
                      r.Projection.ReaderSeededPastFenceReadsHistory)
                  v.Expect.Workload "a strong read observes a turn appended after the reader started" (fun r ->
                      r.Projection.StrongReadObservesNewTurn)
                  v.Expect.Workload "an eventual read is a prefix, never past the checked tail" (fun r ->
                      r.Projection.EventualIsPrefixNotPastTail)
                  v.Expect.Workload "projection lag is exposed as a monotonic AppliedTail" (fun r ->
                      r.Projection.LagExposedAsAppliedTail)
                  // trace evidence: both recorded operations + both spans survive
                  v.Trace.SpanExists
                      "stateview strong-read proof span emitted"
                      "proof.foundation.stateview_strong_read.completed"
                      [ "proof.property", "foundation.read-lag" ]
                  v.Trace.SpanExists
                      "session projection-lag proof span emitted"
                      "proof.foundation.session_projection_lag.completed"
                      [ "proof.property", "foundation.read-lag" ]
                  v.Trace.Operation
                      "stateview strong-read operation was recorded"
                      ({ TraceOperationMatch.named "foundation.stateview_strong_read" with
                          Status = Some "ok"
                          OutputContains =
                              [ "StrongObservesSecondHostAppend"
                                "EventualNeverAheadOfStrong"
                                "EventualCatchesUpAfterStrong" ]
                          Count = Some 1 })
                  v.Trace.Operation
                      "session projection-lag operation was recorded"
                      ({ TraceOperationMatch.named "foundation.session_projection_lag" with
                          Status = Some "ok"
                          OutputContains = [ "ReaderSeededPastFenceReadsHistory"; "StrongReadObservesNewTurn" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.read-lag" {
            describedAs
                "Read grades over the foundation read surfaces, merged from state.stateview-strong-read + session.projection-lag-observable: a strong read is linearizable (observes a second host's acknowledged append; readThrough honors the committed version); an eventual read is a monotonic lagging prefix, never ahead of a strong read, with the lag bounded and observable as AppliedTail data."

            property readLagProperty
        }
