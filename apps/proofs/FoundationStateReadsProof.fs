namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation

/// A3 — `state.stateview-strong-read`.
///
/// Proves the MS-C4 read law through the public `StateReads` surface: a strong
/// read (`readLatest`/`readThrough`) issued after a *second host's* acknowledged
/// append observes that append (linearizable); an eventual read (`readEventual`)
/// is a monotonic prefix that may lag — never ahead of a strong read — and
/// catches up once the fold has applied through the committed tail.
module FoundationStateReadsProof =
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

    type StateReadsProofResult =
        { BaselineStrongCorrect: bool
          StrongObservesSecondHostAppend: bool
          ReadThroughObservesSecondHostAppend: bool
          EventualIsPrefixNotAhead: bool
          EventualNeverAheadOfStrong: bool
          EventualCatchesUpAfterStrong: bool
          SecondHostVersion: int64
          EventualTailAfterSecondHost: int64 }

    let private ver = SubjectHistory.versionNumber

    let private hasHostB (state: CounterState) =
        state.Total = 13 && (state.Labels |> List.contains "host-b")

    let private runWorkload ctx =
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
                    | None -> failwith "state.stateview-strong-read requires an s2 endpoint (declare s2Lite)"

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
                        [ "proof.property", "state.stateview-strong-read"
                          "stateview.strong_observes_second_host", string result.StrongObservesSecondHostAppend
                          "stateview.eventual_is_prefix", string result.EventualIsPrefixNotAhead
                          "stateview.eventual_catches_up", string result.EventualCatchesUpAfterStrong
                          "stateview.second_host_version", string result.SecondHostVersion
                          "stateview.eventual_tail_after_second_host", string result.EventualTailAfterSecondHost ]

                return result
            })

    let stateviewStrongReadProperty =
        property "state.stateview-strong-read" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "baseline strong read folds the seeded state" (fun result ->
                      result.BaselineStrongCorrect)
                  v.Expect.Workload "a strong read observes a second host's acknowledged append" (fun result ->
                      result.StrongObservesSecondHostAppend)
                  v.Expect.Workload "readThrough the committed version observes the second host's append" (fun result ->
                      result.ReadThroughObservesSecondHostAppend)
                  v.Expect.Workload "an eventual read is a prefix, never past the committed tail" (fun result ->
                      result.EventualIsPrefixNotAhead)
                  v.Expect.Workload "an eventual read is never ahead of a strong read" (fun result ->
                      result.EventualNeverAheadOfStrong)
                  v.Expect.Workload "an eventual read catches up after a strong read (monotonic)" (fun result ->
                      result.EventualCatchesUpAfterStrong)
                  v.Trace.SpanExists
                      "stateview strong-read proof span emitted"
                      "proof.foundation.stateview_strong_read.completed"
                      [ "proof.property", "state.stateview-strong-read" ]
                  v.Trace.Operation
                      "stateview strong-read operation was recorded"
                      ({ TraceOperationMatch.named "foundation.stateview_strong_read" with
                          Status = Some "ok"
                          OutputContains =
                              [ "StrongObservesSecondHostAppend"
                                "EventualNeverAheadOfStrong"
                                "EventualCatchesUpAfterStrong" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "state.stateview-strong-read" {
            describedAs "A strong StateReads read observes a second host's acknowledged append; an eventual read is a monotonic prefix that may lag."
            property stateviewStrongReadProperty
        }
