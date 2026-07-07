namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Store.Foundation.Durable

module FoundationDurableKernelProof =
    type DurableReplayResult =
        { FirstPlanRequestsActivity: bool
          CompletedReplayReturnsValue: bool
          PositionalOpIdIsStable: bool
          StepperRecordsCommandBeforeEffect: bool
          StepRecordCodecRoundTripsMailboxAccepted: bool }

    type MailboxAdmissionResult =
        { ScannedBothMailboxRecords: bool
          AcceptedOneDuplicate: bool
          CommittedAcceptedMessage: bool
          AdvancedMailboxCursor: bool
          PreservedSourceProvenance: bool }

    type ProcessorShellResult =
        { SendIntentHasPayloadOnly: bool
          CommitHappenedBeforeDispatch: bool
          DispatchSawCommittedIntentSeq: bool
          HandlerDidNotStampProvenance: bool
          DeposedCommitDidNotDispatch: bool }

    type private ProofStep =
        | Folded of string
        | Applied of string
        | ShellIntent of Intent
        | ShellSeal of string

    let private durableWorkflow =
        durable {
            let! value = Workflow.call "charge-card" "invoice-1"
            do! Workflow.log ("charged:" + value)
            return value
        }

    let private replayWorkload _ctx =
        async {
            let initial = Durable.replay History.empty durableWorkflow

            let firstPlanRequestsActivity =
                match initial with
                | Blocked(OpId 0, NeedsActivity activity) ->
                    activity.Name = "charge-card" && activity.Input = "invoice-1"
                | _ -> false

            let completedHistory =
                History.empty
                |> History.append (ActivityCalled(OpId 0, { Name = "charge-card"; Input = "invoice-1" }))
                |> History.append (ActivityCompleted(OpId 0, "ok"))
                |> History.append (LogEmitted(OpId 1, "charged:ok"))

            let completedReplayReturnsValue =
                Durable.replay completedHistory durableWorkflow = Done "ok"

            let positionalOpIdIsStable =
                let expectedRecords =
                    [ Incoming(HistoryEvent(ActivityCalled(OpId 0, { Name = "charge-card"; Input = "invoice-1" })))
                      Outgoing(Command(CallActivity(OpId 0, { Name = "charge-card"; Input = "invoice-1" }))) ]

                match DurableStepper.plan 123L History.empty durableWorkflow with
                | Commit records -> records = expectedRecords
                | _ -> false

            let stepperRecordsCommandBeforeEffect =
                match DurableStepper.plan 123L History.empty durableWorkflow with
                | Commit [ Incoming(HistoryEvent(ActivityCalled(OpId 0, _))); Outgoing(Command(CallActivity(OpId 0, _))) ] ->
                    true
                | _ -> false

            let envelope =
                { Source = "proof-source"
                  SourceSeqNum = 7L
                  Message = RaiseSignal("approved", "yes") }

            let record = Incoming(MailboxMessageAccepted envelope)
            let encoded = StepRecordCodec.encode record
            let stepRecordCodecRoundTripsMailboxAccepted = StepRecordCodec.decode encoded = Ok record

            return
                { FirstPlanRequestsActivity = firstPlanRequestsActivity
                  CompletedReplayReturnsValue = completedReplayReturnsValue
                  PositionalOpIdIsStable = positionalOpIdIsStable
                  StepperRecordsCommandBeforeEffect = stepperRecordsCommandBeforeEffect
                  StepRecordCodecRoundTripsMailboxAccepted = stepRecordCodecRoundTripsMailboxAccepted }
        }

    let private mailboxWorkload ctx =
        ProofOperation.run
            ctx
            "foundation.durable.mailbox"
            "foundation-durable-mailbox"
            { ProofOperationOptions.empty with
                Key = Some "foundation-durable-mailbox" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-" + suffix
                let key = StorageKey("actor-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key
                let! owned = S2Substrate.claimWith (FenceToken "proof-fence") pair

                let envelope =
                    { Source = "client-a"
                      SourceSeqNum = 1L
                      Message = StartWorkflow(WorkflowName "invoice", "payload") }

                let body = MailboxEnvelopeCodec.encode envelope

                let! _ = S2Substrate.appendMailboxText [ "source", "client-a"; "seq", "1" ] body pair
                let! _ = S2Substrate.appendMailboxText [ "source", "client-a"; "seq", "1" ] body pair

                let! status = Mailbox.runOnce StepRecordCodec.encode StepRecordCodec.decode MailboxEnvelopeCodec.decode 10 owned

                let result =
                    match status with
                    | MailboxStatus.Folded report ->
                        let committedAcceptedMessage =
                            match report.Commit with
                            | Some(Committed _) -> true
                            | _ -> false

                        let preservedSourceProvenance =
                            report.Accepted
                            |> List.exists (fun admitted ->
                                admitted.Envelope.Source = "client-a"
                                && admitted.Envelope.SourceSeqNum = 1L
                                && admitted.MailboxSeqNum = 0L)

                        { ScannedBothMailboxRecords = report.Scanned = 2
                          AcceptedOneDuplicate = List.length report.Accepted = 1 && report.Duplicates = 1
                          CommittedAcceptedMessage = committedAcceptedMessage
                          AdvancedMailboxCursor = report.NextSeqNum = 2L
                          PreservedSourceProvenance = preservedSourceProvenance }
                    | _ ->
                        { ScannedBothMailboxRecords = false
                          AcceptedOneDuplicate = false
                          CommittedAcceptedMessage = false
                          AdvancedMailboxCursor = false
                          PreservedSourceProvenance = false }

                return result
            })

    let private processorWorkload _ctx =
        async {
            let calls = ResizeArray<string>()
            let committedIntents = ResizeArray<CommittedIntent>()

            let handler =
                { Initial = "empty"
                  Fold =
                    fun _ record ->
                        match record.Body with
                        | Folded state -> state
                        | _ -> "unexpected"
                  OnAdmitted =
                    fun state admitted ->
                        let message = RaiseSignal("proof", admitted.Message)
                        let intent = Intent.Send({ Segments = [ "target" ] }, message)

                        { State = "handled:" + state
                          Append = [ Applied admitted.Message; ShellIntent intent ]
                          Intents = [ intent ]
                          Seal = None }
                  OnWake =
                    fun state _ ->
                        { State = state
                          Append = []
                          Intents = []
                          Seal = None } }

            let env =
                { Wake = None
                  Claim =
                    fun _ ->
                        async {
                            calls.Add "claim"
                            return Ok(FenceToken "processor-proof")
                        }
                  Rebuild =
                    fun _ _ ->
                        async {
                            calls.Add "rebuild"
                            return
                                Ok
                                    [ { Seq = 0L
                                        Body = box (Folded "rebuilt")
                                        Timestamp = 1L } ]
                        }
                  Admit =
                    fun _ _ ->
                        async {
                            calls.Add "admit"
                            return
                                Ok
                                    [ { MailboxSeqNum = 4L
                                        Source = "client"
                                        SourceSeqNum = 9L
                                        Message = box "payload" } ]
                        }
                  Commit =
                    fun _ _ append intents seal ->
                        async {
                            calls.Add "commit"

                            let committed =
                                intents
                                |> List.mapi (fun index intent ->
                                    { Seq = int64 (100 + index)
                                      IntentIndex = index
                                      Intent = intent })

                            committed |> List.iter committedIntents.Add

                            return
                                Ok
                                    { Appended = List.length append
                                      Intents = committed
                                      Sealed = Option.isSome seal }
                        }
                  Dispatch =
                    fun _ _ intents ->
                        async {
                            calls.Add "dispatch"
                            committedIntents.Clear()
                            intents |> List.iter committedIntents.Add
                            return Ok()
                        } }

            let! outcome = Processor.drive env { Segments = [ "source" ] } handler

            let sendIntentHasPayloadOnly =
                committedIntents
                |> Seq.exists (fun committed ->
                    match committed.Intent with
                    | Intent.Send(target, message) -> target.Segments = [ "target" ] && message = RaiseSignal("proof", "payload")
                    | _ -> false)

            let commitIndex = calls |> Seq.tryFindIndex ((=) "commit")
            let dispatchIndex = calls |> Seq.tryFindIndex ((=) "dispatch")

            let commitHappenedBeforeDispatch =
                match commitIndex, dispatchIndex with
                | Some commit, Some dispatch -> commit < dispatch
                | _ -> false

            let dispatchSawCommittedIntentSeq =
                committedIntents
                |> Seq.exists (fun committed -> committed.Seq = 100L && committed.IntentIndex = 0)

            let handlerDidNotStampProvenance =
                committedIntents
                |> Seq.forall (fun committed ->
                    match committed.Intent with
                    | Intent.Send(_, message) -> message = RaiseSignal("proof", "payload")
                    | _ -> true)

            let deposedCalls = ResizeArray<string>()

            let deposedEnv =
                { env with
                    Commit =
                        fun _ _ _ _ _ ->
                            async {
                                deposedCalls.Add "commit"
                                return Error(DriveCommitFailure.CommitDeposed "new-fence")
                            }
                    Dispatch =
                        fun _ _ _ ->
                            async {
                                deposedCalls.Add "dispatch"
                                return Ok()
                            } }

            let! deposed = Processor.drive deposedEnv { Segments = [ "source" ] } handler

            return
                { SendIntentHasPayloadOnly = sendIntentHasPayloadOnly
                  CommitHappenedBeforeDispatch = commitHappenedBeforeDispatch
                  DispatchSawCommittedIntentSeq = dispatchSawCommittedIntentSeq
                  HandlerDidNotStampProvenance = handlerDidNotStampProvenance
                  DeposedCommitDidNotDispatch =
                    deposed = DriveOutcome.Deposed "new-fence"
                    && not (deposedCalls |> Seq.exists ((=) "dispatch"))
                    && outcome = DriveOutcome.Advanced }
        }

    let durableReplayProperty =
        property "foundation.durable-replay" {
            workload replayWorkload

            verify (fun v ->
                [ v.Expect.Workload "empty replay blocks on first activity" (fun result ->
                      result.FirstPlanRequestsActivity)
                  v.Expect.Workload "completed replay returns recorded value" (fun result ->
                      result.CompletedReplayReturnsValue)
                  v.Expect.Workload "positional OpId remains stable under replay" (fun result ->
                      result.PositionalOpIdIsStable)
                  v.Expect.Workload "planner records command before external effect" (fun result ->
                      result.StepperRecordsCommandBeforeEffect)
                  v.Expect.Workload "step codec round-trips mailbox acceptance" (fun result ->
                      result.StepRecordCodecRoundTripsMailboxAccepted) ])
        }

    let mailboxAdmissionProperty =
        property "foundation.durable-mailbox" {
            s2Lite ""
            workload mailboxWorkload

            verify (fun v ->
                [ v.Expect.Workload "mailbox scans both appended records" (fun result ->
                      result.ScannedBothMailboxRecords)
                  v.Expect.Workload "mailbox accepts first source seq and dedupes duplicate" (fun result ->
                      result.AcceptedOneDuplicate)
                  v.Expect.Workload "mailbox admission commits accepted message" (fun result ->
                      result.CommittedAcceptedMessage)
                  v.Expect.Workload "mailbox checkpoint advances past admitted records" (fun result ->
                      result.AdvancedMailboxCursor)
                  v.Expect.Workload "mailbox preserves source provenance" (fun result ->
                      result.PreservedSourceProvenance)
                  v.Trace.Operation
                      "mailbox operation was recorded"
                      ({ TraceOperationMatch.named "foundation.durable.mailbox" with
                          Status = Some "ok"
                          OutputContains = [ "AcceptedOneDuplicate"; "PreservedSourceProvenance" ]
                          Count = Some 1 }) ])
        }

    let processorShellProperty =
        property "foundation.durable-processor" {
            workload processorWorkload

            verify (fun v ->
                [ v.Expect.Workload "send intent carries target and payload only" (fun result ->
                      result.SendIntentHasPayloadOnly)
                  v.Expect.Workload "commit happens before dispatch" (fun result ->
                      result.CommitHappenedBeforeDispatch)
                  v.Expect.Workload "dispatch receives committed intent identity" (fun result ->
                      result.DispatchSawCommittedIntentSeq)
                  v.Expect.Workload "handler cannot stamp outbound provenance" (fun result ->
                      result.HandlerDidNotStampProvenance)
                  v.Expect.Workload "deposed commit exits without dispatch" (fun result ->
                      result.DeposedCommitDidNotDispatch) ])
        }

    let proof =
        proof "foundation.durable-kernel" {
            describedAs "P3 durable replay, mailbox admission, and processor drive-loop invariants."
            property durableReplayProperty
            property mailboxAdmissionProperty
            property processorShellProperty
        }
