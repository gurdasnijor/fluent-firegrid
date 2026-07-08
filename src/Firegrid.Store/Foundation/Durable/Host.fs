namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log

[<RequireQualifiedAccess>]
type DurableHostFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | CommitFailed of S2Errors.S2Failure
    | UnexpectedNoCommit
    | Unexpected of string

[<RequireQualifiedAccess>]
type DurableHostStatus<'a> =
    | Completed of 'a
    | ContinuedAsNew of nextInput: Payload
    | Committed of S2.AppendAck
    | Waiting of OpId * Need
    | Deposed of expectedFence: string
    | Failed of DurableHostFailure

type DurableHostTickOptions =
    { HostId: string
      Timestamp: int64
      MaxMailboxRecords: int
      MaxActivityCommands: int
      MaxTimerCommands: int
      MaxDispatchCommands: int
      /// How many due activity commands may execute concurrently within one
      /// tick. Defaults to the whole due batch.
      MaxConcurrentActivities: int }

type DurableHostTickReport<'a> =
    { Key: StorageKey
      Fence: FenceToken
      Inbox: MailboxReport option
      Step: DurableHostStatus<'a> option
      Signals: SignalDeliveryReport option
      Activities: ActivityCommandAdapterReport option
      Timers: TimerCommandAdapterReport option
      ChildStarts: ChildStartAdapterReport option
      ChildResults: ChildResultAdapterReport option
      Generations: GenerationAdapterReport option }

and SignalDelivery =
    { Source: string
      SourceSeqNum: int64
      OpId: OpId
      Name: string
      Payload: Payload
      Commit: S2.AppendAck }

and SignalDeliveryReport =
    { Delivered: SignalDelivery option
      PendingSignals: int
      AlreadyDelivered: int }

[<RequireQualifiedAccess>]
type DurableHostTickFailure =
    | MailboxFailed of MailboxFailure
    | StepFailed of DurableHostFailure
    | SignalFailed of DurableHostFailure
    | ActivityFailed of ActivityCommandAdapterFailure
    | TimerFailed of TimerCommandAdapterFailure
    | ChildStartFailed of ChildStartAdapterFailure
    | ChildResultFailed of ChildResultAdapterFailure
    | GenerationFailed of GenerationAdapterFailure

[<RequireQualifiedAccess>]
type DurableHostTickStatus<'a> =
    | Completed of value: 'a * report: DurableHostTickReport<'a>
    | ContinuedAsNew of nextInput: Payload * report: DurableHostTickReport<'a>
    | Waiting of opId: OpId * need: Need * report: DurableHostTickReport<'a>
    | Advanced of DurableHostTickReport<'a>
    | Deposed of expectedFence: string * report: DurableHostTickReport<'a>
    | Failed of DurableHostTickFailure * report: DurableHostTickReport<'a>

[<RequireQualifiedAccess>]
type DurableWorkflowHostFailure =
    | MailboxFoldFailed of MailboxFailure
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | NoStart
    | WorkflowNotFound of WorkflowName

[<RequireQualifiedAccess>]
type DurableWorkflowHostStatus =
    | Ticked of DurableHostTickStatus<Payload>
    | Deposed of expectedFence: string
    | Failed of DurableWorkflowHostFailure

[<RequireQualifiedAccess>]
module DurableHostTickOptions =
    let create hostId timestamp =
        { HostId = hostId
          Timestamp = timestamp
          MaxMailboxRecords = 100
          MaxActivityCommands = 100
          MaxTimerCommands = 100
          MaxDispatchCommands = 100
          MaxConcurrentActivities = System.Int32.MaxValue }

[<RequireQualifiedAccess>]
module DurableHost =
    let private emptyTickReport (owned: OwnedKey) =
        { Key = owned.Key
          Fence = owned.Fence
          Inbox = None
          Step = None
          Signals = None
          Activities = None
          Timers = None
          ChildStarts = None
          ChildResults = None
          Generations = None }

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (_, Ok record) :: rest -> loop (record :: records) rest
            | (seqNum, Error error) :: _ -> Error(DurableHostFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return Ok decoded
            with error ->
                return Error(DurableHostFailure.LogReadFailed error.Message)
        }

    let stepOnce encode decode timestamp owned program =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return DurableHostStatus.Failed failure
            | Ok decoded ->
                try
                    match decodeLog decoded with
                    | Error failure -> return DurableHostStatus.Failed failure
                    | Ok records ->
                        let history = DurableStepper.historyFromRecords records

                        match DurableStepper.plan timestamp history program with
                        | Complete value -> return DurableHostStatus.Completed value
                        | Continued nextInput -> return DurableHostStatus.ContinuedAsNew nextInput
                        | Waiting(opId, need) -> return DurableHostStatus.Waiting(opId, need)
                        | Commit _ as plan ->
                            let! commit = DurableStepper.commit encode owned plan

                            return
                                match commit with
                                | StepCommitted ack -> DurableHostStatus.Committed ack
                                | StepDeposed expected -> DurableHostStatus.Deposed expected
                                | StepCommitFailed failure ->
                                    DurableHostStatus.Failed(DurableHostFailure.CommitFailed failure)
                                | StepNotRequired -> DurableHostStatus.Failed DurableHostFailure.UnexpectedNoCommit
                with error ->
                    return DurableHostStatus.Failed(DurableHostFailure.Unexpected error.Message)
        }

    let runOnce encode decode timestamp owned program =
        stepOnce encode decode timestamp owned program

    let private mailboxMadeProgress (report: MailboxReport) =
        match report.Commit with
        | Some _ -> true
        | None -> false

    let private activitiesMadeProgress (report: ActivityCommandAdapterReport) =
        not (List.isEmpty report.Completed)
        || not (List.isEmpty report.Sent)
        || report.AlreadyPublished > 0
        || report.AlreadySent > 0
        || report.Ignored > 0
        || match report.Checkpoint with
           | CommandDispatchCheckpointResult.Checkpointed _ -> true
           | CommandDispatchCheckpointResult.NotRequired
           | CommandDispatchCheckpointResult.Deposed _
           | CommandDispatchCheckpointResult.Failed _ -> false

    let private timersMadeProgress (report: TimerCommandAdapterReport) =
        not (List.isEmpty report.Published)
        || report.AlreadyPublished > 0
        || report.Canceled > 0
        || report.Ignored > 0
        || match report.Checkpoint with
           | CommandDispatchCheckpointResult.Checkpointed _ -> true
           | CommandDispatchCheckpointResult.NotRequired
           | CommandDispatchCheckpointResult.Deposed _
           | CommandDispatchCheckpointResult.Failed _ -> false

    let private childStartsMadeProgress (report: ChildStartAdapterReport) =
        not (List.isEmpty report.Dispatched)
        || match report.Checkpoint with
           | CommandDispatchCheckpointResult.Checkpointed _ -> true
           | CommandDispatchCheckpointResult.NotRequired
           | CommandDispatchCheckpointResult.Deposed _
           | CommandDispatchCheckpointResult.Failed _ -> false

    [<RequireQualifiedAccess>]
    type private SignalDeliveryStatus =
        | Delivered of SignalDeliveryReport
        | NotAvailable of SignalDeliveryReport
        | Deposed of expectedFence: string * SignalDeliveryReport
        | Failed of DurableHostFailure * SignalDeliveryReport

    let private signalNeeds =
        function
        | DurableHostStatus.Waiting(opId, NeedsEvent(Signal name)) -> [ opId, name ]
        | DurableHostStatus.Waiting(_, NeedsRace pending) ->
            pending
            |> List.choose (function
                | opId, RaceEvent(Signal name) -> Some(opId, name)
                | _ -> None)
        | _ -> []

    let private deliveredSignals records =
        records
        |> List.choose (function
            | Incoming(SignalDelivered(source, sourceSeqNum, _)) -> Some(source, sourceSeqNum)
            | _ -> None)
        |> Set.ofList

    let private acceptedSignals records =
        records
        |> List.choose (function
            | Incoming(MailboxMessageAccepted envelope) ->
                match envelope.Message with
                | RaiseSignal(name, payload) -> Some(envelope.Source, envelope.SourceSeqNum, name, payload)
                | StartWorkflow _
                | CompleteActivity _
                | FireTimer _
                | StartChildWorkflow _
                | CompleteChild _
                | AckSend _ -> None
            | _ -> None)

    let private tryDeliverSignal owned step =
        async {
            let needs = signalNeeds step

            let emptyReport =
                { Delivered = None
                  PendingSignals = 0
                  AlreadyDelivered = 0 }

            if List.isEmpty needs then
                return SignalDeliveryStatus.NotAvailable emptyReport
            else
                let! log = readLog StepRecordCodec.decode owned

                match log with
                | Error failure -> return SignalDeliveryStatus.Failed(failure, emptyReport)
                | Ok decoded ->
                    match decodeLog decoded with
                    | Error failure -> return SignalDeliveryStatus.Failed(failure, emptyReport)
                    | Ok records ->
                        let delivered = deliveredSignals records
                        let allAccepted = acceptedSignals records

                        let pending =
                            allAccepted
                            |> List.filter (fun (source, sourceSeqNum, _, _) ->
                                not (delivered |> Set.contains (source, sourceSeqNum)))

                        let alreadyDelivered = List.length allAccepted - List.length pending

                        let baseReport =
                            { Delivered = None
                              PendingSignals = List.length pending
                              AlreadyDelivered = alreadyDelivered }

                        let delivery =
                            pending
                            |> List.tryPick (fun (source, sourceSeqNum, name, payload) ->
                                needs
                                |> List.tryFind (fun (_, neededName) -> neededName = name)
                                |> Option.map (fun (opId, _) -> source, sourceSeqNum, opId, name, payload))

                        match delivery with
                        | None -> return SignalDeliveryStatus.NotAvailable baseReport
                        | Some(source, sourceSeqNum, opId, name, payload) ->
                            let records =
                                [ Incoming(HistoryEvent(SignalReceived(opId, name, payload)))
                                  Incoming(SignalDelivered(source, sourceSeqNum, opId)) ]

                            let! commit = S2Substrate.commitText StepRecordCodec.encode records owned

                            match commit with
                            | Committed ack ->
                                let deliveredSignal =
                                    { Source = source
                                      SourceSeqNum = sourceSeqNum
                                      OpId = opId
                                      Name = name
                                      Payload = payload
                                      Commit = ack }

                                return
                                    SignalDeliveryStatus.Delivered
                                        { baseReport with
                                            Delivered = Some deliveredSignal }
                            | Deposed expected -> return SignalDeliveryStatus.Deposed(expected, baseReport)
                            | CommitFailed failure ->
                                return SignalDeliveryStatus.Failed(DurableHostFailure.CommitFailed failure, baseReport)
        }

    let private runAdapters
        (options: DurableHostTickOptions)
        (activities: ActivityRegistry)
        basin
        (owned: OwnedKey)
        (mailboxReport: MailboxReport)
        (step: DurableHostStatus<'a>)
        (reportAfterStep: DurableHostTickReport<'a>)
        =
        async {
            let! activity =
                ActivityCommandAdapter.runOnce
                    StepRecordCodec.encode
                    StepRecordCodec.decode
                    options.MaxActivityCommands
                    options.MaxConcurrentActivities
                    activities
                    owned

            match activity with
            | ActivityCommandAdapterStatus.Deposed expected ->
                return DurableHostTickStatus.Deposed(expected, reportAfterStep)
            | ActivityCommandAdapterStatus.Failed failure ->
                return DurableHostTickStatus.Failed(DurableHostTickFailure.ActivityFailed failure, reportAfterStep)
            | ActivityCommandAdapterStatus.Processed activityReport ->
                let reportAfterActivities =
                    { reportAfterStep with
                        Activities = Some activityReport }

                let! timer =
                    TimerCommandAdapter.runOnce StepRecordCodec.decode options.Timestamp options.MaxTimerCommands owned

                match timer with
                | TimerCommandAdapterStatus.Deposed expected ->
                    return DurableHostTickStatus.Deposed(expected, reportAfterActivities)
                | TimerCommandAdapterStatus.Failed failure ->
                    return
                        DurableHostTickStatus.Failed(DurableHostTickFailure.TimerFailed failure, reportAfterActivities)
                | TimerCommandAdapterStatus.Processed timerReport ->
                    let reportAfterTimers =
                        { reportAfterActivities with
                            Timers = Some timerReport }

                    let! childStart =
                        ChildStartAdapter.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            options.MaxDispatchCommands
                            basin
                            owned

                    match childStart with
                    | ChildStartAdapterStatus.Deposed expected ->
                        return DurableHostTickStatus.Deposed(expected, reportAfterTimers)
                    | ChildStartAdapterStatus.Failed failure ->
                        return
                            DurableHostTickStatus.Failed(
                                DurableHostTickFailure.ChildStartFailed failure,
                                reportAfterTimers
                            )
                    | ChildStartAdapterStatus.Processed childStartReport ->
                        let report =
                            { reportAfterTimers with
                                ChildStarts = Some childStartReport }

                        match step with
                        | DurableHostStatus.Waiting(opId, need) when
                            not (mailboxMadeProgress mailboxReport)
                            && not (activitiesMadeProgress activityReport)
                            && not (timersMadeProgress timerReport)
                            && not (childStartsMadeProgress childStartReport)
                            ->
                            return DurableHostTickStatus.Waiting(opId, need, report)
                        | _ -> return DurableHostTickStatus.Advanced report
        }

    /// A completed instance with a WorkflowParent binding must deliver its
    /// terminal result to the parent: commit the DeliverChildResult command
    /// once (fenced), then dispatch it (checkpointed, receiver-deduped). Both
    /// halves are idempotent, so a kill anywhere is recovered by the next tick.
    let private deliverChildResult options basin (owned: OwnedKey) value report =
        async {
            let! ensured = ChildResultAdapter.ensureCommand StepRecordCodec.encode StepRecordCodec.decode owned value

            match ensured with
            | ChildResultCommandStatus.Deposed expected -> return Error(DurableHostTickStatus.Deposed(expected, report))
            | ChildResultCommandStatus.Failed failure ->
                return Error(DurableHostTickStatus.Failed(DurableHostTickFailure.ChildResultFailed failure, report))
            | ChildResultCommandStatus.NotChild -> return Ok report
            | ChildResultCommandStatus.AlreadyCommitted
            | ChildResultCommandStatus.Committed _ ->
                let! dispatched =
                    ChildResultAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        options.MaxDispatchCommands
                        basin
                        owned

                match dispatched with
                | ChildResultAdapterStatus.Deposed expected ->
                    return Error(DurableHostTickStatus.Deposed(expected, report))
                | ChildResultAdapterStatus.Failed failure ->
                    return
                        Error(DurableHostTickStatus.Failed(DurableHostTickFailure.ChildResultFailed failure, report))
                | ChildResultAdapterStatus.Processed childResultReport ->
                    return
                        Ok
                            { report with
                                ChildResults = Some childResultReport }
        }

    /// A rolled-over instance must dispatch the next generation's deduped
    /// start before reporting terminal ContinuedAsNew.
    let private dispatchNextGeneration options basin (owned: OwnedKey) report =
        async {
            let! generation =
                GenerationAdapter.runOnce
                    StepRecordCodec.encode
                    StepRecordCodec.decode
                    options.MaxDispatchCommands
                    basin
                    owned

            match generation with
            | GenerationAdapterStatus.Deposed expected -> return Error(DurableHostTickStatus.Deposed(expected, report))
            | GenerationAdapterStatus.Failed failure ->
                return Error(DurableHostTickStatus.Failed(DurableHostTickFailure.GenerationFailed failure, report))
            | GenerationAdapterStatus.Processed generationReport ->
                return
                    Ok
                        { report with
                            Generations = Some generationReport }
        }

    let runOwnedTick options activities basin (owned: OwnedKey) program =
        async {
            let initial = emptyTickReport owned

            let! inbox =
                Mailbox.runOnce
                    StepRecordCodec.encode
                    StepRecordCodec.decode
                    MailboxEnvelopeCodec.decode
                    options.MaxMailboxRecords
                    owned

            match inbox with
            | MailboxStatus.Deposed expected -> return DurableHostTickStatus.Deposed(expected, initial)
            | MailboxStatus.Failed failure ->
                return DurableHostTickStatus.Failed(DurableHostTickFailure.MailboxFailed failure, initial)
            | MailboxStatus.Folded mailboxReport ->
                let reportAfterInbox =
                    { initial with
                        Inbox = Some mailboxReport }

                let! step = stepOnce StepRecordCodec.encode StepRecordCodec.decode options.Timestamp owned program

                let reportAfterStep =
                    { reportAfterInbox with
                        Step = Some step }

                match step with
                | DurableHostStatus.Deposed expected -> return DurableHostTickStatus.Deposed(expected, reportAfterStep)
                | DurableHostStatus.Failed failure ->
                    return DurableHostTickStatus.Failed(DurableHostTickFailure.StepFailed failure, reportAfterStep)
                | DurableHostStatus.Completed value ->
                    let! delivered = deliverChildResult options basin owned value reportAfterStep

                    match delivered with
                    | Error status -> return status
                    | Ok report -> return DurableHostTickStatus.Completed(value, report)
                | DurableHostStatus.ContinuedAsNew nextInput ->
                    let! dispatched = dispatchNextGeneration options basin owned reportAfterStep

                    match dispatched with
                    | Error status -> return status
                    | Ok report -> return DurableHostTickStatus.ContinuedAsNew(nextInput, report)
                | DurableHostStatus.Committed _ ->
                    return! runAdapters options activities basin owned mailboxReport step reportAfterStep
                | DurableHostStatus.Waiting _ ->
                    let! signal = tryDeliverSignal owned step

                    match signal with
                    | SignalDeliveryStatus.Delivered signalReport ->
                        return
                            DurableHostTickStatus.Advanced
                                { reportAfterStep with
                                    Signals = Some signalReport }
                    | SignalDeliveryStatus.NotAvailable signalReport ->
                        return!
                            runAdapters
                                options
                                activities
                                basin
                                owned
                                mailboxReport
                                step
                                { reportAfterStep with
                                    Signals = Some signalReport }
                    | SignalDeliveryStatus.Deposed(expected, signalReport) ->
                        return
                            DurableHostTickStatus.Deposed(
                                expected,
                                { reportAfterStep with
                                    Signals = Some signalReport }
                            )
                    | SignalDeliveryStatus.Failed(failure, signalReport) ->
                        return
                            DurableHostTickStatus.Failed(
                                DurableHostTickFailure.SignalFailed failure,
                                { reportAfterStep with
                                    Signals = Some signalReport }
                            )
        }

    let claimAndRunTick options activities basin pair program =
        async {
            let! owned = S2Substrate.claim options.HostId pair
            return! runOwnedTick options activities basin owned program
        }

    let private startedWorkflow decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned

                let rec loop =
                    function
                    | [] -> Ok None
                    | (seqNum, Error error) :: _ -> Error(DurableWorkflowHostFailure.DecodeFailed(seqNum, error))
                    | (_, Ok(Incoming(WorkflowStarted(name, input)))) :: _ -> Ok(Some(name, input))
                    | _ :: rest -> loop rest

                return loop decoded
            with error ->
                return Error(DurableWorkflowHostFailure.LogReadFailed error.Message)
        }

    let runWorkflowTick options workflows activities basin owned =
        async {
            let! startFold =
                Mailbox.runOnce
                    StepRecordCodec.encode
                    StepRecordCodec.decode
                    MailboxEnvelopeCodec.decode
                    options.MaxMailboxRecords
                    owned

            match startFold with
            | MailboxStatus.Deposed expected -> return DurableWorkflowHostStatus.Deposed expected
            | MailboxStatus.Failed failure ->
                return DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.MailboxFoldFailed failure)
            | MailboxStatus.Folded _ ->
                let! started = startedWorkflow StepRecordCodec.decode owned

                match started with
                | Error failure -> return DurableWorkflowHostStatus.Failed failure
                | Ok None -> return DurableWorkflowHostStatus.Failed DurableWorkflowHostFailure.NoStart
                | Ok(Some(workflowName, input)) ->
                    match WorkflowRegistry.require (WorkflowName.value workflowName) workflows with
                    | Error(DurableRegistryError.WorkflowNotFound missing) ->
                        return DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.WorkflowNotFound missing)
                    | Error error ->
                        return DurableWorkflowHostStatus.Failed(DurableWorkflowHostFailure.LogReadFailed(string error))
                    | Ok factory ->
                        let! tick = runOwnedTick options activities basin owned (factory input)
                        return DurableWorkflowHostStatus.Ticked tick
        }

    let claimAndRunWorkflowTick options workflows activities basin pair =
        async {
            let! owned = S2Substrate.claim options.HostId pair
            return! runWorkflowTick options workflows activities basin owned
        }
