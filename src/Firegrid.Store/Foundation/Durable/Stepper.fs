namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log

type StepCommand =
    | CallActivity of OpId * Activity
    | ScheduleTimer of OpId * deadline: int64
    | CancelTimer of OpId
    | WriteLog of OpId * message: string
    | SendActivity of OpId * Activity
    | CallChildWorkflow of OpId * workflow: string * input: Payload
    | DeliverChildResult of parent: string * parentOpId: OpId * output: Payload
    | StartNextGeneration of OpId * nextInput: Payload

type MailboxMessage =
    | StartWorkflow of WorkflowName * Payload
    | RaiseSignal of name: string * payload: Payload
    | CompleteActivity of OpId * Payload
    | FireTimer of OpId * deadline: int64
    | StartChildWorkflow of WorkflowName * input: Payload * parent: string * parentOpId: OpId
    | CompleteChild of OpId * output: Payload
    | AckSend of OpId

type MailboxEnvelope =
    { Source: string
      SourceSeqNum: int64
      Message: MailboxMessage }

type StepRecord =
    | WorkflowStarted of WorkflowName * Payload
    | WorkflowParent of parent: string * parentOpId: OpId
    | HistoryEvent of Event
    | Command of StepCommand
    | SignalDelivered of source: string * sourceSeqNum: int64 * opId: OpId
    | CommandDispatchCheckpoint of dispatcher: string * nextSeqNum: int64
    | MailboxCheckpoint of nextSeqNum: int64
    | MailboxSourceHighwater of source: string * nextSeqNum: int64
    | MailboxMessageAccepted of MailboxEnvelope

type StepPlan<'a> =
    | Complete of 'a
    | Commit of HistoryEntry<StepRecord> list
    | Waiting of OpId * Need
    | Continued of nextInput: Payload

type StepCommitResult =
    | StepCommitted of S2.AppendAck
    | StepNotRequired
    | StepDeposed of expectedFence: string
    | StepCommitFailed of S2Errors.S2Failure

[<RequireQualifiedAccess>]
module DurableStepper =
    let private events history = History.toList history

    let private hasActivityCall opId activity history =
        events history
        |> List.exists (function
            | ActivityCalled(id, called) when id = opId && called = activity -> true
            | _ -> false)

    let private hasTimerCreated opId deadline history =
        events history
        |> List.exists (function
            | TimerCreated(id, created) when id = opId && created = deadline -> true
            | _ -> false)

    let private hasCurrentTime opId history =
        History.currentTime opId history |> Option.isSome

    let private hasLog opId message history = History.logEmitted opId message history

    let private hasTimerCanceled opId history = History.timerCanceled opId history

    let private hasActivitySent opId activity history =
        events history
        |> List.exists (function
            | ActivitySent(id, sent) when id = opId && sent = activity -> true
            | _ -> false)

    let private hasChildCall opId workflow input history =
        events history
        |> List.exists (function
            | ChildWorkflowCalled(id, calledWorkflow, calledInput) when
                id = opId && calledWorkflow = workflow && calledInput = input
                ->
                true
            | _ -> false)

    let private hasContinuedAsNew opId history = History.continuedAsNew opId history

    let private history event = Incoming(HistoryEvent event)

    let private command cmd = Outgoing(Command cmd)

    let private activityRecords opId activity =
        [ history (ActivityCalled(opId, activity))
          command (CallActivity(opId, activity)) ]

    let private timerRecords opId deadline =
        [ history (TimerCreated(opId, deadline))
          command (ScheduleTimer(opId, deadline)) ]

    let private cancelTimerRecords opId =
        [ history (TimerCanceled opId); command (CancelTimer opId) ]

    let private currentTimeRecords opId timestamp =
        [ history (CurrentTimeRecorded(opId, timestamp)) ]

    let private logRecords opId message =
        [ history (LogEmitted(opId, message)); command (WriteLog(opId, message)) ]

    let private sendRecords opId activity =
        [ history (ActivitySent(opId, activity))
          command (SendActivity(opId, activity)) ]

    let private childRecords opId workflow input =
        [ history (ChildWorkflowCalled(opId, workflow, input))
          command (CallChildWorkflow(opId, workflow, input)) ]

    let private rolloverRecords opId nextInput =
        [ history (WorkflowContinuedAsNew(opId, nextInput))
          command (StartNextGeneration(opId, nextInput)) ]

    let private missingForTask history (opId, task) =
        match task with
        | RaceActivity activity ->
            if hasActivityCall opId activity history then
                []
            else
                activityRecords opId activity
        | RaceEvent(Timer deadline) ->
            if hasTimerCreated opId deadline history then
                []
            else
                timerRecords opId deadline
        | RaceEvent(Signal _) -> []

    let private missingForNeed timestamp history opId need =
        match need with
        | NeedsActivity requested ->
            if hasActivityCall opId requested history then
                []
            else
                activityRecords opId requested
        | NeedsActivities pending ->
            pending
            |> List.collect (fun (id, requested) ->
                if hasActivityCall id requested history then
                    []
                else
                    activityRecords id requested)
        | NeedsEvent(Timer deadline) ->
            if hasTimerCreated opId deadline history then
                []
            else
                timerRecords opId deadline
        | NeedsEvent(Signal _) -> []
        | NeedsRace pending -> pending |> List.collect (missingForTask history)
        | NeedsTimerCancellation timers ->
            timers
            |> List.collect (fun timerId ->
                if hasTimerCanceled timerId history then
                    []
                else
                    cancelTimerRecords timerId)
        | NeedsCurrentTime ->
            if hasCurrentTime opId history then
                []
            else
                currentTimeRecords opId timestamp
        | NeedsLog message ->
            if hasLog opId message history then
                []
            else
                logRecords opId message
        | NeedsSend requested ->
            if hasActivitySent opId requested history then
                []
            else
                sendRecords opId requested
        | NeedsChildWorkflow(workflow, input) ->
            if hasChildCall opId workflow input history then
                []
            else
                childRecords opId workflow input
        | NeedsRollover nextInput ->
            if hasContinuedAsNew opId history then
                []
            else
                rolloverRecords opId nextInput

    let plan timestamp history program =
        match Durable.replay history program with
        | Done value -> Complete value
        | ContinuedAsNew nextInput -> Continued nextInput
        | Blocked(opId, need) ->
            match missingForNeed timestamp history opId need with
            | [] -> Waiting(opId, need)
            | records -> Commit records

    let historyFromRecords records =
        records
        |> List.choose (function
            | Incoming(HistoryEvent event) -> Some event
            | _ -> None)
        |> History.ofList

    let commit encode owned plan =
        async {
            match plan with
            | Commit records ->
                let! result = S2Substrate.commitText encode records owned

                return
                    match result with
                    | Committed ack -> StepCommitted ack
                    | Deposed expected -> StepDeposed expected
                    | CommitFailed failure -> StepCommitFailed failure
            | Complete _
            | Waiting _
            | Continued _ -> return StepNotRequired
        }
