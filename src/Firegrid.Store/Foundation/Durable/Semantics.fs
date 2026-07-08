namespace Firegrid.Store.Foundation.Durable

[<Struct>]
type OpId = OpId of int

type Value = string

type Activity = { Name: string; Input: Value }

type EventKey =
    | Timer of deadline: int64
    | Signal of name: string

type RaceTask =
    | RaceActivity of Activity
    | RaceEvent of EventKey

type RaceResult =
    | ActivityWon of index: int * value: Value
    | EventWon of index: int * key: EventKey * value: Value

type Durable<'a> =
    | Return of 'a
    | Perform of Activity * k: (Value -> Durable<'a>)
    | PerformAll of Activity list * k: (Value list -> Durable<'a>)
    | Await of EventKey * k: (Value -> Durable<'a>)
    | WhenAny of RaceTask list * k: (RaceResult -> Durable<'a>)
    | CurrentTime of k: (int64 -> Durable<'a>)
    | Log of message: string * k: (unit -> Durable<'a>)
    | Send of Activity * k: (unit -> Durable<'a>)
    | PerformChild of workflow: string * input: Value * k: (Value -> Durable<'a>)
    | ContinueAsNew of nextInput: Value

type Event =
    | ActivityCalled of OpId * Activity
    | ActivityCompleted of OpId * Value
    | CurrentTimeRecorded of OpId * timestamp: int64
    | LogEmitted of OpId * message: string
    | TimerCreated of OpId * deadline: int64
    | TimerFired of OpId
    | TimerCanceled of OpId
    | SignalReceived of OpId * name: string * payload: Value
    | ActivitySent of OpId * Activity
    | ChildWorkflowCalled of OpId * workflow: string * input: Value
    | ChildWorkflowCompleted of OpId * output: Value
    | WorkflowContinuedAsNew of OpId * nextInput: Value

type History = private History of Event list

type Need =
    | NeedsActivity of Activity
    | NeedsActivities of (OpId * Activity) list
    | NeedsEvent of EventKey
    | NeedsRace of (OpId * RaceTask) list
    | NeedsTimerCancellation of OpId list
    | NeedsCurrentTime
    | NeedsLog of message: string
    | NeedsSend of Activity
    | NeedsChildWorkflow of workflow: string * input: Value
    | NeedsRollover of nextInput: Value

type Outcome<'a> =
    | Done of 'a
    | Blocked of OpId * Need
    | ContinuedAsNew of nextInput: Value

[<RequireQualifiedAccess>]
module OpId =
    let zero = OpId 0

    let add offset (OpId value) = OpId(value + offset)

    let next (OpId value) = OpId(value + 1)

[<RequireQualifiedAccess>]
module History =
    let empty = History []

    let ofList events = History events

    let toList (History events) = events

    let append event (History events) = History(events @ [ event ])

    let completed opId (History events) =
        events
        |> List.tryPick (function
            | ActivityCompleted(id, value) when id = opId -> Some value
            | _ -> None)

    let currentTime opId (History events) =
        events
        |> List.tryPick (function
            | CurrentTimeRecorded(id, timestamp) when id = opId -> Some timestamp
            | _ -> None)

    let logEmitted opId message (History events) =
        events
        |> List.exists (function
            | LogEmitted(id, logged) when id = opId && logged = message -> true
            | _ -> false)

    let resolved opId key (History events) =
        match key with
        | Timer _ ->
            events
            |> List.tryPick (function
                | TimerFired id when id = opId -> Some ""
                | _ -> None)
        | Signal name ->
            events
            |> List.tryPick (function
                | SignalReceived(id, signalName, payload) when id = opId && signalName = name -> Some payload
                | _ -> None)

    let timerCanceled opId (History events) =
        events
        |> List.exists (function
            | TimerCanceled id when id = opId -> true
            | _ -> false)

    let sent opId (History events) =
        events
        |> List.exists (function
            | ActivitySent(id, _) when id = opId -> true
            | _ -> false)

    let childCompleted opId (History events) =
        events
        |> List.tryPick (function
            | ChildWorkflowCompleted(id, output) when id = opId -> Some output
            | _ -> None)

    let continuedAsNew opId (History events) =
        events
        |> List.exists (function
            | WorkflowContinuedAsNew(id, _) when id = opId -> true
            | _ -> false)

    let raceWinner (tasks: (int * OpId * RaceTask) list) (History events) =
        let tryMatch event =
            tasks
            |> List.tryPick (fun (index, opId, task) ->
                match task, event with
                | RaceActivity _, ActivityCompleted(id, value) when id = opId -> Some(ActivityWon(index, value))
                | RaceEvent(Timer deadline), TimerFired id when id = opId -> Some(EventWon(index, Timer deadline, ""))
                | RaceEvent(Signal name), SignalReceived(id, signalName, payload) when id = opId && signalName = name ->
                    Some(EventWon(index, Signal name, payload))
                | _ -> None)

        events |> List.tryPick tryMatch

[<RequireQualifiedAccess>]
module Durable =
    let result value = Return value

    let bind binder program =
        let rec loop =
            function
            | Return value -> binder value
            | Perform(activity, k) -> Perform(activity, k >> loop)
            | PerformAll(activities, k) -> PerformAll(activities, k >> loop)
            | Await(key, k) -> Await(key, k >> loop)
            | WhenAny(tasks, k) -> WhenAny(tasks, k >> loop)
            | CurrentTime k -> CurrentTime(k >> loop)
            | Log(message, k) -> Log(message, k >> loop)
            | Send(activity, k) -> Send(activity, k >> loop)
            | PerformChild(workflow, input, k) -> PerformChild(workflow, input, k >> loop)
            // ContinueAsNew is terminal: nothing bound after it ever runs.
            | ContinueAsNew nextInput -> ContinueAsNew nextInput

        loop program

    let map mapper program = bind (mapper >> result) program

    let perform activity = Perform(activity, Return)

    let performAll activities = PerformAll(activities, Return)

    let await key = Await(key, Return)

    let whenAny tasks = WhenAny(tasks, Return)

    let currentTime = CurrentTime Return

    let log message = Log(message, Return)

    let send activity = Send(activity, Return)

    let performChild workflow input = PerformChild(workflow, input, Return)

    let continueAsNew nextInput = ContinueAsNew nextInput

    let replay history program =
        let rec loop opId current =
            match current with
            | Return value -> Done value
            | Perform(activity, k) ->
                match History.completed opId history with
                | Some value -> loop (OpId.next opId) (k value)
                | None -> Blocked(opId, NeedsActivity activity)
            | PerformAll(activities, k) ->
                let pending =
                    activities |> List.mapi (fun index activity -> OpId.add index opId, activity)

                let completed =
                    pending
                    |> List.map (fun (id, activity) -> id, activity, History.completed id history)

                let missing =
                    completed
                    |> List.choose (fun (id, activity, value) ->
                        match value with
                        | Some _ -> None
                        | None -> Some(id, activity))

                if List.isEmpty missing then
                    let values = completed |> List.choose (fun (_, _, value) -> value)
                    loop (OpId.add activities.Length opId) (k values)
                else
                    Blocked(opId, NeedsActivities missing)
            | Await(key, k) ->
                match History.resolved opId key history with
                | Some value -> loop (OpId.next opId) (k value)
                | None -> Blocked(opId, NeedsEvent key)
            | WhenAny(tasks, k) ->
                let pending =
                    tasks |> List.mapi (fun index task -> index, OpId.add index opId, task)

                match History.raceWinner pending history with
                | None ->
                    pending
                    |> List.map (fun (_, id, task) -> id, task)
                    |> NeedsRace
                    |> fun need -> Blocked(opId, need)
                | Some winner ->
                    let winnerIndex =
                        match winner with
                        | ActivityWon(index, _) -> index
                        | EventWon(index, _, _) -> index

                    let uncanceledTimers =
                        pending
                        |> List.choose (fun (index, id, task) ->
                            match task with
                            | RaceEvent(Timer _) when index <> winnerIndex && not (History.timerCanceled id history) ->
                                Some id
                            | _ -> None)

                    if List.isEmpty uncanceledTimers then
                        loop (OpId.add tasks.Length opId) (k winner)
                    else
                        Blocked(opId, NeedsTimerCancellation uncanceledTimers)
            | CurrentTime k ->
                match History.currentTime opId history with
                | Some timestamp -> loop (OpId.next opId) (k timestamp)
                | None -> Blocked(opId, NeedsCurrentTime)
            | Log(message, k) ->
                if History.logEmitted opId message history then
                    loop (OpId.next opId) (k ())
                else
                    Blocked(opId, NeedsLog message)
            | Send(activity, k) ->
                // Fire-and-forget: replay proceeds once the send is journaled;
                // it never waits for (and never observes) the handler's result.
                if History.sent opId history then
                    loop (OpId.next opId) (k ())
                else
                    Blocked(opId, NeedsSend activity)
            | PerformChild(workflow, input, k) ->
                match History.childCompleted opId history with
                | Some output -> loop (OpId.next opId) (k output)
                | None -> Blocked(opId, NeedsChildWorkflow(workflow, input))
            | ContinueAsNew nextInput ->
                // Terminal: once the rollover is journaled the generation is over.
                if History.continuedAsNew opId history then
                    ContinuedAsNew nextInput
                else
                    Blocked(opId, NeedsRollover nextInput)

        loop OpId.zero program
