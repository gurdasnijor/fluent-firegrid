namespace Firegrid.Runtime

open System.Collections.Generic
open Fable.Core
open Firegrid.Core

/// In-memory `WorkflowExecutionStore` (`in-memory-store.ts`).
[<RequireQualifiedAccess>]
module InMemoryStore =

    type private Subscriber = WorkflowEvent -> int -> unit

    type private TimerRecord =
        { Wakeup: TimerWakeup
          Lease: WorkflowLease option }

    type private ScheduleRecord =
        { ScheduleId: ScheduleId
          WorkflowId: WorkflowId
          WorkflowVersion: WorkflowVersion option
          NextFireAt: float option
          Input: obj
          OverlapPolicy: WorkflowOverlapPolicy
          Enabled: bool }

    type private ScheduleBucketRecord =
        { Bucket: ScheduleBucket
          Status: string // "claimed" | "started"
          Lease: WorkflowLease option }

    // ── clone helpers (faithful to the TS spreads) ────────────────────

    let private cloneRecord (value: WorkflowMetadata option) : WorkflowMetadata option =
        match value with
        | Some v -> Some(RuntimeSdk.shallowClone v)
        | None -> value

    let private cloneAwaitable (a: RunAwaitable) : RunAwaitable =
        match a with
        | AwaitSignal(stepId, signalName, deadline, meta) -> AwaitSignal(stepId, signalName, deadline, cloneRecord meta)
        | AwaitApproval(stepId, approvalId, title, description, meta) ->
            AwaitApproval(stepId, approvalId, title, description, cloneRecord meta)

    let private cloneAwaiting (value: RunAwaitable[] option) : RunAwaitable[] option =
        match value with
        | Some arr -> Some(Array.map cloneAwaitable arr)
        | None -> value

    let private cloneWaitingFor (value: WaitingFor option) : WaitingFor option =
        match value with
        | Some w -> Some { w with Meta = cloneRecord w.Meta }
        | None -> None

    let private clonePendingApproval (value: PendingApproval option) : PendingApproval option =
        match value with
        | Some p -> Some { p with Meta = cloneRecord p.Meta }
        | None -> None

    let private cloneLease (l: WorkflowLease) : WorkflowLease = { l with Owner = l.Owner }

    let private cloneRun (run: WorkflowExecution) : WorkflowExecution =
        { run with
            Awaiting = cloneAwaiting run.Awaiting
            WaitingFor = cloneWaitingFor run.WaitingFor
            PendingApproval = clonePendingApproval run.PendingApproval
            Lease =
                match run.Lease with
                | Some l -> Some(cloneLease l)
                | None -> None }

    let private cloneRunState (state: RunState) : RunState =
        { state with
            Awaiting = cloneAwaiting state.Awaiting
            WaitingFor = cloneWaitingFor state.WaitingFor
            PendingApproval = clonePendingApproval state.PendingApproval }

    let private cloneStoredEvent (e: StoredWorkflowEvent) : StoredWorkflowEvent = { e with RunId = e.RunId }

    let private cloneStoredEvents (events: StoredWorkflowEvent[]) : StoredWorkflowEvent[] =
        Array.map cloneStoredEvent events

    let private cloneTimerWakeup (t: TimerWakeup) : TimerWakeup = { t with RunId = t.RunId }

    let private cloneScheduleBucket (b: ScheduleBucket) : ScheduleBucket = { b with RunId = b.RunId }

    // ── lease / status predicates ─────────────────────────────────────

    let private lease (owner: LeaseOwner) (leaseMs: float) (now: float) : WorkflowLease =
        { Owner = owner; ExpiresAt = now + leaseMs }

    let private canClaim (existing: WorkflowLease option) (owner: LeaseOwner) (now: float) : bool =
        match existing with
        | None -> true
        | Some l -> l.Owner = owner || l.ExpiresAt <= now

    let private isTerminal (status: WorkflowExecutionStatus) : bool =
        status = "finished" || status = "errored" || status = "aborted"

    // ── signal / approval matching ────────────────────────────────────

    let private waitingForSignalMatches (w: WaitingFor) (delivery: SignalDelivery) : bool =
        w.SignalName = delivery.Name
        && (delivery.StepId = None
            || w.StepId = None
            || w.StepId = delivery.StepId)

    let private awaitSignalMatches (a: RunAwaitable) (delivery: SignalDelivery) : bool =
        match a with
        | AwaitSignal(stepId, signalName, _, _) ->
            signalName = delivery.Name
            && (delivery.StepId = None || stepId = None || stepId = delivery.StepId)
        | _ -> false

    let private isRunWaitingForSignal (run: WorkflowExecution) (delivery: SignalDelivery) : bool =
        let waitingForMatches =
            match run.WaitingFor with
            | Some w -> waitingForSignalMatches w delivery
            | None -> false

        let awaitingMatches =
            match run.Awaiting with
            | Some arr -> arr |> Array.exists (fun a -> awaitSignalMatches a delivery)
            | None -> false

        waitingForMatches || awaitingMatches

    let private isRunWaitingForApproval (run: WorkflowExecution) (approval: ApprovalResult) : bool =
        let pendingMatches =
            match run.PendingApproval with
            | Some p -> p.ApprovalId = approval.ApprovalId
            | None -> false

        let awaitingMatches =
            match run.Awaiting with
            | Some arr ->
                arr
                |> Array.exists (fun a ->
                    match a with
                    | AwaitApproval(_, approvalId, _, _, _) -> approvalId = approval.ApprovalId
                    | _ -> false)
            | None -> false

        pendingMatches || awaitingMatches

    // ── key helpers ───────────────────────────────────────────────────

    let private timerKey (runId: RunId) (signalId: string) : string = sprintf "%s:%s" runId signalId
    let private signalKey (runId: RunId) (signalId: string) : string = sprintf "%s:%s" runId signalId

    let private scheduleBucketKey (scheduleId: ScheduleId) (bucketId: ScheduleBucketId) : string =
        sprintf "%s:%s" scheduleId bucketId

    // ── projections ───────────────────────────────────────────────────

    let private toRunSummary (run: WorkflowExecution) : RunSummary =
        { RunId = run.RunId
          WorkflowId = run.WorkflowId
          WorkflowVersion = run.WorkflowVersion
          Status = run.Status
          Awaiting = run.Awaiting
          WaitingFor = run.WaitingFor
          PendingApproval = run.PendingApproval
          WakeAt = run.WakeAt
          CreatedAt = run.CreatedAt
          UpdatedAt = run.UpdatedAt }

    let private executionFromRunState (state: RunState) (leaseValue: WorkflowLease option) : WorkflowExecution =
        let wakeAt =
            match state.WaitingFor with
            | Some w when w.SignalName = "__timer" -> w.Deadline
            | _ -> None

        { RunId = state.RunId
          WorkflowId = state.WorkflowId
          WorkflowVersion = state.WorkflowVersion
          Status = state.Status
          Input = state.Input
          Output = state.Output
          Error = state.Error
          Awaiting = state.Awaiting
          WaitingFor = state.WaitingFor
          PendingApproval = state.PendingApproval
          WakeAt = wakeAt
          Lease =
            match leaseValue with
            | Some l -> Some(cloneLease l)
            | None -> None
          CreatedAt = state.CreatedAt
          UpdatedAt = state.UpdatedAt }

    let private getStepId (event: WorkflowEvent) : string option = WorkflowEvent.eventStepId event

    let private eventTs (event: WorkflowEvent) : float =
        // Every event carries a `ts`; reuse the serialized form to read it.
        RuntimeSdk.prop<float> (WorkflowEvent.toObj event) "ts"

    let private storeEvent (runId: RunId) (eventIndex: int) (event: WorkflowEvent) : StoredWorkflowEvent =
        { RunId = runId
          EventIndex = float eventIndex
          EventType = WorkflowEvent.eventType event
          StepId = getStepId event
          Event = event
          CreatedAt = eventTs event }

    let private publish (subscribers: Dictionary<RunId, HashSet<Subscriber>>) (runId: RunId) (event: WorkflowEvent) (index: int) =
        match subscribers.TryGetValue runId with
        | true, subs ->
            for subscriber in List.ofSeq subs do
                try
                    subscriber event index
                with _ ->
                    () // Subscriber errors must not break persistence.
        | _ -> ()

    /// `inMemoryWorkflowExecutionStore()` — returns the combined store.
    let create () : WorkflowExecutionStore =
        let runs = Dictionary<RunId, WorkflowExecution>()
        let runStates = Dictionary<RunId, RunState>()
        let logs = Dictionary<RunId, ResizeArray<StoredWorkflowEvent>>()
        let timers = Dictionary<string, TimerRecord>()
        let signalDeliveries = Dictionary<string, bool>()
        let schedules = Dictionary<ScheduleId, ScheduleRecord>()
        let scheduleBuckets = Dictionary<string, ScheduleBucketRecord>()
        let subscribers = Dictionary<RunId, HashSet<Subscriber>>()

        let setRun (run: WorkflowExecution) = runs[run.RunId] <- cloneRun run

        let getRun (runId: RunId) : WorkflowExecution option =
            match runs.TryGetValue runId with
            | true, run -> Some(cloneRun run)
            | _ -> None

        let updateRun (runId: RunId) (updater: WorkflowExecution -> WorkflowExecution) : WorkflowExecution option =
            match runs.TryGetValue runId with
            | true, existing ->
                let next = updater (cloneRun existing)
                setRun next
                Some(cloneRun next)
            | _ -> None

        let getLog (runId: RunId) : ResizeArray<StoredWorkflowEvent> =
            match logs.TryGetValue runId with
            | true, l -> l
            | _ -> ResizeArray<StoredWorkflowEvent>()

        { LoadRunState =
            fun runId ->
                let result =
                    match runStates.TryGetValue runId with
                    | true, state -> Some(cloneRunState state)
                    | _ -> None

                RuntimeSdk.promiseResolve result

          SaveRunState =
            fun args ->
                let state = cloneRunState args.State
                runStates[state.RunId] <- state

                let existingLease =
                    match runs.TryGetValue state.RunId with
                    | true, r -> r.Lease
                    | _ -> None

                setRun (executionFromRunState state existingLease)
                RuntimeSdk.promiseResolveUnit ()

          DeleteRun =
            fun runId _reason ->
                runs.Remove runId |> ignore
                runStates.Remove runId |> ignore
                logs.Remove runId |> ignore
                subscribers.Remove runId |> ignore

                for kv in List.ofSeq timers do
                    if kv.Value.Wakeup.RunId = runId then
                        timers.Remove kv.Key |> ignore

                for key in List.ofSeq signalDeliveries.Keys do
                    if RuntimeSdk.startsWith key (sprintf "%s:" runId) then
                        signalDeliveries.Remove key |> ignore

                RuntimeSdk.promiseResolveUnit ()

          AppendEvents =
            fun args ->
                let log = getLog args.RunId

                if float log.Count <> args.ExpectedNextIndex then
                    let existing =
                        let idx = int args.ExpectedNextIndex

                        if idx >= 0 && idx < log.Count then
                            Some(WorkflowEvent.toObj log[idx].Event)
                        else
                            None

                    promise { return raise (LogConflictError.create args.RunId args.ExpectedNextIndex existing) }
                else
                    for event in args.Events do
                        let stored = storeEvent args.RunId log.Count event
                        log.Add stored
                        publish subscribers args.RunId stored.Event (int stored.EventIndex)

                    logs[args.RunId] <- log
                    RuntimeSdk.promiseResolve { NextIndex = float log.Count }

          ReadEvents =
            fun args ->
                let fromIndex = args.FromIndex |> Option.defaultValue 0.0 |> int
                let log = getLog args.RunId

                let sliced =
                    if fromIndex <= 0 then log.ToArray()
                    elif fromIndex >= log.Count then [||]
                    else log.GetRange(fromIndex, log.Count - fromIndex).ToArray()

                RuntimeSdk.promiseResolve (cloneStoredEvents sliced)

          SubscribeEvents =
            Some(fun runId fromIndex onEvent ->
                let log = getLog runId

                for index in fromIndex .. log.Count - 1 do
                    let stored = log[index]
                    onEvent stored.Event (int stored.EventIndex)

                let runSubscribers =
                    match subscribers.TryGetValue runId with
                    | true, s -> s
                    | _ ->
                        let s = HashSet<Subscriber>()
                        subscribers[runId] <- s
                        s

                runSubscribers.Add onEvent |> ignore

                fun () ->
                    runSubscribers.Remove onEvent |> ignore

                    if runSubscribers.Count = 0 then
                        subscribers.Remove runId |> ignore)

          CreateRun =
            fun args ->
                match getRun args.RunId with
                | Some existing -> RuntimeSdk.promiseResolve (CreateRunExisting existing)
                | None ->
                    let run =
                        { RunId = args.RunId
                          WorkflowId = args.WorkflowId
                          WorkflowVersion = args.WorkflowVersion
                          Status = "queued"
                          Input = args.Input
                          Output = None
                          Error = None
                          Awaiting = None
                          WaitingFor = None
                          PendingApproval = None
                          WakeAt = None
                          Lease = None
                          CreatedAt = args.Now
                          UpdatedAt = args.Now }

                    setRun run
                    RuntimeSdk.promiseResolve (CreateRunCreated(cloneRun run))

          LoadRun = fun runId -> RuntimeSdk.promiseResolve (getRun runId)

          LoadExecution =
            fun runId ->
                match getRun runId with
                | None -> RuntimeSdk.promiseResolve None
                | Some run ->
                    let result =
                        { Run = run
                          Events = cloneStoredEvents ((getLog runId).ToArray()) }

                    RuntimeSdk.promiseResolve (Some result)

          ClaimRun =
            fun args ->
                match getRun args.RunId with
                | None -> RuntimeSdk.promiseResolve ClaimRunNotFound
                | Some existing ->
                    if isTerminal existing.Status then
                        RuntimeSdk.promiseResolve (ClaimRunNotClaimable existing)
                    elif not (canClaim existing.Lease args.LeaseOwner args.Now) then
                        RuntimeSdk.promiseResolve (ClaimRunNotClaimable existing)
                    else
                        let claimed =
                            updateRun args.RunId (fun run ->
                                { run with
                                    Status = "running"
                                    Lease = Some(lease args.LeaseOwner args.LeaseMs args.Now)
                                    UpdatedAt = args.Now })

                        RuntimeSdk.promiseResolve (ClaimRunClaimed claimed.Value)

          HeartbeatRunLease =
            fun args ->
                updateRun args.RunId (fun run ->
                    match run.Lease with
                    | Some l when l.Owner = args.LeaseOwner ->
                        { run with
                            Lease = Some(lease args.LeaseOwner args.LeaseMs args.Now)
                            UpdatedAt = args.Now }
                    | _ -> run)
                |> ignore

                RuntimeSdk.promiseResolveUnit ()

          ReleaseRunLease =
            fun args ->
                updateRun args.RunId (fun run ->
                    match run.Lease with
                    | Some l when l.Owner = args.LeaseOwner -> { run with Lease = None }
                    | _ -> run)
                |> ignore

                RuntimeSdk.promiseResolveUnit ()

          MarkRunPaused =
            fun args ->
                updateRun args.RunId (fun run ->
                    { run with
                        Status = "paused"
                        Awaiting = args.Awaiting
                        WaitingFor = args.WaitingFor
                        PendingApproval = args.PendingApproval
                        WakeAt = args.WakeAt
                        Lease = None
                        UpdatedAt = args.Now })
                |> ignore

                RuntimeSdk.promiseResolveUnit ()

          MarkRunFinished =
            fun args ->
                updateRun args.RunId (fun run ->
                    { run with
                        Status = "finished"
                        Output = Some args.Output
                        Awaiting = None
                        WaitingFor = None
                        PendingApproval = None
                        WakeAt = None
                        Lease = None
                        UpdatedAt = args.Now })
                |> ignore

                RuntimeSdk.promiseResolveUnit ()

          MarkRunErrored =
            fun args ->
                updateRun args.RunId (fun run ->
                    { run with
                        Status = "errored"
                        Error = Some args.Error
                        Awaiting = None
                        WaitingFor = None
                        PendingApproval = None
                        WakeAt = None
                        Lease = None
                        UpdatedAt = args.Now })
                |> ignore

                RuntimeSdk.promiseResolveUnit ()

          ScheduleTimer =
            fun args ->
                timers[timerKey args.RunId args.SignalId] <-
                    { Wakeup =
                        { RunId = args.RunId
                          WorkflowId = args.WorkflowId
                          WorkflowVersion = args.WorkflowVersion
                          WakeAt = args.WakeAt
                          SignalId = args.SignalId
                          SignalName = args.SignalName }
                      Lease = None }

                updateRun args.RunId (fun run ->
                    { run with
                        WakeAt = Some args.WakeAt
                        UpdatedAt = args.Now })
                |> ignore

                RuntimeSdk.promiseResolveUnit ()

          ClaimDueTimers =
            fun args ->
                let due = ResizeArray<TimerWakeup>()
                let mutable broke = false

                for kv in List.ofSeq timers do
                    if not broke then
                        if due.Count >= args.Limit then
                            broke <- true
                        else
                            let timer = kv.Value

                            if timer.Wakeup.WakeAt > args.Now then ()
                            elif not (canClaim timer.Lease args.LeaseOwner args.Now) then
                                ()
                            else
                                timers[kv.Key] <-
                                    { timer with Lease = Some(lease args.LeaseOwner args.LeaseMs args.Now) }

                                due.Add(cloneTimerWakeup timer.Wakeup)

                RuntimeSdk.promiseResolve (due.ToArray())

          DeliverSignal =
            fun args ->
                match getRun args.RunId with
                | None -> RuntimeSdk.promiseResolve DeliverSignalNotFound
                | Some run ->
                    let key = signalKey args.RunId args.Delivery.SignalId

                    if signalDeliveries.ContainsKey key then
                        RuntimeSdk.promiseResolve (DeliverSignalDuplicate run)
                    elif not (isRunWaitingForSignal run args.Delivery) then
                        RuntimeSdk.promiseResolve (DeliverSignalNotWaiting run)
                    else
                        signalDeliveries[key] <- true
                        timers.Remove(timerKey args.RunId args.Delivery.SignalId) |> ignore

                        let updated =
                            updateRun args.RunId (fun current ->
                                { current with
                                    Status = "queued"
                                    Awaiting = None
                                    WaitingFor = None
                                    PendingApproval = None
                                    WakeAt = None
                                    UpdatedAt = args.Now })

                        RuntimeSdk.promiseResolve (DeliverSignalDelivered updated.Value)

          DeliverApproval =
            fun args ->
                match getRun args.RunId with
                | None -> RuntimeSdk.promiseResolve DeliverApprovalNotFound
                | Some run ->
                    let key = signalKey args.RunId (sprintf "approval:%s" args.Approval.ApprovalId)

                    if signalDeliveries.ContainsKey key then
                        RuntimeSdk.promiseResolve (DeliverApprovalDuplicate run)
                    elif not (isRunWaitingForApproval run args.Approval) then
                        RuntimeSdk.promiseResolve (DeliverApprovalNotWaiting run)
                    else
                        signalDeliveries[key] <- true

                        let updated =
                            updateRun args.RunId (fun current ->
                                { current with
                                    Status = "queued"
                                    Awaiting = None
                                    WaitingFor = None
                                    PendingApproval = None
                                    WakeAt = None
                                    UpdatedAt = args.Now })

                        RuntimeSdk.promiseResolve (DeliverApprovalDelivered updated.Value)

          UpsertSchedule =
            fun args ->
                schedules[args.ScheduleId] <-
                    { ScheduleId = args.ScheduleId
                      WorkflowId = args.WorkflowId
                      WorkflowVersion = args.WorkflowVersion
                      NextFireAt = args.NextFireAt
                      Input = args.Input |> Option.defaultValue RuntimeSdk.undefinedValue
                      OverlapPolicy = args.OverlapPolicy
                      Enabled = args.Enabled }

                RuntimeSdk.promiseResolveUnit ()

          ClaimDueScheduleBuckets =
            fun args ->
                let due = ResizeArray<ScheduleBucket>()
                let mutable broke = false

                for schedule in List.ofSeq schedules.Values do
                    if not broke then
                        if due.Count >= args.Limit then
                            broke <- true
                        elif not schedule.Enabled || schedule.NextFireAt.IsNone then
                            ()
                        else
                            let nextFireAt = schedule.NextFireAt.Value

                            if nextFireAt > args.Now then
                                ()
                            else
                                let bucketId = sprintf "%g" nextFireAt
                                let key = scheduleBucketKey schedule.ScheduleId bucketId
                                let existing =
                                    match scheduleBuckets.TryGetValue key with
                                    | true, b -> Some b
                                    | _ -> None

                                let skip =
                                    match existing with
                                    | Some e when e.Status = "started" -> true
                                    | Some e when not (canClaim e.Lease args.LeaseOwner args.Now) -> true
                                    | _ -> false

                                if skip then
                                    ()
                                else
                                    let bucket =
                                        { ScheduleId = schedule.ScheduleId
                                          BucketId = bucketId
                                          WorkflowId = schedule.WorkflowId
                                          WorkflowVersion = schedule.WorkflowVersion
                                          RunId = sprintf "%s:%s:%s" schedule.WorkflowId schedule.ScheduleId bucketId
                                          FireAt = nextFireAt
                                          Input = schedule.Input
                                          OverlapPolicy = schedule.OverlapPolicy }

                                    scheduleBuckets[key] <-
                                        { Bucket = bucket
                                          Status = "claimed"
                                          Lease = Some(lease args.LeaseOwner args.LeaseMs args.Now) }

                                    due.Add(cloneScheduleBucket bucket)

                RuntimeSdk.promiseResolve (due.ToArray())

          MarkScheduleBucketStarted =
            fun args ->
                let key = scheduleBucketKey args.ScheduleId args.BucketId

                match scheduleBuckets.TryGetValue key with
                | true, bucket ->
                    scheduleBuckets[key] <-
                        { bucket with
                            Bucket = { bucket.Bucket with RunId = args.RunId }
                            Status = "started" }
                | _ -> ()

                RuntimeSdk.promiseResolveUnit ()

          ClaimStaleRuns =
            fun args ->
                let claims = ResizeArray<RunClaim>()
                let mutable broke = false

                for run in List.ofSeq runs.Values do
                    if not broke then
                        if claims.Count >= args.Limit then
                            broke <- true
                        elif run.Status <> "running" then
                            ()
                        else
                            match run.Lease with
                            | Some l when l.ExpiresAt <= args.Now ->
                                let nextLease = lease args.LeaseOwner args.LeaseMs args.Now

                                let claimed =
                                    updateRun run.RunId (fun current ->
                                        { current with
                                            Lease = Some nextLease
                                            UpdatedAt = args.Now })

                                match claimed with
                                | Some c -> claims.Add { Run = c; Lease = cloneLease nextLease }
                                | None -> ()
                            | _ -> ()

                RuntimeSdk.promiseResolve (claims.ToArray())

          ListRuns =
            fun args ->
                let offset =
                    match args.Cursor with
                    | Some c -> RuntimeSdk.numberValue (box c)
                    | None -> 0.0

                let start =
                    if RuntimeSdk.numberIsFinite (box offset) && offset > 0.0 then
                        int offset
                    else
                        0

                let all = Seq.toArray runs.Values

                let filtered =
                    all
                    |> Array.filter (fun run ->
                        match args.WorkflowId with
                        | Some wid -> run.WorkflowId = wid
                        | None -> true)
                    |> Array.filter (fun run ->
                        match args.Status with
                        | Some s -> run.Status = s
                        | None -> true)

                // Stable sort by updatedAt descending (b - a), matching Array.sort.
                let sorted =
                    filtered
                    |> Array.sortWith (fun a b -> compare b.UpdatedAt a.UpdatedAt)

                let sliced =
                    let count = sorted.Length

                    if start >= count then
                        [||]
                    else
                        let stop = min count (start + args.Limit)
                        sorted[start .. stop - 1]

                RuntimeSdk.promiseResolve (Array.map toRunSummary sliced)

          GetRunTimeline =
            fun runId ->
                match getRun runId with
                | None -> RuntimeSdk.promiseResolve None
                | Some run ->
                    let result =
                        { Run = run
                          Events = cloneStoredEvents ((getLog runId).ToArray()) }

                    RuntimeSdk.promiseResolve (Some result) }
