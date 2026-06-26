namespace Firegrid.Runtime

open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Core

/// `runtime-driver.ts` — `createRuntimeDriver` and friends.
[<RequireQualifiedAccess>]
module RuntimeDriver =

    let private DEFAULT_LEASE_MS = 30000.0
    let private DEFAULT_SWEEP_LIMIT = 25

    // ── collectWorkflowEvents ─────────────────────────────────────────

    type private CollectedEvents =
        { Events: WorkflowEvent[]
          EventCount: int
          EventsTruncated: bool }

    type private CollectOptions =
        { IncludeEvents: bool
          MaxEvents: float option }

    /// Faithful port of the `for await` loop in `collectWorkflowEvents`. We
    /// drive Core's async-iterable (returned by `runWorkflow`) directly via
    /// `forAwaitEach`, applying the per-event include/maxEvents/truncation logic
    /// in F#, so counting and slicing match the TS exactly (rather than
    /// collecting first and slicing, which would lose the truncation flag).
    let private collectWorkflowEvents (iterable: obj) (options: CollectOptions) : JS.Promise<CollectedEvents> =
        promise {
            match options.MaxEvents with
            | Some maxEvents when not (RuntimeSdk.numberIsInteger (box maxEvents)) || maxEvents < 0.0 ->
                return failwith "Workflow event collection maxEvents must be a non-negative integer."
            | _ ->
                let events = ResizeArray<WorkflowEvent>()
                let mutable eventCount = 0
                let mutable eventsTruncated = false

                do!
                    RuntimeSdk.forAwaitEach iterable (fun rawEvent ->
                        eventCount <- eventCount + 1

                        if options.IncludeEvents then
                            match options.MaxEvents with
                            | None -> events.Add(WorkflowEvent.ofObj rawEvent)
                            | Some maxEvents ->
                                if float events.Count < maxEvents then
                                    events.Add(WorkflowEvent.ofObj rawEvent)
                                else
                                    eventsTruncated <- true)

                return
                    { Events = events.ToArray()
                      EventCount = eventCount
                      EventsTruncated = eventsTruncated }
        }

    // ── helpers ───────────────────────────────────────────────────────

    let private normalizeWorkflowLoaderResult (result: WorkflowLoaderResult) : AnyWorkflowDefinition =
        // TS checks `"__kind" in result` to detect an already-normalized workflow
        // definition. Core's F# `WorkflowDefinition` carries its discriminant as
        // the record field `Kind` (value "workflow"), so we detect that field —
        // the `{ default }` / `{ workflow }` wrappers do not carry it.
        if RuntimeSdk.hasKey result "Kind" then
            result :?> AnyWorkflowDefinition
        elif RuntimeSdk.hasKey result "default" then
            RuntimeSdk.prop<AnyWorkflowDefinition> result "default"
        else
            RuntimeSdk.prop<AnyWorkflowDefinition> result "workflow"

    let private loadWorkflow (config: WorkflowRuntimeConfig) (workflowId: string) : JS.Promise<AnyWorkflowDefinition> =
        promise {
            let registrationRaw = RuntimeSdk.prop<obj> config.Workflows workflowId

            if RuntimeSdk.isNullish registrationRaw then
                return failwithf "Workflow \"%s\" is not registered." workflowId
            else
                let registration = registrationRaw :?> WorkflowRegistration
                let! loaded = registration.Load()
                let workflow = normalizeWorkflowLoaderResult loaded
                let previousVersions = ResizeArray<AnyWorkflowDefinition>()

                let loaders =
                    match registration.PreviousVersions with
                    | Some pv -> RuntimeSdk.objectValues<WorkflowLoader> pv
                    | None -> [||]

                for loadPrevious in loaders do
                    let! prev = loadPrevious ()
                    previousVersions.Add(normalizeWorkflowLoaderResult prev)

                if registration.Version.IsSome || previousVersions.Count > 0 then
                    let version =
                        match registration.Version with
                        | Some v -> Some v
                        | None -> workflow.Version

                    let merged =
                        Array.append
                            workflow.PreviousVersions
                            (previousVersions.ToArray())

                    return
                        { workflow with
                            Version = version
                            PreviousVersions = merged }
                else
                    return workflow
        }

    /// `mergeResumeStateContext(durableInput, transientInput)`.
    let private mergeResumeStateContext (durableInput: obj) (transientInput: obj) : obj =
        let isObj (v: obj) = RuntimeSdk.isTypeofObject v && not (isNull v)

        if
            not (isObj durableInput)
            || not (isObj transientInput)
            || not (RuntimeSdk.hasKey transientInput "stateContext")
        then
            durableInput
        else
            let merged = RuntimeSdk.shallowClone durableInput
            RuntimeSdk.setProp merged "stateContext" (RuntimeSdk.prop<obj> transientInput "stateContext")
            merged

    let private normalizeSweepLimit (value: int option) (fallback: int) (label: string) : int =
        let limit = value |> Option.defaultValue fallback

        // value is already an int option in F#; the TS Number.isInteger / < 0
        // guard is preserved against the resolved limit.
        if not (RuntimeSdk.numberIsInteger (box (float limit))) || limit < 0 then
            failwithf "Workflow sweep %s must be a non-negative integer." label

        limit

    let private isPastSweepDeadline (startedAt: float) (maxDurationMs: float option) : bool =
        match maxDurationMs with
        | Some d -> RuntimeSdk.nowMillis () - startedAt >= d
        | None -> false

    let private classifyRun (run: WorkflowExecution option) (eventCount: int) : WorkflowRuntimeRunResultKind =
        match run with
        | Some r when r.Status = "finished" -> "completed"
        | Some r when r.Status = "paused" -> "paused"
        | Some r when r.Status = "errored" || r.Status = "aborted" -> "errored"
        | Some r when r.Status = "running" || r.Status = "queued" -> "running"
        | _ -> if eventCount > 0 then "running" else "not-found"

    let private resultFromSignalDelivery (runId: string) (result: DeliverSignalResult) : WorkflowRuntimeRunResult =
        let run = DeliverSignalResult.run result

        { Kind = DeliverSignalResult.kind result
          RunId = runId
          Run = run
          WorkflowId = run |> Option.map (fun r -> r.WorkflowId)
          Events = [||]
          EventCount = 0
          EventsTruncated = None }

    let private resultFromApprovalDelivery (runId: string) (result: DeliverApprovalResult) : WorkflowRuntimeRunResult =
        let run = DeliverApprovalResult.run result

        { Kind = DeliverApprovalResult.kind result
          RunId = runId
          Run = run
          WorkflowId = run |> Option.map (fun r -> r.WorkflowId)
          Events = [||]
          EventCount = 0
          EventsTruncated = None }

    // ── driveClaimedRun ───────────────────────────────────────────────

    type private DriveArgs =
        { Workflow: AnyWorkflowDefinition
          WorkflowId: string
          RunId: string
          Input: obj option
          SignalDelivery: SignalDelivery option
          Approval: ApprovalResult option
          Resume: bool option
          Now: float
          LeaseOwner: string option
          LeaseMs: float option
          ThreadId: string option
          IncludeEvents: bool option
          MaxEvents: float option }

    let private syncTimerFromRunState
        (config: WorkflowRuntimeConfig)
        (runId: string)
        (workflowId: string)
        (now: float)
        : JS.Promise<unit> =
        promise {
            let! state = config.Store.LoadRunState runId

            let deadline =
                state |> Option.bind (fun s -> s.WaitingFor) |> Option.bind (fun w -> w.Deadline)

            let signalName =
                state |> Option.bind (fun s -> s.WaitingFor) |> Option.map (fun w -> w.SignalName)

            match deadline, signalName with
            | Some d, Some name ->
                let workflowVersion = state |> Option.bind (fun s -> s.WorkflowVersion)

                do!
                    config.Store.ScheduleTimer
                        { RunId = runId
                          WorkflowId = workflowId
                          WorkflowVersion = workflowVersion
                          WakeAt = d
                          SignalId = sprintf "timer:%s:%g" runId d
                          SignalName = Some name
                          Now = now }
            | _ -> return ()
        }

    let private driveClaimedRun (config: WorkflowRuntimeConfig) (args: DriveArgs) : JS.Promise<WorkflowRuntimeRunResult> =
        promise {
            let leaseOwner = args.LeaseOwner |> Option.defaultValue (sprintf "runtime:%s" args.RunId)

            let leaseMs =
                args.LeaseMs
                |> Option.orElse config.DefaultLeaseMs
                |> Option.defaultValue DEFAULT_LEASE_MS

            let! claim =
                config.Store.ClaimRun
                    { RunId = args.RunId
                      LeaseOwner = leaseOwner
                      LeaseMs = leaseMs
                      Now = args.Now }

            match claim with
            | ClaimRunNotFound ->
                return
                    { Kind = "not-found"
                      RunId = args.RunId
                      WorkflowId = Some args.WorkflowId
                      Run = None
                      EventCount = 0
                      Events = [||]
                      EventsTruncated = None }
            | ClaimRunNotClaimable run ->
                return
                    { Kind = "not-claimable"
                      RunId = args.RunId
                      WorkflowId = Some args.WorkflowId
                      Run = Some run
                      EventCount = 0
                      Events = [||]
                      EventsTruncated = None }
            | ClaimRunClaimed claimedRun ->
                let runStore = RunStoreAdapter.create (WorkflowExecutionStore.asAdapterStore config.Store)

                let resume = args.Resume

                let resumeInput =
                    match resume with
                    | Some true -> Some(mergeResumeStateContext claimedRun.Input (args.Input |> Option.defaultValue RuntimeSdk.undefinedValue))
                    | _ -> None

                let runOptions: RunWorkflow.RunWorkflowOptions =
                    { Workflow = args.Workflow
                      RunStore = runStore
                      Input = args.Input
                      RunId = Some args.RunId
                      SignalDelivery = args.SignalDelivery
                      Approval = args.Approval
                      Resume = resume
                      ResumeInput = resumeInput
                      Attach = None
                      Signal = None
                      ThreadId = args.ThreadId
                      Publish = None
                      OutputSink = None }

                let iterable = RunWorkflow.runWorkflow runOptions

                let! collected =
                    collectWorkflowEvents
                        iterable
                        { IncludeEvents = args.IncludeEvents |> Option.defaultValue true
                          MaxEvents = args.MaxEvents }

                do! syncTimerFromRunState config args.RunId args.WorkflowId args.Now
                do! config.Store.ReleaseRunLease { RunId = args.RunId; LeaseOwner = leaseOwner }

                let! run = config.Store.LoadRun args.RunId

                return
                    { Kind = classifyRun run collected.EventCount
                      RunId = args.RunId
                      WorkflowId = Some args.WorkflowId
                      Run = run
                      Events = collected.Events
                      EventCount = collected.EventCount
                      EventsTruncated = if collected.EventsTruncated then Some true else None }
        }

    // ── public driver entry points ────────────────────────────────────

    let private startRun (config: WorkflowRuntimeConfig) (args: WorkflowRuntimeStartRunArgs) : JS.Promise<WorkflowRuntimeRunResult> =
        promise {
            let now = args.Now |> Option.defaultValue (RuntimeSdk.nowMillis ())
            let! workflow = loadWorkflow config args.WorkflowId
            let workflowVersion = workflow.Version

            let! created =
                config.Store.CreateRun
                    { RunId = args.RunId
                      WorkflowId = args.WorkflowId
                      WorkflowVersion = workflowVersion
                      Input = args.Input
                      Now = now }

            return!
                driveClaimedRun
                    config
                    { Workflow = workflow
                      WorkflowId = args.WorkflowId
                      RunId = args.RunId
                      Input = Some args.Input
                      SignalDelivery = None
                      Approval = None
                      Resume = Some(CreateRunResult.kind created = "existing")
                      Now = now
                      LeaseOwner = args.LeaseOwner
                      LeaseMs = args.LeaseMs
                      ThreadId = args.ThreadId
                      IncludeEvents = args.IncludeEvents
                      MaxEvents = args.MaxEvents }
        }

    let private deliverSignal
        (config: WorkflowRuntimeConfig)
        (args: WorkflowRuntimeDeliverSignalArgs)
        : JS.Promise<WorkflowRuntimeRunResult> =
        promise {
            let now = args.Now |> Option.defaultValue (RuntimeSdk.nowMillis ())

            let delivery: SignalDelivery =
                { SignalId = args.SignalId
                  StepId = args.StepId
                  Name = args.Name
                  Payload = args.Payload
                  Meta = args.Meta }

            let! delivered =
                config.Store.DeliverSignal
                    { RunId = args.RunId
                      Delivery = delivery
                      Now = now }

            match delivered with
            | DeliverSignalDelivered deliveredRun ->
                let! workflow = loadWorkflow config deliveredRun.WorkflowId

                return!
                    driveClaimedRun
                        config
                        { Workflow = workflow
                          WorkflowId = deliveredRun.WorkflowId
                          RunId = args.RunId
                          Input = None
                          SignalDelivery = Some delivery
                          Approval = None
                          Resume = None
                          Now = now
                          LeaseOwner = args.LeaseOwner
                          LeaseMs = args.LeaseMs
                          ThreadId = args.ThreadId
                          IncludeEvents = args.IncludeEvents
                          MaxEvents = args.MaxEvents }
            | other -> return resultFromSignalDelivery args.RunId other
        }

    let private deliverApproval
        (config: WorkflowRuntimeConfig)
        (args: WorkflowRuntimeDeliverApprovalArgs)
        : JS.Promise<WorkflowRuntimeRunResult> =
        promise {
            let now = args.Now |> Option.defaultValue (RuntimeSdk.nowMillis ())

            let! delivered =
                config.Store.DeliverApproval
                    { RunId = args.RunId
                      Approval = args.Approval
                      Now = now }

            match delivered with
            | DeliverApprovalDelivered deliveredRun ->
                let! workflow = loadWorkflow config deliveredRun.WorkflowId

                return!
                    driveClaimedRun
                        config
                        { Workflow = workflow
                          WorkflowId = deliveredRun.WorkflowId
                          RunId = args.RunId
                          Input = None
                          SignalDelivery = None
                          Approval = Some args.Approval
                          Resume = None
                          Now = now
                          LeaseOwner = args.LeaseOwner
                          LeaseMs = args.LeaseMs
                          ThreadId = args.ThreadId
                          IncludeEvents = args.IncludeEvents
                          MaxEvents = args.MaxEvents }
            | other -> return resultFromApprovalDelivery args.RunId other
        }

    type private DeliverTimerArgs =
        { Timer: TimerWakeup
          Now: float
          LeaseOwner: string
          LeaseMs: float
          IncludeEvents: bool option
          MaxEvents: float option }

    let private deliverTimer (config: WorkflowRuntimeConfig) (args: DeliverTimerArgs) : JS.Promise<WorkflowRuntimeRunResult> =
        deliverSignal
            config
            { RunId = args.Timer.RunId
              SignalId = args.Timer.SignalId
              StepId = None
              Name = args.Timer.SignalName |> Option.defaultValue "__timer"
              Payload = RuntimeSdk.undefinedValue
              Meta = None
              Now = Some args.Now
              LeaseOwner = Some args.LeaseOwner
              LeaseMs = Some args.LeaseMs
              ThreadId = None
              IncludeEvents = args.IncludeEvents
              MaxEvents = args.MaxEvents }

    // ── sweep summary helpers ─────────────────────────────────────────

    let private countRunKinds (runs: WorkflowRuntimeRunResult[]) : WorkflowRuntimeRunKindCounts =
        let counts = createObj []

        for run in runs do
            let current = RuntimeSdk.prop<obj> counts run.Kind
            let prev = if RuntimeSdk.isNullish current then 0.0 else RuntimeSdk.numberValue current
            RuntimeSdk.setProp counts run.Kind (box (prev + 1.0))

        counts

    let private sumEventCounts (runs: WorkflowRuntimeRunResult[]) : int =
        runs |> Array.fold (fun sum run -> sum + run.EventCount) 0

    let private sumReturnedEventCounts (runs: WorkflowRuntimeRunResult[]) : int =
        runs |> Array.fold (fun sum run -> sum + run.Events.Length) 0

    let private summarizeSweep
        (scheduled: WorkflowRuntimeRunResult[])
        (timers: WorkflowRuntimeRunResult[])
        : WorkflowRuntimeSweepSummary =
        { Scheduled = countRunKinds scheduled
          Timers = countRunKinds timers
          EventCount = sumEventCounts scheduled + sumEventCounts timers
          ReturnedEventCount = sumReturnedEventCounts scheduled + sumReturnedEventCounts timers }

    let private sweep (config: WorkflowRuntimeConfig) (args: WorkflowRuntimeSweepArgs) : JS.Promise<WorkflowRuntimeSweepResult> =
        promise {
            let now = args.Now |> Option.defaultValue (RuntimeSdk.nowMillis ())
            let startedAt = RuntimeSdk.nowMillis ()

            let maxScheduledRuns =
                normalizeSweepLimit (args.MaxScheduledRuns |> Option.orElse args.Limit) DEFAULT_SWEEP_LIMIT "maxScheduledRuns"

            let maxTimers =
                normalizeSweepLimit (args.MaxTimers |> Option.orElse args.Limit) DEFAULT_SWEEP_LIMIT "maxTimers"

            let leaseOwner = args.LeaseOwner |> Option.defaultValue (sprintf "sweep:%g" now)

            let leaseMs =
                args.LeaseMs
                |> Option.orElse config.DefaultLeaseMs
                |> Option.defaultValue DEFAULT_LEASE_MS

            let scheduled = ResizeArray<WorkflowRuntimeRunResult>()
            let timers = ResizeArray<WorkflowRuntimeRunResult>()
            let mutable deadlineReached = false

            let mutable scheduledDone = false

            while not scheduledDone && scheduled.Count < maxScheduledRuns do
                if isPastSweepDeadline startedAt args.MaxDurationMs then
                    deadlineReached <- true
                    scheduledDone <- true
                else
                    let! buckets =
                        config.Store.ClaimDueScheduleBuckets
                            { Now = now
                              Limit = 1
                              LeaseOwner = leaseOwner
                              LeaseMs = leaseMs }

                    if buckets.Length = 0 then
                        scheduledDone <- true
                    else
                        let bucket = buckets[0]

                        let! result =
                            startRun
                                config
                                { WorkflowId = bucket.WorkflowId
                                  RunId = bucket.RunId
                                  Input = bucket.Input
                                  Now = Some now
                                  LeaseOwner = Some leaseOwner
                                  LeaseMs = Some leaseMs
                                  ThreadId = None
                                  IncludeEvents = args.IncludeEvents
                                  MaxEvents = args.MaxEvents }

                        if result.Kind <> "not-claimable" && result.Kind <> "not-found" then
                            do!
                                config.Store.MarkScheduleBucketStarted
                                    { ScheduleId = bucket.ScheduleId
                                      BucketId = bucket.BucketId
                                      RunId = bucket.RunId
                                      Now = now }

                        scheduled.Add result

            let mutable timersDone = false

            while not timersDone && timers.Count < maxTimers do
                if isPastSweepDeadline startedAt args.MaxDurationMs then
                    deadlineReached <- true
                    timersDone <- true
                else
                    let! dueTimers =
                        config.Store.ClaimDueTimers
                            { Now = now
                              Limit = 1
                              LeaseOwner = leaseOwner
                              LeaseMs = leaseMs }

                    if dueTimers.Length = 0 then
                        timersDone <- true
                    else
                        let timer = dueTimers[0]

                        let! result =
                            deliverTimer
                                config
                                { Timer = timer
                                  Now = now
                                  LeaseOwner = leaseOwner
                                  LeaseMs = leaseMs
                                  IncludeEvents = args.IncludeEvents
                                  MaxEvents = args.MaxEvents }

                        timers.Add result

            let scheduledArr = scheduled.ToArray()
            let timersArr = timers.ToArray()

            return
                { Scheduled = scheduledArr
                  Timers = timersArr
                  Summary = summarizeSweep scheduledArr timersArr
                  DeadlineReached = deadlineReached
                  RemainingMayExist =
                    deadlineReached
                    || scheduled.Count >= maxScheduledRuns
                    || timers.Count >= maxTimers }
        }

    /// `createRuntimeDriver(config)`.
    let create (config: WorkflowRuntimeConfig) : WorkflowRuntimeDriver =
        { StartRun = fun args -> startRun config args
          DeliverSignal = fun args -> deliverSignal config args
          DeliverApproval = fun args -> deliverApproval config args
          Sweep = fun args -> sweep config args }
