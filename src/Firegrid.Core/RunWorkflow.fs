namespace Firegrid.Core

open System.Collections.Generic
open Fable.Core
open Fable.Core.JsInterop

/// The workflow engine (`engine/run-workflow.ts`).
[<RequireQualifiedAccess>]
module RunWorkflow =

    // ============================================================
    // Public options
    // ============================================================

    type RunWorkflowOptions =
        { Workflow: AnyWorkflowDefinition
          RunStore: RunStore
          Input: obj option
          RunId: string option
          SignalDelivery: SignalDelivery option
          Approval: ApprovalResult option
          Resume: bool option
          ResumeInput: obj option
          Attach: bool option
          Signal: CoreSdk.AbortSignal option
          ThreadId: string option
          Publish: (string -> WorkflowEvent -> JS.Promise<unit>) option
          OutputSink: (obj -> unit) option }

        static member Create(workflow: AnyWorkflowDefinition, runStore: RunStore) =
            { Workflow = workflow
              RunStore = runStore
              Input = None
              RunId = None
              SignalDelivery = None
              Approval = None
              Resume = None
              ResumeInput = None
              Attach = None
              Signal = None
              ThreadId = None
              Publish = None
              OutputSink = None }

    type private DeliveryLostCode =
        | SignalLost
        | ApprovalLost

    type private SeedAppendOutcome =
        | Appended
        | Idempotent
        | Lost of code: DeliveryLostCode * message: string

    // ============================================================
    // Engine runtime — shared mutable state across primitives
    // ============================================================

    type private Counters =
        { mutable Wait: int
          mutable Sleep: int
          mutable Approve: int
          mutable Now: int
          mutable Uuid: int }

    type private EngineRuntime =
        { RunId: string
          Workflow: AnyWorkflowDefinition
          RunStore: RunStore
          Emit: WorkflowEvent -> unit
          AbortController: CoreSdk.AbortController
          History: WorkflowEvent[]
          mutable NextLogIndex: float
          Consumed: HashSet<int>
          Counters: Counters
          mutable PrevStateSnapshot: obj
          State: obj
          mutable Paused: bool }

    // ============================================================
    // Helpers
    // ============================================================

    /// `serializeError(err)`.
    let private serializeError (err: obj) : SerializedError =
        if CoreSdk.isError err then
            { Name = CoreSdk.errorName err
              Message = CoreSdk.errorMessage err
              Stack =
                let s = CoreSdk.errorStack err
                if CoreSdk.isUndefined s then None else Some(CoreSdk.stringValue s) }
        else
            { Name = "UnknownError"
              Message = CoreSdk.stringValue err
              Stack = None }

    /// `rehydrateError(serialized)`.
    let private rehydrateError (serialized: SerializedError) : exn =
        let stack =
            match serialized.Stack with
            | Some s -> box s
            | None -> CoreSdk.undefinedValue

        CoreSdk.makeError serialized.Message serialized.Name stack

    /// `computeBackoffMs(policy, attempt)`.
    let private computeBackoffMs (policy: StepRetryOptions option) (attempt: int) : float =
        match policy with
        | None -> 0.0
        | Some p ->
            let baseMs = p.BaseMs |> Option.defaultValue 500.0

            match p.Backoff with
            | Some(Custom f) -> f attempt
            | Some Fixed -> baseMs
            | _ -> baseMs * CoreSdk.mathPow 2.0 (float (attempt - 1))

    /// `Math.random().toString(36).slice(2, 9)`.
    [<Emit("Math.random().toString(36).slice(2, 9)")>]
    let private randomBase36 () : string = jsNative

    /// `generateId(prefix)` → `${prefix}_${Date.now()}_${rand36}`.
    let private generateId (prefix: string) : string =
        sprintf "%s_%g_%s" prefix (CoreSdk.nowMillis ()) (randomBase36 ())

    /// `setupAbort(external)`.
    let private setupAbort (externalSignal: CoreSdk.AbortSignal option) : CoreSdk.AbortController =
        let ctrl = CoreSdk.newAbortController ()

        match externalSignal with
        | Some ext ->
            if CoreSdk.signalAborted ext then
                CoreSdk.controllerAbort ctrl
            else
                CoreSdk.addEventListener ext "abort" (fun () -> CoreSdk.controllerAbort ctrl) CoreSdk.onceOptions
        | None -> ()

        ctrl

    /// `validateSyncSchema(schema, value, label)`.
    let private validateSyncSchema (schema: SchemaInput) (value: obj) (label: string) : obj =
        match Schema.validate schema value with
        | Async -> raise (CoreSdk.makeError (sprintf "%s schema validates asynchronously, which is not supported." label) "Error" CoreSdk.undefinedValue)
        | Issues -> raise (CoreSdk.makeError (sprintf "%s failed schema validation." label) "Error" CoreSdk.undefinedValue)
        | Validated v -> v

    let private validateWorkflowInput (workflow: AnyWorkflowDefinition) (input: obj) : obj =
        match workflow.InputSchema with
        | None -> input
        | Some schema -> validateSyncSchema schema input (sprintf "Workflow \"%s\" input" workflow.Id)

    let private validateWorkflowOutput (workflow: AnyWorkflowDefinition) (output: obj) : obj =
        match workflow.OutputSchema with
        | None -> output
        | Some schema -> validateSyncSchema schema output (sprintf "Workflow \"%s\" output" workflow.Id)

    let private buildInitialState (workflow: AnyWorkflowDefinition) (input: obj) : obj =
        let initial =
            match workflow.Initialize with
            | Some init -> init (createObj [ "input" ==> input ])
            | None -> createObj []

        match workflow.StateSchema with
        | None -> initial
        | Some schema -> validateSyncSchema schema initial (sprintf "Workflow \"%s\" initial state" workflow.Id)

    /// `selectVersionForRun(current, runState)`.
    let private selectVersionForRun (current: AnyWorkflowDefinition) (runState: RunState) : AnyWorkflowDefinition option =
        match runState.WorkflowVersion with
        | None ->
            // No recorded version: fall back to current (legacy compat).
            Some current
        | Some version ->
            if current.Version = Some version then
                Some current
            else
                current.PreviousVersions
                |> Array.tryFind (fun prev -> prev.Version = Some version)

    type private CheckpointMatch =
        { Event: WorkflowEvent
          Index: int }

    /// `findCheckpoint(engine, predicate)` — first unconsumed matching event;
    /// marks it consumed.
    let private findCheckpoint (engine: EngineRuntime) (predicate: WorkflowEvent -> int -> bool) : CheckpointMatch option =
        let mutable result = None
        let mutable i = 0

        while result.IsNone && i < engine.History.Length do
            if not (engine.Consumed.Contains i) then
                let e = engine.History[i]

                if predicate e i then
                    engine.Consumed.Add i |> ignore
                    result <- Some { Event = e; Index = i }

            i <- i + 1

        result

    /// `emitAndAppend` — append-first, then emit observably.
    let private emitAndAppend
        (runStore: RunStore)
        (runId: string)
        (index: float)
        (emit: WorkflowEvent -> unit)
        (event: WorkflowEvent)
        : JS.Promise<unit> =
        promise {
            do! runStore.AppendEvent runId index event
            emit event
        }

    /// `flushStateDelta(engine)`.
    let private flushStateDelta (engine: EngineRuntime) : unit =
        let delta = StateDiff.diffState engine.PrevStateSnapshot engine.State

        if not (List.isEmpty delta) then
            engine.PrevStateSnapshot <- StateDiff.snapshotState engine.State

            engine.Emit(
                StateDelta
                    { Ts = CoreSdk.nowMillis ()
                      Delta = List.toArray delta
                      Audience = None }
            )

    // ============================================================
    // Primitives — replay-aware durable steps
    // ============================================================

    /// `engineStep`.
    let rec private engineStep
        (engine: EngineRuntime)
        (stepId: string)
        (fn: StepContext -> obj)
        (options: StepOptions option)
        : JS.Promise<obj> =
        promise {
            flushStateDelta engine

            // Replay short-circuit.
            let cached =
                findCheckpoint engine (fun e i ->
                    not (engine.Consumed.Contains i)
                    && (match e with
                        | StepFinished se -> se.StepId = stepId
                        | StepFailed se -> se.StepId = stepId
                        | _ -> false))
            // NOTE: findCheckpoint already filters consumed; the inner check is
            // redundant but kept faithful to the TS predicate.

            match cached with
            | Some m ->
                match m.Event with
                | StepFailed se -> return raise (rehydrateError se.Error)
                | StepFinished se -> return se.Result
                | _ -> return CoreSdk.undefinedValue
            | None ->
                // Fresh execution.
                engine.Emit(
                    StepStarted
                        { Ts = CoreSdk.nowMillis ()
                          StepId = stepId
                          Meta = options |> Option.bind (fun o -> o.Meta)
                          Audience = None }
                )

                let startedAt = CoreSdk.nowMillis ()
                ignore startedAt

                let retryPolicy =
                    match options |> Option.bind (fun o -> o.Retry) with
                    | Some r -> Some r
                    | None -> engine.Workflow.DefaultStepRetry

                let maxAttempts =
                    int (CoreSdk.mathMax 1.0 (float (retryPolicy |> Option.map (fun r -> r.MaxAttempts) |> Option.defaultValue 1)))

                let attempts = ResizeArray<StepAttempt>()
                let mutable lastError: obj = CoreSdk.undefinedValue
                let mutable result: obj = CoreSdk.undefinedValue
                let mutable succeeded = false
                let timeout = options |> Option.bind (fun o -> o.Timeout)

                let mutable attempt = 1
                let mutable stop = false

                while not stop && attempt <= maxAttempts do
                    let attemptStart = CoreSdk.nowMillis ()
                    let attemptController = CoreSdk.newAbortController ()

                    // Eager propagation.
                    if CoreSdk.signalAborted (CoreSdk.controllerSignal engine.AbortController) then
                        CoreSdk.controllerAbort attemptController

                    let onParentAbort = fun () -> CoreSdk.controllerAbort attemptController

                    CoreSdk.addEventListener
                        (CoreSdk.controllerSignal engine.AbortController)
                        "abort"
                        onParentAbort
                        CoreSdk.onceOptions

                    let mutable timeoutHandle: CoreSdk.TimeoutHandle = null
                    let mutable timedOut = false

                    match timeout with
                    | Some t when t > 0.0 ->
                        timeoutHandle <-
                            CoreSdk.setTimeout
                                (fun () ->
                                    timedOut <- true
                                    CoreSdk.controllerAbort attemptController)
                                t
                    | _ -> ()

                    let mutable threw = false
                    let mutable thrownErr: obj = CoreSdk.undefinedValue

                    let! _ =
                        promise {
                            try
                                let fnPromise =
                                    CoreSdk.promiseResolve (
                                        fn
                                            { Id = sprintf "%s:%s" engine.RunId stepId
                                              Attempt = attempt
                                              Signal = CoreSdk.controllerSignal attemptController }
                                    )

                                let! r =
                                    match timeout with
                                    | Some t when t > 0.0 ->
                                        let timeoutPromise =
                                            CoreSdk.newPromise<obj> (fun _resolve reject ->
                                                CoreSdk.addEventListener
                                                    (CoreSdk.controllerSignal attemptController)
                                                    "abort"
                                                    (fun () ->
                                                        if timedOut then
                                                            reject (box (StepTimeoutError.create stepId t))
                                                        elif CoreSdk.signalAborted (CoreSdk.controllerSignal engine.AbortController) then
                                                            reject (box (CoreSdk.makeError "Workflow aborted" "Error" CoreSdk.undefinedValue))
                                                        else
                                                            reject (box (StepTimeoutError.create stepId t)))
                                                    CoreSdk.onceOptions)

                                        CoreSdk.promiseRace [| fnPromise; timeoutPromise |]
                                    | _ -> fnPromise

                                result <- r

                                attempts.Add
                                    { StartedAt = attemptStart
                                      FinishedAt = CoreSdk.nowMillis ()
                                      Result = Some r
                                      Error = None }

                                succeeded <- true

                                if not (isNull timeoutHandle) then
                                    CoreSdk.clearTimeout timeoutHandle

                                CoreSdk.removeEventListener
                                    (CoreSdk.controllerSignal engine.AbortController)
                                    "abort"
                                    onParentAbort

                                stop <- true
                            with err ->
                                threw <- true
                                thrownErr <- box err

                            return ()
                        }

                    if threw then
                        if not (isNull timeoutHandle) then
                            CoreSdk.clearTimeout timeoutHandle

                        CoreSdk.removeEventListener
                            (CoreSdk.controllerSignal engine.AbortController)
                            "abort"
                            onParentAbort

                        lastError <- thrownErr

                        attempts.Add
                            { StartedAt = attemptStart
                              FinishedAt = CoreSdk.nowMillis ()
                              Result = None
                              Error = Some(serializeError thrownErr) }

                        let shouldRetry =
                            attempt < maxAttempts
                            && (match retryPolicy |> Option.bind (fun r -> r.ShouldRetry) with
                                | Some pred -> pred thrownErr attempt
                                | None -> true)

                        if not shouldRetry then
                            stop <- true
                        else
                            let delayMs = computeBackoffMs retryPolicy attempt

                            if delayMs > 0.0 then
                                do!
                                    CoreSdk.newPromise<unit> (fun resolve _reject ->
                                        let mutable resolved = false

                                        let settle =
                                            fun () ->
                                                if not resolved then
                                                    resolved <- true
                                                    resolve ()

                                        let t = CoreSdk.setTimeout settle delayMs

                                        CoreSdk.addEventListener
                                            (CoreSdk.controllerSignal engine.AbortController)
                                            "abort"
                                            (fun () ->
                                                CoreSdk.clearTimeout t
                                                settle ())
                                            CoreSdk.onceOptions)

                                if CoreSdk.signalAborted (CoreSdk.controllerSignal engine.AbortController) then
                                    stop <- true

                    attempt <- attempt + 1

                if not succeeded then
                    let failedEvent =
                        StepFailed
                            { Ts = CoreSdk.nowMillis ()
                              StepId = stepId
                              Error = serializeError lastError
                              Attempts = (if attempts.Count > 1 then Some(attempts.ToArray()) else None)
                              Meta = options |> Option.bind (fun o -> o.Meta)
                              Audience = None }

                    let idx = engine.NextLogIndex
                    engine.NextLogIndex <- engine.NextLogIndex + 1.0
                    do! emitAndAppend engine.RunStore engine.RunId idx engine.Emit failedEvent
                    return raise (rehydrateError (serializeError lastError))
                else
                    let finishedEvent =
                        StepFinished
                            { Ts = CoreSdk.nowMillis ()
                              StepId = stepId
                              Result = result
                              Attempts = (if attempts.Count > 1 then Some(attempts.ToArray()) else None)
                              Meta = options |> Option.bind (fun o -> o.Meta)
                              Audience = None }

                    let idx = engine.NextLogIndex
                    engine.NextLogIndex <- engine.NextLogIndex + 1.0
                    do! emitAndAppend engine.RunStore engine.RunId idx engine.Emit finishedEvent
                    return result
        }

    /// `engineWaitForEvent`.
    and private engineWaitForEvent
        (engine: EngineRuntime)
        (name: string)
        (options: WaitForEventOptions option)
        : JS.Promise<obj> =
        promise {
            flushStateDelta engine

            let stepId =
                match options |> Option.bind (fun o -> o.Id) with
                | Some id -> id
                | None ->
                    let n = sprintf "__wait-%s-%d" name engine.Counters.Wait
                    engine.Counters.Wait <- engine.Counters.Wait + 1
                    n

            let cached =
                findCheckpoint engine (fun e _ ->
                    match e with
                    | SignalResolved se -> se.Name = name && se.StepId = stepId
                    | _ -> false)

            match cached with
            | Some m ->
                match m.Event with
                | SignalResolved se ->
                    let payload = se.Payload

                    match options |> Option.bind (fun o -> o.Schema) with
                    | Some schema ->
                        match Schema.validate schema payload with
                        | Async ->
                            return
                                raise (
                                    CoreSdk.makeError
                                        (sprintf "waitForEvent(\"%s\"): schema validates asynchronously, which is not supported." name)
                                        "Error"
                                        CoreSdk.undefinedValue
                                )
                        | Issues ->
                            return
                                raise (
                                    CoreSdk.makeError
                                        (sprintf "waitForEvent(\"%s\"): payload failed schema validation." name)
                                        "Error"
                                        CoreSdk.undefinedValue
                                )
                        | Validated v -> return v
                    | None -> return payload
                | _ -> return CoreSdk.undefinedValue
            | None ->
                // Not yet resolved — pause the run.
                let deadline = options |> Option.bind (fun o -> o.Deadline)
                let meta = options |> Option.bind (fun o -> o.Meta)

                let idx = engine.NextLogIndex
                engine.NextLogIndex <- engine.NextLogIndex + 1.0

                do!
                    emitAndAppend
                        engine.RunStore
                        engine.RunId
                        idx
                        engine.Emit
                        (SignalAwaited
                            { Ts = CoreSdk.nowMillis ()
                              StepId = stepId
                              Name = name
                              Deadline = deadline
                              Meta = meta
                              Audience = None })

                let! persisted = engine.RunStore.GetRunState engine.RunId

                match persisted with
                | Some p ->
                    do!
                        engine.RunStore.SetRunState
                            engine.RunId
                            { p with
                                Status = "paused"
                                Awaiting = Some [| AwaitSignal(Some stepId, name, deadline, meta) |]
                                WaitingFor =
                                    Some
                                        { StepId = Some stepId
                                          SignalName = name
                                          Deadline = deadline
                                          Meta = meta }
                                PendingApproval = None
                                UpdatedAt = CoreSdk.nowMillis () }
                | None -> ()

                engine.Paused <- true
                return raise (WorkflowPaused.create ())
        }

    /// `engineSleepUntil`.
    and private engineSleepUntil
        (engine: EngineRuntime)
        (timestamp: float)
        (options: SleepOptions option)
        : JS.Promise<unit> =
        let id =
            match options |> Option.bind (fun o -> o.Id) with
            | Some id -> id
            | None ->
                let n = sprintf "__sleep-%d" engine.Counters.Sleep
                engine.Counters.Sleep <- engine.Counters.Sleep + 1
                n

        let waitOpts =
            { Id = Some id
              Meta = options |> Option.bind (fun o -> o.Meta)
              Deadline = Some timestamp
              Schema = None }

        promise {
            let! _ = engineWaitForEvent engine "__timer" (Some waitOpts)
            return ()
        }

    /// `engineSleep`.
    and private engineSleep (engine: EngineRuntime) (ms: float) (options: SleepOptions option) : JS.Promise<unit> =
        engineSleepUntil engine (CoreSdk.nowMillis () + ms) options

    /// `engineApprove`.
    and private engineApprove (engine: EngineRuntime) (approveOptions: ApproveOptions) : JS.Promise<ApprovalResult> =
        promise {
            flushStateDelta engine

            let stepId =
                match approveOptions.Id with
                | Some id -> id
                | None ->
                    let n = sprintf "__approve-%d" engine.Counters.Approve
                    engine.Counters.Approve <- engine.Counters.Approve + 1
                    n

            let cached =
                findCheckpoint engine (fun e _ ->
                    match e with
                    | ApprovalResolved se -> se.StepId = stepId
                    | _ -> false)

            match cached with
            | Some m ->
                match m.Event with
                | ApprovalResolved se ->
                    return
                        { Approved = se.Approved
                          ApprovalId = se.ApprovalId
                          Feedback = se.Feedback
                          Meta = se.Meta }
                | _ -> return Unchecked.defaultof<ApprovalResult>
            | None ->
                let approvalId = generateId "approval"

                let idx = engine.NextLogIndex
                engine.NextLogIndex <- engine.NextLogIndex + 1.0

                do!
                    emitAndAppend
                        engine.RunStore
                        engine.RunId
                        idx
                        engine.Emit
                        (ApprovalRequested
                            { Ts = CoreSdk.nowMillis ()
                              StepId = stepId
                              ApprovalId = approvalId
                              Title = approveOptions.Title
                              Description = approveOptions.Description
                              Meta = approveOptions.Meta
                              Audience = None })

                let! persisted = engine.RunStore.GetRunState engine.RunId

                match persisted with
                | Some p ->
                    do!
                        engine.RunStore.SetRunState
                            engine.RunId
                            { p with
                                Status = "paused"
                                Awaiting =
                                    Some
                                        [| AwaitApproval(
                                               Some stepId,
                                               approvalId,
                                               approveOptions.Title,
                                               approveOptions.Description,
                                               approveOptions.Meta
                                           ) |]
                                WaitingFor = None
                                PendingApproval =
                                    Some
                                        { StepId = Some stepId
                                          ApprovalId = approvalId
                                          Title = approveOptions.Title
                                          Description = approveOptions.Description
                                          Meta = approveOptions.Meta }
                                UpdatedAt = CoreSdk.nowMillis () }
                | None -> ()

                engine.Paused <- true
                return raise (WorkflowPaused.create ())
        }

    /// `engineNow`.
    and private engineNow (engine: EngineRuntime) (options: DeterministicValueOptions option) : JS.Promise<float> =
        promise {
            flushStateDelta engine

            let stepId =
                match options |> Option.bind (fun o -> o.Id) with
                | Some id -> id
                | None ->
                    let n = sprintf "__now-%d" engine.Counters.Now
                    engine.Counters.Now <- engine.Counters.Now + 1
                    n

            let cached =
                findCheckpoint engine (fun e _ ->
                    match e with
                    | NowRecorded se -> se.StepId = stepId
                    | _ -> false)

            match cached with
            | Some m ->
                match m.Event with
                | NowRecorded se -> return se.Value
                | _ -> return 0.0
            | None ->
                let value = CoreSdk.nowMillis ()
                let idx = engine.NextLogIndex
                engine.NextLogIndex <- engine.NextLogIndex + 1.0

                do!
                    emitAndAppend
                        engine.RunStore
                        engine.RunId
                        idx
                        engine.Emit
                        (NowRecorded
                            { Ts = value
                              StepId = stepId
                              Value = value
                              Meta = options |> Option.bind (fun o -> o.Meta)
                              Audience = None })

                return value
        }

    /// `engineUuid`.
    and private engineUuid (engine: EngineRuntime) (options: DeterministicValueOptions option) : JS.Promise<string> =
        promise {
            flushStateDelta engine

            let stepId =
                match options |> Option.bind (fun o -> o.Id) with
                | Some id -> id
                | None ->
                    let n = sprintf "__uuid-%d" engine.Counters.Uuid
                    engine.Counters.Uuid <- engine.Counters.Uuid + 1
                    n

            let cached =
                findCheckpoint engine (fun e _ ->
                    match e with
                    | UuidRecorded se -> se.StepId = stepId
                    | _ -> false)

            match cached with
            | Some m ->
                match m.Event with
                | UuidRecorded se -> return se.Value
                | _ -> return ""
            | None ->
                let value = CoreSdk.randomUuid ()
                let idx = engine.NextLogIndex
                engine.NextLogIndex <- engine.NextLogIndex + 1.0

                do!
                    emitAndAppend
                        engine.RunStore
                        engine.RunId
                        idx
                        engine.Emit
                        (UuidRecorded
                            { Ts = CoreSdk.nowMillis ()
                              StepId = stepId
                              Value = value
                              Meta = options |> Option.bind (fun o -> o.Meta)
                              Audience = None })

                return value
        }

    // ============================================================
    // Middleware composition
    // ============================================================

    let private reservedCtxFields =
        HashSet<string>(
            [ "runId"
              "input"
              "state"
              "signal"
              "step"
              "sleep"
              "sleepUntil"
              "waitForEvent"
              "approve"
              "now"
              "uuid"
              "emit" ]
        )

    /// `composeMiddlewares(middlewares, ctx, handler)`.
    let private composeMiddlewares
        (middlewares: Middleware[])
        (ctx: Ctx)
        (handler: Ctx -> JS.Promise<obj>)
        : JS.Promise<obj> =
        let rec compose (index: int) : JS.Promise<obj> =
            promise {
                if index >= middlewares.Length then
                    return! handler ctx
                else
                    let m = middlewares[index]
                    let mutable returned: obj = CoreSdk.undefinedValue
                    let mutable advanced = false

                    let next =
                        fun (opts: obj) ->
                            promise {
                                if advanced then
                                    return
                                        raise (
                                            CoreSdk.makeError
                                                "middleware.next() must be called at most once per invocation"
                                                "Error"
                                                CoreSdk.undefinedValue
                                        )
                                else
                                    advanced <- true
                                    let ext = CoreSdk.prop<obj> opts "context"

                                    if not (CoreSdk.isNullish ext) && CoreSdk.isTypeofObject ext then
                                        for key in CoreSdk.objectKeys ext do
                                            if reservedCtxFields.Contains key then
                                                raise (
                                                    CoreSdk.makeError
                                                        (sprintf "Middleware extension may not shadow reserved ctx field: %s" key)
                                                        "Error"
                                                        CoreSdk.undefinedValue
                                                )

                                        CoreSdk.objectAssign ctx ext |> ignore

                                    let! r = compose (index + 1)
                                    returned <- r
                                    return r
                            }

                    let! _ = m.Server ctx next
                    return returned
            }

        compose 0

    // ============================================================
    // Build the JS ctx object from a BaseCtx
    // ============================================================

    // Built-in ctx primitives are stored as `System.Func<...>` so Fable emits
    // *uncurried* JS functions — i.e. they are callable as `ctx.step(id, fn,
    // opts)` exactly like the TS, not as curried `ctx.step(id)(fn)(opts)`.
    let private buildCtxObject (engine: EngineRuntime) (input: obj) : Ctx =
        let ctx = createObj []
        CoreSdk.setProp ctx "runId" (box engine.RunId)
        CoreSdk.setProp ctx "input" input
        CoreSdk.setProp ctx "state" engine.State
        CoreSdk.setProp ctx "signal" (CoreSdk.controllerSignal engine.AbortController)

        CoreSdk.setProp
            ctx
            "step"
            (box (System.Func<string, (StepContext -> obj), StepOptions option, JS.Promise<obj>>(fun id fn opts ->
                engineStep engine id fn opts)))

        CoreSdk.setProp
            ctx
            "sleep"
            (box (System.Func<float, SleepOptions option, JS.Promise<unit>>(fun ms opts -> engineSleep engine ms opts)))

        CoreSdk.setProp
            ctx
            "sleepUntil"
            (box (System.Func<float, SleepOptions option, JS.Promise<unit>>(fun ts opts ->
                engineSleepUntil engine ts opts)))

        CoreSdk.setProp
            ctx
            "waitForEvent"
            (box (System.Func<string, WaitForEventOptions option, JS.Promise<obj>>(fun name opts ->
                engineWaitForEvent engine name opts)))

        CoreSdk.setProp
            ctx
            "approve"
            (box (System.Func<ApproveOptions, JS.Promise<ApprovalResult>>(fun opts -> engineApprove engine opts)))

        CoreSdk.setProp
            ctx
            "now"
            (box (System.Func<DeterministicValueOptions option, JS.Promise<float>>(fun opts -> engineNow engine opts)))

        CoreSdk.setProp
            ctx
            "uuid"
            (box (System.Func<DeterministicValueOptions option, JS.Promise<string>>(fun opts -> engineUuid engine opts)))

        CoreSdk.setProp
            ctx
            "emit"
            (box (System.Func<string, obj, unit>(fun name value ->
                engine.Emit(
                    Custom
                        { Ts = CoreSdk.nowMillis ()
                          Name = name
                          Value = value
                          Audience = None }
                ))))

        ctx

    // ============================================================
    // Handler drive (the closure replay loop)
    // ============================================================

    let private driveHandler
        (options: RunWorkflowOptions)
        (emit: WorkflowEvent -> unit)
        (runId: string)
        (runState: RunState)
        (input: obj)
        (state: obj)
        (history: WorkflowEvent[])
        (abortController: CoreSdk.AbortController)
        : JS.Promise<unit> =
        promise {
            let workflow = options.Workflow
            let runStore = options.RunStore

            let engine =
                { RunId = runId
                  Workflow = workflow
                  RunStore = runStore
                  Emit = emit
                  AbortController = abortController
                  History = Array.copy history
                  NextLogIndex = float history.Length
                  Consumed = HashSet<int>()
                  Counters =
                    { Wait = 0
                      Sleep = 0
                      Approve = 0
                      Now = 0
                      Uuid = 0 }
                  PrevStateSnapshot = StateDiff.snapshotState state
                  State = state
                  Paused = false }

            let ctx = buildCtxObject engine input

            // mutable cell to hold output / error
            let mutable runState = runState

            let mutable failed = false
            let mutable thrownErr: obj = CoreSdk.undefinedValue
            let mutable output: obj = CoreSdk.undefinedValue

            let! _ =
                promise {
                    try
                        let! raw = composeMiddlewares workflow.Middlewares ctx workflow.Handler
                        output <- validateWorkflowOutput workflow raw
                        flushStateDelta engine
                    with err ->
                        failed <- true
                        thrownErr <- box err

                    return ()
                }

            if failed then
                flushStateDelta engine

                if engine.Paused then
                    // The pausing primitive already wrote pause state.
                    return ()
                elif CoreSdk.signalAborted (CoreSdk.controllerSignal abortController) then
                    runState <-
                        { runState with
                            Status = "aborted"
                            UpdatedAt = CoreSdk.nowMillis () }

                    do! runStore.SetRunState runId runState

                    let errEvent =
                        RunErrored
                            { Ts = CoreSdk.nowMillis ()
                              RunId = runId
                              Error =
                                { Name = "Aborted"
                                  Message = "Workflow aborted"
                                  Stack = None }
                              Code = "aborted"
                              Audience = None }

                    let idx = engine.NextLogIndex
                    engine.NextLogIndex <- engine.NextLogIndex + 1.0
                    do! emitAndAppend runStore runId idx emit errEvent
                    return ()
                else
                    let serialized = serializeError thrownErr

                    runState <-
                        { runState with
                            Status = "errored"
                            Error = Some serialized
                            UpdatedAt = CoreSdk.nowMillis () }

                    do! runStore.SetRunState runId runState

                    let errEvent =
                        RunErrored
                            { Ts = CoreSdk.nowMillis ()
                              RunId = runId
                              Error = serialized
                              Code = "error"
                              Audience = None }

                    let idx = engine.NextLogIndex
                    engine.NextLogIndex <- engine.NextLogIndex + 1.0
                    do! emitAndAppend runStore runId idx emit errEvent
                    return ()
            else
                // Success.
                match options.OutputSink with
                | Some sink -> sink output
                | None -> ()

                runState <-
                    { runState with
                        Status = "finished"
                        Output = Some output
                        UpdatedAt = CoreSdk.nowMillis () }

                do! runStore.SetRunState runId runState

                let finishedEvent =
                    RunFinished
                        { Ts = CoreSdk.nowMillis ()
                          RunId = runId
                          Output = output
                          Audience = None }

                let idx = engine.NextLogIndex
                engine.NextLogIndex <- engine.NextLogIndex + 1.0
                do! emitAndAppend runStore runId idx emit finishedEvent
                return ()
        }

    // ============================================================
    // Seed delivery for resume
    // ============================================================

    let private findApprovalRequestStepId (history: WorkflowEvent[]) (approval: ApprovalResult) : string option =
        let mutable result = None
        let mutable i = history.Length - 1

        while result.IsNone && i >= 0 do
            match history[i] with
            | ApprovalRequested e when e.ApprovalId = approval.ApprovalId -> result <- Some e.StepId
            | _ -> ()

            i <- i - 1

        result

    let rec private appendSeed
        (runStore: RunStore)
        (runId: string)
        (history: WorkflowEvent[])
        (persistedState: RunState)
        (signalDelivery: SignalDelivery option)
        (approval: ApprovalResult option)
        (emit: WorkflowEvent -> unit)
        : JS.Promise<SeedAppendOutcome> =
        promise {
            match signalDelivery with
            | Some sd ->
                let waitingFor = persistedState.WaitingFor

                let mismatch =
                    match waitingFor with
                    | None -> true
                    | Some wf ->
                        wf.SignalName <> sd.Name
                        || (sd.StepId.IsSome && wf.StepId.IsSome && wf.StepId <> sd.StepId)

                if mismatch then
                    return
                        Lost(SignalLost, sprintf "Signal delivery lost: run is not waiting for \"%s\"." sd.Name)
                else
                    let targetStepId =
                        match sd.StepId with
                        | Some s -> Some s
                        | None -> waitingFor |> Option.bind (fun wf -> wf.StepId)

                    // Locate the most recent SIGNAL_AWAITED for this name/id.
                    let mutable awaitedIdx = -1
                    let mutable awaitedStepId = targetStepId
                    let mutable i = history.Length - 1
                    let mutable foundAwait = false

                    while not foundAwait && i >= 0 do
                        match history[i] with
                        | SignalAwaited e when
                            e.Name = sd.Name
                            && (targetStepId.IsNone || Some e.StepId = targetStepId)
                            ->
                            awaitedIdx <- i
                            awaitedStepId <- Some e.StepId
                            foundAwait <- true
                        | _ -> ()

                        i <- i - 1

                    let mutable earlyOutcome: SeedAppendOutcome option = None

                    if awaitedIdx >= 0 then
                        // Walk forward: a SIGNAL_RESOLVED may already have landed.
                        let mutable j = awaitedIdx + 1

                        while earlyOutcome.IsNone && j < history.Length do
                            match history[j] with
                            | SignalResolved e when
                                e.Name = sd.Name
                                && (awaitedStepId.IsNone || Some e.StepId = awaitedStepId)
                                ->
                                if e.SignalId = Some sd.SignalId then
                                    earlyOutcome <- Some Idempotent
                                else
                                    earlyOutcome <-
                                        Some(Lost(SignalLost, "Signal delivery lost: another delivery won the race."))
                            | _ -> ()

                            j <- j + 1

                    match earlyOutcome with
                    | Some o -> return o
                    | None ->
                        // Append a fresh resolution.
                        let resolvedStepId =
                            match awaitedStepId with
                            | Some s -> s
                            | None -> sprintf "__resolve-%s" sd.Name

                        let event =
                            SignalResolved
                                { Ts = CoreSdk.nowMillis ()
                                  StepId = resolvedStepId
                                  Name = sd.Name
                                  SignalId = Some sd.SignalId
                                  Payload = sd.Payload
                                  Meta = sd.Meta
                                  Audience = None }

                        let! appendResult =
                            promise {
                                try
                                    do! runStore.AppendEvent runId (float history.Length) event
                                    emit event
                                    return Ok()
                                with err ->
                                    return Error(box err)
                            }

                        match appendResult with
                        | Ok() -> return Appended
                        | Error err ->
                            if LogConflictError.is err then
                                let! refreshed = runStore.GetEvents runId
                                let mutable idem = false
                                let mutable k = history.Length

                                while not idem && k < refreshed.Length do
                                    match refreshed[k] with
                                    | SignalResolved e when
                                        e.Name = sd.Name
                                        && (awaitedStepId.IsNone || Some e.StepId = awaitedStepId)
                                        && e.SignalId = Some sd.SignalId
                                        ->
                                        idem <- true
                                    | _ -> ()

                                    k <- k + 1

                                if idem then
                                    return Idempotent
                                else
                                    return Lost(SignalLost, "Signal delivery lost: another delivery won the race.")
                            else
                                return raise (unbox<exn> err)
            | None ->
                match approval with
                | Some ap ->
                    let pendingApproval = persistedState.PendingApproval

                    let mismatch =
                        match pendingApproval with
                        | Some pa -> pa.ApprovalId <> ap.ApprovalId
                        | None -> true

                    if mismatch then
                        return
                            Lost(
                                ApprovalLost,
                                sprintf "Approval delivery lost: run is not waiting for approval \"%s\"." ap.ApprovalId
                            )
                    else
                        let stepId =
                            match pendingApproval |> Option.bind (fun pa -> pa.StepId) with
                            | Some s -> Some s
                            | None -> findApprovalRequestStepId history ap

                        let resolvedStepId =
                            match stepId with
                            | Some s -> s
                            | None -> "__resolve-approval"

                        let event =
                            ApprovalResolved
                                { Ts = CoreSdk.nowMillis ()
                                  StepId = resolvedStepId
                                  ApprovalId = ap.ApprovalId
                                  Approved = ap.Approved
                                  Feedback = ap.Feedback
                                  Meta = ap.Meta
                                  Audience = None }

                        let! appendResult =
                            promise {
                                try
                                    do! runStore.AppendEvent runId (float history.Length) event
                                    emit event
                                    return Ok()
                                with err ->
                                    return Error(box err)
                            }

                        match appendResult with
                        | Ok() -> return Appended
                        | Error err ->
                            if LogConflictError.is err then
                                let! refreshed = runStore.GetEvents runId
                                let mutable idem = false
                                let mutable k = history.Length

                                while not idem && k < refreshed.Length do
                                    match refreshed[k] with
                                    | ApprovalResolved e when
                                        (stepId.IsNone || Some e.StepId = stepId)
                                        && e.ApprovalId = ap.ApprovalId
                                        ->
                                        idem <- true
                                    | _ -> ()

                                    k <- k + 1

                                if idem then
                                    return Idempotent
                                else
                                    return Lost(ApprovalLost, "Approval delivery lost: another delivery won the race.")
                            else
                                return raise (unbox<exn> err)
                | None -> return Appended
        }

    // ============================================================
    // Attach (read-only snapshot)
    // ============================================================

    let rec private attachRun (options: RunWorkflowOptions) (emit: WorkflowEvent -> unit) : JS.Promise<unit> =
        promise {
            let runStore = options.RunStore
            let runId = options.RunId.Value

            let! persistedState = runStore.GetRunState runId

            match persistedState with
            | None ->
                emit (
                    RunErrored
                        { Ts = CoreSdk.nowMillis ()
                          RunId = runId
                          Error =
                            { Name = "RunLost"
                              Message = sprintf "Run %s not found." runId
                              Stack = None }
                          Code = "run_lost"
                          Audience = None }
                )

                return ()
            | Some ps ->
                emit (
                    RunStarted
                        { Ts = CoreSdk.nowMillis ()
                          RunId = runId
                          ThreadId = options.ThreadId
                          Audience = None }
                )

                let! events = runStore.GetEvents runId

                for event in events do
                    emit event

                let hasPersistedTerminal =
                    events
                    |> Array.exists (fun e ->
                        match e with
                        | RunFinished _ -> true
                        | RunErrored _ -> true
                        | _ -> false)

                if ps.Status = "finished" && not hasPersistedTerminal then
                    emit (
                        RunFinished
                            { Ts = CoreSdk.nowMillis ()
                              RunId = runId
                              Output = (ps.Output |> Option.defaultValue CoreSdk.undefinedValue)
                              Audience = None }
                    )

                    return ()
                elif not hasPersistedTerminal && (ps.Status = "errored" || ps.Status = "aborted") then
                    let err =
                        match ps.Error with
                        | Some e -> e
                        | None ->
                            { Name = "Unknown"
                              Message = "Run ended in non-terminal state"
                              Stack = None }

                    emit (
                        RunErrored
                            { Ts = CoreSdk.nowMillis ()
                              RunId = runId
                              Error = err
                              Code = (if ps.Status = "aborted" then "aborted" else "error")
                              Audience = None }
                    )

                    return ()
                else
                    // paused / running — caller has the snapshot.
                    return ()
        }

    // ============================================================
    // Start
    // ============================================================

    and private startRun (options: RunWorkflowOptions) (emit: WorkflowEvent -> unit) : JS.Promise<unit> =
        promise {
            let runStore = options.RunStore

            let runId =
                match options.RunId with
                | Some id -> id
                | None -> generateId "run"

            // Idempotency: caller-supplied runId already exists → attach.
            match options.RunId with
            | Some _ ->
                let! existing = runStore.GetRunState runId

                match existing with
                | Some _ ->
                    do! attachRun { options with Attach = Some true } emit
                    return ()
                | None -> do! startRunFresh options emit runId
            | None -> do! startRunFresh options emit runId
        }

    and private startRunFresh
        (options: RunWorkflowOptions)
        (emit: WorkflowEvent -> unit)
        (runId: string)
        : JS.Promise<unit> =
        promise {
            let workflow = options.Workflow
            let runStore = options.RunStore
            let abortController = setupAbort options.Signal

            // Validate + build initial state.
            let mutable validationFailed = false
            let mutable validationErr: obj = CoreSdk.undefinedValue
            let mutable input: obj = CoreSdk.undefinedValue
            let mutable state: obj = CoreSdk.undefinedValue

            (try
                input <- validateWorkflowInput workflow (options.Input |> Option.defaultValue CoreSdk.undefinedValue)
                state <- buildInitialState workflow input
             with err ->
                validationFailed <- true
                validationErr <- box err)

            if validationFailed then
                emit (
                    RunErrored
                        { Ts = CoreSdk.nowMillis ()
                          RunId = runId
                          Error = serializeError validationErr
                          Code = "validation_error"
                          Audience = None }
                )

                return ()
            else
                let runState =
                    { RunId = runId
                      Status = "running"
                      WorkflowId = workflow.Id
                      WorkflowVersion = workflow.Version
                      Input = input
                      Output = None
                      Error = None
                      Awaiting = None
                      WaitingFor = None
                      PendingApproval = None
                      CreatedAt = CoreSdk.nowMillis ()
                      UpdatedAt = CoreSdk.nowMillis () }

                do! runStore.SetRunState runId runState

                // RUN_STARTED is trace-only (not persisted).
                emit (
                    RunStarted
                        { Ts = CoreSdk.nowMillis ()
                          RunId = runId
                          ThreadId = options.ThreadId
                          Audience = None }
                )

                do! driveHandler options emit runId runState input state [||] abortController
                return ()
        }

    // ============================================================
    // Resume
    // ============================================================

    and private resumeRun (options: RunWorkflowOptions) (emit: WorkflowEvent -> unit) : JS.Promise<unit> =
        promise {
            let workflow = options.Workflow
            let runStore = options.RunStore
            let runId = options.RunId.Value

            let! persistedState = runStore.GetRunState runId

            match persistedState with
            | None ->
                emit (
                    RunErrored
                        { Ts = CoreSdk.nowMillis ()
                          RunId = runId
                          Error =
                            { Name = "RunLost"
                              Message = sprintf "Run %s not found." runId
                              Stack = None }
                          Code = "run_lost"
                          Audience = None }
                )

                return ()
            | Some ps ->
                if ps.Status = "finished" || ps.Status = "errored" || ps.Status = "aborted" then
                    do! attachRun { options with Attach = Some true } emit
                    return ()
                else
                    match selectVersionForRun workflow ps with
                    | None ->
                        let versionLabel = ps.WorkflowVersion |> Option.defaultValue "(none)"

                        emit (
                            RunErrored
                                { Ts = CoreSdk.nowMillis ()
                                  RunId = runId
                                  Error =
                                    { Name = "WorkflowVersionMismatch"
                                      Message =
                                        sprintf
                                            "No registered workflow version matches the run's persisted version \"%s\". Register the version via `previousVersions` on the current workflow."
                                            versionLabel
                                      Stack = None }
                                  Code = "workflow_version_mismatch"
                                  Audience = None }
                        )

                        return ()
                    | Some effectiveWorkflow ->
                        let! history = runStore.GetEvents runId

                        let! seedAppendOutcome =
                            appendSeed
                                runStore
                                runId
                                history
                                ps
                                options.SignalDelivery
                                options.Approval
                                emit

                        match seedAppendOutcome with
                        | Lost(code, message) ->
                            let name =
                                match code with
                                | ApprovalLost -> "ApprovalLost"
                                | SignalLost -> "SignalLost"

                            let codeStr =
                                match code with
                                | ApprovalLost -> "approval_lost"
                                | SignalLost -> "signal_lost"

                            emit (
                                RunErrored
                                    { Ts = CoreSdk.nowMillis ()
                                      RunId = runId
                                      Error =
                                        { Name = name
                                          Message = message
                                          Stack = None }
                                      Code = codeStr
                                      Audience = None }
                            )

                            return ()
                        | _ ->
                            let! updatedHistory = runStore.GetEvents runId
                            let abortController = setupAbort options.Signal

                            let input =
                                match options.ResumeInput with
                                | Some ri -> ri
                                | None -> ps.Input

                            let state = buildInitialState effectiveWorkflow input

                            let runState =
                                { ps with
                                    Status = "running"
                                    WorkflowVersion = effectiveWorkflow.Version
                                    Awaiting = None
                                    WaitingFor = None
                                    PendingApproval = None
                                    UpdatedAt = CoreSdk.nowMillis () }

                            do! runStore.SetRunState runId runState

                            emit (
                                RunStarted
                                    { Ts = CoreSdk.nowMillis ()
                                      RunId = runId
                                      ThreadId = options.ThreadId
                                      Audience = None }
                            )

                            do!
                                driveHandler
                                    { options with Workflow = effectiveWorkflow }
                                    emit
                                    runId
                                    runState
                                    input
                                    state
                                    updatedHistory
                                    abortController

                            return ()
        }

    // ============================================================
    // Internal driver — entry-point dispatch
    // ============================================================

    and private drive (options: RunWorkflowOptions) (emit: WorkflowEvent -> unit) : JS.Promise<unit> =
        promise {
            match options.RunId with
            | Some _ when options.Attach = Some true -> do! attachRun options emit
            | Some _ when options.SignalDelivery.IsSome || options.Approval.IsSome -> do! resumeRun options emit
            | Some _ when options.Resume = Some true -> do! resumeRun options emit
            | _ ->
                if options.Input.IsNone then
                    return
                        raise (
                            CoreSdk.makeError
                                "runWorkflow: provide `input` (start), `runId` + `signalDelivery`/`approval` (resume), or `runId` + `attach: true` (attach)."
                                "Error"
                                CoreSdk.undefinedValue
                        )
                else
                    do! startRun options emit
        }

    // ============================================================
    // Public API
    // ============================================================

    /// `runWorkflow(options)` — returns a JS async-iterable of every event the
    /// engine appends, in order. Mirrors the TS async generator: a queue +
    /// resolver handshake, with best-effort `publish` fan-out before each yield.
    let runWorkflow (options: RunWorkflowOptions) : obj =
        // Capture runId as it emerges from RUN_STARTED so publish carries the key.
        let runIdForPublish = ref options.RunId

        let onEventJs: obj =
            match options.Publish with
            | None -> CoreSdk.undefinedValue
            | Some publish ->
                box (
                    System.Func<obj, JS.Promise<unit>>(fun eventObj ->
                        let event = WorkflowEvent.ofObj eventObj

                        match runIdForPublish.Value, event with
                        | None, RunStarted e -> runIdForPublish.Value <- Some e.RunId
                        | _ -> ()

                        match runIdForPublish.Value with
                        | Some rid ->
                            // Best-effort: swallow publisher errors.
                            CoreSdk.promiseCatchSwallow (publish rid event)
                        | None -> CoreSdk.promiseResolveUnit ())
                )

        let channel = CoreSdk.makeEventChannel onEventJs
        let iterable = CoreSdk.prop<obj> channel "iterable"
        let emitJs = CoreSdk.prop<obj> channel "emit"
        let doneJs = CoreSdk.prop<obj> channel "done"

        let emit (event: WorkflowEvent) : unit =
            // Push the JS-object form so the channel's onEvent/publish see plain
            // events identical to the persisted/transport shape.
            CoreSdk.callEmit (WorkflowEvent.toObj event) emitJs

        // Start execution in the background; route errors through the swallow
        // then signal completion (the async generator's `.finally`).
        let exec =
            promise {
                do! CoreSdk.promiseCatchSwallow (drive options emit)
                CoreSdk.callDone doneJs
            }

        CoreSdk.promiseStartIgnore exec

        iterable

    /// Collector form: drains `runWorkflow` to a `WorkflowEvent[]` (the
    /// `for await` loop used by `handleWorkflowWebhook`). Events come back as
    /// the typed DU.
    let runWorkflowCollect (options: RunWorkflowOptions) : JS.Promise<WorkflowEvent[]> =
        promise {
            let iterable = runWorkflow options
            let! raw = CoreSdk.collectAsyncIterable<obj> iterable
            return Array.map WorkflowEvent.ofObj raw
        }
