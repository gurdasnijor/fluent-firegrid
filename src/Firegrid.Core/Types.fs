namespace Firegrid.Core

open Fable.Core
open Fable.Core.JsInterop

// ============================================================
// Standard Schema input
// ============================================================

/// `SchemaInput` (Standard Schema v1). Held as `obj`; validate it via
/// `Schema.validate` which calls `schema["~standard"].validate(value)`.
type SchemaInput = obj

/// Outcome of a synchronous standard-schema validation.
type SchemaValidation =
    | Validated of value: obj
    | Issues
    | Async

[<RequireQualifiedAccess>]
module Schema =

    /// Call `schema["~standard"].validate(value)` and interpret the result.
    let validate (schema: SchemaInput) (value: obj) : SchemaValidation =
        let result = CoreSdk.schemaValidate schema value

        if CoreSdk.isPromise result then
            Async
        else
            let issues = CoreSdk.prop<obj> result "issues"

            if not (CoreSdk.isNullish issues) then
                Issues
            else
                Validated(CoreSdk.prop<obj> result "value")

// ============================================================
// Misc shared aliases
// ============================================================

/// `WorkflowMetadata` — free-form host/UI metadata.
type WorkflowMetadata = obj

type DurableOperationOptions =
    { Id: string option
      Meta: WorkflowMetadata option }

    static member Empty = { Id = None; Meta = None }

// ============================================================
// Step support types
// ============================================================

/// `StepContext` — per-attempt scope passed to `ctx.step`'s fn.
type StepContext =
    { Id: string
      Attempt: int
      Signal: CoreSdk.AbortSignal }

/// Backoff strategy for `StepRetryOptions`.
type StepBackoff =
    | Exponential
    | Fixed
    | Custom of (int -> float)

type StepRetryOptions =
    { MaxAttempts: int
      Backoff: StepBackoff option
      BaseMs: float option
      ShouldRetry: (obj -> int -> bool) option }

type StepOptions =
    { Meta: WorkflowMetadata option
      Retry: StepRetryOptions option
      Timeout: float option }

    static member Empty =
        { Meta = None
          Retry = None
          Timeout = None }

type StepAttempt =
    { StartedAt: float
      FinishedAt: float
      Result: obj option
      Error: SerializedError option }

// ============================================================
// Wait-for-event / approve options
// ============================================================

type WaitForEventOptions =
    { Id: string option
      Meta: WorkflowMetadata option
      Deadline: float option
      Schema: SchemaInput option }

    static member Empty =
        { Id = None
          Meta = None
          Deadline = None
          Schema = None }

type SleepOptions =
    { Id: string option
      Meta: WorkflowMetadata option }

    static member Empty = { Id = None; Meta = None }

type DeterministicValueOptions =
    { Id: string option
      Meta: WorkflowMetadata option }

    static member Empty = { Id = None; Meta = None }

type ApproveOptions =
    { Id: string option
      Meta: WorkflowMetadata option
      Title: string
      Description: string option }

type ApprovalResult =
    { Approved: bool
      ApprovalId: string
      Feedback: string option
      Meta: WorkflowMetadata option }

// ============================================================
// Workflow event (the unified log + transport event)
// ============================================================

type RunStartedEvent =
    { Ts: float
      RunId: string
      ThreadId: string option
      Audience: string option }

type RunFinishedEvent =
    { Ts: float
      RunId: string
      Output: obj
      Audience: string option }

type RunErroredEvent =
    { Ts: float
      RunId: string
      Error: SerializedError
      Code: string
      Audience: string option }

type StepStartedEvent =
    { Ts: float
      StepId: string
      Meta: WorkflowMetadata option
      Audience: string option }

type StepFinishedEvent =
    { Ts: float
      StepId: string
      Result: obj
      Attempts: StepAttempt[] option
      Meta: WorkflowMetadata option
      Audience: string option }

type StepFailedEvent =
    { Ts: float
      StepId: string
      Error: SerializedError
      Attempts: StepAttempt[] option
      Meta: WorkflowMetadata option
      Audience: string option }

type SignalAwaitedEvent =
    { Ts: float
      StepId: string
      Name: string
      Deadline: float option
      Meta: WorkflowMetadata option
      Audience: string option }

type SignalResolvedEvent =
    { Ts: float
      StepId: string
      Name: string
      SignalId: string option
      Payload: obj
      Meta: WorkflowMetadata option
      Audience: string option }

type ApprovalRequestedEvent =
    { Ts: float
      StepId: string
      ApprovalId: string
      Title: string
      Description: string option
      Meta: WorkflowMetadata option
      Audience: string option }

type ApprovalResolvedEvent =
    { Ts: float
      StepId: string
      ApprovalId: string
      Approved: bool
      Feedback: string option
      Meta: WorkflowMetadata option
      Audience: string option }

type NowRecordedEvent =
    { Ts: float
      StepId: string
      Value: float
      Meta: WorkflowMetadata option
      Audience: string option }

type UuidRecordedEvent =
    { Ts: float
      StepId: string
      Value: string
      Meta: WorkflowMetadata option
      Audience: string option }

type StateDeltaEvent =
    { Ts: float
      Delta: Operation[]
      Audience: string option }

type CustomEvent =
    { Ts: float
      Name: string
      Value: obj
      Audience: string option }

/// `WorkflowEvent` — one case per `type` discriminant in types.ts.
type WorkflowEvent =
    | RunStarted of RunStartedEvent
    | RunFinished of RunFinishedEvent
    | RunErrored of RunErroredEvent
    | StepStarted of StepStartedEvent
    | StepFinished of StepFinishedEvent
    | StepFailed of StepFailedEvent
    | SignalAwaited of SignalAwaitedEvent
    | SignalResolved of SignalResolvedEvent
    | ApprovalRequested of ApprovalRequestedEvent
    | ApprovalResolved of ApprovalResolvedEvent
    | NowRecorded of NowRecordedEvent
    | UuidRecorded of UuidRecordedEvent
    | StateDelta of StateDeltaEvent
    | Custom of CustomEvent

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module WorkflowEvent =

    /// The `type` string discriminant (replay matches on this).
    let eventType (event: WorkflowEvent) : string =
        match event with
        | RunStarted _ -> "RUN_STARTED"
        | RunFinished _ -> "RUN_FINISHED"
        | RunErrored _ -> "RUN_ERRORED"
        | StepStarted _ -> "STEP_STARTED"
        | StepFinished _ -> "STEP_FINISHED"
        | StepFailed _ -> "STEP_FAILED"
        | SignalAwaited _ -> "SIGNAL_AWAITED"
        | SignalResolved _ -> "SIGNAL_RESOLVED"
        | ApprovalRequested _ -> "APPROVAL_REQUESTED"
        | ApprovalResolved _ -> "APPROVAL_RESOLVED"
        | NowRecorded _ -> "NOW_RECORDED"
        | UuidRecorded _ -> "UUID_RECORDED"
        | StateDelta _ -> "STATE_DELTA"
        | Custom _ -> "CUSTOM"

    /// The `stepId` field where present (`"stepId" in event` in the TS).
    let eventStepId (event: WorkflowEvent) : string option =
        match event with
        | StepStarted e -> Some e.StepId
        | StepFinished e -> Some e.StepId
        | StepFailed e -> Some e.StepId
        | SignalAwaited e -> Some e.StepId
        | SignalResolved e -> Some e.StepId
        | ApprovalRequested e -> Some e.StepId
        | ApprovalResolved e -> Some e.StepId
        | NowRecorded e -> Some e.StepId
        | UuidRecorded e -> Some e.StepId
        | _ -> None

    // ── JS-object serialization (store / wire boundary) ───────────────

    let private setOpt (o: obj) (key: string) (v: 'a option) =
        match v with
        | Some value -> CoreSdk.setProp o key (box value)
        | None -> ()

    let private attemptToObj (a: StepAttempt) : obj =
        let o = createObj [ "startedAt" ==> a.StartedAt; "finishedAt" ==> a.FinishedAt ]
        setOpt o "result" a.Result
        match a.Error with
        | Some err -> CoreSdk.setProp o "error" (SerializedError.toObj err)
        | None -> ()
        o

    let private attemptOfObj (o: obj) : StepAttempt =
        { StartedAt = CoreSdk.prop<float> o "startedAt"
          FinishedAt = CoreSdk.prop<float> o "finishedAt"
          Result =
            let r = CoreSdk.prop<obj> o "result"
            if CoreSdk.isUndefined r then None else Some r
          Error =
            let e = CoreSdk.prop<obj> o "error"
            if CoreSdk.isNullish e then None else Some(SerializedError.ofObj e) }

    let private attemptsToObj (a: StepAttempt[] option) : obj =
        match a with
        | Some arr -> box (Array.map attemptToObj arr)
        | None -> CoreSdk.undefinedValue

    let private attemptsOfObj (o: obj) : StepAttempt[] option =
        if CoreSdk.isNullish o then
            None
        else
            Some(Array.map attemptOfObj (unbox<obj[]> o))

    /// JS-object form of an event for `runStore.appendEvent` / wire transport.
    let toObj (event: WorkflowEvent) : obj =
        let o = createObj [ "type" ==> eventType event ]

        match event with
        | RunStarted e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "runId" (box e.RunId)
            setOpt o "threadId" e.ThreadId
            setOpt o "audience" e.Audience
        | RunFinished e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "runId" (box e.RunId)
            CoreSdk.setProp o "output" e.Output
            setOpt o "audience" e.Audience
        | RunErrored e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "runId" (box e.RunId)
            CoreSdk.setProp o "error" (SerializedError.toObj e.Error)
            CoreSdk.setProp o "code" (box e.Code)
            setOpt o "audience" e.Audience
        | StepStarted e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | StepFinished e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "result" e.Result
            CoreSdk.setProp o "attempts" (attemptsToObj e.Attempts)
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | StepFailed e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "error" (SerializedError.toObj e.Error)
            CoreSdk.setProp o "attempts" (attemptsToObj e.Attempts)
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | SignalAwaited e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "name" (box e.Name)
            setOpt o "deadline" e.Deadline
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | SignalResolved e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "name" (box e.Name)
            setOpt o "signalId" e.SignalId
            CoreSdk.setProp o "payload" e.Payload
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | ApprovalRequested e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "approvalId" (box e.ApprovalId)
            CoreSdk.setProp o "title" (box e.Title)
            setOpt o "description" e.Description
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | ApprovalResolved e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "approvalId" (box e.ApprovalId)
            CoreSdk.setProp o "approved" (box e.Approved)
            setOpt o "feedback" e.Feedback
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | NowRecorded e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "value" (box e.Value)
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | UuidRecorded e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "stepId" (box e.StepId)
            CoreSdk.setProp o "value" (box e.Value)
            setOpt o "meta" e.Meta
            setOpt o "audience" e.Audience
        | StateDelta e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "delta" (box (Array.map Operation.toObj e.Delta))
            setOpt o "audience" e.Audience
        | Custom e ->
            CoreSdk.setProp o "ts" (box e.Ts)
            CoreSdk.setProp o "name" (box e.Name)
            CoreSdk.setProp o "value" e.Value
            setOpt o "audience" e.Audience

        o

    let private optStr (o: obj) (key: string) : string option =
        let v = CoreSdk.prop<obj> o key
        if CoreSdk.isNullish v then None else Some(CoreSdk.stringValue v)

    let private optNum (o: obj) (key: string) : float option =
        let v = CoreSdk.prop<obj> o key
        if CoreSdk.isNullish v then None else Some(CoreSdk.numberValue v)

    let private optMeta (o: obj) (key: string) : WorkflowMetadata option =
        let v = CoreSdk.prop<obj> o key
        if CoreSdk.isNullish v then None else Some v

    /// Reconstruct a `WorkflowEvent` from its JS-object form.
    let ofObj (o: obj) : WorkflowEvent =
        match CoreSdk.prop<string> o "type" with
        | "RUN_STARTED" ->
            RunStarted
                { Ts = CoreSdk.prop<float> o "ts"
                  RunId = CoreSdk.prop<string> o "runId"
                  ThreadId = optStr o "threadId"
                  Audience = optStr o "audience" }
        | "RUN_FINISHED" ->
            RunFinished
                { Ts = CoreSdk.prop<float> o "ts"
                  RunId = CoreSdk.prop<string> o "runId"
                  Output = CoreSdk.prop<obj> o "output"
                  Audience = optStr o "audience" }
        | "RUN_ERRORED" ->
            RunErrored
                { Ts = CoreSdk.prop<float> o "ts"
                  RunId = CoreSdk.prop<string> o "runId"
                  Error = SerializedError.ofObj (CoreSdk.prop<obj> o "error")
                  Code = CoreSdk.prop<string> o "code"
                  Audience = optStr o "audience" }
        | "STEP_STARTED" ->
            StepStarted
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "STEP_FINISHED" ->
            StepFinished
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Result = CoreSdk.prop<obj> o "result"
                  Attempts = attemptsOfObj (CoreSdk.prop<obj> o "attempts")
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "STEP_FAILED" ->
            StepFailed
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Error = SerializedError.ofObj (CoreSdk.prop<obj> o "error")
                  Attempts = attemptsOfObj (CoreSdk.prop<obj> o "attempts")
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "SIGNAL_AWAITED" ->
            SignalAwaited
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Name = CoreSdk.prop<string> o "name"
                  Deadline = optNum o "deadline"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "SIGNAL_RESOLVED" ->
            SignalResolved
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Name = CoreSdk.prop<string> o "name"
                  SignalId = optStr o "signalId"
                  Payload = CoreSdk.prop<obj> o "payload"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "APPROVAL_REQUESTED" ->
            ApprovalRequested
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  ApprovalId = CoreSdk.prop<string> o "approvalId"
                  Title = CoreSdk.prop<string> o "title"
                  Description = optStr o "description"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "APPROVAL_RESOLVED" ->
            ApprovalResolved
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  ApprovalId = CoreSdk.prop<string> o "approvalId"
                  Approved = CoreSdk.prop<bool> o "approved"
                  Feedback = optStr o "feedback"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "NOW_RECORDED" ->
            NowRecorded
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Value = CoreSdk.prop<float> o "value"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "UUID_RECORDED" ->
            UuidRecorded
                { Ts = CoreSdk.prop<float> o "ts"
                  StepId = CoreSdk.prop<string> o "stepId"
                  Value = CoreSdk.prop<string> o "value"
                  Meta = optMeta o "meta"
                  Audience = optStr o "audience" }
        | "STATE_DELTA" ->
            StateDelta
                { Ts = CoreSdk.prop<float> o "ts"
                  Delta = [||] // delta ops are not re-parsed on read; replay ignores STATE_DELTA
                  Audience = optStr o "audience" }
        | _ ->
            Custom
                { Ts = CoreSdk.prop<float> o "ts"
                  Name = CoreSdk.prop<string> o "name"
                  Value = CoreSdk.prop<obj> o "value"
                  Audience = optStr o "audience" }

// ============================================================
// Ctx — single argument to every workflow handler
// ============================================================

/// `BaseCtx` durable primitives. Modeled as a record of functions returning
/// JS promises (the TS returns `Promise`). Middleware extensions live on
/// `Extensions` (a JS object) so they can be merged in place.
type BaseCtx =
    { mutable RunId: string
      mutable Input: obj
      mutable State: obj
      Signal: CoreSdk.AbortSignal
      Step: string -> (StepContext -> obj) -> StepOptions option -> JS.Promise<obj>
      Sleep: float -> SleepOptions option -> JS.Promise<unit>
      SleepUntil: float -> SleepOptions option -> JS.Promise<unit>
      WaitForEvent: string -> WaitForEventOptions option -> JS.Promise<obj>
      Approve: ApproveOptions -> JS.Promise<ApprovalResult>
      Now: DeterministicValueOptions option -> JS.Promise<float>
      Uuid: DeterministicValueOptions option -> JS.Promise<string>
      Emit: string -> obj -> unit }

/// `Ctx` — the JS object actually handed to handlers/middleware. The built-in
/// `BaseCtx` fields are merged onto it, plus any middleware extensions; held
/// as `obj` so `Object.assign` can layer extensions in place (matching TS).
type Ctx = obj

// ============================================================
// Middleware
// ============================================================

/// `MiddlewareServerFn` — receives `{ ctx, next }`; `next` takes
/// `{ context }` and returns a promise.
type MiddlewareServerFn = Ctx -> (obj -> JS.Promise<obj>) -> JS.Promise<obj>

type Middleware =
    { Kind: string // "middleware"
      Server: MiddlewareServerFn }

// ============================================================
// Workflow definition
// ============================================================

type WorkflowDefinition =
    { Kind: string // "workflow"
      Id: string
      Description: string option
      Version: string option
      PreviousVersions: WorkflowDefinition[]
      InputSchema: SchemaInput option
      OutputSchema: SchemaInput option
      StateSchema: SchemaInput option
      Initialize: (obj -> obj) option
      DefaultStepRetry: StepRetryOptions option
      Middlewares: Middleware[]
      Handler: Ctx -> JS.Promise<obj> }

type AnyWorkflowDefinition = WorkflowDefinition

// ============================================================
// Signal delivery (resume calls)
// ============================================================

type SignalDelivery =
    { SignalId: string
      StepId: string option
      Name: string
      Payload: obj
      Meta: WorkflowMetadata option }

// ============================================================
// Run state (persistence shape)
// ============================================================

/// `RunStatus` — "running" | "paused" | "finished" | "errored" | "aborted".
type RunStatus = string

type RunAwaitable =
    | AwaitSignal of stepId: string option * signalName: string * deadline: float option * meta: WorkflowMetadata option
    | AwaitApproval of
        stepId: string option *
        approvalId: string *
        title: string *
        description: string option *
        meta: WorkflowMetadata option

type WaitingFor =
    { StepId: string option
      SignalName: string
      Deadline: float option
      Meta: WorkflowMetadata option }

type PendingApproval =
    { StepId: string option
      ApprovalId: string
      Title: string
      Description: string option
      Meta: WorkflowMetadata option }

type RunState =
    { RunId: string
      Status: RunStatus
      WorkflowId: string
      WorkflowVersion: string option
      Input: obj
      Output: obj option
      Error: SerializedError option
      Awaiting: RunAwaitable[] option
      WaitingFor: WaitingFor option
      PendingApproval: PendingApproval option
      CreatedAt: float
      UpdatedAt: float }

// ============================================================
// RunStore — backing storage
// ============================================================

/// `DeleteReason` — "finished" | "errored" | "aborted".
type DeleteReason = string

/// `RunStore` modeled as a record of functions (the TS interface of methods).
/// `Subscribe` is optional (push-based stores only).
type RunStore =
    { GetRunState: string -> JS.Promise<RunState option>
      SetRunState: string -> RunState -> JS.Promise<unit>
      DeleteRun: string -> DeleteReason -> JS.Promise<unit>
      AppendEvent: string -> float -> WorkflowEvent -> JS.Promise<unit>
      GetEvents: string -> JS.Promise<WorkflowEvent[]>
      Subscribe: (string -> int -> (WorkflowEvent -> int -> unit) -> (unit -> unit)) option }
