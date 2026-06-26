namespace Firegrid.Runtime

open Fable.Core
open Firegrid.Core

// ============================================================
// Id aliases
// ============================================================

type WorkflowId = string
type WorkflowVersion = string
type RunId = string
type ScheduleId = string
type ScheduleBucketId = string
type LeaseOwner = string

/// `WorkflowExecutionStatus` = `RunStatus | "queued"`. Held as a string,
/// exactly like Core's `RunStatus`.
type WorkflowExecutionStatus = string

// ============================================================
// Lease / execution
// ============================================================

type WorkflowLease =
    { Owner: LeaseOwner
      ExpiresAt: float }

type WorkflowExecution =
    { RunId: RunId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      Status: WorkflowExecutionStatus
      Input: obj
      Output: obj option
      Error: SerializedError option
      Awaiting: RunAwaitable[] option
      WaitingFor: WaitingFor option
      PendingApproval: PendingApproval option
      WakeAt: float option
      Lease: WorkflowLease option
      CreatedAt: float
      UpdatedAt: float }

type StoredWorkflowEvent =
    { RunId: RunId
      EventIndex: float
      EventType: string
      StepId: string option
      Event: WorkflowEvent
      CreatedAt: float }

type LoadedExecution =
    { Run: WorkflowExecution
      Events: StoredWorkflowEvent[] }

// ============================================================
// *Args / *Result records
// ============================================================

type CreateRunArgs =
    { RunId: RunId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      Input: obj
      Now: float }

/// `CreateRunResult` — discriminated on `kind`.
type CreateRunResult =
    | CreateRunCreated of run: WorkflowExecution
    | CreateRunExisting of run: WorkflowExecution

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CreateRunResult =
    let kind (r: CreateRunResult) : string =
        match r with
        | CreateRunCreated _ -> "created"
        | CreateRunExisting _ -> "existing"

type ReadEventsArgs =
    { RunId: RunId
      FromIndex: float option }

type AppendEventsArgs =
    { RunId: RunId
      ExpectedNextIndex: float
      Events: WorkflowEvent[] }

type AppendEventsResult = { NextIndex: float }

type ClaimRunArgs =
    { RunId: RunId
      LeaseOwner: LeaseOwner
      LeaseMs: float
      Now: float }

/// `ClaimRunResult` — discriminated on `kind`.
type ClaimRunResult =
    | ClaimRunClaimed of run: WorkflowExecution
    | ClaimRunNotFound
    | ClaimRunNotClaimable of run: WorkflowExecution

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ClaimRunResult =
    let kind (r: ClaimRunResult) : string =
        match r with
        | ClaimRunClaimed _ -> "claimed"
        | ClaimRunNotFound -> "not-found"
        | ClaimRunNotClaimable _ -> "not-claimable"

type HeartbeatRunLeaseArgs =
    { RunId: RunId
      LeaseOwner: LeaseOwner
      LeaseMs: float
      Now: float }

type ReleaseRunLeaseArgs =
    { RunId: RunId
      LeaseOwner: LeaseOwner }

type MarkRunPausedArgs =
    { RunId: RunId
      Awaiting: RunAwaitable[] option
      WaitingFor: WaitingFor option
      PendingApproval: PendingApproval option
      WakeAt: float option
      Now: float }

type MarkRunFinishedArgs =
    { RunId: RunId
      Output: obj
      Now: float }

type MarkRunErroredArgs =
    { RunId: RunId
      Error: SerializedError
      Code: string
      Now: float }

type ScheduleTimerArgs =
    { RunId: RunId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      WakeAt: float
      SignalId: string
      SignalName: string option
      Now: float }

type ClaimDueTimersArgs =
    { Now: float
      Limit: int
      LeaseOwner: LeaseOwner
      LeaseMs: float }

type TimerWakeup =
    { RunId: RunId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      WakeAt: float
      SignalId: string
      SignalName: string option }

type DeliverSignalArgs =
    { RunId: RunId
      Delivery: SignalDelivery
      Now: float }

/// `DeliverSignalResult` — discriminated on `kind`.
type DeliverSignalResult =
    | DeliverSignalDelivered of run: WorkflowExecution
    | DeliverSignalDuplicate of run: WorkflowExecution
    | DeliverSignalNotWaiting of run: WorkflowExecution
    | DeliverSignalNotFound

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DeliverSignalResult =
    let kind (r: DeliverSignalResult) : string =
        match r with
        | DeliverSignalDelivered _ -> "delivered"
        | DeliverSignalDuplicate _ -> "duplicate"
        | DeliverSignalNotWaiting _ -> "not-waiting"
        | DeliverSignalNotFound -> "not-found"

    let run (r: DeliverSignalResult) : WorkflowExecution option =
        match r with
        | DeliverSignalDelivered run
        | DeliverSignalDuplicate run
        | DeliverSignalNotWaiting run -> Some run
        | DeliverSignalNotFound -> None

type DeliverApprovalArgs =
    { RunId: RunId
      Approval: ApprovalResult
      Now: float }

/// `DeliverApprovalResult` — discriminated on `kind`.
type DeliverApprovalResult =
    | DeliverApprovalDelivered of run: WorkflowExecution
    | DeliverApprovalDuplicate of run: WorkflowExecution
    | DeliverApprovalNotWaiting of run: WorkflowExecution
    | DeliverApprovalNotFound

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DeliverApprovalResult =
    let kind (r: DeliverApprovalResult) : string =
        match r with
        | DeliverApprovalDelivered _ -> "delivered"
        | DeliverApprovalDuplicate _ -> "duplicate"
        | DeliverApprovalNotWaiting _ -> "not-waiting"
        | DeliverApprovalNotFound -> "not-found"

    let run (r: DeliverApprovalResult) : WorkflowExecution option =
        match r with
        | DeliverApprovalDelivered run
        | DeliverApprovalDuplicate run
        | DeliverApprovalNotWaiting run -> Some run
        | DeliverApprovalNotFound -> None

// ============================================================
// Schedules
// ============================================================

/// `WorkflowOverlapPolicy` — held as a string literal union.
type WorkflowOverlapPolicy = string

/// `WorkflowScheduleSpec` — discriminated on `kind` ("cron" | "interval").
type WorkflowScheduleSpec =
    | CronSpec of expression: string * timezone: string option
    | IntervalSpec of everyMs: float * timezone: string option

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module WorkflowScheduleSpec =
    let kind (s: WorkflowScheduleSpec) : string =
        match s with
        | CronSpec _ -> "cron"
        | IntervalSpec _ -> "interval"

type WorkflowScheduleDefinition =
    { Id: ScheduleId option
      Schedule: WorkflowScheduleSpec
      OverlapPolicy: WorkflowOverlapPolicy option
      /// `unknown | (() => unknown | Promise<unknown>)` — held as `obj`.
      Input: obj option
      Enabled: bool option }

type UpsertScheduleArgs =
    { ScheduleId: ScheduleId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      Schedule: WorkflowScheduleSpec
      OverlapPolicy: WorkflowOverlapPolicy
      Input: obj option
      NextFireAt: float option
      Enabled: bool
      Now: float }

type ClaimDueScheduleBucketsArgs =
    { Now: float
      Limit: int
      LeaseOwner: LeaseOwner
      LeaseMs: float }

type ScheduleBucket =
    { ScheduleId: ScheduleId
      BucketId: ScheduleBucketId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      RunId: RunId
      FireAt: float
      Input: obj
      OverlapPolicy: WorkflowOverlapPolicy }

type MarkScheduleBucketStartedArgs =
    { ScheduleId: ScheduleId
      BucketId: ScheduleBucketId
      RunId: RunId
      Now: float }

type ClaimStaleRunsArgs =
    { Now: float
      Limit: int
      LeaseOwner: LeaseOwner
      LeaseMs: float }

type RunClaim =
    { Run: WorkflowExecution
      Lease: WorkflowLease }

type ListRunsArgs =
    { WorkflowId: WorkflowId option
      Status: WorkflowExecutionStatus option
      Limit: int
      Cursor: string option }

type RunSummary =
    { RunId: RunId
      WorkflowId: WorkflowId
      WorkflowVersion: WorkflowVersion option
      Status: WorkflowExecutionStatus
      Awaiting: RunAwaitable[] option
      WaitingFor: WaitingFor option
      PendingApproval: PendingApproval option
      WakeAt: float option
      CreatedAt: float
      UpdatedAt: float }

type RunTimeline =
    { Run: WorkflowExecution
      Events: StoredWorkflowEvent[] }

type SaveRunStateArgs = { State: RunState }

// ============================================================
// Store interfaces (records of functions returning JS.Promise)
// ============================================================

/// `WorkflowRunStoreAdapterStore` — the subset a `RunStore` adapter needs.
type WorkflowRunStoreAdapterStore =
    { LoadRunState: RunId -> JS.Promise<RunState option>
      SaveRunState: SaveRunStateArgs -> JS.Promise<unit>
      DeleteRun: RunId -> DeleteReason -> JS.Promise<unit>
      AppendEvents: AppendEventsArgs -> JS.Promise<AppendEventsResult>
      ReadEvents: ReadEventsArgs -> JS.Promise<StoredWorkflowEvent[]>
      SubscribeEvents: (RunId -> int -> (WorkflowEvent -> int -> unit) -> (unit -> unit)) option }

/// `WorkflowRunStoreAdapter` = `RunStore` (Core).
type WorkflowRunStoreAdapter = RunStore

/// `WorkflowExecutionStore` — extends `WorkflowRunStoreAdapterStore`.
type WorkflowExecutionStore =
    { // ── inherited from WorkflowRunStoreAdapterStore ──
      LoadRunState: RunId -> JS.Promise<RunState option>
      SaveRunState: SaveRunStateArgs -> JS.Promise<unit>
      DeleteRun: RunId -> DeleteReason -> JS.Promise<unit>
      AppendEvents: AppendEventsArgs -> JS.Promise<AppendEventsResult>
      ReadEvents: ReadEventsArgs -> JS.Promise<StoredWorkflowEvent[]>
      SubscribeEvents: (RunId -> int -> (WorkflowEvent -> int -> unit) -> (unit -> unit)) option
      // ── execution-store methods ──
      CreateRun: CreateRunArgs -> JS.Promise<CreateRunResult>
      LoadRun: RunId -> JS.Promise<WorkflowExecution option>
      LoadExecution: RunId -> JS.Promise<LoadedExecution option>
      ClaimRun: ClaimRunArgs -> JS.Promise<ClaimRunResult>
      HeartbeatRunLease: HeartbeatRunLeaseArgs -> JS.Promise<unit>
      ReleaseRunLease: ReleaseRunLeaseArgs -> JS.Promise<unit>
      MarkRunPaused: MarkRunPausedArgs -> JS.Promise<unit>
      MarkRunFinished: MarkRunFinishedArgs -> JS.Promise<unit>
      MarkRunErrored: MarkRunErroredArgs -> JS.Promise<unit>
      ScheduleTimer: ScheduleTimerArgs -> JS.Promise<unit>
      ClaimDueTimers: ClaimDueTimersArgs -> JS.Promise<TimerWakeup[]>
      DeliverSignal: DeliverSignalArgs -> JS.Promise<DeliverSignalResult>
      DeliverApproval: DeliverApprovalArgs -> JS.Promise<DeliverApprovalResult>
      UpsertSchedule: UpsertScheduleArgs -> JS.Promise<unit>
      ClaimDueScheduleBuckets: ClaimDueScheduleBucketsArgs -> JS.Promise<ScheduleBucket[]>
      MarkScheduleBucketStarted: MarkScheduleBucketStartedArgs -> JS.Promise<unit>
      ClaimStaleRuns: ClaimStaleRunsArgs -> JS.Promise<RunClaim[]>
      ListRuns: ListRunsArgs -> JS.Promise<RunSummary[]>
      GetRunTimeline: RunId -> JS.Promise<RunTimeline option> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module WorkflowExecutionStore =
    /// Project the inherited adapter-store view (used by `createRunStoreAdapter`).
    let asAdapterStore (s: WorkflowExecutionStore) : WorkflowRunStoreAdapterStore =
        { WorkflowRunStoreAdapterStore.LoadRunState = s.LoadRunState
          SaveRunState = s.SaveRunState
          DeleteRun = s.DeleteRun
          AppendEvents = s.AppendEvents
          ReadEvents = s.ReadEvents
          SubscribeEvents = s.SubscribeEvents }

// ============================================================
// Workflow loader / registration
// ============================================================

/// `WorkflowLoaderResult` — `TWorkflow | { default } | { workflow }`.
/// Held as `obj`; `normalizeWorkflowLoaderResult` resolves it.
type WorkflowLoaderResult = obj

/// `WorkflowLoader` — `() => Promise<WorkflowLoaderResult>`.
type WorkflowLoader = unit -> JS.Promise<WorkflowLoaderResult>

type WorkflowRegistration =
    { Load: WorkflowLoader
      Version: WorkflowVersion option
      /// `Record<WorkflowVersion, WorkflowLoader>` — kept as a JS object of
      /// loaders so `Object.values` preserves insertion order, matching the TS.
      PreviousVersions: obj option
      Schedules: WorkflowScheduleDefinition[] option }

/// `WorkflowRegistrationMap` — `Record<string, WorkflowRegistration>`. Held as a
/// JS object so `Object.entries`/lookup matches the TS exactly.
type WorkflowRegistrationMap = obj

type WorkflowRuntimeConfig =
    { Workflows: WorkflowRegistrationMap
      Store: WorkflowExecutionStore
      DefaultLeaseMs: float option }

// ============================================================
// Runtime run results
// ============================================================

/// `WorkflowRuntimeRunResultKind` — string literal union.
type WorkflowRuntimeRunResultKind = string

type WorkflowRuntimeRunResult =
    { Kind: WorkflowRuntimeRunResultKind
      RunId: RunId
      WorkflowId: WorkflowId option
      Run: WorkflowExecution option
      Events: WorkflowEvent[]
      EventCount: int
      EventsTruncated: bool option }

type WorkflowRuntimeStartRunArgs =
    { WorkflowId: WorkflowId
      RunId: RunId
      Input: obj
      Now: float option
      LeaseOwner: LeaseOwner option
      LeaseMs: float option
      ThreadId: string option
      IncludeEvents: bool option
      MaxEvents: float option }

type WorkflowRuntimeDeliverSignalArgs =
    { RunId: RunId
      SignalId: string
      StepId: string option
      Name: string
      Payload: obj
      Meta: WorkflowMetadata option
      Now: float option
      LeaseOwner: LeaseOwner option
      LeaseMs: float option
      ThreadId: string option
      IncludeEvents: bool option
      MaxEvents: float option }

type WorkflowRuntimeDeliverApprovalArgs =
    { RunId: RunId
      Approval: ApprovalResult
      Now: float option
      LeaseOwner: LeaseOwner option
      LeaseMs: float option
      ThreadId: string option
      IncludeEvents: bool option
      MaxEvents: float option }

type WorkflowRuntimeSweepArgs =
    { Now: float option
      Limit: int option
      MaxScheduledRuns: int option
      MaxTimers: int option
      MaxDurationMs: float option
      LeaseOwner: LeaseOwner option
      LeaseMs: float option
      IncludeEvents: bool option
      MaxEvents: float option }

    static member Empty =
        { Now = None
          Limit = None
          MaxScheduledRuns = None
          MaxTimers = None
          MaxDurationMs = None
          LeaseOwner = None
          LeaseMs = None
          IncludeEvents = None
          MaxEvents = None }

/// `WorkflowRuntimeRunKindCounts` — `Partial<Record<kind, number>>`. Held as a
/// JS object built incrementally to match TS counting semantics.
type WorkflowRuntimeRunKindCounts = obj

type WorkflowRuntimeSweepSummary =
    { Scheduled: WorkflowRuntimeRunKindCounts
      Timers: WorkflowRuntimeRunKindCounts
      EventCount: int
      ReturnedEventCount: int }

type WorkflowRuntimeSweepResult =
    { Scheduled: WorkflowRuntimeRunResult[]
      Timers: WorkflowRuntimeRunResult[]
      Summary: WorkflowRuntimeSweepSummary
      DeadlineReached: bool
      RemainingMayExist: bool }

/// `WorkflowRuntimeDefinition` — config plus the driver methods, tagged
/// `__kind: "workflow-runtime"`.
type WorkflowRuntimeDefinition =
    { Kind: string // "workflow-runtime"
      Workflows: WorkflowRegistrationMap
      Store: WorkflowExecutionStore
      DefaultLeaseMs: float option
      StartRun: WorkflowRuntimeStartRunArgs -> JS.Promise<WorkflowRuntimeRunResult>
      DeliverSignal: WorkflowRuntimeDeliverSignalArgs -> JS.Promise<WorkflowRuntimeRunResult>
      DeliverApproval: WorkflowRuntimeDeliverApprovalArgs -> JS.Promise<WorkflowRuntimeRunResult>
      Sweep: WorkflowRuntimeSweepArgs -> JS.Promise<WorkflowRuntimeSweepResult> }

/// The driver record returned by `createRuntimeDriver` (the subset of the
/// definition that is the four driver methods).
type WorkflowRuntimeDriver =
    { StartRun: WorkflowRuntimeStartRunArgs -> JS.Promise<WorkflowRuntimeRunResult>
      DeliverSignal: WorkflowRuntimeDeliverSignalArgs -> JS.Promise<WorkflowRuntimeRunResult>
      DeliverApproval: WorkflowRuntimeDeliverApprovalArgs -> JS.Promise<WorkflowRuntimeRunResult>
      Sweep: WorkflowRuntimeSweepArgs -> JS.Promise<WorkflowRuntimeSweepResult> }
