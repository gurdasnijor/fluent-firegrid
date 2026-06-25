// Adapted from TanStack Workflow runtime/core types.
// Reference: repos/tanstack-workflow @ 602cdec439876335168d96f5443c0dc59e4cc436.

import * as Data from "effect/Data"

export type WorkflowId = string
export type WorkflowVersion = string
export type RunId = string
export type ScheduleId = string
export type ScheduleBucketId = string
export type LeaseOwner = string

export type RunStatus = "running" | "paused" | "finished" | "errored" | "aborted"
export type WorkflowExecutionStatus = RunStatus | "queued"
export type DeleteReason = "finished" | "errored" | "aborted"

export interface WorkflowMetadata {
  readonly [key: string]: unknown
}

export interface SerializedError {
  readonly name?: string
  readonly message: string
  readonly stack?: string
  readonly cause?: unknown
}

export interface StepAttempt {
  readonly attempt: number
  readonly startedAt: number
  readonly finishedAt?: number
  readonly error?: SerializedError
}

export type WorkflowEvent = {
  readonly type: string
  readonly ts: number
  readonly stepId?: string
  readonly [key: string]: unknown
}

export interface RunAwaitable {
  readonly type: "signal" | "approval"
  readonly stepId?: string
  readonly signalName?: string
  readonly approvalId?: string
  readonly deadline?: number
  readonly meta?: WorkflowMetadata
}

export interface RunState<TInput = unknown, TOutput = unknown> {
  readonly runId: string
  readonly status: RunStatus
  readonly workflowId: string
  readonly workflowVersion?: string
  readonly input: TInput
  readonly output?: TOutput
  readonly error?: SerializedError
  readonly awaiting?: ReadonlyArray<RunAwaitable>
  readonly waitingFor?: {
    readonly stepId?: string
    readonly signalName: string
    readonly deadline?: number
    readonly meta?: WorkflowMetadata
  }
  readonly pendingApproval?: {
    readonly stepId?: string
    readonly approvalId: string
    readonly title: string
    readonly description?: string
    readonly meta?: WorkflowMetadata
  }
  readonly createdAt: number
  readonly updatedAt: number
}

export class LogConflictError extends Data.TaggedError("LogConflictError")<{
  readonly attemptedIndex: number
  readonly existing?: WorkflowEvent
  readonly message: string
  readonly runId: string
}> {
  constructor(runId: string, attemptedIndex: number, existing?: WorkflowEvent) {
    super({
      attemptedIndex,
      ...(existing === undefined ? {} : { existing }),
      message: `Log conflict for run ${runId} at index ${attemptedIndex}: another writer has already committed.`,
      runId
    })
  }
}

export interface WorkflowLease {
  readonly owner: LeaseOwner
  readonly expiresAt: number
}

export interface WorkflowExecution {
  readonly runId: RunId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly status: WorkflowExecutionStatus
  readonly input: unknown
  readonly output?: unknown
  readonly error?: SerializedError
  readonly awaiting?: RunState["awaiting"]
  readonly waitingFor?: RunState["waitingFor"]
  readonly pendingApproval?: RunState["pendingApproval"]
  readonly wakeAt?: number
  readonly lease?: WorkflowLease
  readonly createdAt: number
  readonly updatedAt: number
}

export interface StoredWorkflowEvent {
  readonly runId: RunId
  readonly eventIndex: number
  readonly eventType: WorkflowEvent["type"]
  readonly stepId?: string
  readonly event: WorkflowEvent
  readonly createdAt: number
}

export interface LoadedExecution {
  readonly run: WorkflowExecution
  readonly events: ReadonlyArray<StoredWorkflowEvent>
}

export interface CreateRunArgs {
  readonly runId: RunId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly input: unknown
  readonly now: number
}

export type CreateRunResult =
  | { readonly kind: "created"; readonly run: WorkflowExecution }
  | { readonly kind: "existing"; readonly run: WorkflowExecution }

export interface ReadEventsArgs {
  readonly runId: RunId
  readonly fromIndex?: number
}

export interface AppendEventsArgs {
  readonly runId: RunId
  readonly expectedNextIndex: number
  readonly events: ReadonlyArray<WorkflowEvent>
}

export interface AppendEventsResult {
  readonly nextIndex: number
}

export interface ClaimRunArgs {
  readonly runId: RunId
  readonly leaseOwner: LeaseOwner
  readonly leaseMs: number
  readonly now: number
}

export type ClaimRunResult =
  | { readonly kind: "claimed"; readonly run: WorkflowExecution }
  | { readonly kind: "not-found" }
  | { readonly kind: "not-claimable"; readonly run: WorkflowExecution }

export interface HeartbeatRunLeaseArgs {
  readonly runId: RunId
  readonly leaseOwner: LeaseOwner
  readonly leaseMs: number
  readonly now: number
}

export interface ReleaseRunLeaseArgs {
  readonly runId: RunId
  readonly leaseOwner: LeaseOwner
}

export interface MarkRunPausedArgs {
  readonly runId: RunId
  readonly awaiting?: RunState["awaiting"]
  readonly waitingFor?: RunState["waitingFor"]
  readonly pendingApproval?: RunState["pendingApproval"]
  readonly wakeAt?: number
  readonly now: number
}

export interface MarkRunFinishedArgs {
  readonly runId: RunId
  readonly output: unknown
  readonly now: number
}

export interface MarkRunErroredArgs {
  readonly runId: RunId
  readonly error: SerializedError
  readonly code: string
  readonly now: number
}

export interface ScheduleTimerArgs {
  readonly runId: RunId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly wakeAt: number
  readonly signalId: string
  readonly now: number
}

export interface ClaimDueTimersArgs {
  readonly now: number
  readonly limit: number
  readonly leaseOwner: LeaseOwner
  readonly leaseMs: number
}

export interface TimerWakeup {
  readonly runId: RunId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly wakeAt: number
  readonly signalId: string
}

export interface SignalDelivery<TPayload = unknown> {
  readonly signalId: string
  readonly stepId?: string
  readonly name: string
  readonly payload: TPayload
  readonly meta?: Record<string, unknown>
}

export interface DeliverSignalArgs<TPayload = unknown> {
  readonly runId: RunId
  readonly delivery: SignalDelivery<TPayload>
  readonly now: number
}

export type DeliverSignalResult =
  | { readonly kind: "delivered"; readonly run: WorkflowExecution }
  | { readonly kind: "duplicate"; readonly run: WorkflowExecution }
  | { readonly kind: "not-waiting"; readonly run: WorkflowExecution }
  | { readonly kind: "not-found" }

export interface ApprovalResult {
  readonly approvalId: string
  readonly approved: boolean
  readonly feedback?: string
  readonly meta?: Record<string, unknown>
}

export interface DeliverApprovalArgs {
  readonly runId: RunId
  readonly approval: ApprovalResult
  readonly now: number
}

export type DeliverApprovalResult =
  | { readonly kind: "delivered"; readonly run: WorkflowExecution }
  | { readonly kind: "duplicate"; readonly run: WorkflowExecution }
  | { readonly kind: "not-waiting"; readonly run: WorkflowExecution }
  | { readonly kind: "not-found" }

export type WorkflowOverlapPolicy = "skip" | "allow" | "buffer-one" | "cancel-previous" | "terminate-previous"

export type WorkflowScheduleSpec =
  | { readonly kind: "cron"; readonly expression: string; readonly timezone?: string }
  | { readonly kind: "interval"; readonly everyMs: number; readonly timezone?: string }

export interface UpsertScheduleArgs {
  readonly scheduleId: ScheduleId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly schedule: WorkflowScheduleSpec
  readonly overlapPolicy: WorkflowOverlapPolicy
  readonly input?: unknown
  readonly nextFireAt?: number
  readonly enabled: boolean
  readonly now: number
}

export interface ClaimDueScheduleBucketsArgs {
  readonly now: number
  readonly limit: number
  readonly leaseOwner: LeaseOwner
  readonly leaseMs: number
}

export interface ScheduleBucket {
  readonly scheduleId: ScheduleId
  readonly bucketId: ScheduleBucketId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly runId: RunId
  readonly fireAt: number
  readonly input: unknown
  readonly overlapPolicy: WorkflowOverlapPolicy
}

export interface MarkScheduleBucketStartedArgs {
  readonly scheduleId: ScheduleId
  readonly bucketId: ScheduleBucketId
  readonly runId: RunId
  readonly now: number
}

export interface ClaimStaleRunsArgs {
  readonly now: number
  readonly limit: number
  readonly leaseOwner: LeaseOwner
  readonly leaseMs: number
}

export interface RunClaim {
  readonly run: WorkflowExecution
  readonly lease: WorkflowLease
}

export interface ListRunsArgs {
  readonly workflowId?: WorkflowId
  readonly status?: WorkflowExecutionStatus
  readonly limit: number
  readonly cursor?: string
}

export interface RunSummary {
  readonly runId: RunId
  readonly workflowId: WorkflowId
  readonly workflowVersion?: WorkflowVersion
  readonly status: WorkflowExecutionStatus
  readonly awaiting?: RunState["awaiting"]
  readonly waitingFor?: RunState["waitingFor"]
  readonly pendingApproval?: RunState["pendingApproval"]
  readonly wakeAt?: number
  readonly createdAt: number
  readonly updatedAt: number
}

export interface RunTimeline {
  readonly run: WorkflowExecution
  readonly events: ReadonlyArray<StoredWorkflowEvent>
}

export interface SaveRunStateArgs {
  readonly state: RunState
}

export interface WorkflowRunStoreAdapterStore {
  readonly loadRunState: (runId: RunId) => Promise<RunState | undefined>
  readonly saveRunState: (args: SaveRunStateArgs) => Promise<void>
  readonly deleteRun: (runId: RunId, reason: DeleteReason) => Promise<void>
  readonly appendEvents: (args: AppendEventsArgs) => Promise<AppendEventsResult>
  readonly readEvents: (args: ReadEventsArgs) => Promise<ReadonlyArray<StoredWorkflowEvent>>
  readonly subscribeEvents?: (
    runId: RunId,
    fromIndex: number,
    onEvent: (event: WorkflowEvent, index: number) => void
  ) => () => void
}

export interface WorkflowExecutionStore extends WorkflowRunStoreAdapterStore {
  readonly createRun: (args: CreateRunArgs) => Promise<CreateRunResult>
  readonly loadRun: (runId: RunId) => Promise<WorkflowExecution | undefined>
  readonly loadExecution: (runId: RunId) => Promise<LoadedExecution | undefined>
  readonly claimRun: (args: ClaimRunArgs) => Promise<ClaimRunResult>
  readonly heartbeatRunLease: (args: HeartbeatRunLeaseArgs) => Promise<void>
  readonly releaseRunLease: (args: ReleaseRunLeaseArgs) => Promise<void>
  readonly markRunPaused: (args: MarkRunPausedArgs) => Promise<void>
  readonly markRunFinished: (args: MarkRunFinishedArgs) => Promise<void>
  readonly markRunErrored: (args: MarkRunErroredArgs) => Promise<void>
  readonly scheduleTimer: (args: ScheduleTimerArgs) => Promise<void>
  readonly claimDueTimers: (args: ClaimDueTimersArgs) => Promise<ReadonlyArray<TimerWakeup>>
  readonly deliverSignal: <TPayload = unknown>(args: DeliverSignalArgs<TPayload>) => Promise<DeliverSignalResult>
  readonly deliverApproval: (args: DeliverApprovalArgs) => Promise<DeliverApprovalResult>
  readonly upsertSchedule: (args: UpsertScheduleArgs) => Promise<void>
  readonly claimDueScheduleBuckets: (args: ClaimDueScheduleBucketsArgs) => Promise<ReadonlyArray<ScheduleBucket>>
  readonly markScheduleBucketStarted: (args: MarkScheduleBucketStartedArgs) => Promise<void>
  readonly claimStaleRuns: (args: ClaimStaleRunsArgs) => Promise<ReadonlyArray<RunClaim>>
  readonly listRuns: (args: ListRunsArgs) => Promise<ReadonlyArray<RunSummary>>
  readonly getRunTimeline: (runId: RunId) => Promise<RunTimeline | undefined>
}
