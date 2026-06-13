import { Exit, Option } from "effect"
import { Workflow } from "effect/unstable/workflow"

export type WorkflowResult = Workflow.Result<unknown, unknown>

export type PersistedExit =
  | {
      readonly _tag: "Success"
      readonly value: unknown
    }
  | {
      readonly _tag: "Failure"
      readonly error: unknown
    }

export type PersistedWorkflowResult =
  | {
      readonly _tag: "Complete"
      readonly exit: PersistedExit
    }
  | {
      readonly _tag: "Suspended"
    }

export type WorkflowRecord =
  | ExecutionStarted
  | ExecutionCompleted
  | ActivityCompleted
  | DeferredCompleted
  | TimerScheduled
  | InterruptRequested

export interface ExecutionStarted {
  readonly _tag: "ExecutionStarted"
  readonly workflowName: string
  readonly executionId: string
  readonly payload: object
  readonly parentExecutionId?: string
  readonly createdAt: number
}

export interface ExecutionCompleted {
  readonly _tag: "ExecutionCompleted"
  readonly executionId: string
  readonly result: unknown
  readonly completedAt: number
}

export interface ActivityCompleted {
  readonly _tag: "ActivityCompleted"
  readonly activityId: string
  readonly result: unknown
  readonly completedAt: number
}

export interface DeferredCompleted {
  readonly _tag: "DeferredCompleted"
  readonly deferredId: string
  readonly exit: unknown
  readonly completedAt: number
}

export interface TimerScheduled {
  readonly _tag: "TimerScheduled"
  readonly timerId: string
  readonly executionId: string
  readonly deferredName: string
  readonly dueAt: number
  readonly scheduledAt: number
}

export interface InterruptRequested {
  readonly _tag: "InterruptRequested"
  readonly unsafe: boolean
  readonly requestedAt: number
}

const encodeExit = (exit: Exit.Exit<unknown, unknown>): PersistedExit =>
  Exit.isSuccess(exit)
    ? { _tag: "Success", value: exit.value }
    : { _tag: "Failure", error: Option.getOrUndefined(Exit.findErrorOption(exit)) }

export const decodeExit = (encoded: unknown): Exit.Exit<unknown, unknown> => {
  const persisted = encoded as PersistedExit
  if (persisted._tag === "Success") {
    return Exit.succeed(persisted.value)
  }
  return Exit.fail(persisted.error)
}

const encodeResult = (result: WorkflowResult): PersistedWorkflowResult =>
  result._tag === "Complete"
    ? { _tag: "Complete", exit: encodeExit(result.exit) }
    : { _tag: "Suspended" }

export const decodeResult = (encoded: unknown): WorkflowResult => {
  const persisted = encoded as PersistedWorkflowResult
  if (persisted._tag === "Complete") {
    return new Workflow.Complete({ exit: decodeExit(persisted.exit) })
  }
  return new Workflow.Suspended({})
}

export const encodeRecord = (record: WorkflowRecord): string =>
  JSON.stringify(record)

export const decodeRecord = (body: string): WorkflowRecord =>
  JSON.parse(body) as WorkflowRecord

export const executionStarted = (options: {
  readonly workflowName: string
  readonly executionId: string
  readonly payload: object
  readonly parentExecutionId?: string | undefined
  readonly createdAt: number
}): ExecutionStarted => ({
  _tag: "ExecutionStarted",
  workflowName: options.workflowName,
  executionId: options.executionId,
  payload: options.payload,
  ...(options.parentExecutionId === undefined
    ? {}
    : { parentExecutionId: options.parentExecutionId }),
  createdAt: options.createdAt,
})

export const executionCompleted = (
  executionId: string,
  result: WorkflowResult,
  completedAt: number,
): ExecutionCompleted => ({
  _tag: "ExecutionCompleted",
  executionId,
  result: encodeResult(result),
  completedAt,
})

export const activityCompleted = (
  activityId: string,
  result: WorkflowResult,
  completedAt: number,
): ActivityCompleted => ({
  _tag: "ActivityCompleted",
  activityId,
  result: encodeResult(result),
  completedAt,
})

export const deferredCompleted = (
  deferredId: string,
  exit: Exit.Exit<unknown, unknown>,
  completedAt: number,
): DeferredCompleted => ({
  _tag: "DeferredCompleted",
  deferredId,
  exit: encodeExit(exit),
  completedAt,
})

export const timerScheduled = (options: {
  readonly timerId: string
  readonly executionId: string
  readonly deferredName: string
  readonly dueAt: number
  readonly scheduledAt: number
}): TimerScheduled => ({
  _tag: "TimerScheduled",
  timerId: options.timerId,
  executionId: options.executionId,
  deferredName: options.deferredName,
  dueAt: options.dueAt,
  scheduledAt: options.scheduledAt,
})

export const interruptRequested = (
  unsafe: boolean,
  requestedAt: number,
): InterruptRequested => ({
  _tag: "InterruptRequested",
  unsafe,
  requestedAt,
})
