import { Option, type Exit } from "effect"
import type { Workflow } from "effect/unstable/workflow"
import {
  decodeExit,
  decodeResult,
  type ExecutionStarted,
  type TimerScheduled,
  type WorkflowRecord,
  type WorkflowResult,
} from "./records.ts"

export interface FoldedExecution {
  readonly started: ExecutionStarted | undefined
  readonly completed: WorkflowResult | undefined
  readonly activities: ReadonlyMap<string, WorkflowResult>
  readonly deferreds: ReadonlyMap<string, Exit.Exit<unknown, unknown>>
  readonly timers: ReadonlyMap<string, TimerScheduled>
  readonly interrupted: boolean
}

export const deferredId = (executionId: string, deferredName: string): string =>
  `${executionId}/${deferredName}`

export const activityId = (
  executionId: string,
  activityName: string,
  attempt: number,
): string => `${executionId}/${activityName}/${attempt}`

export const timerId = (executionId: string, clockName: string): string =>
  `${executionId}/${clockName}`

export const foldRecords = (
  records: Iterable<WorkflowRecord>,
): FoldedExecution =>
  Array.from(records).reduce<{
    started: ExecutionStarted | undefined
    completed: WorkflowResult | undefined
    interrupted: boolean
    activities: Map<string, WorkflowResult>
    deferreds: Map<string, Exit.Exit<unknown, unknown>>
    timers: Map<string, TimerScheduled>
  }>((state, record) => {
    switch (record._tag) {
      case "ExecutionStarted": {
        state.started ??= record
        break
      }
      case "ExecutionCompleted": {
        state.completed ??= decodeResult(record.result)
        break
      }
      case "ActivityCompleted": {
        if (!state.activities.has(record.activityId)) {
          state.activities.set(record.activityId, decodeResult(record.result))
        }
        break
      }
      case "DeferredCompleted": {
        if (!state.deferreds.has(record.deferredId)) {
          state.deferreds.set(record.deferredId, decodeExit(record.exit))
        }
        break
      }
      case "TimerScheduled": {
        state.timers.set(record.timerId, record)
        break
      }
      case "InterruptRequested": {
        state.interrupted = true
        break
      }
    }
    return state
  }, {
    started: undefined,
    completed: undefined,
    activities: new Map(),
    deferreds: new Map(),
    timers: new Map(),
    interrupted: false,
  })

export const completedResultOption = <A, E>(
  folded: FoldedExecution,
): Option.Option<Workflow.Result<A, E>> =>
  folded.completed === undefined
    ? Option.none()
    : Option.some(folded.completed as Workflow.Result<A, E>)
