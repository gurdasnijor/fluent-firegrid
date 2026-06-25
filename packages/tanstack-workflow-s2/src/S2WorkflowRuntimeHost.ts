/* oxlint-disable effect/restricted-syntax -- This host is a Promise-based TanStack Workflow runtime boundary. */
import {
  defineWorkflowRuntime,
  type MaterializedWorkflowSchedule,
  materializeWorkflowSchedules,
  type MaterializeWorkflowSchedulesOptions,
  type WorkflowRegistrationMap,
  type WorkflowRuntimeDefinition,
  type WorkflowRuntimeRunResult,
  type WorkflowRuntimeSweepArgs,
  type WorkflowRuntimeSweepResult
} from "@tanstack/workflow-runtime"

import { s2WorkflowExecutionStore, type S2WorkflowExecutionStoreConfig } from "./s2WorkflowExecutionStore.ts"
import type { RunClaim, WorkflowExecutionStore } from "./types.ts"

export interface S2WorkflowRuntimeConfig<TWorkflows extends WorkflowRegistrationMap>
  extends S2WorkflowExecutionStoreConfig
{
  readonly workflows: TWorkflows
  readonly defaultLeaseMs?: number
}

export interface S2WorkflowRuntimeHostRecoverArgs {
  readonly now?: number
  readonly limit?: number
  readonly leaseOwner?: string
  readonly leaseMs?: number
  readonly includeEvents?: boolean
  readonly maxEvents?: number
}

export interface S2WorkflowRuntimeHostRecoverResult {
  readonly claims: ReadonlyArray<RunClaim>
  readonly runs: ReadonlyArray<WorkflowRuntimeRunResult>
}

export interface S2WorkflowRuntimeHostTickArgs extends MaterializeWorkflowSchedulesOptions, WorkflowRuntimeSweepArgs {
  readonly materializeSchedules?: boolean
  readonly recoverStaleRuns?: boolean
  readonly staleRunLimit?: number
  readonly staleLeaseOwner?: string
  readonly staleLeaseMs?: number
}

export interface S2WorkflowRuntimeHostTickResult {
  readonly materialized: ReadonlyArray<MaterializedWorkflowSchedule>
  readonly recovered: S2WorkflowRuntimeHostRecoverResult
  readonly sweep: WorkflowRuntimeSweepResult
}

export interface S2WorkflowRuntimeHostLoopArgs extends S2WorkflowRuntimeHostTickArgs {
  readonly intervalMs?: number
  readonly signal?: AbortSignal
  readonly onTick?: (result: S2WorkflowRuntimeHostTickResult) => void | Promise<void>
}

export interface S2WorkflowRuntimeHost<TWorkflows extends WorkflowRegistrationMap> {
  readonly runtime: WorkflowRuntimeDefinition<TWorkflows>
  readonly store: WorkflowExecutionStore
  readonly materializeSchedules: (
    options?: MaterializeWorkflowSchedulesOptions
  ) => Promise<ReadonlyArray<MaterializedWorkflowSchedule>>
  readonly recoverStaleRuns: (
    args?: S2WorkflowRuntimeHostRecoverArgs
  ) => Promise<S2WorkflowRuntimeHostRecoverResult>
  readonly sweep: (args?: WorkflowRuntimeSweepArgs) => Promise<WorkflowRuntimeSweepResult>
  readonly tick: (args?: S2WorkflowRuntimeHostTickArgs) => Promise<S2WorkflowRuntimeHostTickResult>
  readonly runLoop: (args?: S2WorkflowRuntimeHostLoopArgs) => Promise<void>
}

export const defineS2WorkflowRuntime = <const TWorkflows extends WorkflowRegistrationMap>(
  config: S2WorkflowRuntimeConfig<TWorkflows>
): WorkflowRuntimeDefinition<TWorkflows> =>
  defineWorkflowRuntime({
    store: s2WorkflowExecutionStore(config),
    workflows: config.workflows,
    ...(config.defaultLeaseMs === undefined ? {} : { defaultLeaseMs: config.defaultLeaseMs })
  })

export const createS2WorkflowRuntimeHost = <const TWorkflows extends WorkflowRegistrationMap>(
  config: S2WorkflowRuntimeConfig<TWorkflows>
): S2WorkflowRuntimeHost<TWorkflows> => {
  const store = s2WorkflowExecutionStore(config)
  const runtime = defineWorkflowRuntime({
    store,
    workflows: config.workflows,
    ...(config.defaultLeaseMs === undefined ? {} : { defaultLeaseMs: config.defaultLeaseMs })
  })

  const materializeSchedulesForRuntime = (options: MaterializeWorkflowSchedulesOptions = {}) =>
    materializeWorkflowSchedules(runtime, options)

  const sweep = (args: WorkflowRuntimeSweepArgs = {}) => runtime.sweep(args)

  const recoverStaleRuns = async (
    args: S2WorkflowRuntimeHostRecoverArgs = {}
  ): Promise<S2WorkflowRuntimeHostRecoverResult> => {
    const now = args.now ?? Date.now()
    const leaseOwner = args.leaseOwner ?? `s2-host:recover:${now}`
    const leaseMs = args.leaseMs ?? config.defaultLeaseMs ?? 30_000
    const claims = await store.claimStaleRuns({
      leaseMs,
      leaseOwner,
      limit: args.limit ?? 25,
      now
    })
    const runs: Array<WorkflowRuntimeRunResult> = []
    for (const claim of claims) {
      runs.push(
        await runtime.startRun({
          input: claim.run.input,
          leaseMs,
          leaseOwner,
          now,
          runId: claim.run.runId,
          workflowId: claim.run.workflowId,
          ...(args.includeEvents === undefined ? {} : { includeEvents: args.includeEvents }),
          ...(args.maxEvents === undefined ? {} : { maxEvents: args.maxEvents })
        })
      )
    }
    return { claims, runs }
  }

  const tick = async (args: S2WorkflowRuntimeHostTickArgs = {}): Promise<S2WorkflowRuntimeHostTickResult> => {
    const now = args.now ?? Date.now()
    const staleLeaseMs = args.staleLeaseMs ?? args.leaseMs
    const staleLeaseOwner = args.staleLeaseOwner ?? args.leaseOwner
    const materialized = args.materializeSchedules === false
      ? []
      : await materializeSchedulesForRuntime({
        now,
        ...(args.cronLookbackMs === undefined ? {} : { cronLookbackMs: args.cronLookbackMs })
      })
    const recovered = args.recoverStaleRuns === false
      ? { claims: [], runs: [] }
      : await recoverStaleRuns({
        now,
        ...(args.includeEvents === undefined ? {} : { includeEvents: args.includeEvents }),
        ...(staleLeaseMs === undefined ? {} : { leaseMs: staleLeaseMs }),
        ...(staleLeaseOwner === undefined ? {} : { leaseOwner: staleLeaseOwner }),
        ...(args.staleRunLimit === undefined ? {} : { limit: args.staleRunLimit }),
        ...(args.maxEvents === undefined ? {} : { maxEvents: args.maxEvents })
      })
    const sweepResult = await sweep(args)
    return { materialized, recovered, sweep: sweepResult }
  }

  const runLoop = async (args: S2WorkflowRuntimeHostLoopArgs = {}): Promise<void> => {
    const intervalMs = args.intervalMs ?? 1_000
    while (args.signal?.aborted !== true) {
      const result = await tick(args)
      await args.onTick?.(result)
      await sleep(intervalMs, args.signal)
    }
  }

  return {
    materializeSchedules: materializeSchedulesForRuntime,
    recoverStaleRuns,
    runLoop,
    runtime,
    store,
    sweep,
    tick
  }
}

const sleep = (intervalMs: number, signal: AbortSignal | undefined): Promise<void> =>
  new Promise((resolve) => {
    if (signal?.aborted === true || intervalMs <= 0) {
      resolve()
      return
    }
    let settled = false
    const finish = () => {
      if (settled) return
      settled = true
      resolve()
    }
    const timeout = setTimeout(finish, intervalMs)
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout)
        finish()
      },
      { once: true }
    )
  })
