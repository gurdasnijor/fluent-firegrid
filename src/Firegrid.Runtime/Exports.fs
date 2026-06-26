module Firegrid.Runtime.Exports

open Fable.Core
open Firegrid.Core
open Firegrid.Runtime

// index.ts also re-exports all of `@firegrid/core`. In F#/Fable, consumers
// should `open Firegrid.Core` (or reference `Firegrid.Core.Exports`) alongside
// this module to obtain the Core surface; the Runtime-specific surface follows.

// ===== define-runtime =====
let defineWorkflowRuntime (config: WorkflowRuntimeConfig) : WorkflowRuntimeDefinition =
    DefineRuntime.defineWorkflowRuntime config

let cron (expression: string) (timezone: string option) : WorkflowScheduleSpec = DefineRuntime.cron expression timezone

/// `every` interval helpers (`every.milliseconds`, `every.seconds`, ...).
module every =
    let milliseconds (everyMs: float) : WorkflowScheduleSpec = DefineRuntime.every.milliseconds everyMs
    let seconds (seconds: float) : WorkflowScheduleSpec = DefineRuntime.every.seconds seconds
    let minutes (minutes: float) : WorkflowScheduleSpec = DefineRuntime.every.minutes minutes
    let hours (hours: float) : WorkflowScheduleSpec = DefineRuntime.every.hours hours

// ===== in-memory-store =====
let inMemoryWorkflowExecutionStore () : WorkflowExecutionStore = InMemoryStore.create ()

// ===== run-store-adapter =====
let createRunStoreAdapter (store: WorkflowRunStoreAdapterStore) : WorkflowRunStoreAdapter =
    RunStoreAdapter.create store

// ===== runtime-driver =====
let createRuntimeDriver (config: WorkflowRuntimeConfig) : WorkflowRuntimeDriver = RuntimeDriver.create config

// ===== schedule-materializer =====
let materializeWorkflowSchedules
    (runtime: WorkflowRuntimeDefinition)
    (options: ScheduleMaterializer.MaterializeWorkflowSchedulesOptions)
    : JS.Promise<ScheduleMaterializer.MaterializedWorkflowSchedule[]> =
    ScheduleMaterializer.materialize runtime options
