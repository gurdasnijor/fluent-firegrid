namespace Firegrid.Runtime

open Firegrid.Core

/// `define-runtime.ts` — `defineWorkflowRuntime`, `cron`, `every`.
[<RequireQualifiedAccess>]
module DefineRuntime =

    /// `defineWorkflowRuntime(config)`.
    let defineWorkflowRuntime (config: WorkflowRuntimeConfig) : WorkflowRuntimeDefinition =
        let driver = RuntimeDriver.create config

        { Kind = "workflow-runtime"
          Workflows = config.Workflows
          Store = config.Store
          DefaultLeaseMs = config.DefaultLeaseMs
          StartRun = driver.StartRun
          DeliverSignal = driver.DeliverSignal
          DeliverApproval = driver.DeliverApproval
          Sweep = driver.Sweep }

    /// `cron(expression, { timezone? })`.
    let cron (expression: string) (timezone: string option) : WorkflowScheduleSpec = CronSpec(expression, timezone)

    /// `every` — interval helpers.
    [<RequireQualifiedAccess>]
    module every =

        let milliseconds (everyMs: float) : WorkflowScheduleSpec = IntervalSpec(everyMs, None)

        let seconds (seconds: float) : WorkflowScheduleSpec = IntervalSpec(seconds * 1000.0, None)

        let minutes (minutes: float) : WorkflowScheduleSpec = IntervalSpec(minutes * 60.0 * 1000.0, None)

        let hours (hours: float) : WorkflowScheduleSpec = IntervalSpec(hours * 60.0 * 60.0 * 1000.0, None)
