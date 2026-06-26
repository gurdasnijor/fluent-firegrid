module Firegrid.Runtime.Exports

open Firegrid.Core
open Firegrid.Runtime

let createWorkflow workflowId =
    { WorkflowId = workflowId
      Version = None }

let defineWorkflowRuntime workflows = Runtime.define workflows

let every expression = expression

let cron expression = expression

let materializeWorkflowSchedules schedules = schedules
