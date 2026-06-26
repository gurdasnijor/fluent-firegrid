module Firegrid.Core.Exports

open Fable.Core
open Firegrid.Core

// ===== Errors =====
let fluentFiregridError (message: string) : exn = FluentFiregridError.create message
let logConflictError (runId: string) (attemptedIndex: float) (existing: obj option) : exn =
    LogConflictError.create runId attemptedIndex existing
let stepTimeoutError (stepId: string) (timeoutMs: float) : exn = StepTimeoutError.create stepId timeoutMs

// ===== Workflow definition =====
let createWorkflow (config: CreateWorkflowConfig) : WorkflowBuilder = DefineWorkflow.createWorkflow config

// ===== Middleware =====
let createMiddleware () : Middleware.CreateMiddlewareBuilder = Middleware.createMiddleware ()

// ===== Result helpers =====
let succeed (data: obj) : obj = Result.succeed data
let fail (reason: string) : obj = Result.fail reason

// ===== State diff =====
let diffState (prev: obj) (next: obj) : Operation list = StateDiff.diffState prev next
let snapshotState<'T> (state: 'T) : 'T = StateDiff.snapshotState state

// ===== Engine =====
let runWorkflow (options: RunWorkflow.RunWorkflowOptions) : obj = RunWorkflow.runWorkflow options

let runWorkflowCollect (options: RunWorkflow.RunWorkflowOptions) : JS.Promise<WorkflowEvent[]> =
    RunWorkflow.runWorkflowCollect options

let handleWorkflowWebhook (options: HandleWebhookOptions) : JS.Promise<WorkflowEvent[]> =
    HandleWebhook.handleWorkflowWebhook options

// ===== Server helpers =====
let parseWorkflowRequest (request: obj) : JS.Promise<WorkflowRequestParams> =
    ParseRequest.parseWorkflowRequest request

let workflowRequestParseError (message: string) : exn = ParseRequest.parseError message

// ===== Cross-version registry =====
let selectWorkflowVersion
    (versions: WorkflowDefinition[])
    (runId: string)
    (runStore: RunStore)
    : JS.Promise<WorkflowDefinition option> =
    Registry.selectWorkflowVersion versions runId runStore

let createWorkflowRegistry (defaultWorkflow: WorkflowDefinition option) : WorkflowRegistry =
    Registry.createWorkflowRegistry defaultWorkflow

// ===== Run store =====
let inMemoryRunStore (options: InMemoryRunStore.InMemoryRunStoreOptions) : RunStore =
    InMemoryRunStore.create options
