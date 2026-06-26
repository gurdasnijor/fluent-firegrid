namespace Firegrid.Core

/// `createWorkflow(config)` builder chain (`define/define-workflow.ts`).

/// Configuration passed to `createWorkflow`.
type CreateWorkflowConfig =
    { Id: string
      Description: string option
      Version: string option
      Input: SchemaInput option
      Output: SchemaInput option
      State: SchemaInput option
      Initialize: (obj -> obj) option
      DefaultStepRetry: StepRetryOptions option }

    static member Create(id: string) =
        { Id = id
          Description = None
          Version = None
          Input = None
          Output = None
          State = None
          Initialize = None
          DefaultStepRetry = None }

type WorkflowBuilder =
    { Middleware: Middleware[] -> WorkflowBuilder
      PreviousVersions: WorkflowDefinition[] -> WorkflowBuilder
      Handler: (Ctx -> Fable.Core.JS.Promise<obj>) -> WorkflowDefinition }

[<RequireQualifiedAccess>]
module DefineWorkflow =

    type private InternalState =
        { Config: CreateWorkflowConfig
          Middlewares: Middleware[]
          Previous: WorkflowDefinition[] }

    let rec private buildBuilder (state: InternalState) : WorkflowBuilder =
        { Middleware =
            fun middlewares ->
                buildBuilder
                    { state with
                        Middlewares = Array.append state.Middlewares middlewares }
          PreviousVersions = fun versions -> buildBuilder { state with Previous = versions }
          Handler =
            fun fn ->
                { Kind = "workflow"
                  Id = state.Config.Id
                  Description = state.Config.Description
                  Version = state.Config.Version
                  PreviousVersions = state.Previous
                  InputSchema = state.Config.Input
                  OutputSchema = state.Config.Output
                  StateSchema = state.Config.State
                  Initialize = state.Config.Initialize
                  DefaultStepRetry = state.Config.DefaultStepRetry
                  Middlewares = state.Middlewares
                  Handler = fn } }

    /// `createWorkflow(config)`.
    let createWorkflow (config: CreateWorkflowConfig) : WorkflowBuilder =
        buildBuilder
            { Config = config
              Middlewares = [||]
              Previous = [||] }
