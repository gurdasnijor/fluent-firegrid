namespace Firegrid.Fluent

open Firegrid.Core

type DefinitionKind =
    | Workflow
    | Service
    | Object

type Definition =
    { Name: string
      Kind: DefinitionKind
      Workflow: WorkflowDefinition option }

[<RequireQualifiedAccess>]
module Definition =
    let workflow name =
        { Name = name
          Kind = Workflow
          Workflow = Some { WorkflowId = name; Version = None } }

    let service name =
        { Name = name
          Kind = Service
          Workflow = None }

    let object name =
        { Name = name
          Kind = Object
          Workflow = None }
