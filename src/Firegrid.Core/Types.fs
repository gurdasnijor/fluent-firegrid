namespace Firegrid.Core

type RunId = string
type WorkflowId = string
type WorkflowVersion = string
type WorkflowEvent = obj
type WorkflowInput = obj
type WorkflowOutput = obj
type SerializedError = obj

type RunStatus =
    | Pending
    | Running
    | Completed
    | Failed
    | Cancelled

type RunState =
    { RunId: RunId
      WorkflowId: WorkflowId
      Status: RunStatus
      CreatedAt: float
      UpdatedAt: float }

type WorkflowDefinition =
    { WorkflowId: WorkflowId
      Version: WorkflowVersion option }

type WorkflowMetadata =
    { WorkflowId: WorkflowId
      Version: WorkflowVersion option }
