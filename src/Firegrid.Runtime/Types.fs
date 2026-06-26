namespace Firegrid.Runtime

open Effect
open Firegrid.Core

type StoredWorkflowEvent =
    { RunId: RunId
      EventIndex: float
      Event: WorkflowEvent
      EventType: string
      StepId: string option
      CreatedAt: float }

type AppendEventsArgs =
    { RunId: RunId
      ExpectedNextIndex: float
      Events: WorkflowEvent list }

type AppendEventsResult = { NextIndex: float }

type ReadEventsArgs =
    { RunId: RunId
      FromIndex: float option }

type WorkflowExecutionStore<'Error> =
    { AppendEvents: AppendEventsArgs -> Effect<AppendEventsResult, 'Error, unit>
      ReadEvents: ReadEventsArgs -> Effect<StoredWorkflowEvent list, 'Error, unit> }

type WorkflowRuntimeDefinition =
    { Workflows: Map<WorkflowId, WorkflowDefinition> }
