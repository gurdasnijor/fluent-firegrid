namespace Firegrid.Store

open Effect
open Firegrid.Log

type S2ObjectStateBackendConfig =
    { S2Endpoint: string
      AccessToken: string option
      Basin: string option
      Namespace: string option }

type S2ObjectStateAddress = { ObjectName: string; Key: string }

type S2ObjectStateOwner =
    { CallId: string
      InvocationStreamName: string
      OwnerId: string }

type S2Runtime =
    { Basin: string
      Namespace: string
      Layer: Layer<S2Error, unit> }

type S2StateAppend =
    { Address: S2ObjectStateAddress
      BodyJson: string
      MatchSeqNum: float option }

type S2StateRead =
    { Address: S2ObjectStateAddress
      FromSeqNum: float option
      MaxRecords: int option }

type RunId = string

type WorkflowEvent = obj

type EventEnvelope =
    { EventIndex: float
      Event: WorkflowEvent }

type AppendEventsArgs =
    { RunId: RunId
      ExpectedNextIndex: float
      Events: WorkflowEvent list }

type AppendEventsResult = { NextIndex: float }

type ReadEventsArgs =
    { RunId: RunId
      FromIndex: float option }

type StoredWorkflowEvent =
    { RunId: RunId
      EventIndex: float
      Event: WorkflowEvent
      EventType: string
      StepId: string option
      CreatedAt: float }

type ReadJsonRecordsResult<'A> =
    { NextSeqNum: float
      Records: 'A list }
