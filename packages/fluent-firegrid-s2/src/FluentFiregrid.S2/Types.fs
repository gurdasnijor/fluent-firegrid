namespace Firegrid.FluentFiregrid.S2

open Effect

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
