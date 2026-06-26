namespace Firegrid.Core

type FluentFiregridError =
    { Message: string
      Cause: obj option }

type LogConflictError =
    { ExpectedSeqNum: float option
      ActualSeqNum: float option
      Message: string }

type StepTimeoutError =
    { StepId: string
      Message: string }
