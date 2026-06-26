namespace Firegrid.Core

type InvocationId = string
type InvocationTarget = string

type InvocationRequest =
    { Target: InvocationTarget
      Payload: obj
      IdempotencyKey: string option }

type InvocationResult =
    | InvocationSucceeded of obj
    | InvocationFailed of SerializedError
