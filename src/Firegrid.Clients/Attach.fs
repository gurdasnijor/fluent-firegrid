namespace Firegrid.Clients

open Firegrid.Core

type InvocationHandle =
    { InvocationId: InvocationId
      Target: InvocationTarget }

[<RequireQualifiedAccess>]
module InvocationHandle =
    let create invocationId target =
        { InvocationId = invocationId
          Target = target }
