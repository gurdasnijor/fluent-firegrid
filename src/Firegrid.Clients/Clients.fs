namespace Firegrid.Clients

open Effect
open Firegrid.Core

type InvocationBinding =
    { Send: InvocationRequest -> Effect<InvocationId, FluentFiregridError, unit>
      Call: InvocationRequest -> Effect<InvocationResult, FluentFiregridError, unit> }

type Client =
    { Binding: InvocationBinding
      Target: InvocationTarget }

[<RequireQualifiedAccess>]
module Client =
    let make binding target = { Binding = binding; Target = target }
