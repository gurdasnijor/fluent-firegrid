namespace Firegrid.Clients

open Effect
open Firegrid.Core

[<RequireQualifiedAccess>]
module GenericInvocation =
    let send (client: Client) payload idempotencyKey =
        client.Binding.Send
            { Target = client.Target
              Payload = payload
              IdempotencyKey = idempotencyKey }

    let call (client: Client) payload idempotencyKey =
        client.Binding.Call
            { Target = client.Target
              Payload = payload
              IdempotencyKey = idempotencyKey }
