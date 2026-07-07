# Firegrid.Log

Fable-native bindings for S2 durable streams.

This package is authored in F# and compiled with Fable. Generated JavaScript is
the runtime artifact and is intentionally outside the repository TypeScript
typecheck path.

The public F# surface is `Firegrid.Log.S2`. The module exposes low-level request
records for precise S2 SDK operations and ergonomic helpers for common stream
workflows:

```fsharp
open Firegrid.Log

let program =
    async {
        let client = S2.connect "s2_access_token"
        let basin = client |> S2.basin "firegrid"
        do! basin |> S2.ensureStream "orders"
        let stream = basin |> S2.stream "orders"
        let! _ = stream |> S2.appendStrings [ """{"type":"OrderPlaced"}""" ]
        let! tail = stream |> S2.checkTail
        return tail.SeqNum
    }
```

Useful local commands:

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build src/Firegrid.Log/Firegrid.Log.fsproj -v:q
```
