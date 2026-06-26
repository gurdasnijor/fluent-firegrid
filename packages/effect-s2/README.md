# effect-s2

Fable-native EffSharp bindings for S2 durable streams.

This package is authored in F# and compiled with Fable. Generated JavaScript is
the runtime artifact and is intentionally not typechecked by the repository
TypeScript configuration.

The public F# surface is `Effect.S2`. The module exposes low-level request
records for precise S2 SDK operations and ergonomic helpers for common stream
workflows:

```fsharp
open Effect

let program =
    effect {
        let target = S2.streamRef "firegrid" "orders"
        let! _ = S2.Stream.appendString target """{"type":"OrderPlaced"}""" None
        let! tail = S2.Stream.tail target
        return tail.SeqNum
    }
```

Useful local commands:

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build packages/effect-s2/Effect.S2.fsproj -v:q
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter effect-s2 build
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter effect-s2 smoke
```
