# @firegrid/fluent-firegrid-s2

Fable-native S2 adapters for fluent-firegrid.

This package is being transactionally cut over from the former TypeScript
implementation to F# compiled by Fable. It depends on the F# `effect-s2` project
directly and treats generated JavaScript as a runtime artifact.

The current foundation includes:

- S2-backed fluent runtime configuration;
- deterministic object state, invocation, and delayed-start stream names;
- S2 runtime/layer construction;
- helpers for appending and reading JSON state events through `Effect.S2`.

Useful local commands:

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build packages/fluent-firegrid-s2/FluentFiregrid.S2.fsproj -v:q
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter @firegrid/fluent-firegrid-s2 build
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter @firegrid/fluent-firegrid-s2 smoke
```
