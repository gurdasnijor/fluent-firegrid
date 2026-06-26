# @firegrid/store

Fable-native S2-backed durable store adapters for Firegrid.

This package is authored in F# and compiled with Fable. It depends on
`@firegrid/log` for the lower-level S2 durable stream binding.

The current cutover surface exposes the `Firegrid.FluentFiregrid.S2` modules
compiled to JavaScript. The package is intentionally Fable-first on the
`eff-sharp` branch; TypeScript store APIs from `main` are reference material for
the migration rather than active implementation files here.

Useful local commands:

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build packages/store/FluentFiregrid.S2.fsproj -v:q
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter @firegrid/store build
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter @firegrid/store typecheck
```
