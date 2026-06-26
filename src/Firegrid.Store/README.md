# Firegrid.Store

Fable-native S2-backed durable store adapters for Firegrid.

This package is authored in F# and compiled with Fable. It depends on
`Firegrid.Log` for the lower-level S2 durable stream binding.

The current cutover surface exposes the `Firegrid.Store` modules
compiled to JavaScript. The package is intentionally Fable-first on the
`eff-sharp` branch; TypeScript store APIs from `main` are reference material for
the migration rather than active implementation files here.

Useful local commands:

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build src/Firegrid.Store/Firegrid.Store.fsproj -v:q
```
