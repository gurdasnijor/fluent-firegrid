# @firegrid/effect-s2-effsharp

Spike package for consuming the EffSharp Fable/NuGet package from fluent-firegrid.

This package depends on `EffSharp` from the gurdasnijor GitHub Packages NuGet
feed. The public direction is:

1. install `EffSharp` / `EffSharp.Platform.Node` from NuGet,
2. let Fable compile the F# sources,
3. let Femto install npm-side dependencies declared by Fable package metadata.

Authenticate to GitHub Packages outside the repo before restoring:

```sh
dotnet nuget add source \
  --username gurdasnijor \
  --password "$GITHUB_TOKEN" \
  --store-password-in-clear-text \
  --name github \
  "https://nuget.pkg.github.com/gurdasnijor/index.json"
```

This spike treats F# as the authored surface and Fable JavaScript as the runtime
artifact. Generated Fable output is intentionally not typechecked by the
repository TypeScript configuration.

Useful local commands:

```sh
NUGET_PACKAGES=$PWD/.nuget/packages dotnet build packages/effect-s2-effsharp/EffectS2EffSharp.fsproj -v:q
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter @firegrid/effect-s2-effsharp build
NUGET_PACKAGES=$PWD/.nuget/packages pnpm --filter @firegrid/effect-s2-effsharp smoke
```
