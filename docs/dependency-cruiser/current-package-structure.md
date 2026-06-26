# Current Package Structure

This is the checked-in version of the package-structure sketch from the PR #70
cutover discussion. It describes the TypeScript `main` production package
layout only.

Do not use this `packages/*` layout as the target mental model for the
`eff-sharp` F#/Fable cutover branch. The F#/Fable branch should be organized
around F# projects and a solution file, not npm workspace packages.

`apps/*` entries are composition examples, ACP process work, and proof harnesses.
They are intentionally excluded from the production runtime package DAG.

## Production Directories

Current TypeScript `main` layout:

```text
packages/
  core/
    src/
      define/
      engine/
      middleware/
      registry/
      run-store/
      server/
  fluent/
    src/
      bindTanStack.ts
      clients.ts
      combinators.ts
      context.ts
      definitions.ts
      externalEvents.ts
      http.ts
      interface.ts
      run.ts
      state.ts
      statePredicate.ts
  log/
    src/
      generated/
      S2Client.ts
  runtime/
    src/
      define-runtime.ts
      in-memory-store.ts
      run-store-adapter.ts
      runtime-driver.ts
      schedule-materializer.ts
      types.ts
  store/
    src/
      S2ObjectRuntimeBinding.ts
      S2ObjectStateBackend.ts
      S2WorkflowRuntimeHost.ts
      s2WorkflowExecutionStore.ts
      types.ts
  trace/
    src/
      ChdbClient.ts
      ChdbExporter.ts
```

## F#/Fable Cutover Target

For `eff-sharp`, prefer a source-project-first layout. F# project files are the
real package and dependency boundaries; `package.json` should stay at the root
unless a concrete JS app/package boundary requires its own npm package.

```text
fluent-firegrid/
  Firegrid.slnx
  global.json
  nuget.config
  Directory.Build.props
  Directory.Packages.props

  src/
    Firegrid.Log/
      Firegrid.Log.fsproj
      S2/
        Types.fs
        InternalSdk.fs
        S2.fs

    Firegrid.Core/
      Firegrid.Core.fsproj
      Types.fs
      Errors.fs
      Invocation.fs
      StatePredicates.fs

    Firegrid.Clients/
      Firegrid.Clients.fsproj
      Clients.fs
      Attach.fs
      GenericInvocation.fs

    Firegrid.Runtime/
      Firegrid.Runtime.fsproj
      Types.fs
      Runtime.fs

    Firegrid.Store/
      Firegrid.Store.fsproj
      Types.fs
      WorkflowLog.fs
      ObjectState.fs
      Runtime.fs

    Firegrid.Fluent/
      Firegrid.Fluent.fsproj
      Definitions.fs
      State.fs
      Run.fs

  tests/
    Firegrid.Store.Tests/
      Firegrid.Store.Tests.fsproj
    Firegrid.Fluent.Tests/
      Firegrid.Fluent.Tests.fsproj

  apps/
    NodeHost/
      NodeHost.fsproj

  package.json
  pnpm-lock.yaml
```

F#/Fable cutover rules:

- do not add new F# production code under `packages/log` or `packages/store`;
- use `src/Firegrid.*/*.fsproj` as the project/package boundary;
- encode dependencies with `ProjectReference`;
- keep a root `Firegrid.slnx` or `Firegrid.sln` as the project index;
- keep root `package.json` for JS/Fable tooling only;
- write generated Fable JavaScript to `dist/` or `build/` and keep it ignored;
- declare native npm dependencies such as `@s2-dev/streamstore` through
  Fable/Femto project metadata, mirroring them in root npm tooling only when
  the build actually needs it.

Expected F#/Fable project DAG:

```mermaid
flowchart LR

  fluent["Firegrid.Fluent"]
  clients["Firegrid.Clients"]
  store["Firegrid.Store"]
  runtime["Firegrid.Runtime"]
  core["Firegrid.Core"]
  log["Firegrid.Log"]

  fluent --> core
  fluent --> clients
  fluent --> runtime
  clients --> core
  store --> core
  store --> runtime
  store --> log
  runtime --> core
```

Important boundary: `Firegrid.Store` must not depend on `Firegrid.Fluent` or a
client authoring package. Store code can implement low-level invocation/runtime
contracts from `Firegrid.Core`, but clients and authoring ergonomics sit above
the store.

`Firegrid.Clients` owns pure invocation clients over a supplied binding:
call/send clients, attach-by-reference, generic invocation helpers, and typed
handles. It depends on `Firegrid.Core` only. Ambient Effect/Fable authoring
ergonomics belong in `Firegrid.Fluent`.

## Current Package DAG

This is the actual package-level import graph generated from
`.dependency-cruiser.cjs` for `packages/*`.

```mermaid
flowchart LR

  fluent["@firegrid/fluent"]
  runtime["@firegrid/runtime"]
  core["@firegrid/core"]
  store["@firegrid/store"]
  log["@firegrid/log"]
  trace["@firegrid/trace"]

  fluent --> runtime
  fluent --> core
  runtime --> core
  store --> fluent
  store --> log
  store --> runtime
```

Packages with no Firegrid package dependencies:

- `@firegrid/core`
- `@firegrid/log`
- `@firegrid/trace`

## Source-Level Shape

This is the compact source-file version of the same graph. It is useful when the
package graph looks surprising and the next question is "which files create the
edge?"

```mermaid
flowchart LR

subgraph packages
  subgraph core
    coreIndex["src/index.ts"]
    coreDefine["src/define/define-workflow.ts"]
    coreHandleWebhook["src/engine/handle-webhook.ts"]
    coreRunWorkflow["src/engine/run-workflow.ts"]
    coreStateDiff["src/engine/state-diff.ts"]
    coreMiddleware["src/middleware/create-middleware.ts"]
    coreRegistry["src/registry/select-version.ts"]
    coreRunStore["src/run-store/in-memory.ts"]
    coreServer["src/server/index.ts"]
    coreTypes["src/types.ts"]
  end

  subgraph fluent
    fluentIndex["src/index.ts"]
    fluentBind["src/bindTanStack.ts"]
    fluentClients["src/clients.ts"]
    fluentContext["src/context.ts"]
    fluentDefinitions["src/definitions.ts"]
    fluentExternalEvents["src/externalEvents.ts"]
    fluentHttp["src/http.ts"]
    fluentRun["src/run.ts"]
    fluentState["src/state.ts"]
    fluentStatePredicate["src/statePredicate.ts"]
  end

  subgraph runtime
    runtimeIndex["src/index.ts"]
    runtimeDefine["src/define-runtime.ts"]
    runtimeDriver["src/runtime-driver.ts"]
    runtimeRunStoreAdapter["src/run-store-adapter.ts"]
    runtimeInMemory["src/in-memory-store.ts"]
    runtimeSchedules["src/schedule-materializer.ts"]
    runtimeTypes["src/types.ts"]
  end

  subgraph log
    logIndex["src/index.ts"]
    logClient["src/S2Client.ts"]
    logGenerated["src/generated/index.ts"]
  end

  subgraph store
    storeIndex["src/index.ts"]
    storeObjectBinding["src/S2ObjectRuntimeBinding.ts"]
    storeStateBackend["src/S2ObjectStateBackend.ts"]
    storeHost["src/S2WorkflowRuntimeHost.ts"]
    storeExecutionStore["src/s2WorkflowExecutionStore.ts"]
    storeTypes["src/types.ts"]
  end

  subgraph trace
    traceIndex["src/index.ts"]
    traceChdbClient["src/ChdbClient.ts"]
    traceChdbExporter["src/ChdbExporter.ts"]
  end
end

coreIndex --> coreDefine
coreIndex --> coreHandleWebhook
coreIndex --> coreRunWorkflow
coreIndex --> coreMiddleware
coreIndex --> coreRegistry
coreIndex --> coreRunStore
coreIndex --> coreServer
coreIndex --> coreTypes
coreHandleWebhook --> coreRunWorkflow
coreRunWorkflow --> coreStateDiff
coreRunWorkflow --> coreTypes
coreRunStore --> coreTypes

runtimeIndex --> runtimeDefine
runtimeIndex --> runtimeDriver
runtimeIndex --> runtimeRunStoreAdapter
runtimeIndex --> runtimeInMemory
runtimeIndex --> runtimeSchedules
runtimeIndex --> coreIndex
runtimeDefine --> runtimeDriver
runtimeDriver --> runtimeRunStoreAdapter
runtimeDriver --> coreIndex
runtimeInMemory --> coreIndex

fluentIndex --> fluentBind
fluentIndex --> fluentClients
fluentIndex --> fluentContext
fluentIndex --> fluentDefinitions
fluentIndex --> fluentExternalEvents
fluentIndex --> fluentHttp
fluentIndex --> fluentRun
fluentIndex --> fluentState
fluentBind --> runtimeIndex
fluentBind --> fluentContext
fluentClients --> fluentContext
fluentContext --> runtimeIndex
fluentDefinitions --> runtimeIndex
fluentExternalEvents --> fluentContext
fluentRun --> runtimeIndex
fluentState --> fluentStatePredicate

logIndex --> logClient
logIndex --> logGenerated

storeIndex --> storeObjectBinding
storeIndex --> storeStateBackend
storeIndex --> storeExecutionStore
storeIndex --> storeHost
storeIndex --> storeTypes
storeObjectBinding --> fluentIndex
storeObjectBinding --> logIndex
storeStateBackend --> fluentState
storeStateBackend --> logIndex
storeHost --> storeExecutionStore
storeHost --> runtimeIndex
storeExecutionStore --> logIndex
storeTypes --> runtimeIndex

traceIndex --> traceChdbClient
traceIndex --> traceChdbExporter
```

## Applications

Applications are composition roots or harnesses, not production runtime packages:

```text
apps/
  examples/full-stack-service/  Node HTTP + S2 composition example
  acp-process/                  ACP process adapter work
  proofs/                       real-substrate verification harness
```

## Cutover Mapping

| Before PR #70 | Current location |
| --- | --- |
| `packages/effect-s2` | `packages/log` |
| `packages/tanstack-workflow-core` | `packages/core` |
| `packages/tanstack-workflow-runtime` | `packages/runtime` |
| `packages/tanstack-workflow-s2` | `packages/store` |
| `packages/fluent-firegrid` | `packages/fluent` |
| `packages/fluent-firegrid-http` | `packages/fluent/src/http.ts` |
| `packages/fluent-firegrid-s2` | `packages/store` |
| `packages/fluent-firegrid-node` | `apps/examples/full-stack-service` |
| `packages/observability` | `packages/trace` |
| `packages/verification` | `apps/proofs` |
| `packages/fluent-acp-process` | `apps/acp-process` |

## Regeneration

The smaller checked-in generated diagrams live beside this file:

- `production-packages.mmd`
- `production-packages.svg`
- `core-focus.svg`
- `fluent-focus.svg`
- `runtime-focus.svg`
- `store-focus.svg`

Regenerate them with the commands in `docs/dependency-cruiser/README.md`.
