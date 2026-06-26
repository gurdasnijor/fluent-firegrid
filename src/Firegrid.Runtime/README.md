# @firegrid/runtime

Fable-native port of `packages/runtime`: the workflow runtime
(`defineWorkflowRuntime`/`createRuntimeDriver`), the in-memory execution store,
the run-store adapter, and the UTC cron / interval schedule materializer.

Depends on `@firegrid/core`. The package's `index.ts` also re-exports all of
`@firegrid/core`; in F#, `open Firegrid.Core` alongside `Firegrid.Runtime` (or
reference `Firegrid.Core.Exports`) to obtain that surface.
