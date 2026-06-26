# @firegrid/store

Compatibility package for the S2-backed Fluent runtime adapter.

The implementation now lives under `@firegrid/fluent/s2` while the package
boundary is being consolidated. Prefer importing S2 runtime helpers from
`@firegrid/fluent/s2` in first-party code.

See
[`docs/sdds/package-boundary-consolidation-sdd.md`](../../docs/sdds/package-boundary-consolidation-sdd.md)
for the consolidation rationale.

## Surfaces

Use `s2WorkflowExecutionStore` when wiring TanStack Workflow directly:

```ts
import { defineWorkflowRuntime } from "@firegrid/fluent/runtime"
import { s2WorkflowExecutionStore } from "@firegrid/fluent/s2"

const runtime = defineWorkflowRuntime({
  store: s2WorkflowExecutionStore({ s2Endpoint, namespace: "orders" }),
  workflows
})
```

Use `createS2WorkflowRuntimeHost` for an operational host that owns the common
runtime loops:

```ts
import { createS2WorkflowRuntimeHost } from "@firegrid/fluent/s2"

const host = createS2WorkflowRuntimeHost({ s2Endpoint, namespace: "orders", workflows })

await host.tick({ now: Date.now() })
```

`tick` materializes schedules, recovers stale leased runs, and sweeps due
schedules/timers through the TanStack runtime. `runLoop` repeats `tick` until an
optional `AbortSignal` is aborted.

This helper is convenient, but it is not really a store abstraction. It now
lives under the S2 runtime adapter namespace.
