# @firegrid/store

S2-backed `WorkflowExecutionStore` for the TanStack Workflow runtime contract.

This package intentionally does not define a workflow authoring API. It provides
the persistence/runtime store boundary described in
`docs/sdds/store-store-sdd.md`.

## Surfaces

Use `s2WorkflowExecutionStore` when wiring TanStack Workflow directly:

```ts
import { defineWorkflowRuntime } from "@firegrid/runtime"
import { s2WorkflowExecutionStore } from "@firegrid/store"

const runtime = defineWorkflowRuntime({
  store: s2WorkflowExecutionStore({ s2Endpoint, namespace: "orders" }),
  workflows
})
```

Use `createS2WorkflowRuntimeHost` for an operational host that owns the common
runtime loops:

```ts
import { createS2WorkflowRuntimeHost } from "@firegrid/store"

const host = createS2WorkflowRuntimeHost({ s2Endpoint, namespace: "orders", workflows })

await host.tick({ now: Date.now() })
```

`tick` materializes schedules, recovers stale leased runs, and sweeps due
schedules/timers through the TanStack runtime. `runLoop` repeats `tick` until an
optional `AbortSignal` is aborted.
