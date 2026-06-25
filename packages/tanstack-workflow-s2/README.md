# @firegrid/tanstack-workflow-s2

S2-backed `WorkflowExecutionStore` for the TanStack Workflow runtime contract.

This package intentionally does not define a workflow authoring API. It provides
the persistence/runtime store boundary described in
`docs/sdds/tanstack-workflow-s2-store-sdd.md`.

## Surfaces

Use `s2WorkflowExecutionStore` when wiring TanStack Workflow directly:

```ts
import { defineWorkflowRuntime } from "@tanstack/workflow-runtime"
import { s2WorkflowExecutionStore } from "@firegrid/tanstack-workflow-s2"

const runtime = defineWorkflowRuntime({
  store: s2WorkflowExecutionStore({ s2Endpoint, namespace: "orders" }),
  workflows
})
```

Use `createS2WorkflowRuntimeHost` for an operational host that owns the common
runtime loops:

```ts
import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"

const host = createS2WorkflowRuntimeHost({ s2Endpoint, namespace: "orders", workflows })

await host.tick({ now: Date.now() })
```

`tick` materializes schedules, recovers stale leased runs, and sweeps due
schedules/timers through the TanStack runtime. `runLoop` repeats `tick` until an
optional `AbortSignal` is aborted.
