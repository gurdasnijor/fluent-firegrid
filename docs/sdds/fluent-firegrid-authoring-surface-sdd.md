# SDD: Fluent Firegrid Authoring Surface

### Restate-like contracts and clients over the TanStack/S2 substrate

|   |   |
| --- | --- |
| Status | Implemented through transport binding |
| Date | 2026-06-25 |
| Package | `@firegrid/fluent-firegrid`, `@firegrid/fluent-firegrid-http` |
| Lower runtime | TanStack Workflow over `@firegrid/tanstack-workflow-s2` |

---

## Decision

The next layer above the completed TanStack/S2 and object-state substrate is the
fluent authoring/client surface. It should look like generated Restate SDK
contracts, but it must stay Effect-native:

- `Effect` is the operation type.
- Handlers are generator functions.
- Durable primitives such as `run`, `sleep`, and `waitForSignal` lower to the
  TanStack runtime.
- Transport-specific HTTP/server binding remains outside this package.

This layer must not add a second scheduler, a second object runtime, or
proof-only packages.

## Public Shape

Descriptor-first contracts are supported through `iface`:

```ts
import { iface, implement, run } from "@firegrid/fluent-firegrid"
import { Schema } from "effect"

export const ordersContract = iface.service("orders", {
  submit: iface.schemas({
    input: Schema.Struct({ orderId: Schema.String }),
    output: Schema.Struct({ accepted: Schema.Boolean })
  })
})

export const orders = implement(ordersContract, {
  handlers: {
    *submit(input) {
      yield* run(() => reserve(input.orderId), { name: "reserve" })
      return { accepted: true }
    }
  }
})
```

Direct definitions can also attach descriptors:

```ts
export const orders = service({
  name: "orders",
  handlers: { /* generator handlers */ },
  descriptors: { /* optional handler codecs */ }
})
```

Typed send clients return durable invocation handles:

```ts
const handle = yield* sendServiceClient(orders).submit({ orderId: "order-1" })
const result = yield* handle.attach()
```

The handle remains structurally compatible with the plain `SendReference` wire
shape. Its Effect-returning methods are non-enumerable, and the method for
awaiting output is named `outputEffect()` to avoid colliding with the existing
`output` data field on completed send references.

## Acceptance Ladder

### A. Descriptor-First Contracts

**Status:** Implemented.

**Claim.** Users can define a generated-SDK-like contract without implementation
and later bind it to generator handlers while preserving input/output
descriptors.

**Forces:** `iface.service/object/workflow`, `iface.schemas/json/serdes`,
`implement`, descriptor preservation in typed clients.

**Proof:** `packages/fluent-firegrid/test/public-surface.test.ts` covers
descriptor metadata, interface implementation, and typed call/send request
metadata.

### B. Runtime Schema Validation

**Status:** Implemented.

**Claim.** Optional handler descriptors are enforced at runtime when the
TanStack binding enters and exits a handler.

**Forces:** decode input before invoking the generator, validate output before
storing run completion, map schema failures to `FluentFiregridError`.

**Proof:** `packages/fluent-firegrid/test/public-surface.test.ts` runs an
in-memory TanStack runtime and verifies descriptor-valid input completes while
descriptor-invalid input fails at the fluent handler boundary.

### C. Ambient Handler Clients

**Status:** Implemented.

**Claim.** Handlers can call other services/workflows/objects through typed
clients without manually threading a process-level binding through business
logic.

**Forces:** ambient invocation binding in `FluentDurableContext`, service and
workflow call/send helpers, object-keyed helper shape, no transport coupling.

**Proof:** `packages/fluent-firegrid/test/public-surface.test.ts` verifies
`serviceClient(definition)` resolves through `FluentDurableContext`, emits the
same descriptor-bearing request shape as process-level clients, and works
through `bindFluentDefinitions({ invocationBinding: () => binding })` in an
in-memory TanStack runtime.

### D. Transport Binding Package

**Status:** Implemented.

**Claim.** HTTP or other ingress binding can expose fluent definitions without
placing servers in fluent core.

**Forces:** separate package or fixture-only adapter, descriptor-driven routing,
schema-aware request/response handling, no dependency from core to Node HTTP.

**Proof:** `@firegrid/fluent-firegrid-http` exposes
`createFluentHttpHandler(Request -> Response)` with no listener ownership.
`packages/fluent-firegrid-http/test/http-handler.test.ts` covers call/send
routes, keyed object routing, descriptor validation before invocation, response
encoding, and `runId` forwarding.

### E. Restate-Like Send Handles

**Status:** Implemented.

**Claim.** Typed send clients return durable handles instead of forcing callers
to manually pass `runId` back through call clients.

**Forces:** preserve `SendReference` transport compatibility, attach by
invocation id, support explicit and ambient invocation bindings, keep handle
methods out of JSON/enumeration.

**Proof:** `packages/fluent-firegrid/test/public-surface.test.ts` verifies
typed explicit send handles, ambient send handles, descriptor-bearing attach
requests, and context-free `attach()` after the ambient handle has been created.
