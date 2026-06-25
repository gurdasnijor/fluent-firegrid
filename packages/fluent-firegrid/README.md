# @firegrid/fluent-firegrid

Restate-like service definitions over the Firegrid TanStack/S2 durable runtime.

Handlers are plain generator functions that yield Effect values:

```ts
import { run, service } from "@firegrid/fluent-firegrid"

export const orders = service({
  name: "orders",
  handlers: {
    *submit(input: { readonly orderId: string }) {
      const reserved = yield* run(() => reserve(input.orderId), { name: "reserve" })
      return { reserved }
    }
  }
})
```

This package does not implement a second durable engine. `run` lowers to
TanStack `ctx.step`, `sleep` lowers to TanStack sleep primitives, and hosting is
provided by `@firegrid/tanstack-workflow-s2`. `waitForSignal` lowers to
TanStack `ctx.waitForEvent`.

Transport-specific serving belongs in a separate binding package or process
entrypoint. The core package only exposes descriptors, typed clients, and
runtime bindings such as `createTanStackRuntimeBinding`.
