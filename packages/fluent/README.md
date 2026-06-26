# @firegrid/fluent

Restate-like service definitions over the Firegrid TanStack/S2 durable runtime.

Handlers are plain generator functions that yield Effect values:

```ts
import { run, service } from "@firegrid/fluent"

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

Generated-SDK-style contracts can be declared separately from their
implementation:

```ts
import { iface, implement, run } from "@firegrid/fluent"
import { Schema } from "effect"

export const incidentContract = iface.service("incident", {
  triage: iface.schemas({
    input: Schema.String,
    output: Schema.String
  })
})

export const incident = implement(incidentContract, {
  handlers: {
    *triage(input: string) {
      return yield* run(() => `triaged:${input}`, { name: "triage" })
    }
  }
})
```

Handlers can use ambient clients once the host binding provides an invocation
binding to `bindFluentDefinitions`:

```ts
import { serviceClient } from "@firegrid/fluent"

const reviews = serviceClient(incident)
const result = yield* reviews.triage("INC-1")
```

Send clients return durable invocation handles. A handle keeps the plain
`SendReference` fields for transport and JSON compatibility, and adds
non-enumerable `attach()` / `outputEffect()` methods for waiting on the typed
result:

```ts
import { sendServiceClient } from "@firegrid/fluent"

const handle = yield* sendServiceClient(incident).triage("INC-2")
const result = yield* handle.attach()
```

This package does not implement a second durable engine. `run` lowers to
TanStack `ctx.step`, `sleep` lowers to TanStack sleep primitives, and S2 hosting
is exposed through `@firegrid/fluent/s2`. `waitForSignal` lowers to
TanStack `ctx.waitForEvent`.

Transport-specific serving belongs in a separate binding package or process
entrypoint. The core package only exposes descriptors, descriptor-only
interfaces, typed clients, and runtime bindings such as
`createTanStackRuntimeBinding`.

Composition helpers such as `all` and `race` are Effect aliases. Firegrid does
not introduce a separate Operation/Future scheduler.

Keyed object invocations use `objectClient(binding, definition)(key)`, and the
handler can read the durable invocation key with `yield* objectKey`.

Virtual object state follows a table/materialization shape: define rows with
`Table` and `primaryKey`, then use `state(Table)` inside object handlers. The
fluent core owns this authoring surface while S2 object-owner storage provides
the runtime backend.
