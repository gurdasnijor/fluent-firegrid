# `effect-durable-execution`

Effect-native durable execution primitives over Durable Streams.

This package is the higher-level authoring layer for named durable steps and
handler definitions. It is intentionally thin: Durable Streams owns the durable
coordination substrate, while this package owns the authoring API and lowering
rules.

```ts
import { all, run, service, type Operation } from "effect-durable-execution"

export const basics = service({
  name: "basics",
  handlers: {
    *hello(name: string): Operation<string> {
      return yield* run(() => `Hello, ${name}!`, { name: "compose" })
    },

    *parallel(): Operation<string> {
      const a = run(() => fetchA(), { name: "a" })
      const b = run(() => fetchB(), { name: "b" })
      const [av, bv] = yield* all([a, b])
      return `${av}+${bv}`
    },
  },
})
```

`run(action, { name })` lowers to a named journal step backed by producer-fenced
Durable Streams appends. `execute(ctx, effect)` remains available as the
lower-level handler-edge API.

The package does not expose a bespoke scheduler, predicate registry, or
module-global current scheduler slot. Composition delegates to Effect and the
Durable Streams substrate.

## Design

The package-level pushdown design is documented in
[docs/substrate-pushdown-design.md](docs/substrate-pushdown-design.md).
