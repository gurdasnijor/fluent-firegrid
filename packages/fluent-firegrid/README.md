# @firegrid/fluent-firegrid

Effect-native Firegrid primitives with named durable steps over Durable
Streams.

The Part 1 engine is intentionally substrate-free above the handler edge.
User-facing handlers can use Restate-like generator syntax while the backing
implementation remains ordinary Effect plus journal services:

```ts
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

`run(action, { name })` lowers to a named journal step, and `execute(ctx,
effect)` remains available as the lower-level handler-edge API. The package
does not expose a bespoke `Future` scheduler or module-global current scheduler
slot; composition delegates to Effect.

Definitions carry public handler descriptors (`_handlers`) so
`@firegrid/fluent-runtime` can bind entity control-plane operations without
importing internals. `client` and `sendClient` derive typed call/send clients
from those descriptors over a runtime-provided ingress; hosting and entity
control remain outside this package.
