# @firegrid/fluent-firegrid-node

Node HTTP server binding for S2-backed `@firegrid/fluent-firegrid` definitions.

This package owns process/server concerns that stay out of fluent core:

- Node `http` listener lifecycle;
- health/readiness routes;
- S2-backed TanStack runtime host construction;
- S2 object runtime binding;
- host loop startup and graceful shutdown.

```ts
import { serveFluentS2 } from "@firegrid/fluent-firegrid-node"

const server = await serveFluentS2({
  definitions: [orders, counter],
  namespace: "orders-prod",
  port: 8080,
  s2Endpoint: "http://127.0.0.1:7070"
})

await server.close()
```

The fluent transport routes are provided by `@firegrid/fluent-firegrid-http`.
This package adds only the Node process binding around them.

Webhook routes can be mounted as transport-specific aliases for normal fluent
handlers. The route derives the durable idempotency key before forwarding to the
existing invocation binding.

```ts
await serveFluentS2({
  definitions: [stripeWebhook],
  namespace: "billing",
  s2Endpoint: "http://127.0.0.1:7070",
  webhooks: {
    "/webhooks/stripe": {
      definition: stripeWebhook,
      handler: "onEvent",
      idempotencyKey: (request) => request.headers.get("stripe-event-id") ?? undefined,
      verify: (request, body) => verifyStripeSignature(request.headers, body)
    }
  }
})
```
