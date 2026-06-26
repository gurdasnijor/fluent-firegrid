# @firegrid/example-full-stack-service

Node HTTP server binding and runnable service examples for S2-backed
`@firegrid/fluent` definitions.

The package has two roles:

- Node `http` listener lifecycle;
- health/readiness routes;
- S2-backed TanStack runtime host construction;
- S2 object runtime binding;
- host loop startup and graceful shutdown.
- concrete service, workflow, and object definitions showing how an application
  composes those pieces.

The `examples/ordering.ts` module is the primary tutorial surface. It models:

- a `shopping-cart` virtual object with durable cart state;
- a `checkout` workflow that reserves inventory and authorizes payment;
- an `order` virtual object with durable order state and `waitUntilShipped`;
- a `fulfillment` service that uses delayed send for shipping work.

```ts
import { serveFluentS2 } from "@firegrid/example-full-stack-service"
import { orderingDefinitions } from "@firegrid/example-full-stack-service/examples/ordering"

const server = await serveFluentS2({
  definitions: orderingDefinitions,
  namespace: "orders-prod",
  port: 8080,
  s2Endpoint: "http://127.0.0.1:7070"
})

await server.close()
```

The fluent transport routes are provided by `@firegrid/fluent/http`.
This package adds only the Node process binding around them.

The same definitions can be invoked over the generic transport:

```ts
await fetch("http://localhost:8080/call/object/shopping-cart/cart-123/addItem", {
  body: JSON.stringify({
    customerId: "customer-1",
    quantity: 2,
    sku: "sku-1"
  }),
  headers: { "content-type": "application/json" },
  method: "POST"
})

await fetch("http://localhost:8080/send/object/shopping-cart/cart-123/checkout", {
  body: JSON.stringify({
    customerId: "customer-1",
    paymentToken: "tok_visa",
    quantity: 2,
    requestId: "req-1",
    shippingAddress: "1 Market St",
    sku: "sku-1"
  }),
  headers: { "content-type": "application/json" },
  method: "POST"
})
```

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

The durable webhook example and retry/dedupe notes live in
`docs/guides/durable-webhooks.md`.
