# Durable Webhooks With Fluent Firegrid

Durable webhook routes are transport-specific aliases for normal fluent
handlers. The Node binding persists ingress by forwarding the request into the
same invocation path used by `/call/...`, with a durable idempotency key derived
from the external delivery.

See the compiled example in
`apps/examples/full-stack-service/examples/durable-webhook.ts`.

```ts
import { serveFluentS2 } from "@firegrid/example-full-stack-service"
import {
  stripeWebhookRoutes,
  webhookDefinitions
} from "@firegrid/example-full-stack-service/examples/durable-webhook"

await serveFluentS2({
  definitions: webhookDefinitions,
  namespace: "billing",
  port: 8080,
  s2Endpoint: process.env.S2_ENDPOINT!,
  webhooks: stripeWebhookRoutes
})
```

The example demonstrates:

- a webhook service handler mounted at `/webhooks/stripe`;
- a `verify` hook that receives the raw request body before admission;
- an `idempotencyKey` hook that maps `stripe-event-id` to the durable run id;
- `objectSendClient(paymentTracker, invoiceId)` to route events by external
  entity id;
- table-shaped object state for invoice event materialization;
- `run` for side effects;
- delayed object sends for retry/backoff after failed payments.

External sender retries should reuse the same idempotency key. When the same
provider event is delivered again, the route forwards the same durable run id to
the runtime so the handler replays or returns the already recorded result rather
than admitting a second logical webhook execution.
