# External Events With Awakeables

Awakeables are durable task tokens. A fluent handler creates a token, sends it to
an external system, and then waits for the token to be resolved or rejected.

```ts
import { awakeable, run, service } from "@firegrid/fluent"

export const reviews = service({
  name: "reviews",
  handlers: {
    *request(input: { readonly documentId: string }) {
      const decision = yield* awakeable<string>({ name: "review" })

      yield* run(
        () => sendReviewEmail({ documentId: input.documentId, token: decision.id }),
        { name: "send-review-email" }
      )

      return yield* decision.await
    }
  }
})
```

The Node/S2 HTTP binding exposes transport routes for external systems:

```text
POST /firegrid/awakeables/:id/resolve
POST /firegrid/awakeables/:id/reject
```

The HTTP helper builds those routes and payloads for callback services.

```ts
import { createAwakeableHttpClient } from "@firegrid/fluent/http"

const firegrid = createAwakeableHttpClient({
  baseUrl: "https://orders.example.com",
  headers: () => ({ authorization: `Bearer ${process.env.FIREGRID_CALLBACK_TOKEN}` })
})

await firegrid.resolve(token, "approved")
await firegrid.reject(token, { code: "denied" })
```

Duplicate token delivery is durable and idempotent at the runtime layer. The
first delivery resumes the waiting run; later deliveries of the same token return
the runtime `duplicate` result and do not re-run completed handler work.

Common patterns:

- Human approval: email or chat message contains the awakeable token; an approval
  service calls `resolve`.
- Webhook callback: an external provider receives the token in metadata and
  calls `resolve` or `reject` when the remote job finishes.
- Async task token: a worker queue stores the token with its job id and resolves
  it after the worker commits the result.
