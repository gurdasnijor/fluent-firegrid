# @firegrid/fluent-firegrid-http

HTTP transport binding for `@firegrid/fluent-firegrid`.

This package does not start a server. It exposes a standard
`Request -> Response` handler that frameworks or Node entrypoints can mount:

```ts
import { createFluentHttpHandler } from "@firegrid/fluent-firegrid-http"

const handle = createFluentHttpHandler({
  binding,
  definitions: [orders]
})
```

Routes:

- `POST /call/:kind/:name/:handler`
- `POST /send/:kind/:name/:handler`
- `POST /call/object/:name/:key/:handler`
- `POST /send/object/:name/:key/:handler`

The request body is the handler input JSON. `runId` may be supplied as a query
parameter or `x-firegrid-run-id` header.
