# `effect-durable-streams`

Effect-native Durable Streams server implementation.

The package is being built from `docs/sdds/effect-native-server-sdd.md` and
`features/durable-streams/effect-server.feature.yaml`.

The current slice is a memory-backed Effect server for the base stream data
plane. It exposes a launchable `Server.layer`, a domain-shaped `Store` algebra,
an STM `MemoryStore`, and thin `HttpRouter` route composition over a
`StreamHttp` service boundary.

`OrderedKvStore` is a lower driver seam for durable backends such as LMDB. It is
not the public protocol store; the server store remains Durable-Streams-shaped.

Implemented in this slice:

- base `PUT` / `POST` / `HEAD` / `GET` / `DELETE` stream routes
- Store-free wire schemas and header constants
- Schema-based protocol header decoding
- producer append decisions, duplicate handling, fencing, and idempotent close
  retry
- route precedence for reserved `/v1/stream/__ds/*` paths

Not implemented yet: durable SQL/PGlite storage, subscriptions, pull-wake,
webhooks, schedules, filters, long-poll, SSE, forks, retention, and the shared
reserved-control-plane `HttpApi` implementation.
