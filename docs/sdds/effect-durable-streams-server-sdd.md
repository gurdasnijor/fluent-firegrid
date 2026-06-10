# Effect Durable Streams Server SDD

Status: draft
Owner: Firegrid / Durable Streams
Source of truth: `PROTOCOL.md`
Transport contract: `typespec/`
Primary package: `packages/effect-durable-streams`
Reference snapshot: `docs/reference/durable-streams/`

Superseded by: `docs/sdds/effect-durable-streams-server-v2-sdd.md`

This first-pass SDD is retained for context only. It includes older backend and
route-design assumptions that are not active requirements.

## Purpose

This document defines the revised server design for the Effect-native Durable
Streams server in this repository.

The previous upstream server SDD is copied at
`docs/reference/durable-streams/docs/sdds/effect-native-server-sdd.md`. That
document is useful background, but this SDD supersedes it for local work. The
main changes are:

- TypeSpec/OpenAPI is the external transport contract.
- The server is split into data-plane stream interactions and control-plane
  coordination APIs.
- Control routes must not depend on precedence against a greedy stream path.
- The in-process server API is protocol-shaped and transport-neutral.
- Higher-level event sourcing, state, execution, and Firegrid semantics sit
  above Durable Streams rather than inside the stream transport layer.

## Design Goals

The server must provide a conformant implementation of `PROTOCOL.md` while
remaining a reusable substrate for higher-level systems.

Required properties:

- Durable append-only byte streams with offset replay and live reads.
- Atomic stream append, stream metadata update, and producer-state update.
- Idempotent producer fencing with the precedence rules in `PROTOCOL.md` §5.2.
- Explicit stream closure as a durable EOF signal.
- Fork semantics, TTL/expiry, retention, and soft-delete behavior.
- Subscription management, explicit stream membership, webhook delivery, pull
  wake, generation fencing, leases, and scheduled append extensions.
- TypeSpec-generated OpenAPI for server endpoints.
- Effect-native composition with `Layer`, typed errors, scoped resources,
  streams, schedules, telemetry, and testable stores.

Non-goals:

- Reimplementing application event sourcing in the transport server.
- Letting raw harnesses, clients, or application code mutate coordination state
  outside server-owned protocol operations.
- Treating generated scaffold servers as production logic.
- Encoding hidden routing order as part of the public API design.

## Conceptual Split

Durable Streams has three layers.

```text
Application/event layer
  typed domain events, projections, state machines, workflows, Firegrid runtime

Durable Streams protocol layer
  append/read/head/delete/close/fork, producer fencing, subscriptions, schedules

HTTP binding layer
  TypeSpec/OpenAPI routes, headers, query parameters, SSE, raw byte bodies
```

The server package owns the protocol layer and its HTTP binding. It does not
own application event schemas or application projections. This keeps the server
similar to an event store substrate: it stores and serves ordered durable facts,
while typed consumers interpret those facts above the byte stream layer.

## HTTP Surface

The current protocol draft models control APIs under a reserved `__ds` prefix
inside the stream URL root. That creates a real API design problem for generated
routers:

```text
/v1/stream/{+path}
/v1/stream/__ds/subscriptions/{id}
/v1/stream/__ds/schedules/{id}
```

The protocol says `__ds` must route first. TypeSpec/OpenAPI cannot express that
precedence. A handwritten router can enforce it, but generated routers and mock
servers can mis-route or require non-obvious implementation notes.

The revised server design therefore separates logical route ownership:

```text
Data plane
  /v1/streams/{+path}

Control plane
  /v1/subscriptions/{id}
  /v1/subscriptions/{id}/streams/{+path}
  /v1/subscription-delivery/{id}/callback
  /v1/subscription-delivery/{id}/claim
  /v1/subscription-delivery/{id}/ack
  /v1/subscription-delivery/{id}/release
  /v1/schedules/{id}
  /v1/jwks.json
```

If the protocol keeps compatibility aliases under `/v1/stream/__ds/...`, those
aliases should be an adapter concern, not the canonical OpenAPI surface. The
canonical TypeSpec should prefer non-overlapping routes because client
generation, mock servers, and scaffolds should not need route-order folklore.

## TypeSpec Contract

`typespec/` is the public HTTP contract and should remain decomposed by protocol
responsibility:

- `streams.tsp`: data-plane stream operations.
- `subscriptions.tsp`: subscription management and explicit membership.
- `subscription-delivery.tsp`: callback, claim, ack, and release.
- `schedules.tsp`: scheduled append.
- `common.tsp`: shared scalars, headers, error envelopes, and response models.

The TypeSpec model should use the language and libraries directly:

- `@typespec/http` for routes, status codes, path/query/header/body placement.
- `@typespec/rest` where collection/resource conventions fit control-plane
  APIs.
- `@typespec/streams` for streaming bodies where OpenAPI can represent them.
- `@typespec/sse` for SSE read mode.
- `@typespec/events` for event payloads and callback-like contracts where
  practical.

Known OpenAPI limits should stay documented near the TypeSpec source:

- OpenAPI 3.1 cannot fully represent stream `itemSchema`; OpenAPI 3.2 improves
  this but is not a safe default target yet.
- OpenAPI cannot express path reserved expansion with `{+path}` precisely.
- OpenAPI cannot enforce all cross-field constraints, such as body XOR
  `body_base64` or at least one of `pattern`/`streams`.
- Outbound webhook delivery is not a server endpoint unless modeled as a
  callback contract.

## Package Boundary

`packages/effect-durable-streams` should expose:

```text
src/
  Api.ts                 TypeSpec-aligned HTTP API contract or generated bridge
  ApiLive.ts             HttpApiBuilder handlers and HTTP lowering
  Protocol.ts            transport-neutral protocol schemas and decisions
  ProtocolError.ts       typed protocol/domain errors
  Store.ts               transport-neutral store algebra
  MemoryStore.ts         STM development/conformance store
  Server.ts              Node server layer
  Telemetry.ts           spans, metrics, log annotations
  DurableStreamsServer.ts optional in-process RPC adapter
```

The current package already follows this direction for the base stream data
plane. The next revisions should extend the same pattern to control-plane
protocol operations instead of adding independent HTTP plumbing.

## Dependency Direction

Dependencies flow inward from transport to protocol to storage.

```text
HttpApiBuilder / HttpRouter
  -> protocol request decoding and response lowering
  -> managers / protocol state machines
  -> Store algebra
  -> MemoryStore, SQL store, or platform backend
```

Rules:

- Store implementations must not depend on HTTP request/response types.
- Managers must not depend on concrete stores.
- HTTP handlers must lower protocol errors to declared HTTP errors.
- Shared protocol contracts must be importable by client and server without
  importing server internals.
- RPC adapters are peer transports over the same `Store` and protocol schemas,
  not a second semantic implementation.

## Store Algebra

The `Store` service is the server's correctness boundary. It should expose
protocol-shaped atomic operations, not database-shaped CRUD.

Baseline operations:

- `createStream`
- `append`
- `read`
- `head`
- `deleteStream`

Required extensions:

- `forkStream`
- `listStreams` or indexed matching for subscription backfill.
- `putSubscription`, `getSubscription`, `deleteSubscription`.
- `putSubscriptionStream`, `deleteSubscriptionStream`.
- `claimWake`, `ackWake`, `releaseWake`.
- `putSchedule`, `getSchedule`, `deleteSchedule`.
- `recordTailAdvanced` or an equivalent post-commit notification path.

Append atomicity requirement:

```text
validate stream state
validate content type and Stream-Seq
validate producer epoch/seq
append bytes or framed messages
update stream tail and close state
update producer state
publish or persist tail-advanced fact
commit
```

No durable backend is acceptable unless this operation is atomic per stream and
serialized per `(stream, producerId)`.

## Persistence Strategy

The server must support multiple store layers.

`MemoryStore.layer`:

- STM-backed.
- Development and conformance target.
- No production durability.
- Must still obey protocol atomicity because tests should exercise real state
  transitions.

SQL-shaped durable store:

- Preferred strategic backend.
- Uses ordinary transactions for stream metadata, records, producer state,
  subscription state, wake snapshots, and schedules.
- Needs serializable isolation, row locks, advisory locks, or an equivalent
  per-stream/per-producer serialization strategy.
- Can provide derived tables for host observation and query acceleration.

Platform-specific backends:

- Cloudflare Durable Objects or similar actors can satisfy per-stream
  serialization structurally if all writes for a stream are colocated.
- They still need explicit tests for producer-state/log atomicity and restart
  behavior.

## Data Plane

The data plane owns ordinary stream URLs.

Operations:

- `PUT` creates or idempotently re-confirms a stream, optionally with initial
  bytes, close state, TTL/expiry, and fork headers.
- `POST` appends bytes or closes the stream, with optional producer fencing and
  `Stream-Seq`.
- `HEAD` returns stream metadata.
- `GET` reads catch-up, long-poll, or SSE mode from an offset.
- `DELETE` deletes or soft-deletes stream data according to fork references.

The data-plane handler must preserve raw bytes. JSON/text decoders must not sit
between the HTTP request and the stream store for arbitrary content types.

Read modes:

- Catch-up returns available bytes from `offset` to current tail.
- Long-poll waits only when the requested offset is at the relevant live tail.
- SSE emits data and control events, including closure and cursor fields.
- Fork reads stitch source and fork segments without exposing storage layout.

## Control Plane

The control plane owns coordination resources. It should use `HttpApi` and
TypeSpec-modeled payloads because these endpoints are structured JSON APIs.

Subscription management:

- Create or re-confirm by normalized config hash.
- Support webhook and pull-wake delivery.
- Support `pattern`, explicit `streams`, optional CEL filter, lease TTL, and
  description.
- Backfill pattern subscriptions against existing streams at current tail.
- Store per-linked-stream public ack cursor and internal evaluated cursor.

Explicit stream membership:

- Link and unlink streams by stream-root-relative path.
- Linking starts at current tail unless protocol explicitly says otherwise.
- Unlinking must not corrupt other subscription state.

Delivery:

- Webhook delivery sends server-to-worker POSTs with signed payloads and a
  callback URL/token.
- Pull-wake delivery produces claimable wake snapshots.
- Claim, ack, and release are generation-fenced and lease-aware.

Schedules:

- Store durable scheduled append requests.
- Execute due schedules exactly through the same append path as client writes.
- Use producer fencing so schedule retries are idempotent.
- Deleting a pending schedule cancels it; completed schedules remain observable.

JWKS:

- Publish webhook verification keys through a stable control-plane route.
- Rotation must overlap old/new verification windows.

## Wake Evaluation

Wake evaluation runs after an append transaction commits. It must not run
arbitrary filter effects inside the append transaction.

Flow:

```text
append commits TailAdvanced(stream, tail)
  -> wake evaluator finds linked subscriptions
  -> evaluates filters from prior evaluated cursor to new tail
  -> updates internal evaluated cursor
  -> if pending work exists, bumps generation and stores wake snapshot
  -> webhook dispatcher or pull-wake event delivery observes snapshot
```

This split keeps append latency bounded and preserves transactional correctness.

CEL pushdown is optional and backend-capability-driven. A SQL backend may push
filters into the database. If unavailable, the Effect evaluator must produce the
same wake decisions.

## Leases And Fencing

Pull-wake work is owned by the server, not by clients.

Rules:

- `lease_ttl_ms` is bounded by protocol: 1000 to 600000.
- Claim returns wake id, generation, token, linked streams, and lease expiry.
- Ack succeeds only with the current generation token.
- Release succeeds only with the current generation token.
- Stale tokens fail without mutating newer wake state.
- Lease expiry makes the wake claimable again.
- Durable outcome recording must happen before ack in higher-level hosts.

## Telemetry

The server should expose telemetry at protocol boundaries:

- HTTP route, method, status, stream path, subscription id, schedule id.
- Append decision, producer decision, content type mismatch, sequence gap.
- Store transaction duration and retry count.
- Read mode, bytes served, live wait duration, SSE connection duration.
- Wake evaluation count, filter duration, generation created, delivery result.
- Schedule due lag and execution result.

Telemetry helpers should wrap `Store` and manager services rather than living
only in HTTP handlers, so RPC/in-process adapters receive the same spans.

## Security

Minimum server rules:

- Normalize and validate stream-root-relative paths.
- Reserve control route prefixes outside the data-plane path space.
- Reject path traversal and ambiguous path normalization.
- Preserve content type and use `X-Content-Type-Options: nosniff` on reads.
- Sign webhook deliveries and publish verification keys.
- Treat lease/callback tokens as bearer secrets.
- Do not expose producer state or internal evaluated cursors unless protocol
  explicitly requires it.
- Rate-limit or otherwise protect long-poll, SSE, webhook retry, and schedule
  execution surfaces.

## Conformance Strategy

Conformance should be layered:

1. Protocol decision unit tests for create, append, producer fencing, close,
   fork, TTL, and delete.
2. Store contract tests run against every store layer.
3. HTTP conformance against the generated OpenAPI and protocol test cases.
4. Mock/server smoke tests for TypeSpec-generated artifacts.
5. Durable backend stress tests for concurrent same-stream producer appends and
   crash/retry windows.
6. Subscription/wake tests for backfill, filter evaluation, generation fencing,
   lease expiry, ack/release, webhook signing, and schedules.

Passing `MemoryStore` is necessary but not sufficient for durable backends.
Durable backends need their own isolation and recovery tests.

## Implementation Order

1. Align `PROTOCOL.md` and `typespec/` around non-overlapping canonical
   data-plane and control-plane routes.
2. Regenerate `openapi/durable-streams.yaml` and keep Prism mock/viewer flows
   working.
3. Update `packages/effect-durable-streams/src/Api.ts` from the TypeSpec
   contract and remove hand-maintained drift where practical.
4. Extend `Protocol.ts` and `Store.ts` for fork, TTL, subscription, delivery,
   and schedule operations.
5. Implement and test those operations in `MemoryStore`.
6. Add control-plane `HttpApiBuilder` groups for subscriptions, delivery,
   schedules, and JWKS.
7. Add long-poll and SSE read modes for the data plane.
8. Add webhook dispatcher, wake evaluator, lease expiry, and schedule runner.
9. Add a durable backend behind the same `Store` contract.
10. Run preflight plus server conformance against both memory and durable
    layers.

## Open Design Decisions

- Whether `/v1/stream/{+path}` remains as a compatibility alias or is replaced
  entirely by `/v1/streams/{+path}`.
- Whether outbound webhook delivery should be emitted as OpenAPI callbacks or
  documented as a sibling webhook contract.
- Whether the shared `HttpApi` contract is generated from TypeSpec, manually
  mirrored, or colocated with TypeSpec-derived validation tests.
- Which durable backend lands first: SQL/PGlite/Postgres or a platform actor
  backend.
- How much TypeSpec-generated scaffold code is used locally without committing
  generated server artifacts.
