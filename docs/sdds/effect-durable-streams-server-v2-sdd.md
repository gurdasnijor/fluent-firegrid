# Effect Durable Streams Server SDD v2

Status: draft
Version: 2
Owner: Firegrid / Durable Streams
Primary package: `packages/effect-durable-streams`
Protocol source: `PROTOCOL.md`
HTTP contract source: `typespec/`
Reference snapshot: `docs/reference/durable-streams/`

## Purpose

This SDD turns the Durable Streams server direction into an implementation plan.
It supersedes the first local draft in
`docs/sdds/effect-durable-streams-server-sdd.md` where the two disagree.

The core correction in v2 is that the protocol should not require generated
routers to understand hidden route precedence. Durable stream data operations
and server-owned coordination resources are different API surfaces. They should
be modeled that way in TypeSpec, OpenAPI, and the Effect server.

## Decisions

1. Canonical server routes are split into a data plane and a control plane.
2. `__ds`-under-stream routes are compatibility aliases only if we keep them.
3. TypeSpec is the external HTTP contract.
4. Effect services own the protocol state machines.
5. Store APIs are protocol-shaped and transactional, not CRUD-shaped.
6. Generated scaffold code is a local tool output, not committed source.
7. Application event schemas and projections are above Durable Streams.

## API Shape

### Data Plane

The data plane owns user-addressed stream bytes and stream metadata.

```text
PUT    /v1/streams/{+path}
POST   /v1/streams/{+path}
HEAD   /v1/streams/{+path}
GET    /v1/streams/{+path}
DELETE /v1/streams/{+path}
```

The path parameter is stream-root-relative. It is greedy because stream paths
are hierarchical. It must not share a prefix with control resources.

Data-plane routes support:

- create or reconfirm stream
- fork stream
- append bytes
- close stream
- metadata read
- catch-up read
- long-poll read
- SSE read
- delete or soft-delete

### Control Plane

The control plane owns server coordination resources.

```text
PUT    /v1/subscriptions/{id}
GET    /v1/subscriptions/{id}
DELETE /v1/subscriptions/{id}
PUT    /v1/subscriptions/{id}/streams/{+path}
DELETE /v1/subscriptions/{id}/streams/{+path}

POST   /v1/subscription-delivery/{id}/callback
POST   /v1/subscription-delivery/{id}/claim
POST   /v1/subscription-delivery/{id}/ack
POST   /v1/subscription-delivery/{id}/release

PUT    /v1/schedules/{id}
GET    /v1/schedules/{id}
DELETE /v1/schedules/{id}

GET    /v1/jwks.json
```

This gives OpenAPI and generated routers a clean path tree. It also makes the
server architecture honest: subscription and schedule resources are not stream
data.

### Compatibility Aliases

If we need compatibility with the current draft shape:

```text
/v1/stream/{+path}
/v1/stream/__ds/...
```

then aliases are implemented at the HTTP adapter edge. They must lower to the
same protocol operations as the canonical routes. They are not the canonical
TypeSpec surface unless we deliberately choose legacy compatibility over clean
generation.

## TypeSpec Layout

Keep the top-level `typespec/` folder. The v2 route split should be reflected
without changing the conceptual module split.

```text
typespec/
  main.tsp
  common.tsp
  streams.tsp
  subscriptions.tsp
  subscription-delivery.tsp
  schedules.tsp
```

Required v2 TypeSpec changes:

- Move stream operation routes to `/v1/streams/{+path}`.
- Move subscription management routes to `/v1/subscriptions/...`.
- Move delivery routes to `/v1/subscription-delivery/...`.
- Move schedules to `/v1/schedules/...`.
- Move JWKS to `/v1/jwks.json`.
- Keep stream-root-relative path models for request bodies.
- Keep protocol names aligned with `PROTOCOL.md` concepts.
- Add a README note for any compatibility aliases outside the canonical
  OpenAPI surface.

The TypeSpec source should model HTTP placement directly with `@typespec/http`
and use library types where they help:

- `@typespec/rest` for resource-shaped control-plane routes.
- `@typespec/sse` for SSE response events.
- `@typespec/streams` for streaming bodies, with the known OpenAPI 3.1 warning.
- `@typespec/events` for outbound webhook event shapes if we model them as a
  sibling event contract.

## Server Components

```text
HTTP binding
  DataPlaneHttp
  ControlPlaneHttp
  CompatibilityHttp

Protocol services
  StreamService
  SubscriptionService
  DeliveryService
  ScheduleService
  WakeService
  WebhookService
  KeyService

Persistence
  Store
  MemoryStore
  SqlStore
  OrderedKvStore

Runtime
  Server
  Telemetry
  Config
```

### HTTP Binding

The HTTP binding performs transport work only:

- decode path, query, headers, and body
- preserve raw bytes for data-plane writes
- call protocol services
- lower typed protocol decisions to HTTP responses
- add cache and security headers
- expose OpenAPI-compatible response shapes

It must not implement producer-state logic, wake generation, schedule state
transitions, or filter evaluation.

### Protocol Services

Protocol services own state-machine semantics.

`StreamService`:

- create, append, close, fork, read, head, delete
- content-type checks
- stream sequence checks
- producer fencing
- tail-advance emission
- read mode behavior

`SubscriptionService`:

- normalized subscription config hash
- idempotent reconfirmation
- pattern backfill
- explicit stream membership
- subscription deletion

`WakeService`:

- consume committed tail-advance facts
- evaluate linked subscriptions
- update internal evaluated cursors
- create generation-fenced wake snapshots

`DeliveryService`:

- callback completion
- claim
- ack
- release
- lease expiry

`WebhookService`:

- build outbound webhook payloads
- sign deliveries
- retry according to policy
- handle callback token validation

`ScheduleService`:

- durable schedule create, reconfirm, read, cancel
- due schedule scanning
- append through `StreamService`
- idempotent retries through producer headers

`KeyService`:

- webhook signing key lifecycle
- JWKS publication
- key rotation overlap

## Effect Package Layout

The package should move toward this shape:

```text
packages/effect-durable-streams/src/
  Api.ts
  ApiLive.ts
  DataPlaneApi.ts
  ControlPlaneApi.ts
  Protocol.ts
  ProtocolError.ts
  Store.ts
  StreamService.ts
  SubscriptionService.ts
  WakeService.ts
  DeliveryService.ts
  WebhookService.ts
  ScheduleService.ts
  KeyService.ts
  MemoryStore.ts
  Server.ts
  Telemetry.ts
  Config.ts
  DurableStreamsServer.ts
  index.ts
```

`Api.ts` should be TypeSpec-aligned. Until there is a reliable generator from
TypeSpec to Effect `HttpApi`, it may be hand-authored, but it must be checked
against generated OpenAPI in tests or review.

`DurableStreamsServer.ts` remains an optional in-process RPC adapter over the
same services. It is not the source of public HTTP semantics.

## Store Model

The store is the durable correctness boundary. It exposes atomic protocol
operations and hides physical storage.

### Required Tables Or Collections

```text
streams
  path
  content_type
  tail_offset
  closed
  closed_at
  deleted
  soft_deleted
  ttl_seconds
  expires_at
  source_path
  fork_offset
  fork_sub_offset
  ref_count

stream_records
  path
  offset
  sub_offset
  byte_start
  byte_end
  body
  created_at

producer_state
  path
  producer_id
  epoch
  highest_accepted_seq
  expires_at

stream_sequence_state
  path
  writer_scope
  last_stream_seq

subscriptions
  id
  type
  config_hash
  pattern
  filter
  wake_stream
  webhook_url
  callback_token_hash
  lease_ttl_ms
  status
  generation

subscription_streams
  subscription_id
  path
  link_type
  public_acked_offset
  internal_evaluated_offset
  internal_evaluated_sub_offset

wake_snapshots
  subscription_id
  wake_id
  generation
  token_hash
  lease_expires_at
  claimed_by
  status
  payload

schedules
  id
  config_hash
  due_at
  target_path
  request
  status
  result

tail_advances
  path
  tail_offset
  closed
  created_at
  processed_at
```

Backends can store these differently, but they must provide equivalent
transactional behavior.

### Atomic Append Transaction

```text
begin transaction
  load stream metadata for update
  reject missing, gone, closed, content-type mismatch, Stream-Seq regression
  load producer state for update when producer headers are present
  apply producer fencing and gap rules
  insert stream record when accepted and body is non-empty
  update stream tail and close state
  update producer state
  update stream sequence state
  insert tail_advance when tail or closure changed
commit transaction
notify wake evaluator after commit
```

The transaction must serialize per `(stream, producerId)` and must not allow a
log write to commit without the corresponding producer-state update.

### Fork Transaction

```text
begin transaction
  load target stream
  if target exists, compare normalized fork/create config
  load source stream
  validate source availability and fork offset
  validate content type compatibility
  insert target stream with source pointer
  increment source ref count
commit transaction
```

Fork reads may stitch source records lazily. The fork storage model must not
copy source bytes unless a backend intentionally chooses copy-on-fork.

### Delete Transaction

```text
begin transaction
  load stream metadata
  if ref_count == 0:
    mark deleted or physically remove according to retention policy
    decrement source ref count if this stream is itself a fork
    cascade cleanup for soft-deleted ancestors with ref_count == 0
  else:
    mark soft_deleted
commit transaction
```

Direct operations against soft-deleted streams return `410 Gone`. Fork reads
may still stitch retained source data.

## Read Model

Reads are byte-oriented.

Catch-up:

- returns immediately with bytes from requested offset to tail
- returns `204` or empty body according to protocol when no data is available
- includes `Stream-Next-Offset`
- includes closure and up-to-date headers when applicable

Long-poll:

- requires an offset
- returns immediately if data exists or stream is closed
- waits only when offset is at the relevant tail
- wakes on append or close for that stream
- must not be awakened by source appends after a fork boundary

SSE:

- uses protocol control and data events
- encodes non-text bytes safely
- includes stream cursor while the stream remains open
- emits terminal control event and closes when stream is closed
- should close periodically for CDN friendliness

## Wake Model

Wake evaluation is post-commit.

```text
append transaction commits
tail advance is durable
wake evaluator reads tail advance
subscriptions are matched by explicit link and pattern link
filter is evaluated over new records
internal evaluated cursor advances
pending work creates or updates wake snapshot
delivery mechanism observes wake snapshot
```

This keeps arbitrary CEL/effectful evaluation out of storage transactions.

Pattern subscription creation performs an eager backfill of existing streams.
Backfill links each matching stream at its current tail so historical data is
not replayed by default.

## Webhook Delivery

Outbound webhook delivery is part of the protocol contract even though it is
not a server endpoint.

Payload:

```text
subscription_id
wake_id
generation
streams[]
callback_url
callback_token
```

Headers:

```text
Webhook-Signature
```

The server signs deliveries with an asymmetric key and publishes public keys at
`/v1/jwks.json`.

Delivery succeeds only when the worker calls the callback endpoint with the
current generation token. Retries are allowed, but retrying a stale generation
must not mutate newer wake state.

OpenAPI can model this as callbacks or as a sibling event contract. The server
implementation should not wait for that modeling decision before defining the
payload and signer service.

## Schedule Model

Schedules are durable append requests with a due time.

Create or reconfirm:

- normalize config and compare hash
- store pending schedule
- reject conflicting schedule ids

Runner:

- scans due pending schedules
- claims a schedule transactionally
- appends through `StreamService`
- records completed or failed result
- retries only in ways that preserve producer idempotency

Delete:

- cancels pending schedules
- does not erase completed schedule history unless retention policy allows it

## Error Model

Protocol services return typed errors. HTTP handlers lower them to declared
status codes.

Important response distinctions:

- `400 Bad Request`: malformed offset/header/body, producer bootstrap gap,
  invalid fork sub-offset, invalid lease TTL
- `403 Forbidden`: producer fenced, invalid token
- `404 Not Found`: stream or resource missing
- `409 Conflict`: closed stream, content type mismatch, Stream-Seq regression,
  producer gap after bootstrap, conflicting create/reconfirm config
- `410 Gone`: retention or soft-delete state
- `503 Service Unavailable`: optional overload/backpressure surface

Error precedence must live in protocol services or store transactions, not in
route handlers.

## Telemetry

Telemetry is a service-layer concern.

Required spans and attributes:

- `stream.create`: path, content type, decision
- `stream.append`: path, bytes, close, producer present, decision
- `stream.read`: path, mode, offset, bytes, wait duration
- `stream.delete`: path, hard/soft decision
- `subscription.put`: id, type, config hash decision
- `wake.evaluate`: subscription id, path, records scanned, matched
- `wake.claim`, `wake.ack`, `wake.release`: id, generation, outcome
- `webhook.deliver`: id, generation, attempt, status
- `schedule.run`: id, due lag, append outcome
- `store.transaction`: backend, operation, duration, retry count

Telemetry wrappers should be reusable across HTTP, RPC, and in-process
composition.

## Server Runtime

The runtime layer composes:

```text
Config
Store layer
Protocol services
HTTP API live layers
Webhook dispatcher
Wake evaluator
Schedule runner
Telemetry/logging
NodeHttpServer
```

Background fibers are scoped to the server layer. Interrupting the server must
interrupt dispatchers and runners cleanly.

For local development:

- `MemoryStore` is the default.
- OpenAPI viewer and Prism mock remain tooling, not server code.
- Generated scaffold output remains ignored or regenerated on demand.

For production:

- a durable store layer is required.
- background workers must be safe to run in multiple processes or explicitly
  single-owner through backend claims.

## Conformance Plan

### Unit Tests

- protocol header decoding
- path normalization
- append precedence
- producer fencing
- close retry idempotency
- fork validation
- lease token validation
- schedule config hashing

### Store Contract Tests

Run against every store layer:

- create/reconfirm conflict matrix
- concurrent append serialization
- producer state/log atomicity
- stream sequence regression
- soft delete and fork GC
- subscription backfill
- wake generation fencing
- schedule claim/complete/cancel

### HTTP Tests

- TypeSpec/OpenAPI route smoke tests
- canonical route tests
- compatibility alias tests if aliases exist
- raw byte preservation
- content negotiation for read modes
- SSE framing
- cache headers and nosniff headers

### Durable Backend Tests

- crash after append before notification
- crash after schedule claim before append
- crash after webhook delivery before callback
- retry after serialization conflict
- multi-worker claim contention

## Implementation Slices

### Slice 1: Contract Cleanup

- Update `PROTOCOL.md` route examples to canonical split.
- Update `typespec/` route prefixes.
- Regenerate OpenAPI.
- Update mock/viewer smoke tests.
- Document any compatibility aliases.

### Slice 2: API Alignment

- Split `Api.ts` into data-plane and control-plane groups.
- Add missing control-plane models to Effect schemas.
- Add drift checks against TypeSpec/OpenAPI where practical.
- Keep raw byte handling explicit at the data-plane boundary.

### Slice 3: Store Expansion

- Add stream fork, TTL, expiry, list/match, and soft-delete operations.
- Add subscription, membership, wake, and schedule operations.
- Keep `MemoryStore` passing all existing stream tests.

### Slice 4: Live Reads

- Implement long-poll waiters from committed tail-advance notifications.
- Implement SSE stream events.
- Add fork-aware wake behavior for reads.

### Slice 5: Coordination Plane

- Implement subscription CRUD and explicit stream membership.
- Implement wake evaluator.
- Implement pull-wake claim/ack/release.
- Implement webhook callback.
- Add JWKS endpoint and signing service.

### Slice 6: Schedules

- Implement schedule CRUD.
- Add schedule runner.
- Route due schedules through append state machine.

### Slice 7: Durable Store

- Choose first durable backend.
- Implement store contract.
- Add concurrent and crash/recovery tests.

## Open Questions

- Do we keep `/v1/stream/{+path}` as an alias for `/v1/streams/{+path}`?
- Do we expose compatibility aliases in OpenAPI or keep them undocumented?
- Do we generate Effect `HttpApi` contracts from TypeSpec or maintain them with
  automated drift checks?
- Which durable backend should land first?
- Should outbound webhook delivery be modeled as OpenAPI callbacks now, or as a
  separate event contract first?
- How much of pull-wake event payload shape should be first-class TypeSpec
  rather than protocol prose?

## Acceptance Criteria For v2

The v2 design is implemented when:

- canonical routes no longer depend on `__ds` precedence;
- TypeSpec and OpenAPI match the canonical route split;
- the Effect server has separate data-plane and control-plane API groups;
- all protocol state transitions flow through typed services and `Store`;
- memory store passes stream, control-plane, wake, and schedule contract tests;
- at least one durable backend passes the same store contract tests;
- Prism/mock tooling and generated scaffold tooling can be run without committing
  generated server artifacts.
