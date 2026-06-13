# S2-Aligned Durable Streams Coordination SDD

Status: draft

Related docs:

- `docs/reference/durable-streams/S2_DS_PROTOCOL.md`
- `docs/reference/durable-streams/S2_DS_STATE_PROTOCOL.md`
- S2 OpenAPI: https://github.com/s2-streamstore/s2-specs/blob/main/s2/v1/openapi.json
- S2 append concepts: https://s2.dev/docs/concepts/appends
- S2 concurrency control: https://s2.dev/docs/concepts/concurrency-control

## Final Verdict

Use S2 as the durable stream core. Do not fork S2 and do not rebuild the
original Durable Streams protocol unless an existing client compatibility
requirement forces it.

The S2-aligned core should be:

- S2 basins and streams;
- S2 `AppendInput`, `AppendRecord`, `AppendAck`, `ReadBatch`, `TailResponse`;
- S2 `seq_num` as the public stream coordinate;
- S2 append batches as the atomic write unit;
- S2 append sessions and SDK `Producer` for ordered high-throughput writes;
- S2 `match_seq_num` for expected-tail / optimistic concurrency;
- S2 `fencing_token` and native `fence` command records for cooperative writer
  ownership;
- S2 read sessions / SSE for live reads;
- record headers for application metadata such as content type and state
  routing.

Drop these from the S2-aligned core:

- Durable Streams raw-body HTTP append/read;
- opaque Durable Streams offsets;
- `Stream-Next-Offset`, `Stream-Cursor`, `Stream-Up-To-Date`;
- Durable Streams SSE `data` / `control` projection;
- server-side `application/json` flattening;
- Durable Streams `Producer-Id` / `Producer-Epoch` / `Producer-Seq`;
- `Stream-Seq`;
- strict protocol-level content-type equality;
- strict close enforcement.

These inherited Durable Streams features should return only as product-scoped
services:

- scheduling;
- forking;
- subscriptions;
- wake delivery.

## Why

S2 already owns the load-bearing log behavior:

- appends are durable before acknowledgement;
- batches are atomic;
- streams are totally ordered by `seq_num`;
- `match_seq_num` gives expected-tail admission;
- `fencing_token` gives strongly consistent cooperative fencing;
- sessions provide ordered, backpressured writes and live reads.

The features removed from core are either conveniences of the old wire format or
protocol-level duplicates of S2 primitives. Keeping them in the core would make
the system harder to reason about without adding storage guarantees.

Scheduling, forking, subscriptions, and wake delivery are different. They are
not data-plane stream primitives. They are control-plane services that use
streams as durable facts.

## Package Direction

Keep `packages/fluent-durable-streams` HTTPAPI-first and S2-native:

```text
packages/fluent-durable-streams/
  src/
    api.ts
    s2.ts
    server.ts
    errors.ts
    index.ts
```

Do not create protocol transports, in-memory protocol packages, path-to-stream
name helpers, or reserved-header profile helpers ahead of a concrete product
feature. Tests should use Effect HTTPAPI client/schema tests for the boundary and
S2 Lite or the S2 SDK against a real S2-compatible endpoint for backend
integration.

Scheduling, forks, subscriptions, and wakes should be added later as explicit
HTTPAPI groups, not hidden helper modules.

## S2 Lite Harness

Use S2 Lite exactly as documented by S2 for local development and CI. It is a
single S2-compatible server and should replace any fake in-memory protocol
surface.

Run without external dependencies:

```bash
s2 lite --port 8080
```

or with Docker:

```bash
docker run -p 8080:80 ghcr.io/s2-streamstore/s2 lite
```

For persistent local tests, use S2 Lite local disk mode. For durable
object-storage-backed tests, run S2 Lite with a bucket. The spike should start
with the no-bucket emulator mode for repeatable integration tests.

SDK configuration for local Lite:

```ts
const s2 = new S2({
  accessToken: "local-token",
  endpoints: {
    account: "http://localhost:8080",
    basin: "http://localhost:8080"
  }
})
```

Equivalent environment variables:

```bash
export S2_ACCOUNT_ENDPOINT="http://localhost:8080"
export S2_BASIN_ENDPOINT="http://localhost:8080"
export S2_ACCESS_TOKEN="ignored"
```

S2 Lite can pre-create basins and streams with an init file:

```bash
s2 lite --init-file init.json
```

Example init file:

```json
{
  "$schema": "https://raw.githubusercontent.com/s2-streamstore/s2/main/cli/schema.json",
  "basins": [
    {
      "name": "fluent-dev",
      "config": {
        "create_stream_on_append": true,
        "create_stream_on_read": true
      }
    }
  ]
}
```

S2 Lite implements the S2 API, and the S2 OpenAPI spec is generated from Lite.
Known Lite gaps from the docs: access-token APIs and metrics APIs are not
implemented yet; tokens are ignored. That is fine for this spike because the
profile exercises basin, stream, append, read, tail, and session behavior.

## S2 Record Conventions

Reserve `ds-` record headers for profile metadata:

```text
ds-kind: data | state | schedule | fork | subscription | wake | close
ds-content-type: <media-type>
ds-id: <stable id>
ds-schema: <schema/version>
```

Use S2 native command records for native S2 behavior:

- `fence` for fencing tokens;
- `trim` for trim points.

Do not encode S2-native commands as `ds-*` records.

## Scheduling

### Purpose

Durable scheduled append: at or after a timestamp, append one S2 batch to a
target stream exactly once according to a schedule id.

### Storage

Use S2 streams as durable schedule state:

```text
__ds/schedules           # schedule facts by id
__ds/schedules-due       # due-index facts by time bucket
```

Schedule record:

```json
{
  "id": "timer-1",
  "status": "pending",
  "at": "2026-06-12T18:00:00.000Z",
  "target_stream": "sessions/abc",
  "append": {
    "records": [
      {
        "body": "{\"type\":\"timer.fired\",\"timer_id\":\"timer-1\"}",
        "headers": [["ds-content-type", "application/json"]]
      }
    ],
    "match_seq_num": null,
    "fencing_token": null
  },
  "created_at": "2026-06-12T17:00:00.000Z"
}
```

State transitions:

```text
pending -> firing -> fired
pending -> cancelled
pending -> firing -> failed
```

### Worker

A scheduler worker:

1. Reads due schedule facts.
2. Claims a schedule by appending a `firing` fact with `match_seq_num` against
   the schedule stream tail observed by the worker.
3. Appends the configured S2 batch to the target stream.
4. Appends `fired` with the target `AppendAck`, or `failed` with the S2 error.

### Guarantees

- A schedule must not fire before `at`.
- A schedule may fire late.
- Re-confirming the same schedule id with identical normalized config is
  idempotent.
- Re-confirming the same id with different config is a conflict.
- Deleting a pending schedule appends a `cancelled` fact.

### Difficulty

Medium. The core is straightforward. Operational correctness comes from worker
claiming, retries, and replay on restart.

## Forking

### Purpose

Create a stream that starts with a historical prefix of another stream and then
continues independently.

### Recommendation

Start with copy-on-fork. Defer pointer-stitch until copy cost is proven too
high.

### Copy-On-Fork

Fork creation:

1. Read source records up to `source_seq_num`.
2. Create target stream if needed.
3. Append copied source records to target stream in S2 batches.
4. Append a fork metadata record to `__ds/forks`.

Fork metadata:

```json
{
  "fork_stream": "sessions/forked",
  "source_stream": "sessions/source",
  "source_end_seq_num": 42,
  "mode": "copy",
  "created_at": "2026-06-12T17:00:00.000Z"
}
```

### Pointer-Stitch Later

Pointer-stitch requires:

- fork-local stream;
- fork metadata stream;
- read planner that emits `source[..divergence) ++ forkLocal`;
- retention rules that prevent trimming source records needed by forks;
- tombstone/refcount state.

That is a real server-side read service, not a simple SDK helper.

### Difficulty

Copy-on-fork: Medium.

Pointer-stitch plus soft-delete/refcount retention: High.

## Subscriptions

### Purpose

Durable cursors over one or more streams with optional pattern matching and
optional filters.

### Storage

Use S2 metadata streams:

```text
__ds/subscriptions          # subscription config facts
__ds/subscription-links     # stream membership / cursor facts
__ds/subscription-wakes     # wake generation facts
```

Subscription config:

```json
{
  "id": "sub-1",
  "type": "pull-wake",
  "pattern": "events/*",
  "streams": ["events/manual"],
  "wake_stream": "wakes/workers",
  "filter": {
    "language": "json-path",
    "expression": "$.type == 'ready'"
  },
  "lease_ttl_ms": 30000,
  "config_hash": "sha256:..."
}
```

Subscription link:

```json
{
  "subscription_id": "sub-1",
  "stream": "events/orders",
  "link_type": "glob",
  "acked_seq_num": 42,
  "evaluated_seq_num": 45
}
```

### Matching

A subscription service watches relevant S2 streams using read sessions or scans
recent records on demand.

Pattern matching uses S2 stream names or an application path mapping.

Filtered subscriptions evaluate records after decoding them. Non-matching
records may advance `evaluated_seq_num` but must not advance `acked_seq_num`.

### Guarantees

- Subscription creation is idempotent by normalized config hash.
- Cursor advancement is durable.
- Acks are explicit.
- Pattern subscriptions must backfill matching existing streams from current
  tail, not historical head, unless configured otherwise.

### Difficulty

High. The log storage is easy; durable membership, cursor advancement, filters,
and wake generation are the hard parts.

## Wake Delivery

### Purpose

Notify workers that subscribed streams have pending records.

### Pull-Wake

Use a normal S2 stream as the wake stream:

```text
wakes/workers
```

Wake record:

```json
{
  "subscription_id": "sub-1",
  "wake_id": "wake-123",
  "generation": 7,
  "streams": [
    {
      "stream": "events/orders",
      "acked_seq_num": 42,
      "tail_seq_num": 45
    }
  ],
  "created_at": "2026-06-12T17:00:00.000Z"
}
```

Workers read the wake stream, then claim work through the subscription service.

Claim state:

```json
{
  "subscription_id": "sub-1",
  "wake_id": "wake-123",
  "generation": 7,
  "worker": "worker-a",
  "lease_expires_at": "2026-06-12T17:00:30.000Z"
}
```

Ack:

```json
{
  "subscription_id": "sub-1",
  "wake_id": "wake-123",
  "generation": 7,
  "acks": [
    { "stream": "events/orders", "seq_num": 45 }
  ],
  "done": true
}
```

### Webhook

Webhook delivery is a worker on top of the same wake facts:

1. Claim wake.
2. Send HTTPS webhook with signed body.
3. On success, advance ack cursors.
4. On failure, append retry fact with next attempt time.

Signing keys and retry state are service metadata, not data-plane stream
semantics.

### Guarantees

- `generation` fences stale workers.
- `wake_id` prevents replaying one wake into another.
- Lease expiry makes work recoverable.
- Ack cursor advancement must be durable.

### Difficulty

Pull-wake: Medium-high.

Webhook delivery with retries and signing: High.

## Delivery Order

1. S2 profile helpers:
   - names;
   - headers;
   - append/read helpers;
   - state record helpers.
2. Scheduling:
   - smallest standalone control-plane feature;
   - easy to test against S2 Lite;
   - validates durable worker/replay model.
3. Pull-wake subscriptions:
   - subscriptions without webhook complexity.
4. Webhook delivery:
   - signing, retries, callback/ack.
5. Copy-on-fork:
   - feature useful for branching histories, but independent of scheduling.
6. Pointer-stitch forks and soft-delete:
   - only after copy-on-fork cost or retention semantics require it.

## Non-Goals

- Reintroducing the original Durable Streams raw HTTP wire.
- Reintroducing the old producer tuple unless product requirements demand
  duplicate response replay.
- Building a local/in-memory protocol transport.
- Forking S2 before an S2-wrapper implementation proves an unsolved limitation.

## Open Questions

1. Is close/EOF a product requirement, or can terminal state be an application
   record?
2. Is copy-on-fork enough for expected usage?
3. Do subscriptions need glob discovery over all S2 streams, or only explicit
   stream lists?
4. Should schedules use one global due stream or bucketed due streams?
5. Are webhooks required, or is pull-wake sufficient initially?
