# Durable Streams Coordination Pushdown SDD

Status: draft
Target repo: `packages/durable-streams`
Feature spec: `features/durable-streams/coordination-substrate.feature.yaml`

## Purpose

This SDD pins the next implementation slice for pushing durable coordination
semantics into Durable Streams. The goal is to keep `effect-durable-execution`
thin: it should expose authoring primitives and lower them to substrate
capabilities, not rebuild timer, predicate, claim, ack, cursor, or dedupe
machinery above Durable Streams.

## Decisions Already Made

- Capability discovery is not a first-slice concern. Server/client package
  version coupling is acceptable until the substrate stabilizes.
- Filtered subscriptions use the CEL library already used in this codebase.
- This is greenfield. There is no compatibility-preservation requirement for
  older `/consumers` routes.
- Canonical coordination APIs live under the reserved `__ds` namespace.
- Server behavior conformance lives in `packages/server-conformance-tests`.
- Effect client conformance lives in
  `packages/effect-durable-streams/test/conformance`.
- `effect-durable-execution` consumes the Effect client and must not own
  substrate state machines.

## Canonical API Direction

The implementation should converge on:

```text
{stream-root}/__ds/subscriptions/:id
{stream-root}/__ds/subscriptions/:id/streams
{stream-root}/__ds/subscriptions/:id/claim
{stream-root}/__ds/subscriptions/:id/ack
{stream-root}/__ds/subscriptions/:id/release
{stream-root}/__ds/schedules/:id
```

Do not expand `/consumers` for this work. If existing code paths need to be
deleted or rewritten to make `__ds/subscriptions` canonical, do that as part of
the implementation plan instead of preserving both shapes.

## Filtered Subscriptions

### Request Shape

Filtered subscriptions extend subscription creation:

```json
{
  "type": "pull-wake",
  "pattern": "events/*",
  "streams": ["events/manual"],
  "wake_stream": "wake/pool",
  "filter": {
    "language": "cel",
    "expression": "event.type == 'ready' && self.kind == 'job'",
    "self": { "kind": "job" }
  },
  "lease_ttl_ms": 30000,
  "description": "ready jobs"
}
```

`filter` is optional. When present:

| Field | Required | Semantics |
|---|---:|---|
| `filter.language` | yes | Must be `"cel"` for this slice. |
| `filter.expression` | yes | CEL expression that must evaluate to boolean. |
| `filter.self` | no | Immutable JSON value available as `self`; defaults to `{}`. |

`filter` participates in the normalized subscription config hash. Reconfirming
the same subscription id with a different filter returns a conflict.

### Evaluation Context

CEL receives:

| Name | Value |
|---|---|
| `event` | decoded JSON event item being evaluated |
| `stream` | stream-root-relative path |
| `offset` | event offset |
| `self` | immutable filter context |

Evaluation is server-side. The Effect client may validate request shapes and
construct filters, but it must not implement local predicate matching as the
durable wait mechanism.

### Cursor Semantics

Filtered subscriptions maintain:

- public `acked_offset`, advanced only by ack/callback; and
- internal evaluated offset, advanced by the server while testing non-matches.

Non-matching events do not wake the subscription. A later matching event wakes
through the same webhook or pull-wake path as an unfiltered subscription.

The internal evaluated offset is not part of the normal public subscription
response in this slice. It may be exposed later through diagnostics.

### Error Codes

| Condition | Status | Code |
|---|---:|---|
| unsupported filter language | 400 | `FILTER_LANGUAGE_UNSUPPORTED` |
| invalid CEL syntax or non-boolean expression | 400 | `FILTER_INVALID` |
| known target stream has incompatible content type | 409 | `FILTER_CONTENT_TYPE_UNSUPPORTED` |
| filter differs during idempotent reconfirmation | 409 | `SUBSCRIPTION_CONFIG_CONFLICT` |
| runtime CEL evaluation failure | no wake; diagnostic | `FILTER_EVALUATION_FAILED` |

Runtime evaluation failure should not crash the subscription worker. For the
first slice it is acceptable to treat that event as a non-match and record an
operator-visible diagnostic.

## Scheduled Append

### Request Shape

```http
PUT {stream-root}/__ds/schedules/:id
Content-Type: application/json

{
  "at": "2026-06-08T12:00:00.000Z",
  "stream": "sessions/abc",
  "content_type": "application/json",
  "body": { "type": "timer.fired", "timer_id": "t1" },
  "producer": {
    "id": "timer:t1",
    "epoch": 0,
    "seq": 0
  },
  "close": false
}
```

Fields:

| Field | Required | Semantics |
|---|---:|---|
| `at` | yes | RFC3339 timestamp. Fire must not happen before this instant. |
| `stream` | yes | stream-root-relative target path. |
| `content_type` | yes | content type for the target append. |
| `body` | conditional | JSON value when `content_type` is `application/json`. |
| `body_base64` | conditional | bytes for non-JSON appends; mutually exclusive with `body`. |
| `producer` | no | producer tuple applied at final append. |
| `close` | no | if true, append-and-close target stream. |

Schedule creation is idempotent by schedule id plus normalized config.

### Response Shape

```json
{
  "id": "t1",
  "status": "pending",
  "at": "2026-06-08T12:00:00.000Z",
  "stream": "sessions/abc",
  "created_at": "2026-06-08T11:00:00.000Z",
  "fired_at": null,
  "error": null
}
```

`status` is one of:

- `pending`
- `fired`
- `cancelled`
- `failed`

### Delete Semantics

`DELETE /__ds/schedules/:id`:

- `pending` -> `cancelled`, returns `204`
- `cancelled` -> idempotent `204`
- `fired` or `failed` -> `409 SCHEDULE_TERMINAL`
- missing schedule -> `404 SCHEDULE_NOT_FOUND`

Deleting a terminal schedule never mutates the target stream.

### Fire Semantics

The fire path uses the normal append implementation. It must apply content-type
checks, stream closure checks, producer dedupe, append-and-close behavior, and
subscription wake hooks.

Firing may be late. Firing must not be early.

If the target append is rejected, schedule status becomes `failed` and `error`
records the protocol error code. The scheduler must not retry forever after a
deterministic target append rejection.

### Error Codes

| Condition | Status | Code |
|---|---:|---|
| invalid request body | 400 | `SCHEDULE_INVALID` |
| same id with different normalized config | 409 | `SCHEDULE_CONFIG_CONFLICT` |
| schedule not found | 404 | `SCHEDULE_NOT_FOUND` |
| delete fired/failed schedule | 409 | `SCHEDULE_TERMINAL` |
| target append rejected during fire | stored status | target protocol error code |

## Commit-once / Named Steps

For this slice, commit-once remains the protocol's producer tuple:

```text
(stream, Producer-Id, Producer-Epoch, Producer-Seq)
```

`effect-durable-execution` named steps can continue to replay by scanning the
session stream for a terminal step fact and appending new terminal facts through
producer fencing.

Do not add a server-side unique step-key index yet. Revisit only if conformance
or benchmarks show scan plus producer fencing is insufficient.

## Child And Attachment Composition

No new server protocol is required for this slice. Child and attachment helpers
should lower to:

1. derive/create child stream;
2. append invocation fact with producer tuple;
3. subscribe parent to terminal/progress facts;
4. ack after parent commits its reaction.

Any helper added to `effect-durable-streams` or `effect-durable-execution` must
lower to stream creation, append, subscription, ack, and release.

## Conformance Plan

### Server Conformance

Add portable behavior coverage in `packages/server-conformance-tests`:

- `filtered-subscription-tests.ts`
- `scheduled-append-tests.ts`

These tests prove the server protocol behavior independent of Effect.

### Effect Client Conformance

Add accepted client coverage in
`packages/effect-durable-streams/test/conformance`:

- `filtered-subscription.conformance.test.ts`
- `scheduled-append.conformance.test.ts`
- `subscription-claim-loop.conformance.test.ts`
- `coordination-helpers.conformance.test.ts`

These tests prove the public Effect client API drives a real server correctly.
They should not duplicate every server edge case.

### Durable Execution Package Tests

`packages/effect-durable-execution` tests should prove lowering behavior:

- `run` replays from producer-fenced journal facts;
- future `sleep` lowers to scheduled append plus subscription wait;
- future `wait` lowers to filtered subscriptions;
- future `spawn`/`attach` lower to child streams plus subscriptions.

## Implementation Order

1. Canonicalize `__ds/subscriptions` as the target API for new work.
2. Implement filtered subscriptions in server and server conformance tests.
3. Expose filtered subscriptions in `effect-durable-streams` and add Effect
   client conformance.
4. Implement scheduled append in server and server conformance tests.
5. Expose scheduled append in `effect-durable-streams` and add Effect client
   conformance.
6. Lower `effect-durable-execution` wait/sleep helpers onto those client
   capabilities.

## Non-goals

- Capability discovery or negotiation.
- Preserving `/consumers` compatibility.
- Product-specific webhook/provider authentication.
- Runtime-owned predicate registries.
- Runtime-owned timer heaps.
- Runtime-owned dedupe databases.
- Direct imports from `packages/server` into Effect client or authoring
  packages.
