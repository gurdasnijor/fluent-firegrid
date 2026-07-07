# 8. Record Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A record envelope SHOULD have this abstract shape:

```json
{
  "type": "agent.prompt.requested",
  "key": "session:<session-id>:request:<request-id>",
  "value": {},
  "headers": {
    "producer": "runtime-1",
    "schema": "agent.prompt.requested.v1",
    "traceId": "...",
    "causationId": "...",
    "correlationId": "..."
  }
}
```

## 8.1 Required Envelope Fields

A conforming envelope format is encoding-neutral. JSON, Protobuf, CBOR, database rows, and binary frames are all acceptable when they preserve the same logical fields.

A conforming envelope format **MUST** include:

```txt
type
key or subject
value
headers
```

`type` identifies the record kind and version family. `key` or `subject` identifies the semantic entity or operation the record belongs to. `value` carries the domain payload. `headers` carry metadata needed for schema, replay, audit, idempotency, and causal reconstruction.

Envelope headers **MUST** include:

```txt
schema identifier
producer identity
correlation id
causation id, or explicit null/absent value for root-cause records
```

Envelope headers **SHOULD** include:

```txt
logical timestamp or append timestamp
idempotency key, when the record participates in an idempotent operation
```

Envelope headers **MAY** include:

```txt
trace context
tenant id
namespace
authorization metadata
dedupe key
content type
encoding
```

The append result, not the pre-append envelope, owns authoritative cursor information. A writer MAY include a previous cursor or causal cursor in headers for diagnostics, but consumers **MUST** use the log-assigned cursor or projection cursor for ordering and replay.

Schema identifiers **MUST** be stable enough for replay. If a record type evolves, the implementation **MUST** either version the type/schema, provide a deterministic migration rule, or reject unsupported historical records during rebuild with an explicit schema error.

Live adapter metadata has a narrower contract than durable envelope metadata. Data that must be queryable, replayable, or used for recovery **MUST** be written to the durable log. W3C trace context MAY travel separately through protocol metadata. Adapter-private metadata MAY carry request-local extensions such as parent lineage or load errors, but it **MUST NOT** become the durable observation or recovery contract.

## 8.2 Type Naming

Record types SHOULD be stable and versionable.

Examples:

```txt
agent.launch.requested
agent.launch.claimed
agent.runtime.provisioned
agent.session.created
agent.prompt.requested
agent.prompt.claimed
agent.prompt.chunk
agent.prompt.completed
agent.prompt.failed
agent.permission.requested
agent.permission.resolved
agent.awaitable.waiting
agent.awaitable.resolved
agent.timer.scheduled
agent.timer.fired
agent.resource.mounted
agent.session.stopped
```

Implementations MAY use their own naming scheme.

---
