# S2-Aligned Durable Streams State Protocol Profile

Status: draft

This document sketches an S2-aligned version of the Durable Streams State
Protocol. It keeps the useful state-change vocabulary from
`packages/state/STATE-PROTOCOL.md`, but removes assumptions from the original
Durable Streams raw-byte protocol. State messages are stored as S2 records and
read through S2 `ReadBatch` / `ReadSession` semantics.

References:

- S2-aligned Durable Streams profile:
  `docs/reference/durable-streams/S2_DS_PROTOCOL.md`
- Original Durable Streams State Protocol:
  `docs/reference/durable-streams/packages/state/STATE-PROTOCOL.md`
- Original State JSON Schema:
  `docs/reference/durable-streams/packages/state/state-protocol.schema.json`
- S2 OpenAPI: https://github.com/s2-streamstore/s2-specs/blob/main/s2/v1/openapi.json
- S2 protocol docs: https://s2.dev/docs/api/protocol.md

## 1. Goals

This profile should:

- represent each state event as one S2 record;
- use S2 `seq_num` order as the materialization order;
- use S2 append batches for atomic groups of state changes;
- use S2 record headers for routing and protocol metadata;
- keep state message bodies ordinary JSON;
- work with the official S2 SDKs and S2 Lite.

This profile should not:

- depend on the original Durable Streams `application/json` flattening rule;
- require raw HTTP request/response bodies;
- require Durable Streams opaque offsets;
- require Durable Streams SSE `data/control` events;
- require a custom producer protocol.

## 2. Relationship To S2

State streams are normal S2 streams. The state protocol is a convention for the
records written to those streams.

Core operations are native S2:

```text
POST /streams/{stream}/records       # append state records
GET  /streams/{stream}/records       # read state records
GET  /streams/{stream}/records/tail  # find current tail
```

Clients should use the S2 SDK where possible:

```ts
const stream = s2.basin(basin).stream(streamName)
await stream.append(AppendInput.create(records))
const batch = await stream.read({ start: { from: { seqNum } } })
```

## 3. Record Model

One state protocol message equals one S2 record.

```text
S2 stream
  seq_num 0 -> state change/control message
  seq_num 1 -> state change/control message
  seq_num 2 -> state change/control message
```

The S2 `seq_num` is the authoritative order. The S2 `timestamp` is the durable
append timestamp and may be used as event metadata if the message body omits an
application timestamp.

S2 append batches group multiple records atomically. A transaction or snapshot
chunk can be represented as one S2 `AppendInput` containing multiple state
records.

## 4. Record Headers

This profile reserves the following S2 record headers:

```text
ds-kind: state
ds-content-type: application/vnd.firegrid.state+json
ds-state-kind: change | control
ds-state-type: <entity-type>          # change records only
ds-state-key: <entity-key>            # change records only
ds-state-operation: insert | update | delete
ds-state-control: snapshot-start | snapshot-end | reset
ds-state-txid: <transaction-id>
ds-state-schema: <schema/version identifier>
```

Header rules:

- `ds-kind` must be `state` for records governed by this profile.
- `ds-content-type` should be `application/vnd.firegrid.state+json`.
- `ds-state-kind` distinguishes change records from control records.
- `ds-state-type`, `ds-state-key`, and `ds-state-operation` duplicate body
  fields for efficient routing/filtering without parsing the JSON body.
- When a duplicated header disagrees with the body, the body is authoritative
  for materialization and the record should be treated as malformed by strict
  consumers.

All S2 record headers are byte values. When using S2 JSON APIs, use
`s2-format: raw` for these ASCII header names/values.

## 5. Message Body Encoding

The record body is a UTF-8 JSON object encoded according to S2 rules.

Recommended unary append shape:

```text
POST /streams/{stream}/records
Content-Type: application/json
s2-format: raw
```

```json
{
  "records": [
    {
      "body": "{\"type\":\"user\",\"key\":\"user:123\",\"value\":{\"name\":\"Alice\"},\"headers\":{\"operation\":\"insert\"}}",
      "headers": [
        ["ds-kind", "state"],
        ["ds-content-type", "application/vnd.firegrid.state+json"],
        ["ds-state-kind", "change"],
        ["ds-state-type", "user"],
        ["ds-state-key", "user:123"],
        ["ds-state-operation", "insert"]
      ]
    }
  ]
}
```

For arbitrary binary-safe clients, use `s2-format: base64` and base64-encode the
JSON body and headers according to S2's JSON data representation. Protobuf
transport is also valid when using S2 SDK/session support.

## 6. Message Types

The body vocabulary is inherited from the original State Protocol.

### 6.1 Change Messages

Change messages represent state mutations.

Required fields:

- `type`: non-empty entity type string;
- `key`: non-empty entity key string;
- `headers.operation`: `insert`, `update`, or `delete`.

Required for `insert` and `update`:

- `value`: any JSON value.

Optional fields:

- `old_value`: any JSON value;
- `headers.txid`: transaction/group identifier;
- `headers.timestamp`: RFC 3339 application timestamp;
- additional application fields if a schema version explicitly permits them.

Examples:

```json
{
  "type": "user",
  "key": "user:123",
  "value": {
    "name": "Alice",
    "email": "alice@example.com"
  },
  "headers": {
    "operation": "insert",
    "timestamp": "2026-01-15T10:30:00Z"
  }
}
```

```json
{
  "type": "user",
  "key": "user:123",
  "value": {
    "name": "Alice",
    "email": "alice.new@example.com"
  },
  "old_value": {
    "name": "Alice",
    "email": "alice@example.com"
  },
  "headers": {
    "operation": "update",
    "txid": "tx-001"
  }
}
```

```json
{
  "type": "user",
  "key": "user:123",
  "old_value": {
    "name": "Alice",
    "email": "alice.new@example.com"
  },
  "headers": {
    "operation": "delete"
  }
}
```

### 6.2 Control Messages

Control messages manage materialization state.

Required fields:

- `headers.control`: `snapshot-start`, `snapshot-end`, or `reset`.

Optional fields:

- `headers.txid`: identifier tying a control record to related changes;
- `headers.timestamp`: RFC 3339 application timestamp;
- `headers.seq_num`: S2 sequence number string associated with the control
  event, if useful for cross-system exports.

Unlike the original State Protocol, control messages should not use Durable
Streams offsets. S2 `seq_num` is the read coordinate.

Examples:

```json
{
  "headers": {
    "control": "snapshot-start",
    "txid": "snapshot-2026-01-15"
  }
}
```

```json
{
  "headers": {
    "control": "snapshot-end",
    "txid": "snapshot-2026-01-15"
  }
}
```

```json
{
  "headers": {
    "control": "reset"
  }
}
```

## 7. Append Semantics

### 7.1 Atomic Groups

S2 append batches are atomic. Use one S2 `AppendInput` for a transaction-sized
group of state messages:

```json
{
  "records": [
    { "body": "{...change-1...}", "headers": [["ds-kind", "state"]] },
    { "body": "{...change-2...}", "headers": [["ds-kind", "state"]] }
  ],
  "match_seq_num": 42
}
```

If all records are accepted, S2 returns one `AppendAck`. The appended records
occupy the half-open range:

```text
ack.start.seq_num <= seq_num < ack.end.seq_num
```

### 7.2 Expected Tail

Use S2 `match_seq_num` to avoid lost updates when a writer expects to append at
a particular tail.

If `match_seq_num` fails, S2 returns `412 AppendConditionFailed` with
`seq_num_mismatch`.

### 7.3 Writer Fencing

Use S2 `fencing_token` for writer ownership when needed.

This profile does not define `Producer-Id`, `Producer-Epoch`, or `Producer-Seq`.
If a state application needs per-producer duplicate replay, it should encode
that as application metadata in the state messages or use an application-level
idempotency key.

### 7.4 Idempotency

Recommended idempotency strategy:

- include an application event id in each change message, for example
  `headers.event_id`;
- materializers keep a bounded or persistent seen-event index when exactly-once
  projection is required;
- writers use S2 `match_seq_num` or `fencing_token` when they need append-side
  concurrency control.

This keeps idempotency at the state-application layer instead of creating a
second transport-level producer protocol.

## 8. Read And Materialization

### 8.1 Unary Read

Read with S2:

```text
GET /streams/{stream}/records?seq_num=0&count=1000
s2-format: raw
```

Each returned `SequencedRecord` is decoded as:

1. inspect headers;
2. ignore records whose `ds-kind` is not `state`, unless the application wants a
   mixed stream;
3. parse the JSON body;
4. validate the state message;
5. apply it in increasing `seq_num` order.

### 8.2 Live Read

Use S2 `readSession` or S2 SSE. This profile does not define a custom state SSE
wire format.

Live materializers process each S2 `ReadBatch` in order and persist the last
fully applied `seq_num`.

### 8.3 Resume Position

The resume position is the next S2 `seq_num` to read.

Materializers should persist:

```json
{
  "stream": "state/users",
  "next_seq_num": 43
}
```

This replaces Durable Streams offsets.

### 8.4 Applying Changes

Materialized state is keyed by `(type, key)`.

Rules:

- `insert`: store `value` at `(type, key)`;
- `update`: replace `value` at `(type, key)`;
- `delete`: remove `(type, key)`;
- `snapshot-start`: implementation-defined, commonly marks the start of a full
  replacement snapshot;
- `snapshot-end`: implementation-defined, commonly commits the snapshot;
- `reset`: clear materialized state and continue from the next record unless the
  application specifies another behavior.

The state protocol does not prescribe conflict resolution. Last-writer-wins is
the natural default because S2 provides total order.

## 9. Snapshots

Snapshots are represented as state records.

Recommended shape:

```text
snapshot-start control
change records containing complete snapshot rows
snapshot-end control
```

All records in a snapshot may share the same `headers.txid`.

If a snapshot must be committed atomically, keep it within one S2 append batch.
If the snapshot exceeds S2 append limits, consumers must treat the
`snapshot-start` / `snapshot-end` bracket as the transactional boundary and
handle incomplete snapshots according to application policy.

## 10. Schema Validation

Validation is application-level.

Implementations may validate:

- the outer state message shape;
- the `value` shape for each entity `type`;
- header/body duplication consistency;
- schema version from `ds-state-schema` or `headers.schema`.

Standard Schema remains a reasonable validation abstraction, but the wire does
not require any specific schema library.

Invalid records should be handled according to materializer policy:

- strict mode: stop and surface an error;
- permissive mode: skip malformed records and continue;
- quarantine mode: copy malformed records to a dead-letter stream.

## 11. Recommended JSON Schema Delta

The original `state-protocol.schema.json` can remain the message body schema
with small S2-aligned changes:

- remove `headers.offset`;
- optionally add `headers.seq_num` for exported/control metadata;
- optionally add `headers.event_id`;
- optionally add `headers.schema`;
- allow `headers.txid` on control messages;
- keep `additionalProperties: false` for the core protocol shape unless a
  schema version explicitly permits application extensions.

The S2 record envelope is not part of that JSON Schema. It is represented by S2
`AppendRecord` and `SequencedRecord`.

## 12. Example

Append two state changes atomically:

```ts
const records = [
  AppendRecord.string({
    body: JSON.stringify({
      type: "user",
      key: "user:123",
      value: { name: "Alice" },
      headers: { operation: "insert", txid: "tx-1" }
    }),
    headers: [
      ["ds-kind", "state"],
      ["ds-content-type", "application/vnd.firegrid.state+json"],
      ["ds-state-kind", "change"],
      ["ds-state-type", "user"],
      ["ds-state-key", "user:123"],
      ["ds-state-operation", "insert"],
      ["ds-state-txid", "tx-1"]
    ]
  }),
  AppendRecord.string({
    body: JSON.stringify({
      type: "message",
      key: "msg:456",
      value: { userId: "user:123", text: "hello" },
      headers: { operation: "insert", txid: "tx-1" }
    }),
    headers: [
      ["ds-kind", "state"],
      ["ds-content-type", "application/vnd.firegrid.state+json"],
      ["ds-state-kind", "change"],
      ["ds-state-type", "message"],
      ["ds-state-key", "msg:456"],
      ["ds-state-operation", "insert"],
      ["ds-state-txid", "tx-1"]
    ]
  })
]

const ack = await stream.append(AppendInput.create(records, {
  matchSeqNum: expectedTail
}))
```

Materializer loop:

```ts
let nextSeqNum = 0

for (;;) {
  const batch = await stream.read({
    start: { from: { seqNum: nextSeqNum } },
    stop: { limits: { count: 1000 }, waitSecs: 10 }
  })

  for (const record of batch.records) {
    if (!hasHeader(record, "ds-kind", "state")) continue
    const message = JSON.parse(record.body)
    applyStateMessage(message)
    nextSeqNum = record.seqNum + 1
  }
}
```

## 13. Removed From Core Profile

Compared to the original Durable Streams State Protocol, this profile removes:

- dependence on `Content-Type: application/json` Durable Streams flattening;
- JSON array reads from Durable Streams;
- Durable Streams offsets in control messages;
- raw Durable Streams append/read operations;
- custom Durable Streams SSE assumptions;
- server-side state-protocol validation as part of append admission.

Those behaviors can be implemented by a compatibility gateway, but they are not
part of the S2-aligned state profile.

## 14. Compatibility Gateway

A gateway from the original State Protocol to this profile would:

- accept original Durable Streams JSON appends;
- split JSON arrays into S2 state records;
- map Durable Streams offsets to S2 `seq_num`;
- project S2 `ReadBatch` back into JSON arrays;
- implement original DS SSE if required.

That gateway should be separate from the S2-aligned state core.

## 15. Security Considerations

Consumers must treat state records as untrusted input.

Implementations should:

- validate JSON before materialization;
- enforce allowed entity types and keys;
- limit message size according to S2 append limits and application policy;
- avoid executing schema-provided code from untrusted streams;
- authenticate S2 access with scoped tokens;
- avoid leaking sensitive application paths in stream names.

## 16. Open Questions

1. Should `ds-state-type` / `ds-state-key` headers be required, or should they
   remain an optimization over the JSON body?
2. Should transaction boundaries be defined only by S2 append batches, or should
   `txid` create cross-batch transaction semantics?
3. Should malformed records halt materialization by default?
4. Do we need a standard dead-letter stream naming convention?
5. Should snapshots larger than one S2 append batch be part of the core profile,
   or an application-level extension?
