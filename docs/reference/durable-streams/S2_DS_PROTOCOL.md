# S2-Aligned Durable Streams Protocol Profile

Status: draft

This document sketches a Durable Streams profile that is intentionally aligned
with the S2 v1 API instead of the original raw-byte `PROTOCOL.md` wire. The goal
is to identify the smallest protocol we need if S2 or S2 Lite is the storage and
serving substrate.

References:

- S2 OpenAPI: https://github.com/s2-streamstore/s2-specs/blob/main/s2/v1/openapi.json
- S2 protocol docs: https://s2.dev/docs/api/protocol.md
- Original Durable Streams protocol:
  `docs/reference/durable-streams/PROTOCOL.md`

## 1. Goals

This profile should:

- use S2's stream, append, read, tail, and session wire formats directly;
- avoid inventing a second raw-byte HTTP protocol when S2 already defines one;
- preserve application-level durable stream needs where they are real;
- make extensions explicit, small, and implementable on top of S2 record
  headers/control records;
- remain compatible with the official S2 SDKs where possible.

This profile should not:

- require a fork of S2 for the first implementation;
- expose the original Durable Streams raw-body `PUT/POST/GET` wire;
- require custom client/server transports;
- require forks, subscriptions, or schedules in the S2-native core.

## 2. Relationship To S2

This protocol is an S2 profile. It uses S2's public API shape:

```text
GET    /basins
POST   /basins
PUT    /basins/{basin}
GET    /streams
POST   /streams
PUT    /streams/{stream}
DELETE /streams/{stream}
GET    /streams/{stream}/records
POST   /streams/{stream}/records
GET    /streams/{stream}/records/tail
```

For S2 Lite, stream requests include the basin through the SDK/environment. The
wire detail is owned by S2; Durable Streams code should use the S2 SDK rather
than constructing these requests by hand unless it is implementing an SDK.

## 3. Terminology

**Basin**: S2 namespace for streams. A Durable Streams deployment uses one or
more basins.

**Stream**: An S2 stream. A stream is an ordered sequence of S2 records.

**Record**: The atomic ordered item in a stream. This profile treats one
application message as one S2 record unless chunking is explicitly enabled.

**Position**: S2 `StreamPosition`, containing `seq_num` and `timestamp`.

**Tail**: S2 `tail`, the position assigned to the next record.

**Cursor**: Client-held read coordinate. In this profile the primary cursor is
S2 `seq_num`. Higher-level clients may wrap it in an opaque token, but the
service wire remains S2-shaped.

**Control record**: An S2 record whose headers mark it as protocol metadata
rather than application data.

## 4. Wire Formats

### 4.1 Encodings

S2 supports JSON and protobuf for data-plane operations.

JSON record bodies and headers contain bytes represented according to the
`s2-format` request header:

- omitted or `s2-format: raw`: bytes are represented as Unicode strings;
- `s2-format: base64`: bytes are represented as base64 strings.

Clients should use:

- `raw` for text and JSON payloads known to be valid UTF-8;
- `base64` for arbitrary binary payloads;
- protobuf where binary overhead matters and SDK support is available.

Unlike the original Durable Streams protocol, application bytes are not the HTTP
entity body. They live inside S2 `AppendRecord.body` and
`SequencedRecord.body`.

### 4.2 Record Headers

S2 records support binary headers. This profile reserves lowercase ASCII header
names beginning with `ds-`.

Reserved headers:

```text
ds-kind: data | close | meta
ds-content-type: <media-type>
ds-stream-id: <stable-id>
ds-schema: <schema/version identifier>
```

Application headers must not use the `ds-` prefix.

`ds-kind` defaults to `data` when absent.

S2 also has native command records. A native command record has a single header
with an empty name; the header value names the operation. Current native command
records include:

- `fence`: sets the stream fencing token;
- `trim`: advances the stream trim point.

Profile headers must not try to mimic native command records. Use native command
records through the SDK/CLI when the desired behavior is S2 fencing or trimming.

## 5. Stream Lifecycle

### 5.1 Basin Lifecycle

Basin lifecycle is native S2:

```text
POST /basins
PUT  /basins/{basin}
GET  /basins/{basin}
```

The SDK should be preferred. A Durable Streams application may require a basin
to exist at startup, but basin creation is not part of per-stream protocol
handling.

### 5.2 Stream Names

Durable Streams paths map to S2 stream names:

```text
StreamPath -> StreamNameStr
```

The mapping must be deterministic. If paths may contain sensitive data, the
mapping should use a stable prefix plus a hash rather than the raw path.

This profile does not require opaque URL-addressed stream paths. S2 stream names
are the wire identity.

### 5.3 Stream Creation

Stream creation is native S2:

```text
PUT /streams/{stream}
```

The request body is S2 stream configuration. Retention should use S2
`retention_policy` where possible.

Durable Streams content type is not S2 stream configuration. It is represented
by one of:

1. `ds-content-type` on every data record; or
2. an initial `ds-kind: meta` control record.

The first implementation should prefer per-record `ds-content-type`, because it
requires no additional metadata read before interpreting a record batch.

## 6. Appends

### 6.1 Native S2 Append

Append uses S2 directly:

```text
POST /streams/{stream}/records
Content-Type: application/json
s2-format: raw | base64

{
  "records": [
    {
      "body": "...",
      "headers": [
        ["ds-kind", "data"],
        ["ds-content-type", "application/json"]
      ]
    }
  ],
  "match_seq_num": 42,
  "fencing_token": "writer-token"
}
```

Success returns S2 `AppendAck`:

```json
{
  "start": { "seq_num": 42, "timestamp": 1760000000000 },
  "end": { "seq_num": 43, "timestamp": 1760000000000 },
  "tail": { "seq_num": 43, "timestamp": 1760000000000 }
}
```

The service does not translate this into `204 No Content` or
`Stream-Next-Offset`. Clients use `ack.end.seq_num` or `ack.tail.seq_num` as the
next read coordinate.

### 6.2 Expected Tail

Expected-tail writes use S2 `match_seq_num`.

If the condition fails, S2 returns `412` with `AppendConditionFailed` containing
`seq_num_mismatch`. That is the canonical conflict response for stale expected
tail.

This replaces the original `Stream-Next-Offset`/`409 Conflict` expected-tail
shape.

### 6.3 Writer Fencing

This profile uses S2 `fencing_token` as the native writer-fencing primitive.

If the condition fails, S2 returns `412` with `AppendConditionFailed` containing
`fencing_token_mismatch`.

The fencing token is cooperative: S2 rejects appends that provide the wrong
token, but an append that omits `fencing_token` is still allowed. A writer sets
or clears the current token by appending an S2 native `fence` command record.
Therefore strict writer exclusion requires either all writers to participate in
the fencing convention or all writes to be mediated by a wrapper/service.

The original Durable Streams tuple:

```text
Producer-Id + Producer-Epoch + Producer-Seq
```

is not part of the core S2-aligned profile. If per-producer sequence replay is
still required, it must be specified as a higher-level application protocol over
S2 records, not as a separate transport-level write protocol.

### 6.4 Batching

S2 append batches are atomic. A batch contains 1 to 1000 records and is limited
to S2's metered byte limit.

Applications should model each logical message as one S2 record. A batch of N
messages should be sent as one S2 `AppendInput` containing N records.

This replaces the original server-side `application/json` flattening rule.
Clients that want to append a JSON array as multiple messages should construct
multiple S2 records explicitly.

### 6.5 Content Types

Content type is application metadata in this profile.

Rules:

- `ds-content-type` identifies the media type of a data record;
- implementations may require all data records in a stream to share the same
  `ds-content-type`;
- `application/json` receives no special server-side flattening;
- `application/ndjson`, `text/plain`, protobuf, and custom media types are all
  ordinary record payloads.

This is intentionally simpler than the original Durable Streams content-type
model.

## 7. Reads

### 7.1 Unary Read

Read uses S2 directly:

```text
GET /streams/{stream}/records?seq_num=42&count=100
Accept: application/json
s2-format: raw | base64
```

Success returns S2 `ReadBatch`:

```json
{
  "records": [
    {
      "seq_num": 42,
      "timestamp": 1760000000000,
      "body": "...",
      "headers": [["ds-content-type", "application/json"]]
    }
  ],
  "tail": { "seq_num": 43, "timestamp": 1760000000000 }
}
```

Clients resume by reading from the next `seq_num`.

### 7.2 Tail Check

Tail checks use S2 directly:

```text
GET /streams/{stream}/records/tail
```

Success returns S2 `TailResponse`:

```json
{
  "tail": { "seq_num": 43, "timestamp": 1760000000000 }
}
```

This replaces the original `HEAD` plus `Stream-Next-Offset` metadata endpoint.

### 7.3 Waiting And Live Reads

Live reads should use S2 read sessions through SDKs.

For HTTP clients, S2 read endpoints support waiting and SSE/S2S session behavior
as described by S2. This profile does not define a second SSE event shape.

S2 read SSE emits S2 read events such as `batch`, `error`, and `ping`, carrying
S2 `ReadBatch` payloads. The original Durable Streams `event: data` /
`event: control` SSE shape is not part of this profile.

## 8. Optional Finite Stream Extension

S2 streams do not natively close. If finite stream EOF is required, use a close
control record.

### 8.1 Close Record

A close record is an S2 record with:

```text
ds-kind: close
```

The body should be empty. Additional headers may include:

```text
ds-content-type: application/vnd.firegrid.ds.close
```

Closing with a final append is represented as one S2 append batch containing the
final data record(s) followed by the close record.

### 8.2 Read Semantics

Readers interpret `ds-kind: close` as EOF after all prior records in sequence
order have been delivered.

After observing a close record, clients should not tail for additional
application data. S2 itself may still accept later records unless writers agree
to enforce the close convention. Strict close enforcement requires either:

- a server-side wrapper that rejects writes after a close record; or
- an S2 fork/extension that makes close native.

Therefore close is an application/profile convention unless enforced by a
server.

## 9. Optional Metadata Stream

If stream-level metadata is needed, use a metadata stream rather than changing
S2 stream configuration.

Possible layout:

```text
<stream>                 # data records
__ds/meta/<stream-hash>  # metadata/control records
```

Use this only when per-record headers are not sufficient.

Metadata stream records may describe:

- canonical content type;
- close state;
- logical stream id;
- path tombstone;
- application schema.

The first implementation should avoid this unless strict close enforcement or
path tombstones are required.

## 10. Feature Parity Extensions

S2 covers the durable ordered log, append conditions, read sessions, and stream
lifecycle. Full Durable Streams parity is possible only by adding a profile
layer over S2. Some features can be client/profile conventions. Others require a
server-side control plane or an S2 fork if enforcement must be inside the
storage engine.

### 10.1 Parity Matrix

| Original Durable Streams feature | S2-native support | S2-aligned parity path |
| --- | --- | --- |
| Append/read durable ordered bytes | Yes, as records | Use S2 `AppendInput`, `AppendRecord`, `ReadBatch` |
| Expected-tail append | Yes | Use `match_seq_num` |
| Writer fencing | Partial | Use S2 `fencing_token`; producer tuple replay is an extension |
| Raw byte HTTP body wire | No | Compatibility gateway only |
| Content type per stream | Partial | Use per-record `ds-content-type` or metadata stream |
| `application/json` flattening | No | Client/profile helper or compatibility gateway |
| Close/EOF | No | Close control record; strict enforcement requires server wrapper/fork |
| Opaque DS offsets | No | Encode/decode S2 `seq_num` in profile/gateway if needed |
| DS SSE `data/control` | No | Use S2 SSE natively or gateway projection |
| Forking | No | Metadata/control-plane extension over S2 streams |
| Soft delete for fork retention | No | Metadata/control-plane extension |
| Subscriptions | No | Control-plane service backed by S2 metadata streams |
| Webhook delivery | No | Control-plane worker/service |
| Pull-wake delivery | Partial | Wake stream can be an S2 stream; claim/ack is control plane |
| Schedules | No | Control-plane scheduler backed by S2 metadata streams |
| `Stream-Seq` | No | Application/profile metadata with server-side enforcement if required |

### 10.2 Close / EOF

Close parity is achievable with a close control record:

```text
ds-kind: close
```

This gives readers an EOF marker in S2 order. It is parity for read-side EOF.
It is not strict write-side parity unless a server-side wrapper rejects appends
after the first close record.

Strict close requires one of:

- a gateway/server wrapper that reads close metadata before append and rejects
  writes after close;
- a metadata stream updated atomically enough for the application's guarantees;
- an S2 fork that makes close native in the streamer.

S2 native `trim` command records are not close records. Trimming can remove old
records or participate in S2 stream deletion behavior, but it does not provide a
Durable Streams EOF marker that remains readable to clients.

### 10.3 Content Type And JSON Mode

S2 stores bytes plus record headers; it does not own Durable Streams content
semantics.

Parity options:

1. Store `ds-content-type` on every data record.
2. Store canonical stream content type in a metadata stream.
3. Provide client helpers that turn `application/json` arrays into multiple S2
   records before append.
4. Use a compatibility gateway if existing clients need server-side
   `application/json` flattening and JSON-array reads.

The S2-aligned profile should prefer explicit records over server-side JSON
flattening. A JSON array is one payload unless the client/profile helper chooses
to expand it into multiple records.

### 10.4 Producer Tuple Replay

S2 `fencing_token` handles writer ownership but does not implement the original
Durable Streams tuple:

```text
Producer-Id + Producer-Epoch + Producer-Seq
```

Feature parity is possible with a producer-state control stream:

```text
__ds/producers/<stream-hash>
```

Each accepted append records:

- producer id;
- epoch;
- sequence;
- S2 appended range;
- result metadata needed to replay duplicate responses.

Append admission must check producer state before writing application records.
That means full producer tuple parity requires a server-side wrapper or S2 fork,
not just a passive client library. Without server-side admission, clients can
still use S2 `fencing_token` and application event ids, but that is not the same
semantics.

S2 `match_seq_num` plus retry can be enough for single-writer exactly-once
patterns when the writer knows the expected next sequence number. It does not
provide a general duplicate-response replay table keyed by
`(producerId, epoch, seq)`.

### 10.5 Stream-Seq

`Stream-Seq` can be modeled as application/profile metadata:

```text
ds-stream-seq: <opaque lexicographic string>
```

Strict parity requires server-side enforcement that the new value is strictly
greater than the last accepted value for the writer scope. That state can live in
a metadata stream, but append admission must consult it before accepting writes.

### 10.6 Forks

S2 has no native fork/stitching primitive. Parity is achievable as a control
plane over S2, with two possible implementations.

Copy-on-fork:

- create a new S2 stream for the fork;
- copy source records before the divergence point into the fork;
- append new fork records after the copied prefix.

Pointer-stitch:

- create a new S2 stream for fork-local records;
- store fork metadata:
  - source stream;
  - source divergence `seq_num`;
  - optional sub-position for profile-specific framing;
  - fork stream name;
- read fork as `source[..divergence) ++ fork-local`.

Copy-on-fork is simpler and uses S2 reads/writes directly, but has write
amplification and weakens retention/soft-delete testing. Pointer-stitch matches
the original Durable Streams semantics more closely but requires a server-side
read planner or client helper that understands fork metadata.

Suggested metadata layout:

```text
__ds/forks/<fork-stream-hash>
```

Fork metadata record:

```json
{
  "kind": "fork",
  "fork_stream": "streams/forked",
  "source_stream": "streams/source",
  "source_divergence_seq_num": 42,
  "created_at": "2026-06-12T00:00:00Z"
}
```

### 10.7 Soft Delete

S2 `DELETE /streams/{stream}` removes the stream asynchronously and does not
know about fork reference counts. Durable Streams soft-delete parity requires a
metadata/control-plane tombstone:

```text
__ds/tombstones/<stream-hash>
```

Tombstone metadata:

```json
{
  "stream": "streams/source",
  "deleted_at": "2026-06-12T00:00:00Z",
  "reason": "fork-retention",
  "dependent_forks": ["streams/forked"]
}
```

A server wrapper can return `410 Gone` for direct access while retaining S2 data
for fork reads. A pure S2 client profile cannot prevent another client from
reading or appending to the source unless access is mediated through the wrapper.

### 10.8 Subscriptions

S2 has read sessions but does not have Durable Streams durable subscriptions,
leases, webhook delivery, or ack cursors. Parity requires a control-plane
service.

State can be stored in S2 metadata streams:

```text
__ds/subscriptions/<id>       # normalized config and status
__ds/subscriptions-links/<id> # linked streams and acked seq_nums
__ds/wakes/<id>               # wake events / retry facts
```

Subscription stream link:

```json
{
  "stream": "events/orders",
  "link_type": "glob",
  "acked_seq_num": 42
}
```

Wake snapshot:

```json
{
  "subscription_id": "sub-1",
  "wake_id": "w_123",
  "generation": 7,
  "streams": [
    {
      "stream": "events/orders",
      "acked_seq_num": 42,
      "tail_seq_num": 45,
      "has_pending": true
    }
  ]
}
```

The control-plane service must:

- maintain subscription config hashes for idempotent re-confirmation;
- link explicit and pattern-matched streams;
- advance ack cursors only through valid callback/ack requests;
- generate wake ids and generations;
- enforce leases and fencing;
- retry webhook delivery;
- write pull-wake events to an S2 wake stream where configured.

S2 read sessions can power the watcher side, but the subscription semantics are
not native S2.

### 10.9 Filtered Subscriptions

Filtered subscriptions are possible when records have parseable JSON bodies or
use state-profile headers.

Evaluation context should use S2 names:

| Name | Description |
| --- | --- |
| `event` | Decoded JSON record body |
| `stream` | S2 stream name or application path |
| `seq_num` | S2 sequence number of the evaluated record |
| `headers` | S2 record headers |
| `self` | Immutable subscription filter context |

Non-matching records may advance an internal evaluated `seq_num` without
advancing the public ack cursor.

### 10.10 Scheduled Append

Schedules are not native S2. Parity requires a scheduler control plane backed by
S2 metadata streams:

```text
__ds/schedules/<id>
__ds/schedules-due/<time-bucket>
```

Schedule record:

```json
{
  "id": "t-1",
  "at": "2026-05-09T12:00:00.000Z",
  "stream": "sessions/abc",
  "records": [
    {
      "body": "{\"type\":\"timer.fired\",\"timer_id\":\"t-1\"}",
      "headers": [["ds-content-type", "application/json"]]
    }
  ],
  "match_seq_num": null,
  "fencing_token": null,
  "status": "pending"
}
```

Firing uses normal S2 append. If strict idempotency is required, use
`match_seq_num`, `fencing_token`, or a server-side idempotency record.

### 10.11 Child And Attachment Composition

The original child/attachment model maps well to S2 without new storage
primitives:

- child execution is a normal S2 stream;
- parent appends an invocation fact;
- parent or worker subscribes/tails the child stream for terminal facts;
- progress attachments are ordinary non-terminal records.

If durable wake-up is needed, use the subscription extension above.

## 11. Core Versus Parity Modes

Use these tiers to avoid mixing concerns:

### 11.1 S2-Native Core

This is just S2 plus small header conventions:

- S2 stream names;
- S2 records;
- S2 append/read/tail;
- `ds-content-type`;
- optional `ds-kind` control records.

This mode can be implemented as a small SDK/profile library.

### 11.2 S2-Aligned Durable Streams Profile

This adds conventions that are still mostly data/model-level:

- close control records;
- state-profile records;
- application idempotency keys;
- content-type helper functions;
- optional opaque offset wrappers over `seq_num`.

This can mostly stay client/profile-side, unless strict close enforcement is
required.

### 11.3 Full Durable Streams Compatibility

This adds semantics that require admission control, background work, or
multi-stream metadata:

- producer tuple replay;
- `Stream-Seq` enforcement;
- strict close rejection;
- forks and soft-delete;
- subscriptions;
- webhook retry;
- pull-wake claim/ack/release;
- schedules.

This requires a server-side wrapper/control plane over S2 or a fork of S2.

## 12. Features Not In The S2-Native Core

The following original Durable Streams features are not in the S2-native core:

- raw stream bytes as HTTP request/response bodies;
- `Stream-Next-Offset` response headers;
- opaque lexicographically sortable offset tokens;
- `Stream-Cursor`;
- `Stream-Up-To-Date`;
- `Stream-Closed` HTTP header semantics;
- server-side `application/json` flattening;
- `Producer-Id`, `Producer-Epoch`, `Producer-Seq`;
- `Stream-Seq`;
- Durable Streams-specific SSE `data`/`control` events.

Several of these can be reintroduced through the parity extensions above. They
should not be part of the storage data-plane profile by default.

## 13. Error Mapping

Use S2 errors directly.

Important data-plane mappings:

| S2 status | Meaning in this profile |
| --- | --- |
| `200` | append/read/tail success |
| `201` | stream/basin created |
| `202` | stream/basin delete accepted |
| `400` | malformed request or invalid parameters |
| `403` | authorization or fencing failure |
| `404` | missing basin or stream |
| `408` | request timeout |
| `409` | resource conflict |
| `412` | append condition failed (`match_seq_num` or `fencing_token`) |
| `416` | read range not satisfiable |

Do not translate these into the original Durable Streams status/header grammar
unless implementing an explicit compatibility gateway.

## 14. Client Model

Clients should use the official S2 SDK where possible:

```ts
const stream = s2.basin(basin).stream(streamName)

const ack = await stream.append(
  AppendInput.create([
    AppendRecord.string({
      body: JSON.stringify(value),
      headers: [["ds-content-type", "application/json"]]
    })
  ], {
    matchSeqNum
  })
)

const batch = await stream.read({
  start: { from: { seqNum: ack.start.seqNum } },
  stop: { limits: { count: 100 } }
})
```

High-throughput writers should use S2 `appendSession`, `Producer`, and
`BatchTransform` rather than a custom Durable Streams producer protocol.

S2 `Producer` is a batching/backpressure helper over append sessions. It is not
the same as the original Durable Streams idempotent producer tuple and does not
store duplicate-response replay state by itself.

Live readers should use S2 `readSession` or S2 SSE.

## 15. Implementation Guidance

The first implementation should be a thin S2 profile library, not a server fork:

```text
S2 SDK
  -> profile helpers for names, headers, content typing, close interpretation
  -> application code
```

Only build a server wrapper if a requirement cannot be met by client/profile
helpers, for example strict close enforcement or compatibility with existing
Durable Streams clients.

Only fork S2 if the required semantics must be enforced inside the storage
engine itself.

## 16. Compatibility Gateway

A compatibility gateway from original Durable Streams to this S2 profile is
possible but should be treated as a separate product:

```text
Original DS raw-byte HTTP wire
  -> gateway
    -> S2 AppendInput / ReadBatch / ReadSession
```

The gateway would be responsible for:

- raw body to `AppendRecord` conversion;
- `Stream-Next-Offset` to `seq_num` conversion;
- DS SSE `data/control` projection;
- close enforcement;
- JSON flattening;
- producer tuple replay;
- forks/subscriptions/schedules if still required.

This gateway should not contaminate the S2-aligned core profile.
