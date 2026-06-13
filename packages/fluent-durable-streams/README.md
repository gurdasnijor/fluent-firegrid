# fluent-durable-streams

S2-native Durable Streams HTTP API.

This package uses the official `@s2-dev/streamstore` SDK directly. It does not
wrap S2 into a DurableStreamLog CRUD facade and does not define a client/server
transport abstraction. The public boundary is an Effect `HttpApi` definition
implemented by handlers over S2 basin and stream handles.

## Data Plane

- `PUT /streams/:stream` ensures an S2 stream exists.
- `POST /streams/:stream/records` appends base64-encoded records with optional
  `matchSeqNum` and `fencingToken` conditions.
- `POST /streams/:stream/records/raw` appends one raw byte record.
- `POST /streams/:stream/close` appends a `ds-kind: close` EOF record.
- `GET /streams/:stream/records` reads a bounded or long-poll S2 batch.
- `GET /streams/:stream/records/raw` streams record bodies as bytes.
- `GET /streams/:stream/records/live` opens an S2 read session and emits SSE
  `batch` events until the client disconnects or a close record is observed.
- `GET /streams/:stream/records/tail` returns S2 tail position as `{ tail }`.
- `POST /state/:stream/records` appends typed state messages as S2 records.
- `GET /state/:stream/records` reads typed state messages from S2 records.
- `GET /state/:stream/records/live` streams typed state messages as SSE
  `batch` events.

S2 append-condition failures are surfaced as HTTP 412 API errors. Other S2
status codes are preserved where this profile handles them explicitly.
