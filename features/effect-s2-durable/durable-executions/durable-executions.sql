-- name: service_call
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.createStream') >= 1
  AND countIf(SpanName = 'S2.append') >= 1
  AND countIf(SpanName = 'effect-s2-stream-db.commit') >= 1
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'S2.createStream',
    SpanName = 'S2.append',
    SpanName = 'effect-s2-stream-db.commit'
  ) AS ok
FROM ordered

-- name: service_send_attach
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 1
  AND countIf(SpanName = 'S2.readBatch') >= 1
  AND countIf(SpanName = 'effect-s2-durable.callId.decode') >= 1
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'S2.append',
    SpanName = 'S2.readBatch'
  ) AS ok
FROM ordered

-- name: service_deferred
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 1
  AND countIf(SpanName = 'effect-s2-stream-db.table.upsert') >= 1
  AND countIf(SpanName = 'effect-s2-stream-db.commit') >= 1
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'S2.createStream',
    SpanName = 'S2.append',
    SpanName = 'effect-s2-stream-db.commit'
  ) AS ok
FROM ordered

-- name: service_pending_poll
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 1
  AND countIf(SpanName = 'S2.readBatch') >= 1
  AND countIf(SpanName = 'effect-s2-stream-db.table.get') >= 1
  AND countIf(SpanName = 'S2.deleteStream') = 0
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'S2.append',
    SpanName = 'S2.readBatch'
  ) AS ok
FROM ordered

-- name: service_signal_resume
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 2
  AND countIf(SpanName = 'effect-s2-stream-db.table.upsert') >= 2
  AND countIf(SpanName = 'effect-s2-durable.recover-execution') >= 1
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'S2.createStream',
    SpanName = 'S2.append',
    SpanName = 'effect-s2-stream-db.commit'
  ) AS ok
FROM ordered

-- name: object_state_mutation
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 1
  AND countIf(SpanName = 'effect-s2-durable.log.casAppend') >= 1
  AND countIf(SpanName = 'S2.append') >= 1
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.object.drain'
  ) AS ok
FROM ordered

-- name: object_child_call
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 2
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 2
  AND countIf(SpanName = 'effect-s2-durable.callId.encode') >= 1
  AND countIf(SpanName = 'effect-s2-durable.log.casAppend') >= 2
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.callId.encode'
  ) AS ok
FROM ordered

-- name: object_snapshot_read
SELECT
  countIf(SpanName = 'effect-s2-durable.object.shared') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.snapshot') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.admit') = 0 AS ok
FROM scenario_spans

-- name: object_send_attach
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 2
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 2
  AND countIf(SpanName = 'effect-s2-durable.callId.encode') >= 1
  AND countIf(SpanName = 'effect-s2-durable.log.casAppend') >= 2
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.callId.encode'
  ) AS ok
FROM ordered

-- name: object_recovery
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 1
  AND countIf(SpanName = 'effect-s2-durable.resolveSignal') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.ownerKeys') >= 1
  AND countIf(SpanName = 'effect-s2-durable.object.boot-recover') >= 2
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.resolveSignal'
  ) AS ok
FROM ordered

-- name: workflow_run_once
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 2
  AND countIf(SpanName = 'effect-s2-durable.callId.encode') >= 1
  AND countIf(SpanName = 'effect-s2-durable.log.casAppend') = 1 AS ok
FROM scenario_spans

-- name: workflow_promise_resolution
WITH shared_resolutions AS (
  SELECT count() AS n
  FROM scenario_spans child
  INNER JOIN scenario_spans parent
    ON child.ParentSpanId = parent.SpanId
  WHERE child.SpanName = 'effect-s2-durable.resolveSignal'
    AND parent.SpanName = 'effect-s2-durable.object.shared'
)
SELECT
  countIf(SpanName = 'effect-s2-durable.object.shared') = 1
  AND countIf(SpanName = 'effect-s2-durable.resolveSignal') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 1
  AND (SELECT n FROM shared_resolutions) = 1 AS ok
FROM scenario_spans
