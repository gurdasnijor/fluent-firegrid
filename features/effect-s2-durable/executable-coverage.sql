-- name: service_trace
WITH ordered AS (
  SELECT
    toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts,
    SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 3
  AND countIf(SpanName = 'effect-s2-stream-db.table.upsert') >= 3
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'S2.createStream',
    SpanName = 'S2.append',
    SpanName = 'effect-s2-stream-db.commit'
  ) AS ok
FROM ordered

-- name: object_trace
WITH ordered AS (
  SELECT
    toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts,
    SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') >= 4
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 4
  AND countIf(SpanName = 'effect-s2-durable.object.shared') = 1
  AND countIf(SpanName = 'effect-s2-durable.log.casAppend') >= 4
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.object.drain',
    SpanName = 'effect-s2-durable.object.shared'
  ) AS ok
FROM ordered

-- name: object_recovery_trace
WITH
  ordered AS (
    SELECT
      toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts,
      SpanName
    FROM scenario_spans
  ),
  signal_resolutions AS (
    SELECT count() AS n
    FROM scenario_spans
    WHERE SpanName = 'effect-s2-durable.resolveSignal'
  )
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 2
  AND countIf(SpanName = 'effect-s2-durable.object.ownerKeys') >= 4
  AND countIf(SpanName = 'effect-s2-durable.object.boot-recover') >= 2
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.resolveSignal'
  )
  AND (SELECT n FROM signal_resolutions) = 1 AS ok
FROM ordered

-- name: signal_trace
WITH ordered AS (
  SELECT
    toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts,
    SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 2
  AND countIf(SpanName = 'effect-s2-stream-db.table.upsert') >= 2
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'S2.createStream',
    SpanName = 'S2.append',
    SpanName = 'effect-s2-stream-db.commit'
  ) AS ok
FROM ordered

-- name: workflow_trace
WITH
  ordered AS (
    SELECT
      toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts,
      SpanName
    FROM scenario_spans
  ),
  shared_resolutions AS (
    SELECT count() AS n
    FROM scenario_spans child
    INNER JOIN scenario_spans parent
      ON child.ParentSpanId = parent.SpanId
    WHERE child.SpanName = 'effect-s2-durable.resolveSignal'
      AND parent.SpanName = 'effect-s2-durable.object.shared'
  )
SELECT
  countIf(SpanName = 'effect-s2-durable.object.admit') = 2
  AND countIf(SpanName = 'effect-s2-durable.object.shared') = 1
  AND countIf(SpanName = 'effect-s2-durable.resolveSignal') = 1
  AND countIf(SpanName = 'effect-s2-durable.object.drain') >= 2
  AND sequenceMatch('(?1).*(?2)')(
    ts,
    SpanName = 'effect-s2-durable.object.admit',
    SpanName = 'effect-s2-durable.object.shared'
  )
  AND (SELECT n FROM shared_resolutions) = 1 AS ok
FROM ordered
