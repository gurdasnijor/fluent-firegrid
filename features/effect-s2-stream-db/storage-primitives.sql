-- name: checkpoint_trace_shape
SELECT
  countIf(SpanName = 'effect-s2-stream-db.checkpoint') > 0
  AND countIf(SpanName = 'S2.append') > 0
  AND countIf(SpanName = 'S2.readBatch') > 0 AS ok
FROM scenario_spans
