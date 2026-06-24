const escapeString = (value: string): string => value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

export const bindTrialSql = (sql: string, trialId: string): string =>
  sql.replaceAll("{trial_id:String}", `'${escapeString(trialId)}'`)

export const trialSpansSql = `
(
  SELECT *
  FROM otel_traces
  WHERE TraceId IN (
    SELECT TraceId
    FROM otel_traces
    WHERE SpanAttributes['firegrid.trial.id'] = {trial_id:String}
  )
)
`

export const verificationOperationsSql = `
(
  SELECT
    SpanAttributes['firegrid.trial.id'] AS trial_id,
    toUInt32(SpanAttributes['firegrid.client.id']) AS client_id,
    toUInt64(SpanAttributes['firegrid.operation.id']) AS operation_id,
    SpanAttributes['firegrid.operation.name'] AS operation,
    SpanAttributes['firegrid.operation.key'] AS operation_key,
    Timestamp AS call_time,
    Timestamp + toIntervalNanosecond(Duration) AS return_time,
    SpanAttributes['firegrid.operation.input.json'] AS input_json,
    SpanAttributes['firegrid.operation.output.json'] AS output_json,
    SpanAttributes['firegrid.operation.status'] AS status,
    SpanAttributes['firegrid.operation.failure.kind'] AS failure_kind
  FROM trial_spans
  WHERE SpanName = 'verification.operation'
)
`

export const expandTraceMacros = (sql: string): string =>
  sql
    .replace(/\bverification_operations\b/g, verificationOperationsSql)
    .replace(/\btrial_spans\b/g, trialSpansSql)
