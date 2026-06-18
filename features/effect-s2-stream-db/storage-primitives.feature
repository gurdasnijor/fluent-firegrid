@product:effect-s2-stream-db @feature:storage-primitives
Feature: Storage primitives

  Scenario: checkpoint snapshots the live set and reopens from the compacted stream
    Given an open stream-db:retained at key "cart"
    When I insert item "a" value 1
    And I upsert item "a" value 2
    And I insert item "b" value 3
    And I delete item "a"
    And I checkpoint
    Then reopening, item "a" is absent
    And reopening, item "b" is 3
    And the trace should satisfy:
      """
      SELECT
        countIf(SpanName = 'effect-s2-stream-db.checkpoint') > 0
        AND countIf(SpanName = 'S2.append') > 0
        AND countIf(SpanName = 'S2.readBatch') > 0 AS ok
      FROM otel_traces
      WHERE TraceId IN (
        SELECT TraceId
        FROM otel_traces
        WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
      )
      """
