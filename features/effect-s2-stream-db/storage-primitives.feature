Feature: Storage primitives

  @sql:checkpoint_trace_shape
  Scenario: checkpoint snapshots the live set and reopens from the compacted stream
    Given an open storage db with infinite retention at key "cart"
    When I insert item "a" value 1
    And I upsert item "a" value 2
    And I insert item "b" value 3
    And I delete item "a"
    And I checkpoint
    Then reopening, item "a" is absent
    And reopening, item "b" is 3
