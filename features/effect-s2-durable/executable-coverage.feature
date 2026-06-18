Feature: effect-s2-durable executable coverage
  Executable proofs for the implemented durable runtime surface.

  @sql:service_trace
  Scenario: stateless services support client, sendClient, attach, run, and deferred primitives
    When I call Calculator.double with 21 through the durable client
    And I send Calculator.double with 7 through the durable send client
    And I attach the sent Calculator execution
    And I resolve Calculator.deferredEcho with "ok"
    Then Calculator observed direct 42, attached 14, and deferred "ok"

  @sql:object_trace
  Scenario: keyed objects support durable state, shared reads, objectClient, objectSendClient, and attach
    Given a durable Counter owner and Router owner
    When I add 2 through the Counter object
    And I call the Counter object through Router with 3
    Then the shared Counter read returns 5
    When I send the Counter object through Router with 4 and attach it
    Then the Counter object observed first 2, second 5, shared 5, and sent 9

  @sql:signal_trace
  Scenario: externally resolved signals resume a waiting durable service execution
    When I send a Waiter execution waiting on signal "ready"
    Then polling the Waiter execution is pending
    And I resolve signal "ready" with "ok" for the Waiter execution
    Then attaching the Waiter execution returns "resolved:ok"

  @sql:object_recovery_trace
  Scenario: object boot recovery resumes a parked pending head after a fresh runtime boundary
    Given a durable Gate owner
    When I send Gate.wait for signal "open" and value "object"
    Then polling the Gate execution is pending
    When I resolve signal "open" with "ok" for the Gate execution after a fresh runtime boundary
    Then attaching the Gate execution returns "object:ok"

  @sql:workflow_trace
  Scenario: workflows are run-once objects with shared promise resolution
    When I derive the approval workflow run id
    And I submit the approval workflow with 21
    And I submit the approval workflow again with 99
    And I approve the workflow through its shared handler
    Then the workflow starts are "started" and "alreadyStarted"
    And attaching the approval workflow returns 42
