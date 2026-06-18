Feature: Durable executions remain observable across asynchronous boundaries
  Callers can submit service, object, and workflow work, observe pending
  executions, resume parked work with durable signals, and retrieve results
  from the production S2-backed execution path.

  @sql:service_call
  Scenario: A service execution completes with its durable step result
    Given a service execution will double 21
    When the service execution starts
    Then the service execution result is 42

  @sql:service_send_attach
  Scenario: A submitted service execution keeps a stable id for later attachment
    Given a service execution was submitted without waiting for input 7
    When the caller attaches to the submitted service execution
    Then the submitted service execution result is 14

  @sql:service_deferred
  Scenario: An invocation-local promise resumes the same service execution
    Given a service execution has local promise payload "ok"
    When the service execution resolves its local promise
    Then the local promise result is "ok"

  @sql:service_pending_poll
  Scenario: A parked service execution is observable as pending
    Given a service execution is waiting for signal "ready"
    When the caller checks the execution status
    Then the execution is still pending

  @sql:service_signal_resume
  Scenario: A durable signal completes a parked service execution
    Given a service execution is waiting for signal "ready"
    When signal "ready" is resolved with "ok"
    Then the service execution result is "resolved:ok"

  @sql:object_state_mutation
  Scenario: A keyed owner persists an exclusive state change
    Given a counter owner
    And the increment is 2
    When the owner applies the increment
    Then the owner value is 2

  @sql:object_child_call
  Scenario: An exclusive owner can route work to another keyed owner
    Given a counter owner
    And the routed increment is 3
    When another owner applies the routed increment
    Then the routed update result is 3

  @sql:object_snapshot_read
  Scenario: A snapshot read observes owner state without admitting exclusive work
    Given a counter owner
    When the caller reads the owner snapshot
    Then the owner snapshot value is 0

  @sql:object_send_attach
  Scenario: Submitted owner work keeps a stable id for later attachment
    Given a counter owner
    And the routed increment is 4
    When another owner submits the routed increment without waiting
    Then the submitted object result is 4

  @sql:object_recovery
  Scenario: A parked owner execution resumes after runtime re-entry
    Given a gate owner is waiting for signal "open" with value "object"
    When signal "open" is resolved with "ok" after runtime re-entry
    Then the gate result is "object:ok"

  @sql:workflow_run_once
  Scenario: Workflow admission is idempotent per workflow id
    Given the approval workflow input is 21
    When the workflow is started twice
    Then the workflow starts are "started" and "alreadyStarted"

  @sql:workflow_promise_resolution
  Scenario: A workflow run resumes when approval is recorded on its owner stream
    Given the approval workflow is waiting for approval of 21
    When the run is approved
    Then the workflow result is 42
