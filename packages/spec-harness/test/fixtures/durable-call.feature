Feature: durable object call from a step
  Scenario: a step drives a durable object and reads its result
    Given a fresh durable counter
    When the step adds 7 via the durable counter
    Then the durable counter total is 7
