Feature: durable step state
  Scenario: durable state persists across steps
    Given the counter starts at 5
    When I add 3
    Then the counter is 8
