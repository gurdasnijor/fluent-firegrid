@unverified
Feature: Workflow Execution
  Durable workflow execution semantics. A workflow is a specialization of
  stateful execution: the workflow id is the owner key, the reserved run
  handler is admitted at most once, and shared handlers provide query/signal
  behavior over the workflow owner stream.

  Rule: Workflow as a stateful-execution specialization

    Scenario: A workflow run handler is an exclusive handler admitted at most once per workflow id; a second run admission returns alreadyStarted, not a dedup-returned second run
      Then A workflow run handler is an exclusive handler admitted at most once per workflow id; a second run admission returns alreadyStarted, not a dedup-returned second run.

    Scenario: Workflow signal/query handlers are ordinary shared handlers (concurrent, read-only, ingress via append)
      Then Workflow signal/query handlers are ordinary shared handlers (concurrent, read-only, ingress via append).

    Scenario: Promises a run body awaits use the existing park-and-resume primitives, resolved by shared handlers
      Then Promises a run body awaits use the existing park-and-resume primitives, resolved by shared handlers.

    Scenario: workflowRunId(def, id) deterministically derives the run call id for a workflow id and decodes to the workflow owner plus reserved run method
      Then workflowRunId(def, id) deterministically derives the run call id for a workflow id and decodes to the workflow owner plus reserved run method.

    Scenario: workflowSubmit(def, id, input) starts the workflow at most once and reports started or alreadyStarted across pending, recovered, and completed states
      Then workflowSubmit(def, id, input) starts the workflow at most once and reports started or alreadyStarted across pending, recovered, and completed states.

    Scenario: workflowAttach(def, id) attaches to the workflow run and decodes its result through the run output schema
      Then workflowAttach(def, id) attaches to the workflow run and decodes its result through the run output schema.

  Rule: BOUNDARIES

    Scenario: Workflow execution is not a separate persistence boundary; it reuses stateful-execution owner streams, admission, shared handlers, ingress, recovery, and attach semantics
      Then Workflow execution is not a separate persistence boundary; it reuses stateful-execution owner streams, admission, shared handlers, ingress, recovery, and attach semantics.

    Scenario: The method name run is reserved for the workflow entrypoint and is rejected for shared handlers at both the type boundary and definition-time guard
      Then The method name run is reserved for the workflow entrypoint and is rejected for shared handlers at both the type boundary and definition-time guard.

    Scenario: A long-running workflow with process restart and signal ingress must recover and resume from its owner stream like any stateful execution
      Then A long-running workflow with process restart and signal ingress must recover and resume from its owner stream like any stateful execution.
