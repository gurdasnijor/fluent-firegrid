@product:effect-s2-durable @feature:workflow-execution @spec-only
Feature: Workflow Execution
  Durable workflow execution semantics. A workflow is a specialization of
  stateful execution: the workflow id is the owner key, the reserved run
  handler is admitted at most once, and shared handlers provide query/signal
  behavior over the workflow owner stream.

  @component:WORKFLOW
  Rule: Workflow as a stateful-execution specialization

    @requirement:WORKFLOW.1
    Scenario: A workflow run handler is an exclusive handler admitted at most once per workflow id; a...
      Then the workflow-execution contract includes:
        """
        A workflow run handler is an exclusive handler admitted at most once per
        workflow id; a second run admission returns alreadyStarted, not a
        dedup-returned second run.
        """

    @requirement:WORKFLOW.2
    Scenario: Workflow signal/query handlers are ordinary shared handlers (concurrent, read-only,...
      Then the workflow-execution contract includes:
        """
        Workflow signal/query handlers are ordinary shared handlers (concurrent,
        read-only, ingress via append).
        """

    @requirement:WORKFLOW.3
    Scenario: Promises a run body awaits use the existing park-and-resume primitives, resolved by...
      Then the workflow-execution contract includes:
        """
        Promises a run body awaits use the existing park-and-resume primitives,
        resolved by shared handlers.
        """

    @requirement:WORKFLOW.4
    Scenario: workflowRunId(def, id) deterministically derives the run call id for a workflow id and...
      Then the workflow-execution contract includes:
        """
        workflowRunId(def, id) deterministically derives the run call id for a
        workflow id and decodes to the workflow owner plus reserved run method.
        """

    @requirement:WORKFLOW.5
    Scenario: workflowSubmit(def, id, input) starts the workflow at most once and reports started or...
      Then the workflow-execution contract includes:
        """
        workflowSubmit(def, id, input) starts the workflow at most once and reports
        started or alreadyStarted across pending, recovered, and completed states.
        """

    @requirement:WORKFLOW.6
    Scenario: workflowAttach(def, id) attaches to the workflow run and decodes its result through the...
      Then the workflow-execution contract includes:
        """
        workflowAttach(def, id) attaches to the workflow run and decodes its result
        through the run output schema.
        """

  @constraint:BOUNDARIES
  Rule: BOUNDARIES

    @requirement:BOUNDARIES.1
    Scenario: Workflow execution is not a separate persistence boundary; it reuses stateful-execution...
      Then the workflow-execution contract includes:
        """
        Workflow execution is not a separate persistence boundary; it reuses
        stateful-execution owner streams, admission, shared handlers, ingress,
        recovery, and attach semantics.
        """

    @requirement:BOUNDARIES.2
    Scenario: The method name run is reserved for the workflow entrypoint and is rejected for shared...
      Then the workflow-execution contract includes:
        """
        The method name run is reserved for the workflow entrypoint and is rejected
        for shared handlers at both the type boundary and definition-time guard.
        """

    @requirement:BOUNDARIES.3
    Scenario: A long-running workflow with process restart and signal ingress must recover and resume...
      Then the workflow-execution contract includes:
        """
        A long-running workflow with process restart and signal ingress must recover
        and resume from its owner stream like any stateful execution.
        """
