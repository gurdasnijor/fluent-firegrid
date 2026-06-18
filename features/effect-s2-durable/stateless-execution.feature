@product:effect-s2-durable @feature:stateless-execution @spec-only
Feature: Stateless Execution
  Stateless durable service executions: public service/client/sendClient APIs,
  one execution stream per call, durable primitives, attach/poll, idempotency,
  ingress, and boot recovery. Stateful object semantics are specified in
  stateful-execution and workflow semantics are specified in
  workflow-execution. This feature covers the stateless service path that
  continues to use WorkflowDb/RosterDb until a dedicated stateless-execution
  rewrite.

  @component:PUBLIC_API
  Rule: Service definition and clients

    @requirement:PUBLIC_API.1
    Scenario: service( name, handlers, schemas ) defines a stateless durable service; each method...
      Then the stateless-execution contract includes:
        """
        service({ name, handlers, schemas }) defines a stateless durable service;
        each method input is decoded through its schema before handler execution and
        each result is encoded through its output schema before completion.
        """

    @requirement:PUBLIC_API.2
    Scenario: client(service).method(input) submits the service execution, waits for completion...
      Then the stateless-execution contract includes:
        """
        client(service).method(input) submits the service execution, waits for
        completion through attach, decodes the result, and returns the decoded
        value.
        """

    @requirement:PUBLIC_API.3
    Scenario: sendClient(service).method(input) submits the service execution and returns its...
      Then the stateless-execution contract includes:
        """
        sendClient(service).method(input) submits the service execution and returns
        its execution id without waiting for completion.
        """

    @requirement:PUBLIC_API.4
    Scenario: attach(id, schema) waits for a pending service execution and decodes the completed...
      Then the stateless-execution contract includes:
        """
        attach(id, schema) waits for a pending service execution and decodes the
        completed result through the provided schema; poll(id, schema) is
        non-blocking and returns None while pending.
        """

    @requirement:PUBLIC_API.5
    Scenario: serviceLayer(...services) registers handlers for boot recovery; executions whose handler...
      Then the stateless-execution contract includes:
        """
        serviceLayer(...services) registers handlers for boot recovery; executions
        whose handler name is not registered are skipped rather than crashing
        recovery.
        """

  @component:ADMISSION
  Rule: Service admission and idempotency

    @requirement:ADMISSION.1
    Scenario: A service call uses the provided idempotencyKey as its execution id, or mints a fresh id...
      Then the stateless-execution contract includes:
        """
        A service call uses the provided idempotencyKey as its execution id, or
        mints a fresh id when no idempotencyKey is supplied.
        """

    @requirement:ADMISSION.2
    Scenario: A completed service execution id is idempotent: re-submitting the same id returns the...
      Then the stateless-execution contract includes:
        """
        A completed service execution id is idempotent: re-submitting the same id
        returns the existing result and does not re-run the handler.
        """

    @requirement:ADMISSION.3
    Scenario: A pinned id with divergent input does not alias durable primitive facts: the existing...
      Then the stateless-execution contract includes:
        """
        A pinned id with divergent input does not alias durable primitive facts: the
        existing execution/result wins, and the new input does not mutate the
        recorded execution.
        """

    @requirement:ADMISSION.4
    Scenario: Service execution ids are disjoint from the reserved object-call-id namespace; a service...
      Then the stateless-execution contract includes:
        """
        Service execution ids are disjoint from the reserved object-call-id
        namespace; a service idempotencyKey using the object prefix is rejected.
        """

  @component:RUN
  Rule: Durable run step

    @requirement:RUN.1
    Scenario: run(action, options) records a terminal success or typed failure fact; replay returns...
      Then the stateless-execution contract includes:
        """
        run(action, options) records a terminal success or typed failure fact;
        replay returns the recorded terminal fact without re-executing the action.
        """

    @requirement:RUN.2
    Scenario: run supports named and positional step identity; positional identity is valid only for...
      Then the stateless-execution contract includes:
        """
        run supports named and positional step identity; positional identity is
        valid only for deterministic control flow, while named identity is stable
        across handler refactors.
        """

    @requirement:RUN.3
    Scenario: run retry policy controls attempts before a terminal fact is recorded; once a terminal...
      Then the stateless-execution contract includes:
        """
        run retry policy controls attempts before a terminal fact is recorded; once
        a terminal fact exists, retries are not reattempted on replay.
        """

    @requirement:RUN.4
    Scenario: A run action cannot require DurableExecutionRuntime or call durable primitives; this is...
      Then the stateless-execution contract includes:
        """
        A run action cannot require DurableExecutionRuntime or call durable
        primitives; this is rejected at the type level.
        """

    @requirement:RUN.5
    Scenario: A typed run failure is encoded through the declared error schema and propagates to the...
      Then the stateless-execution contract includes:
        """
        A typed run failure is encoded through the declared error schema and
        propagates to the caller through attach/client.
        """

  @component:STATE
  Rule: Durable service state

    @requirement:STATE.1
    Scenario: state(Table).get/set/delete operate on durable records scoped to the service execution...
      Then the stateless-execution contract includes:
        """
        state(Table).get/set/delete operate on durable records scoped to the service
        execution stream and table schema.
        """

    @requirement:STATE.2
    Scenario: state.get is replay-stable for read-modify-write flows: the read value is journaled so...
      Then the stateless-execution contract includes:
        """
        state.get is replay-stable for read-modify-write flows: the read value is
        journaled so recovery replays the same observed value instead of reading a
        later mutated value.
        """

    @requirement:STATE.3
    Scenario: state.set and state.delete are durable only after their underlying append is...
      Then the stateless-execution contract includes:
        """
        state.set and state.delete are durable only after their underlying append is
        acknowledged; deterministic state writes self-heal by replaying from the
        handler body.
        """

    @requirement:STATE.4
    Scenario: State mutations inside a run action are rejected by the run-action type guard.
      Then the stateless-execution contract includes:
        """
        State mutations inside a run action are rejected by the run-action type
        guard.
        """

  @component:TIMERS
  Rule: Durable sleep

    @requirement:TIMERS.1
    Scenario: sleep(name, duration) records a durable timer intent and delays completion until the...
      Then the stateless-execution contract includes:
        """
        sleep(name, duration) records a durable timer intent and delays completion
        until the duration has elapsed.
        """

    @requirement:TIMERS.2
    Scenario: On replay, a fired sleep fact short-circuits; a pending sleep recomputes the remaining...
      Then the stateless-execution contract includes:
        """
        On replay, a fired sleep fact short-circuits; a pending sleep recomputes the
        remaining delay from the durable deadline.
        """

    @requirement:TIMERS.3
    Scenario: Boot recovery re-arms sleep by re-running the handler from the top and replaying the...
      Then the stateless-execution contract includes:
        """
        Boot recovery re-arms sleep by re-running the handler from the top and
        replaying the durable timer facts.
        """

  @component:PROMISES
  Rule: Durable signals, deferreds, and awakeables

    @requirement:PROMISES.1
    Scenario: signal(name, schema) parks the handler until resolveSignal(id, name, schema, value)...
      Then the stateless-execution contract includes:
        """
        signal(name, schema) parks the handler until resolveSignal(id, name, schema,
        value) writes a durable resolution fact.
        """

    @requirement:PROMISES.2
    Scenario: deferred(name, schema) creates an invocation-scoped promise; resolve writes the durable...
      Then the stateless-execution contract includes:
        """
        deferred(name, schema) creates an invocation-scoped promise; resolve writes
        the durable value and get reads or parks until that value exists.
        """

    @requirement:PROMISES.3
    Scenario: awakeable(schema) returns a replay-stable id and a promise; resolveAwakeable(id,...
      Then the stateless-execution contract includes:
        """
        awakeable(schema) returns a replay-stable id and a promise;
        resolveAwakeable(id, awakeableId, schema, value) resolves it through
        ingress.
        """

    @requirement:PROMISES.4
    Scenario: Resolve-before-await is not lost: a durable resolution fact written before the handler...
      Then the stateless-execution contract includes:
        """
        Resolve-before-await is not lost: a durable resolution fact written before
        the handler parks is picked up when the handler reaches the await.
        """

    @requirement:PROMISES.5
    Scenario: Durable promise rows are the source of truth; in-process waiters are best-effort wakeups...
      Then the stateless-execution contract includes:
        """
        Durable promise rows are the source of truth; in-process waiters are
        best-effort wakeups only.
        """

  @component:RECOVERY
  Rule: Service boot recovery

    @requirement:RECOVERY.1
    Scenario: A fresh engine over the same S2 backend re-drives a non-resident parked service...
      Then the stateless-execution contract includes:
        """
        A fresh engine over the same S2 backend re-drives a non-resident parked
        service execution from the roster plus WorkflowDb, so it becomes resident
        and attachable as if freshly submitted.
        """

    @requirement:RECOVERY.2
    Scenario: A recovered service execution replays run/state/sleep facts without re-executing...
      Then the stateless-execution contract includes:
        """
        A recovered service execution replays run/state/sleep facts without
        re-executing completed side effects or double-applying journaled reads.
        """

    @requirement:RECOVERY.3
    Scenario: An ingress resolution on a recovered service execution settles it; signal or awakeable...
      Then the stateless-execution contract includes:
        """
        An ingress resolution on a recovered service execution settles it; signal or
        awakeable resolution by replay-stable id works after recovery.
        """

    @requirement:RECOVERY.4
    Scenario: Completed service executions are not boot-recovered as pending work.
      Then the stateless-execution contract includes:
        """
        Completed service executions are not boot-recovered as pending work.
        """

  @component:LIFECYCLE
  Rule: Service stream lifecycle

    @requirement:LIFECYCLE.1
    Scenario: Service executions use the ephemeral one-stream-per-call model; service streams may be...
      Then the stateless-execution contract includes:
        """
        Service executions use the ephemeral one-stream-per-call model; service
        streams may be dropped after completion once result/attach semantics no
        longer require them.
        """

    @requirement:LIFECYCLE.2
    Scenario: Result acknowledgement or retention policy is explicit; a service result is not...
      Then the stateless-execution contract includes:
        """
        Result acknowledgement or retention policy is explicit; a service result is
        not reclaimed while attach/poll are still expected to serve it.
        """

  @constraint:SERVICE_BACKEND
  Rule: Service backend stability

    @requirement:SERVICE_BACKEND.1
    Scenario: Services may keep the existing WorkflowDb/RosterDb implementation as long as it...
      Then the stateless-execution contract includes:
        """
        Services may keep the existing WorkflowDb/RosterDb implementation as long as
        it satisfies the public-api primitive contract.
        """

    @requirement:SERVICE_BACKEND.2
    Scenario: The object owner-stream cutover does not require changing the service backend; a...
      Then the stateless-execution contract includes:
        """
        The object owner-stream cutover does not require changing the service
        backend; a separate stateless-execution pass owns any later rewrite.
        """

  @constraint:BOUNDARIES
  Rule: BOUNDARIES

    @requirement:BOUNDARIES.1
    Scenario: Stateless execution behavior is driven through the public...
      Then the stateless-execution contract includes:
        """
        Stateless execution behavior is driven through the public
        service/client/sendClient/attach/poll/resolveSignal/resolveAwakeable APIs,
        not by calling WorkflowDb or RosterDb directly from validation code.
        """

    @requirement:BOUNDARIES.2
    Scenario: S2-backed service behavior is proven by executable Cucumber specs; package-local tests...
      Then the stateless-execution contract includes:
        """
        S2-backed service behavior is proven by executable Cucumber specs;
        package-local tests remain pure/type-level.
        """
