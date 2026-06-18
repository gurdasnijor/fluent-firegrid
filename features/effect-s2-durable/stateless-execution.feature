Feature: Stateless Execution
  Stateless durable service executions: public service/client/sendClient APIs,
  one execution stream per call, durable primitives, attach/poll, idempotency,
  ingress, and boot recovery. Stateful object semantics are specified in
  stateful-execution and workflow semantics are specified in
  workflow-execution. This feature covers the stateless service path that
  continues to use WorkflowDb/RosterDb until a dedicated stateless-execution
  rewrite.

  Rule: Service definition and clients

    Scenario: service({ name, handlers, schemas }) defines a stateless durable service; each method input is decoded through its schema before handler execution and each result is encoded through its output schema before completion
      Then service({ name, handlers, schemas }) defines a stateless durable service; each method input is decoded through its schema before handler execution and each result is encoded through its output schema before completion.

    Scenario: client(service).method(input) submits the service execution, waits for completion through attach, decodes the result, and returns the decoded value
      Then client(service).method(input) submits the service execution, waits for completion through attach, decodes the result, and returns the decoded value.

    Scenario: sendClient(service).method(input) submits the service execution and returns its execution id without waiting for completion
      Then sendClient(service).method(input) submits the service execution and returns its execution id without waiting for completion.

    Scenario: attach(id, schema) waits for a pending service execution and decodes the completed result through the provided schema; poll(id, schema) is non-blocking and returns None while pending
      Then attach(id, schema) waits for a pending service execution and decodes the completed result through the provided schema; poll(id, schema) is non-blocking and returns None while pending.

    Scenario: serviceLayer(...services) registers handlers for boot recovery; executions whose handler name is not registered are skipped rather than crashing recovery
      Then serviceLayer(...services) registers handlers for boot recovery; executions whose handler name is not registered are skipped rather than crashing recovery.

  Rule: Service admission and idempotency

    Scenario: A service call uses the provided idempotencyKey as its execution id, or mints a fresh id when no idempotencyKey is supplied
      Then A service call uses the provided idempotencyKey as its execution id, or mints a fresh id when no idempotencyKey is supplied.

    Scenario: A completed service execution id is idempotent: re-submitting the same id returns the existing result and does not re-run the handler
      Then A completed service execution id is idempotent: re-submitting the same id returns the existing result and does not re-run the handler.

    Scenario: A pinned id with divergent input does not alias durable primitive facts: the existing execution/result wins, and the new input does not mutate the recorded execution
      Then A pinned id with divergent input does not alias durable primitive facts: the existing execution/result wins, and the new input does not mutate the recorded execution.

    Scenario: Service execution ids are disjoint from the reserved object-call-id namespace; a service idempotencyKey using the object prefix is rejected
      Then Service execution ids are disjoint from the reserved object-call-id namespace; a service idempotencyKey using the object prefix is rejected.

  Rule: Durable run step

    Scenario: run(action, options) records a terminal success or typed failure fact; replay returns the recorded terminal fact without re-executing the action
      Then run(action, options) records a terminal success or typed failure fact; replay returns the recorded terminal fact without re-executing the action.

    Scenario: run supports named and positional step identity; positional identity is valid only for deterministic control flow, while named identity is stable across handler refactors
      Then run supports named and positional step identity; positional identity is valid only for deterministic control flow, while named identity is stable across handler refactors.

    Scenario: run retry policy controls attempts before a terminal fact is recorded; once a terminal fact exists, retries are not reattempted on replay
      Then run retry policy controls attempts before a terminal fact is recorded; once a terminal fact exists, retries are not reattempted on replay.

    Scenario: A run action cannot require DurableExecutionRuntime or call durable primitives; this is rejected at the type level
      Then A run action cannot require DurableExecutionRuntime or call durable primitives; this is rejected at the type level.

    Scenario: A typed run failure is encoded through the declared error schema and propagates to the caller through attach/client
      Then A typed run failure is encoded through the declared error schema and propagates to the caller through attach/client.

  Rule: Durable service state

    Scenario: state(Table).get/set/delete operate on durable records scoped to the service execution stream and table schema
      Then state(Table).get/set/delete operate on durable records scoped to the service execution stream and table schema.

    Scenario: state.get is replay-stable for read-modify-write flows: the read value is journaled so recovery replays the same observed value instead of reading a later mutated value
      Then state.get is replay-stable for read-modify-write flows: the read value is journaled so recovery replays the same observed value instead of reading a later mutated value.

    Scenario: state.set and state.delete are durable only after their underlying append is acknowledged; deterministic state writes self-heal by replaying from the handler body
      Then state.set and state.delete are durable only after their underlying append is acknowledged; deterministic state writes self-heal by replaying from the handler body.

    Scenario: State mutations inside a run action are rejected by the run-action type guard
      Then State mutations inside a run action are rejected by the run-action type guard.

  Rule: Durable sleep

    Scenario: sleep(name, duration) records a durable timer intent and delays completion until the duration has elapsed
      Then sleep(name, duration) records a durable timer intent and delays completion until the duration has elapsed.

    Scenario: On replay, a fired sleep fact short-circuits; a pending sleep recomputes the remaining delay from the durable deadline
      Then On replay, a fired sleep fact short-circuits; a pending sleep recomputes the remaining delay from the durable deadline.

    Scenario: Boot recovery re-arms sleep by re-running the handler from the top and replaying the durable timer facts
      Then Boot recovery re-arms sleep by re-running the handler from the top and replaying the durable timer facts.

  Rule: Durable signals, deferreds, and awakeables

    Scenario: signal(name, schema) parks the handler until resolveSignal(id, name, schema, value) writes a durable resolution fact
      Then signal(name, schema) parks the handler until resolveSignal(id, name, schema, value) writes a durable resolution fact.

    Scenario: deferred(name, schema) creates an invocation-scoped promise; resolve writes the durable value and get reads or parks until that value exists
      Then deferred(name, schema) creates an invocation-scoped promise; resolve writes the durable value and get reads or parks until that value exists.

    Scenario: awakeable(schema) returns a replay-stable id and a promise; resolveAwakeable(id, awakeableId, schema, value) resolves it through ingress
      Then awakeable(schema) returns a replay-stable id and a promise; resolveAwakeable(id, awakeableId, schema, value) resolves it through ingress.

    Scenario: Resolve-before-await is not lost: a durable resolution fact written before the handler parks is picked up when the handler reaches the await
      Then Resolve-before-await is not lost: a durable resolution fact written before the handler parks is picked up when the handler reaches the await.

    Scenario: Durable promise rows are the source of truth; in-process waiters are best-effort wakeups only
      Then Durable promise rows are the source of truth; in-process waiters are best-effort wakeups only.

  Rule: Service boot recovery

    Scenario: A fresh engine over the same S2 backend re-drives a non-resident parked service execution from the roster plus WorkflowDb, so it becomes resident and attachable as if freshly submitted
      Then A fresh engine over the same S2 backend re-drives a non-resident parked service execution from the roster plus WorkflowDb, so it becomes resident and attachable as if freshly submitted.

    Scenario: A recovered service execution replays run/state/sleep facts without re-executing completed side effects or double-applying journaled reads
      Then A recovered service execution replays run/state/sleep facts without re-executing completed side effects or double-applying journaled reads.

    Scenario: An ingress resolution on a recovered service execution settles it; signal or awakeable resolution by replay-stable id works after recovery
      Then An ingress resolution on a recovered service execution settles it; signal or awakeable resolution by replay-stable id works after recovery.

    Scenario: Completed service executions are not boot-recovered as pending work
      Then Completed service executions are not boot-recovered as pending work.

  Rule: Service stream lifecycle

    Scenario: Service executions use the ephemeral one-stream-per-call model; service streams may be dropped after completion once result/attach semantics no longer require them
      Then Service executions use the ephemeral one-stream-per-call model; service streams may be dropped after completion once result/attach semantics no longer require them.

    Scenario: Result acknowledgement or retention policy is explicit; a service result is not reclaimed while attach/poll are still expected to serve it
      Then Result acknowledgement or retention policy is explicit; a service result is not reclaimed while attach/poll are still expected to serve it.

  Rule: Service backend stability

    Scenario: Services may keep the existing WorkflowDb/RosterDb implementation as long as it satisfies the public-api primitive contract
      Then Services may keep the existing WorkflowDb/RosterDb implementation as long as it satisfies the public-api primitive contract.

    Scenario: The object owner-stream cutover does not require changing the service backend; a separate stateless-execution pass owns any later rewrite
      Then The object owner-stream cutover does not require changing the service backend; a separate stateless-execution pass owns any later rewrite.

  Rule: BOUNDARIES

    Scenario: Stateless execution behavior is driven through the public service/client/sendClient/attach/poll/resolveSignal/resolveAwakeable APIs, not by calling WorkflowDb or RosterDb directly from validation code
      Then Stateless execution behavior is driven through the public service/client/sendClient/attach/poll/resolveSignal/resolveAwakeable APIs, not by calling WorkflowDb or RosterDb directly from validation code.

    Scenario: S2-backed service behavior is proven by executable Cucumber specs; package-local tests remain pure/type-level
      Then S2-backed service behavior is proven by executable Cucumber specs; package-local tests remain pure/type-level.

