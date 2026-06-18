@product:effect-s2-durable @feature:production-readiness @spec-only
Feature: Production Readiness
  Net-new production requirements needed to consider the durable workstream
  feature-complete beyond the currently proven single-process public API:
  coverage discipline, checkpoint/trim, owner discovery, multi-worker safety,
  version compatibility, batching, and observability.

  @component:SPEC_COVERAGE
  Rule: Spec and proof completeness

    @requirement:SPEC_COVERAGE.1
    Scenario: Every public durable API surface has a feature requirement in public-api,...
      Then the production-readiness contract includes:
        """
        Every public durable API surface has a feature requirement in public-api,
        stateless-execution, stateful-execution, workflow-execution, or
        production-readiness.
        """

    @requirement:SPEC_COVERAGE.2
    Scenario: Every non-deferred feature requirement has a Cucumber proof or a pure package test when...
      Then the production-readiness contract includes:
        """
        Every non-deferred feature requirement has a Cucumber proof or a pure
        package test when the requirement is explicitly pure/type-level.
        """

    @requirement:SPEC_COVERAGE.3
    Scenario: S2-backed behavioral proofs live only in Cucumber; effect-s2-durable and...
      Then the production-readiness contract includes:
        """
        S2-backed behavioral proofs live only in Cucumber; effect-s2-durable and
        effect-s2-stream-db package tests remain pure/type-level.
        """

    @requirement:SPEC_COVERAGE.4
    Scenario: Executable spec checks can run in strict mode for completed features with no...
      Then the production-readiness contract includes:
        """
        Executable spec checks can run in strict mode for completed features with no
        --allow-missing.
        """

  @component:CHECKPOINT_TRIM
  Rule: Object checkpoint and trim implementation

    @requirement:CHECKPOINT_TRIM.1
    Scenario: Object owner streams emit verifiable Checkpointed ActorEvents carrying coveredSeqNum,...
      Then the production-readiness contract includes:
        """
        Object owner streams emit verifiable Checkpointed ActorEvents carrying
        coveredSeqNum, retained projection state, idempotency horizon metadata, and
        a projection fingerprint.
        """

    @requirement:CHECKPOINT_TRIM.2
    Scenario: A trim command record is issued only after the checkpoint is durable and...
      Then the production-readiness contract includes:
        """
        A trim command record is issued only after the checkpoint is durable and
        replay/fingerprint verification succeeds.
        """

    @requirement:CHECKPOINT_TRIM.3
    Scenario: Replay from a checkpoint plus later records reconstructs the same projection as replay...
      Then the production-readiness contract includes:
        """
        Replay from a checkpoint plus later records reconstructs the same projection
        as replay from the untrimmed log.
        """

    @requirement:CHECKPOINT_TRIM.4
    Scenario: Single-batch checkpoints are allowed as a v1 limit; framed/chunked checkpointing is...
      Then the production-readiness contract includes:
        """
        Single-batch checkpoints are allowed as a v1 limit; framed/chunked
        checkpointing is required before advertising support for objects whose live
        footprint exceeds one S2 append batch.
        """

  @component:OWNER_DISCOVERY
  Rule: Owner registry recovery

    @requirement:OWNER_DISCOVERY.1
    Scenario: A basin/namespace owner registry stream records owner keys monotonically before first...
      Then the production-readiness contract includes:
        """
        A basin/namespace owner registry stream records owner keys monotonically
        before first admission for that owner.
        """

    @requirement:OWNER_DISCOVERY.2
    Scenario: Object boot recovery folds the owner registry to discover owners and starts drainers...
      Then the production-readiness contract includes:
        """
        Object boot recovery folds the owner registry to discover owners and starts
        drainers only for owners with a pending head.
        """

    @requirement:OWNER_DISCOVERY.3
    Scenario: Stream listing is not the correctness source for production owner recovery.
      Then the production-readiness contract includes:
        """
        Stream listing is not the correctness source for production owner recovery.
        """

  @component:MULTI_WORKER
  Rule: Multi-worker safety

    @requirement:MULTI_WORKER.1
    Scenario: Multi-worker execution remains disabled or explicitly unsupported until log-backed lease...
      Then the production-readiness contract includes:
        """
        Multi-worker execution remains disabled or explicitly unsupported until
        log-backed lease renewal and S2 fencing are implemented and proven.
        """

    @requirement:MULTI_WORKER.2
    Scenario: Lease renewal, campaign, self-demotion, and token-protected writes are covered by...
      Then the production-readiness contract includes:
        """
        Lease renewal, campaign, self-demotion, and token-protected writes are
        covered by executable specs with two competing workers over one S2 backend.
        """

    @requirement:MULTI_WORKER.3
    Scenario: A stale worker must be unable to append protected owner/checkpoint writes after a newer...
      Then the production-readiness contract includes:
        """
        A stale worker must be unable to append protected owner/checkpoint writes
        after a newer owner has fenced the stream.
        """

  @component:VERSION_COMPATIBILITY
  Rule: Rolling deploy safety

    @requirement:VERSION_COMPATIBILITY.1
    Scenario: ActorEvent schema/runtime versions are encoded in durable records before any...
      Then the production-readiness contract includes:
        """
        ActorEvent schema/runtime versions are encoded in durable records before any
        incompatible event vocabulary can be written.
        """

    @requirement:VERSION_COMPATIBILITY.2
    Scenario: Older readers halt rather than mis-fold newer unsupported events.
      Then the production-readiness contract includes:
        """
        Older readers halt rather than mis-fold newer unsupported events.
        """

    @requirement:VERSION_COMPATIBILITY.3
    Scenario: Deployment guidance requires checkpoint compatibility with the oldest running reader...
      Then the production-readiness contract includes:
        """
        Deployment guidance requires checkpoint compatibility with the oldest
        running reader during rolling upgrades.
        """

  @component:PERFORMANCE
  Rule: Hot-path efficiency

    @requirement:PERFORMANCE.1
    Scenario: Resident owner loops use tail/read-session style incremental consumption for hot keys...
      Then the production-readiness contract includes:
        """
        Resident owner loops use tail/read-session style incremental consumption for
        hot keys instead of whole-log reads per operation.
        """

    @requirement:PERFORMANCE.2
    Scenario: Same-turn durable primitive writes are batched where this reduces append cost without...
      Then the production-readiness contract includes:
        """
        Same-turn durable primitive writes are batched where this reduces append
        cost without changing replay semantics.
        """

    @requirement:PERFORMANCE.3
    Scenario: Cucumber trace evidence captures append count, read count, tail/check-tail count, and...
      Then the production-readiness contract includes:
        """
        Cucumber trace evidence captures append count, read count, tail/check-tail
        count, and wall-clock timing for representative service, object, workflow,
        and recovery scenarios.
        """

  @component:OBSERVABILITY
  Rule: Cucumber proofs over production paths

    @requirement:OBSERVABILITY.1
    Scenario: Cucumber scenarios drive public APIs such as sendClient, client, attach, and...
      Then the production-readiness contract includes:
        """
        Cucumber scenarios drive public APIs such as sendClient, client, attach, and
        resolveSignal while asserting both public results and production evidence
        spans.
        """

    @requirement:OBSERVABILITY.2
    Scenario: Recovery is proven by restarting runtime scopes over the same S2/S2Lite streams; package...
      Then the production-readiness contract includes:
        """
        Recovery is proven by restarting runtime scopes over the same S2/S2Lite
        streams; package Vitest keeps pure and type-level coverage only.
        """

    @requirement:OBSERVABILITY.3
    Scenario: Evidence spans must come from production code paths, not validation-only instrumentation.
      Then the production-readiness contract includes:
        """
        Evidence spans must come from production code paths, not validation-only
        instrumentation.
        """

    @requirement:OBSERVABILITY.4
    Scenario: Cucumber specs must not call Actor.admit, Actor.drain, or any validation-only facade as...
      Then the production-readiness contract includes:
        """
        Cucumber specs must not call Actor.admit, Actor.drain, or any
        validation-only facade as the product path.
        """

  @component:EXAMPLES
  Rule: Restate-style tutorial coverage

    @requirement:EXAMPLES.1
    Scenario: The repo includes executable examples or validations equivalent to basic services,...
      Then the production-readiness contract includes:
        """
        The repo includes executable examples or validations equivalent to basic
        services, virtual objects, durable steps, durable sleeps, signals/promises,
        send/attach, object-to-object calls, workflows, query handlers, and signal
        handlers.
        """

    @requirement:EXAMPLES.2
    Scenario: Examples use the public Effect-native APIs only: service, object, workflow,...
      Then the production-readiness contract includes:
        """
        Examples use the public Effect-native APIs only: service, object, workflow,
        client/sendClient/objectClient/objectSendClient/sharedClient, and free
        primitives.
        """

    @requirement:EXAMPLES.3
    Scenario: Examples do not use internal ActorEvent/log APIs or validation-only shortcuts.
      Then the production-readiness contract includes:
        """
        Examples do not use internal ActorEvent/log APIs or validation-only
        shortcuts.
        """

  @constraint:MIGRATION
  Rule: Superseded object topology

    @requirement:MIGRATION.1
    Scenario: ObjectInboxRow and ObjectStateDb are superseded by stateful-execution admission and...
      Then the production-readiness contract includes:
        """
        ObjectInboxRow and ObjectStateDb are superseded by stateful-execution
        admission and StateChanged facts for the object path.
        """

    @requirement:MIGRATION.2
    Scenario: Object-path per-call WorkflowDb primitive journals are superseded by owner-stream...
      Then the production-readiness contract includes:
        """
        Object-path per-call WorkflowDb primitive journals are superseded by
        owner-stream Journaled facts.
        """

    @requirement:MIGRATION.3
    Scenario: Object-path RosterDb completion/result/recovery rows are superseded by Completed events...
      Then the production-readiness contract includes:
        """
        Object-path RosterDb completion/result/recovery rows are superseded by
        Completed events and owner-stream recovery.
        """

    @requirement:MIGRATION.4
    Scenario: Object-specific drainLoop / drainOne / ensureDrainerLocked code is superseded by the...
      Then the production-readiness contract includes:
        """
        Object-specific drainLoop / drainOne / ensureDrainerLocked code is
        superseded by the resident owner loop and per-key single-writer discipline.
        """

    @requirement:MIGRATION.5
    Scenario: The window-2 idempotent guard is superseded by append-only admission plus done-derived...
      Then the production-readiness contract includes:
        """
        The window-2 idempotent guard is superseded by append-only admission plus
        done-derived completion.
        """

    @requirement:MIGRATION.6
    Scenario: Residency-retry signal behavior is superseded by durable ingress append plus best-effort...
      Then the production-readiness contract includes:
        """
        Residency-retry signal behavior is superseded by durable ingress append plus
        best-effort waiter pokes.
        """
