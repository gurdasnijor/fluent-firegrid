@unverified
Feature: Production Readiness
  Net-new production requirements needed to consider the durable workstream
  feature-complete beyond the currently proven single-process public API:
  coverage discipline, checkpoint/trim, owner discovery, multi-worker safety,
  version compatibility, batching, and observability.

  Rule: Spec and proof completeness

    Scenario: Every public durable API surface has a feature requirement in public-api, stateless-execution, stateful-execution, workflow-execution, or production-readiness
      Then Every public durable API surface has a feature requirement in public-api, stateless-execution, stateful-execution, workflow-execution, or production-readiness.

    Scenario: Every non-deferred feature requirement has a Cucumber proof or a pure package test when the requirement is explicitly pure/type-level
      Then Every non-deferred feature requirement has a Cucumber proof or a pure package test when the requirement is explicitly pure/type-level.

    Scenario: S2-backed behavioral proofs live only in Cucumber; effect-s2-durable and effect-s2-stream-db package tests remain pure/type-level
      Then S2-backed behavioral proofs live only in Cucumber; effect-s2-durable and effect-s2-stream-db package tests remain pure/type-level.

    Scenario: Executable spec checks can run in strict mode for completed features with no --allow-missing
      Then Executable spec checks can run in strict mode for completed features with no --allow-missing.

  Rule: Object checkpoint and trim implementation

    Scenario: Object owner streams emit verifiable Checkpointed ActorEvents carrying coveredSeqNum, retained projection state, idempotency horizon metadata, and a projection fingerprint
      Then Object owner streams emit verifiable Checkpointed ActorEvents carrying coveredSeqNum, retained projection state, idempotency horizon metadata, and a projection fingerprint.

    Scenario: A trim command record is issued only after the checkpoint is durable and replay/fingerprint verification succeeds
      Then A trim command record is issued only after the checkpoint is durable and replay/fingerprint verification succeeds.

    Scenario: Replay from a checkpoint plus later records reconstructs the same projection as replay from the untrimmed log
      Then Replay from a checkpoint plus later records reconstructs the same projection as replay from the untrimmed log.

    Scenario: Single-batch checkpoints are allowed as a v1 limit; framed/chunked checkpointing is required before advertising support for objects whose live footprint exceeds one S2 append batch
      Then Single-batch checkpoints are allowed as a v1 limit; framed/chunked checkpointing is required before advertising support for objects whose live footprint exceeds one S2 append batch.

  Rule: Owner registry recovery

    Scenario: A basin/namespace owner registry stream records owner keys monotonically before first admission for that owner
      Then A basin/namespace owner registry stream records owner keys monotonically before first admission for that owner.

    Scenario: Object boot recovery folds the owner registry to discover owners and starts drainers only for owners with a pending head
      Then Object boot recovery folds the owner registry to discover owners and starts drainers only for owners with a pending head.

    Scenario: Stream listing is not the correctness source for production owner recovery
      Then Stream listing is not the correctness source for production owner recovery.

  Rule: Multi-worker safety

    Scenario: Multi-worker execution remains disabled or explicitly unsupported until log-backed lease renewal and S2 fencing are implemented and proven
      Then Multi-worker execution remains disabled or explicitly unsupported until log-backed lease renewal and S2 fencing are implemented and proven.

    Scenario: Lease renewal, campaign, self-demotion, and token-protected writes are covered by executable specs with two competing workers over one S2 backend
      Then Lease renewal, campaign, self-demotion, and token-protected writes are covered by executable specs with two competing workers over one S2 backend.

    Scenario: A stale worker must be unable to append protected owner/checkpoint writes after a newer owner has fenced the stream
      Then A stale worker must be unable to append protected owner/checkpoint writes after a newer owner has fenced the stream.

  Rule: Rolling deploy safety

    Scenario: ActorEvent schema/runtime versions are encoded in durable records before any incompatible event vocabulary can be written
      Then ActorEvent schema/runtime versions are encoded in durable records before any incompatible event vocabulary can be written.

    Scenario: Older readers halt rather than mis-fold newer unsupported events
      Then Older readers halt rather than mis-fold newer unsupported events.

    Scenario: Deployment guidance requires checkpoint compatibility with the oldest running reader during rolling upgrades
      Then Deployment guidance requires checkpoint compatibility with the oldest running reader during rolling upgrades.

  Rule: Hot-path efficiency

    Scenario: Resident owner loops use tail/read-session style incremental consumption for hot keys instead of whole-log reads per operation
      Then Resident owner loops use tail/read-session style incremental consumption for hot keys instead of whole-log reads per operation.

    Scenario: Same-turn durable primitive writes are batched where this reduces append cost without changing replay semantics
      Then Same-turn durable primitive writes are batched where this reduces append cost without changing replay semantics.

    Scenario: Cucumber trace evidence captures append count, read count, tail/check-tail count, and wall-clock timing for representative service, object, workflow, and recovery scenarios
      Then Cucumber trace evidence captures append count, read count, tail/check-tail count, and wall-clock timing for representative service, object, workflow, and recovery scenarios.

  Rule: Cucumber proofs over production paths

    Scenario: Cucumber scenarios drive public APIs such as sendClient, client, attach, and resolveSignal while asserting both public results and production evidence spans
      Then Cucumber scenarios drive public APIs such as sendClient, client, attach, and resolveSignal while asserting both public results and production evidence spans.

    Scenario: Recovery is proven by restarting runtime scopes over the same S2/S2Lite streams; package Vitest keeps pure and type-level coverage only
      Then Recovery is proven by restarting runtime scopes over the same S2/S2Lite streams; package Vitest keeps pure and type-level coverage only.

    Scenario: Evidence spans must come from production code paths, not validation-only instrumentation
      Then Evidence spans must come from production code paths, not validation-only instrumentation.

    Scenario: Cucumber specs must not call Actor.admit, Actor.drain, or any validation-only facade as the product path
      Then Cucumber specs must not call Actor.admit, Actor.drain, or any validation-only facade as the product path.

  Rule: Restate-style tutorial coverage

    Scenario: The repo includes executable examples or validations equivalent to basic services, virtual objects, durable steps, durable sleeps, signals/promises, send/attach, object-to-object calls, workflows, query handlers, and signal handlers
      Then The repo includes executable examples or validations equivalent to basic services, virtual objects, durable steps, durable sleeps, signals/promises, send/attach, object-to-object calls, workflows, query handlers, and signal handlers.

    Scenario: Examples use the public Effect-native APIs only: service, object, workflow, client/sendClient/objectClient/objectSendClient/sharedClient, and free primitives
      Then Examples use the public Effect-native APIs only: service, object, workflow, client/sendClient/objectClient/objectSendClient/sharedClient, and free primitives.

    Scenario: Examples do not use internal ActorEvent/log APIs or validation-only shortcuts
      Then Examples do not use internal ActorEvent/log APIs or validation-only shortcuts.

  Rule: Superseded object topology

    Scenario: ObjectInboxRow and ObjectStateDb are superseded by stateful-execution admission and StateChanged facts for the object path
      Then ObjectInboxRow and ObjectStateDb are superseded by stateful-execution admission and StateChanged facts for the object path.

    Scenario: Object-path per-call WorkflowDb primitive journals are superseded by owner-stream Journaled facts
      Then Object-path per-call WorkflowDb primitive journals are superseded by owner-stream Journaled facts.

    Scenario: Object-path RosterDb completion/result/recovery rows are superseded by Completed events and owner-stream recovery
      Then Object-path RosterDb completion/result/recovery rows are superseded by Completed events and owner-stream recovery.

    Scenario: Object-specific drainLoop / drainOne / ensureDrainerLocked code is superseded by the resident owner loop and per-key single-writer discipline
      Then Object-specific drainLoop / drainOne / ensureDrainerLocked code is superseded by the resident owner loop and per-key single-writer discipline.

    Scenario: The window-2 idempotent guard is superseded by append-only admission plus done-derived completion
      Then The window-2 idempotent guard is superseded by append-only admission plus done-derived completion.

    Scenario: Residency-retry signal behavior is superseded by durable ingress append plus best-effort waiter pokes
      Then Residency-retry signal behavior is superseded by durable ingress append plus best-effort waiter pokes.
