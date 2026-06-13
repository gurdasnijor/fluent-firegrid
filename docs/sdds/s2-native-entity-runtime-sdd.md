# S2-Native Entity Runtime SDD

Status: draft
Owner: Firegrid

## Purpose

Design a Firegrid runtime that uses S2 as the durability substrate directly.
This is not an S2 backend for Effect Cluster and not a Durable Streams gateway.

The core primitive is:

```text
one S2 stream per entity
  = durable inbox
  + ordered event log
  + state journal
  + completion index
```

The runtime should use S2's strongest guarantees: ordered records, atomic append
batches within one stream, stream fencing, snapshots, trim, and read sessions.

## Deliverable

The product deliverable is an S2-native implementation of Effect's workflow
engine service:

```ts
Layer.Layer<WorkflowEngine.WorkflowEngine, never, S2Client | Scope.Scope | ...>
```

The implementation should target Effect's low-level
`WorkflowEngine.Encoded` contract and wrap it with
`WorkflowEngine.makeUnsafe`, so callers use the standard typed
`WorkflowEngine.WorkflowEngine` service.

Working name:

```text
S2NativeWorkflowEngine
```

This engine is backed by the entity runtime described in this SDD. The entity
runtime is the internal substrate; the public integration point is the
`WorkflowEngine` service.

Package shape can be either:

```text
packages/fluent-s2-workflow-engine
```

or, if we want the entity runtime reusable outside workflows:

```text
packages/fluent-s2-entity-runtime   # lower-level runtime
packages/fluent-s2-workflow-engine  # WorkflowEngine adapter
```

The first useful milestone is a runnable `S2NativeWorkflowEngine` layer that can
execute, poll, resume, interrupt, run activities, persist deferreds, and schedule
durable clocks against S2 Lite.

## Target Consumer

The first real consumer is `effect-encore` PR 2:

```text
https://github.com/gurdasnijor/effect-encore/pull/2
```

At the time this SDD was updated, that PR adds
`encore-ds/src/fluent/*`: a Firegrid-shaped authoring surface with
`service`, `workflow`, free `run` / `all` / `race` / `select` / `spawn`,
`makeRuntime`, `client`, and `sendClient`. It currently lowers onto Effect's
`WorkflowEngine` through the existing `DurableStreamsWorkflowEngine`.

This package should make that PR runnable without Durable Streams by providing
an S2-native `WorkflowEngine` layer with the same service contract.

Integration requirement:

1. Check out `gurdasnijor/effect-encore` PR 2 locally.
2. Wire its fluent runtime/tests to use `S2NativeWorkflowEngine` instead of
   `DurableStreamsWorkflowEngine`.
3. Run the PR's fluent tests against S2 Lite.
4. Keep an integration test or script in this repo that documents and verifies
   the same wiring.

The goal is not just to pass this repo's internal tests. The S2 engine is ready
only when the `effect-encore` fluent authoring surface can execute its real
service/workflow scenarios on top of it.

## Non-Goals

- Do not implement Effect Cluster `MessageStorage` or `RunnerStorage`.
- Do not introduce shard streams, reply buckets, or primary-key claim streams.
- Do not implement Durable Streams `PROTOCOL.md`.
- Do not build an HTTP transport or gateway.
- Do not add synthetic in-memory transports.
- Do not promise exactly-once external side effects. External systems still need
  idempotency keys.

## S2 Capabilities

The runtime should use these S2 capabilities directly:

- **Stream order:** one entity stream gives one authoritative event order.
- **Atomic append batch:** request result, state events, and completion marker
  can be committed together.
- **Fencing token:** stale owners cannot append state/result records with an old
  token.
- **`matchSeqNum`:** compare-and-set append decisions where needed.
- **Read sessions:** owners can follow new records after activation.
- **Snapshots:** compact folded state into a durable recovery point.
- **Trim:** bound replay after a snapshot is durable.
- **S2 Lite:** local and CI tests run against real S2 behavior.

## Entity Identity

An entity is an addressable durable actor-like unit:

```ts
interface EntityAddress {
  readonly namespace: string
  readonly entityType: string
  readonly entityId: string
}
```

The physical stream name is derived from the address:

```text
entity/<namespace>/<entity-type>/<encoded-entity-id>
```

Entity identity is stable. Stream naming is an implementation detail, but the
mapping must be deterministic and versioned.

## Record Model

Every record body is schema encoded. Application payloads use Effect Schema.

```ts
type EntityRecord =
  | RequestRecord
  | CommitRecord
  | TimerRecord
  | SnapshotRecord
  | ControlRecord

interface RequestRecord {
  readonly _tag: "Request"
  readonly requestId: string
  readonly operation: string
  readonly payload: unknown
  readonly primaryKey?: string
  readonly deliverAt?: number
  readonly headers?: Readonly<Record<string, string>>
  readonly traceId?: string
  readonly spanId?: string
  readonly sampled?: boolean
}

interface CommitRecord {
  readonly _tag: "Commit"
  readonly requestId: string
  readonly result: unknown
  readonly stateEvents: ReadonlyArray<unknown>
  readonly completedAt: number
}

interface TimerRecord {
  readonly _tag: "Timer"
  readonly timerId: string
  readonly requestId: string
  readonly operation: string
  readonly payload: unknown
  readonly deliverAt: number
}

interface SnapshotRecord {
  readonly _tag: "Snapshot"
  readonly state: unknown
  readonly lastIncludedSeqNum: number
  readonly createdAt: number
}

interface ControlRecord {
  readonly _tag: "Control"
  readonly kind: "Activated" | "Deactivated" | "Interrupted"
  readonly at: number
}
```

`CommitRecord` is the completion marker. There is no separate processed table.

## Folded State

Activation folds the entity stream from the latest valid snapshot, or from the
retained floor if no snapshot exists.

The fold derives:

- current entity state;
- completed requests: `requestId -> CommitRecord`;
- primary-key winners: `primaryKey -> requestId`;
- pending requests with no commit;
- due timers;
- future timers;
- latest folded sequence number.

The fold is deterministic. Given the same snapshot and suffix records, every
runtime instance must derive the same state.

## Ownership

Only one active owner may commit state for an entity at a time.

Activation:

1. Resolve entity stream.
2. Read tail.
3. Append an S2 `fence` command with a fresh fencing token.
4. Fold retained records.
5. Resume due pending requests oldest-first.
6. Follow the stream for new requests and timers.

Rules:

- State commits must include the current fencing token.
- Timer commits must include the current fencing token.
- Snapshot/trim operations must include the current fencing token.
- Sender request appends may be tokenless so requests can become durable without
  first reaching the owner.
- If an old owner appends with a stale token, S2 must reject it.

This is cooperative fencing: correctness depends on all runtime owner writes
using the token.

## Request Lifecycle

Enqueue:

1. Encode the operation payload.
2. Append `RequestRecord` to the entity stream.
3. Wait for the matching `CommitRecord`, or return the already committed result
   for the request's primary key.

Handle:

1. Owner fold observes a due `RequestRecord` without a commit.
2. If `primaryKey` already maps to a committed request, return/replay the
   original result.
3. Otherwise run the handler.
4. Append one atomic S2 batch:
   - `CommitRecord`;
   - zero or more state events;
   - zero or more timer records created by the handler.
5. Notify waiters from the committed record.

The atomic batch is the load-bearing design choice. Result, state changes, and
completion become durable together.

## Deduplication

Deduplication is entity-local and fold-based.

Rules:

- The first request with a primary key wins.
- A duplicate before completion waits for the winner's commit.
- A duplicate after completion returns the winner's result.
- No separate claim stream is needed.
- Cross-entity dedupe is not provided by this primitive.

For workflows, primary keys should include workflow execution id plus the
operation identity, such as activity name and attempt.

## Timers

Timers are records in the entity stream.

The owner keeps in-memory timers derived from the fold. On restart, timers are
rebuilt from S2. Future timers do not become pending requests until
`deliverAt <= now`.

If no owner is active when a timer becomes due, a scheduler/placement layer must
activate the entity. That layer may use a secondary timer index, but the entity
stream remains the source of truth.

## Snapshots And Trim

Snapshots bound replay.

Process:

1. Append `SnapshotRecord` containing folded state and `lastIncludedSeqNum`.
2. Once the snapshot is durable, issue S2 trim to discard older records that the
   snapshot covers.

Rules:

- Never trim past the latest durable snapshot needed for recovery.
- Snapshot encoding must be schema-versioned.
- If snapshot decoding fails, the runtime must fall back to an earlier retained
  point or fail activation explicitly.

## Workflow Mapping

Workflows are entities.

Mapping:

- workflow execution id -> entity id;
- workflow command -> request record;
- activity result -> commit state event;
- deferred resume -> request record;
- sleep -> timer record;
- interrupt -> request/control record;
- workflow state -> folded state from snapshot plus state events.

The workflow engine should be built on this runtime instead of using a separate
cluster message-storage backend.

## Package Shape

Proposed package:

```text
packages/fluent-s2-entity-runtime
```

Initial files:

```text
src/
  index.ts
  config.ts
  names.ts
  records.ts
  codec.ts
  fold.ts
  owner.ts
  runtime.ts
  timers.ts
  snapshot.ts
  errors.ts
  s2.ts
test/
  s2-lite-entity-runtime.test.ts
```

The package should expose runtime constructors and Effect layers. It should not
export transport abstractions or Effect Cluster compatibility services.

## Required Tests

Use S2 Lite. Do not fake S2.

1. Request durability
   - request appended before handling survives owner crash.
2. Atomic commit
   - result and state events appear together from one append batch.
3. Fold recovery
   - activation rebuilds state, completions, dedupe index, and timers.
4. Deduplication
   - duplicate before completion waits for the original;
   - duplicate after completion returns the original result.
5. Fencing
   - stale owner commit with old token is rejected.
6. Timers
   - future timer is not handled before `deliverAt`;
   - restart rebuilds timer state.
7. Snapshot and trim
   - snapshot restores state;
   - trim does not break recovery.
8. Effect Encore consumer integration
   - checkout `gurdasnijor/effect-encore` PR 2;
   - replace the Durable Streams engine wiring with `S2NativeWorkflowEngine`;
   - run the fluent basics/conformance tests from that PR against S2 Lite;
   - assert service calls, workflow keyed runs, `run`, `all`, `race`, `select`,
     `spawn`, idempotency keys, ingress clients, and send clients pass.

## Open Design Points

- Placement: how entities are assigned to process owners.
- Scheduler index: when sleeping entities need a global due-time index.
- Cross-entity transaction story: not provided by the per-entity stream.
- Queryable views: whether to project state into `fluent-s2-state` tables.
- Backpressure: how many pending requests an owner may process concurrently.

## Relationship To Other SDDs

`s2-cluster-storage-sdd.md` is a compatibility track for Effect Cluster's
existing storage contracts. This document is the S2-native Firegrid track.

`s2-native-state-sdd.md` describes queryable durable table/state projections.
Those projections can be built from this runtime's entity streams, but they are
not the execution primitive.
