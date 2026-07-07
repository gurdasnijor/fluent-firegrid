# SDD: TanStack Workflow S2 Store

Doc-Class: SDD
Status: frozen
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: S2

### S2-backed `WorkflowExecutionStore` for TanStack Workflow

|   |   |
| --- | --- |
| Status | Frozen scaffolding; accurate for the TanStack/S2 store but not the build target |
| Date | 2026-06-25 |
| Package | Historical `@firegrid/fluent/s2`; current S2 package boundary is `@firegrid/store` / `Firegrid.Store` |
| Primary contract | `WorkflowExecutionStore` from `@firegrid/fluent/runtime` |
| Local reference | `repos/tanstack-workflow` @ `602cdec439876335168d96f5443c0dc59e4cc436` |
| Lower dependency | `@firegrid/log` |

---

## Decision

Do not design a bespoke durable-function API in this repo. Use TanStack Workflow's API/runtime shape and implement the S2-backed store/runtime adapter it needs.

The product surface for this phase is implemented as:

```ts
export const s2WorkflowExecutionStore: (config: {
  readonly s2Endpoint: string
  readonly basin?: string
  readonly namespace?: string
}) => WorkflowExecutionStore

export const createS2WorkflowRuntimeHost: (config) => S2WorkflowRuntimeHost
```

TanStack Workflow already separates workflow authoring/runtime from persistence.
Its runtime consumes a `WorkflowExecutionStore`; the bundled
`in-memory-store.ts` is the reference implementation. This repo has replaced
that in-memory store with S2 semantics and added a host wrapper for recovery,
sweeps, timers, schedules, and test/runtime orchestration.

## Source Contract

Use the vendored TanStack source as the contract reference:

- `repos/tanstack-workflow/packages/workflow-runtime/src/types.ts`
- `repos/tanstack-workflow/packages/workflow-runtime/src/in-memory-store.ts`
- `repos/tanstack-workflow/packages/workflow-runtime/src/runtime-driver.ts`
- `repos/tanstack-workflow/research/API_CANDIDATES.md`
- `repos/tanstack-workflow/research/PRIOR_ART_AI_ORCHESTRATION.md`
- `repos/tanstack-workflow/research/COMPETITOR_GAP_ANALYSIS_2026-05-25.md`

The TanStack Workflow reference packages are early, so implementation should pin
an exact version or commit when lifting behavior. If we lift types wholesale
instead of depending on the package, keep source attribution and isolate the
compatibility copy under the Fluent runtime/S2 adapter modules.

## Non-Negotiable Boundaries

- Do not create a parallel durable-function API.
- Do not implement `service(...)`, `object(...)`, `workflow(...)`, generated clients, or virtual-object `state(...)` inside the TanStack/S2 store package. Those are fluent-layer concerns.
- Do not add proof-only runtime APIs.
- Do not add an in-memory S2 substitute.
- Runtime I/O uses `@firegrid/log` / `S2Client.ts`; no second transport facade.
- Existing proofs must not be weakened or deleted to make adapter work pass.

## Store Contract Summary

The adapter must satisfy TanStack's `WorkflowExecutionStore`:

- Run lifecycle: `createRun`, `loadRun`, `loadExecution`, `loadRunState`, `saveRunState`, `deleteRun`.
- CAS event log: `appendEvents`, `readEvents`, optional `subscribeEvents`.
- Leasing: `claimRun`, `heartbeatRunLease`, `releaseRunLease`, `claimStaleRuns`.
- Run terminal state: `markRunPaused`, `markRunFinished`, `markRunErrored`.
- Wakeups: `scheduleTimer`, `claimDueTimers`, `deliverSignal`, `deliverApproval`.
- Schedules: `upsertSchedule`, `claimDueScheduleBuckets`, `markScheduleBucketStarted`.
- Visibility: `listRuns`, `getRunTimeline`.

The in-memory store's behavior is the compatibility oracle. The S2 store mirrors
that behavior and adds real-substrate crash/failover proofs.

## Implementation Status

The A-E store ladder is implemented. The active remaining work is hardening,
cleanup, and the fluent API layer above this substrate, not more narrow
store-proof PRs.

| Slice | Status | Production surface | Real-`s2 lite` proof |
| --- | --- | --- | --- |
| A. Event Log CAS | Implemented | `appendEvents`, `readEvents`, `loadExecution` | `store.event-log-cas` |
| B. Run Lifecycle | Implemented | `createRun`, run state load/save, terminal state, timeline | `store.run-lifecycle`, `store.runtime-end-to-end`, `store.host-crash-restart` |
| C. Leases And Stale Claims | Implemented | `claimRun`, `heartbeatRunLease`, `releaseRunLease`, `claimStaleRuns`, host recovery | `store.leases`, `store.host-crash-restart`, `store.host-tick` |
| D. Timers, Signals, And Approvals | Implemented | `scheduleTimer`, `claimDueTimers`, `deliverSignal`, `deliverApproval`, runtime sweeps | `store.timers-signals`, `store.runtime-timer-sweep`, `store.runtime-approval` |
| E. Schedules | Implemented | `upsertSchedule`, `claimDueScheduleBuckets`, `markScheduleBucketStarted`, schedule sweep | `store.runtime-schedule-sweep` |

### Remaining Hardening

- `subscribeEvents` remains the optional store method most likely to matter for
  UI/devtools or live output streaming. Implement it with `readSession` when a
  concrete consumer needs it.
- Lease facts currently provide the store-level coordination. S2 command-record
  fences should be introduced only where a later writer needs token-enforced S2
  writes after lease ownership, rather than as blanket complexity.
- Timer and schedule bucket retention/granularity are simple v1 choices. Revisit
  when operational load or retention requirements become concrete.

## S2 Physical Model

Use S2 streams as append-only facts and fold them to implement store reads.

Recommended stream layout:

| Purpose | Stream |
| --- | --- |
| Per-run event log | `${namespace}/runs/${runId}/events` |
| Per-run metadata/state | `${namespace}/runs/${runId}/meta` |
| Run index by workflow/status | `${namespace}/indexes/runs/${workflowId}` and status-specific records |
| Timer buckets | `${namespace}/timers/${bucket}` |
| Schedule definitions | `${namespace}/schedules/${scheduleId}` |
| Schedule due buckets | `${namespace}/schedule-buckets/${bucket}` |

The exact bucket scheme can evolve. Start simple with minute buckets for timers/schedules, then make bucket granularity configurable if proofs show pressure.

## Method Mapping

### Event Log

`appendEvents({ runId, expectedNextIndex, events })`

- Append to the run event stream.
- Use `matchSeqNum = expectedNextIndex`.
- Store each `WorkflowEvent` as one S2 record with headers for run id, event index, event type, and step id.
- On S2 CAS mismatch, throw TanStack's `LogConflictError`.
- Return `{ nextIndex }` from the S2 ack end sequence number.

`readEvents({ runId, fromIndex })`

- Read the run event stream from `fromIndex ?? 0`.
- Decode S2 records to `StoredWorkflowEvent`.

`subscribeEvents`

- Implement from `readSession` when needed. It is optional in the interface, so this can be deferred until runtime/tests require live subscription.

### Run Metadata And State

`createRun`

- Append a `RunCreated` metadata fact with `matchSeqNum: 0`.
- If the stream already exists or CAS fails, fold and return `{ kind: "existing" }` when the same run exists.

`saveRunState`

- Append a `RunStateSaved` fact to the metadata stream.
- Fold latest state for `loadRunState`.

`markRunPaused` / `markRunFinished` / `markRunErrored`

- Append state transition facts to the metadata stream.
- Terminal transitions clear lease and wakeup fields in the folded projection.

`deleteRun`

- Append `RunDeleted` tombstone metadata.
- Do not physically delete S2 streams in v1.

### Leases

`claimRun`

- Fold current run metadata.
- Reject terminal or unexpired foreign leases.
- Claim with a S2 `fence` command record or a CAS-protected `LeaseClaimed` metadata fact.
- Prefer S2 fence records when the run owner will perform subsequent S2 writes requiring the token. Otherwise CAS lease facts may be enough.

`heartbeatRunLease`

- Refresh the owner lease only if the folded lease owner matches.
- If using S2 fence records, write the refreshed token with the current token.

`releaseRunLease`

- Clear only if the folded lease owner matches.

`claimStaleRuns`

- Fold run index/status projections, then attempt claims for expired leases up to `limit`.
- A stale run is a non-terminal run whose lease is missing or expired.

### Timers And Signals

`scheduleTimer`

- Append a timer registration fact into a due-time bucket stream.
- Append/update run metadata with `wakeAt`.
- Timer registration is a reconcilable projection; the run's metadata/event log remains the source of truth.

`claimDueTimers`

- Scan due bucket streams up to `now`.
- Claim individual timer records with lease facts in the bucket stream.
- Return at most `limit` `TimerWakeup`s.

`deliverSignal`

- Fold run metadata and signal delivery facts.
- Deduplicate by `signalId`.
- If the run is waiting for the signal, append a signal-delivered fact and transition run status to `queued`.
- Return TanStack's `delivered` / `duplicate` / `not-waiting` / `not-found` result.

`deliverApproval`

- Same as `deliverSignal`, with dedupe key `approval:${approvalId}`.

### Schedules

`upsertSchedule`

- Append schedule definition facts to the schedule stream.

`claimDueScheduleBuckets`

- Fold schedule definitions and due bucket claims.
- Claim due schedule buckets with lease facts.
- Return `ScheduleBucket`s up to `limit`.

`markScheduleBucketStarted`

- Append `ScheduleBucketStarted`.

## Acceptance Ladder

Build the adapter in thin vertical slices. Each slice uses TanStack's runtime contract and a real `s2 lite` process.

### A. Event Log CAS

**Status:** Implemented.

**Claim.** S2 can back TanStack's append-only run log.

**Forces:** `appendEvents`, `readEvents`, `loadExecution` event hydration.

**Proof:** `store.event-log-cas` races two writers with the same
`expectedNextIndex`; one succeeds, one maps to `LogConflictError`, and reading
from S2 returns exactly the committed ordered events.

### B. Run Lifecycle

**Status:** Implemented.

**Claim.** S2 can back run creation, state save/load, and terminal transitions.

**Forces:** `createRun`, `loadRun`, `loadRunState`, `saveRunState`, `markRunPaused`, `markRunFinished`, `markRunErrored`, `getRunTimeline`.

**Proof:** `store.run-lifecycle`,
`store.runtime-end-to-end`, and
`store.host-crash-restart` start workflows through the S2 store,
load execution from S2, and continue without losing run state or duplicating
events.

### C. Leases And Stale Claims

**Status:** Implemented.

**Claim.** S2 fencing/CAS can safely lease a run to one owner and allow failover after expiry.

**Forces:** `claimRun`, `heartbeatRunLease`, `releaseRunLease`, `claimStaleRuns`.

**Proof:** `store.leases` races claims and validates heartbeat
protection; `store.host-crash-restart` and
`store.host-tick` validate stale recovery from S2.

### D. Timers, Signals, And Approvals

**Status:** Implemented.

**Claim.** Paused TanStack runs can wake from S2-backed timers, signals, and approvals.

**Forces:** `scheduleTimer`, `claimDueTimers`, `deliverSignal`, `deliverApproval`.

**Proof:** `store.timers-signals`,
`store.runtime-timer-sweep`, and
`store.runtime-approval` prove timer and approval wakeups resume
paused runs from S2 exactly once.

### E. Schedules

**Status:** Implemented.

**Claim.** S2 can back recurring schedule definitions and due bucket claims.

**Forces:** `upsertSchedule`, `claimDueScheduleBuckets`, `markScheduleBucketStarted`.

**Proof:** `store.runtime-schedule-sweep` proves schedule
materialization and due bucket claiming start the scheduled run exactly once.

## PR Contract

Every implementation PR must state:

- Which adapter slice A-E it targets.
- Which `WorkflowExecutionStore` methods it implements or changes.
- Which TanStack in-memory-store behavior it mirrors.
- Which real-`s2 lite` proof validates the slice.
- Whether any existing proof was weakened or deleted.

Reject proof-only PRs and PRs that expand the workflow authoring API instead of implementing the S2 store.

## Resolved And Deferred Questions

- TanStack source is lifted into local workspace packages for now; keep the
  compatibility boundary explicit while upstream remains early.
- Run leases use CAS-protected metadata facts for the implemented store. S2
  command-record fences are deferred until a production writer needs tokened
  follow-on S2 writes.
- `subscribeEvents` is deferred until a UI/devtools/live-output consumer needs
  it.
- Timer bucket granularity and retention remain operational tuning concerns.
- Schedule indexing is S2-stream backed in the implemented runtime; revisit only
  if sweep scale forces a different projection strategy.

## References

- TanStack Workflow runtime store contract: `repos/tanstack-workflow/packages/workflow-runtime/src/types.ts`
- TanStack in-memory reference store: `repos/tanstack-workflow/packages/workflow-runtime/src/in-memory-store.ts`
- TanStack runtime driver: `repos/tanstack-workflow/packages/workflow-runtime/src/runtime-driver.ts`
- TanStack API candidates: `repos/tanstack-workflow/research/API_CANDIDATES.md`
- TanStack prior art: `repos/tanstack-workflow/research/PRIOR_ART_AI_ORCHESTRATION.md`
- TanStack competitor gap analysis: `repos/tanstack-workflow/research/COMPETITOR_GAP_ANALYSIS_2026-05-25.md`
- S2 concurrency control: https://s2.dev/docs/concepts/concurrency-control
- S2 appends: https://s2.dev/docs/concepts/appends
- S2 command records: https://s2.dev/docs/concepts/command-records
