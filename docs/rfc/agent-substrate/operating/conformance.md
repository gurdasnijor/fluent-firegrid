# 29. Compliance Levels

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

## Firegrid/S2 Conformance Bridge

This section assigns stable invariant IDs to the subset of the RFC currently
claimed by `fluent-firegrid`. The proof names are the implementation evidence;
an invariant without a passing proof is aspirational and must not be cited as
implemented.

Evidence status:

- `ci-green` means the proof workload is executed by the current repository CI
  entrypoint, `.github/workflows/ci.yml` -> `pnpm run check`.
- `registered` means the proof is present in a checked-in proof registry but is
  not invoked by the current CI workflow.
- `ci-compile` means the proof source compiles in CI, but the proof workload is
  not yet executed by a checked-in runner command.

Rows with `registered` or `ci-compile` evidence are traceable but remain
automation gaps. They must not be cited as fully conformance-green until the
listed evidence is promoted to `ci-green` or accompanied by a recorded green
run.

| Invariant | Requirement | Proof evidence | Evidence status |
| --- | --- | --- | --- |
| INV-001 | An acknowledged append is visible to later tail/read operations in append order. | `effect-s2.capability-a.read-after-append` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-002 | Restart from a persisted cursor folds exactly the durable suffix after that cursor. | `effect-s2.capability-a.cursor-fold`; `foundation.subject-history` | TS proof is `registered`; F# proof is `ci-compile` via `Firegrid.Fable.slnx`. |
| INV-003 | Two writers appending at the same expected tail cannot both commit. | `effect-s2.capability-b.match-seq-num-contention`; `foundation.subject-history` | TS proof is `registered`; F# proof is `ci-compile`. |
| INV-004 | Fencing is explicit and cooperative: a deposed owner must be unable to commit after takeover. | `effect-s2.capability-b.fence-semantics`, `store.object-live-fencing`, `store.object-stale-owner` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-005 | Workflow event appends use expected-index CAS; stale writers surface conflict without changing committed order. | `store.event-log-cas`, `store.run-lifecycle` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-006 | Runtime replay reconstructs committed workflow state from durable events without reissuing completed effects. | `store.runtime-end-to-end`, `store.awakeable`, `store.runtime-approval` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-007 | Timers and schedules are durable: overdue work is swept once after restart and does not refire after completion. | `store.timers-signals`, `store.runtime-timer-sweep`, `store.runtime-schedule-sweep`, `store.workflow-schedule` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-008 | Object state is materialized from durable facts and can be replayed/reconstructed by a new owner. | `store.object-state`, `store.object-replay-state`, `store.object-serialization` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-009 | Object waits resolve from durable state/index facts rather than private callbacks. | `store.object-state-wait`, `store.object-index-wait` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-010 | Host recovery can resume due work after crash/restart using durable facts, not process memory. | `store.host-crash-restart`, `store.host-tick`, `store.object-cross-host` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-011 | Delayed sends are represented as durable starts and are drained by host/object recovery. | `store.object-delayed-send`, `store.service-delayed-send` | `registered` in `apps/proofs/proofs/main.ts`; CI execution gap. |
| INV-012 | Subject-scoped history uses exclusive tail versions: expected-version append returns the new tail, stale append reports the actual tail and winning record, and fold-to-target applies records through that target tail in append order. | `foundation.subject-history` | `ci-compile` via `Firegrid.Fable.slnx`; proof execution script gap. |
| INV-013 | A `StateView` strong read observes all records through the checked tail, while an eventual read exposes only the local applied snapshot and pump failures terminalize later reads. | `foundation.state-view` | `ci-compile` via `Firegrid.Fable.slnx`; proof execution script gap. |
| INV-014 | `KvStore` put/delete operations append durable facts, strong reads catch up through the view, and an append acknowledgment is not hidden by a later local projection failure. | `foundation.kv-store` | `ci-compile` via `Firegrid.Fable.slnx`; proof execution script gap. |

The bridge intentionally names proof families, not a required proof runner. An
alternate implementation can claim the same invariant by publishing equivalent
evidence under the same invariant ID.

## Level 0: Log and Projection

A Level 0 implementation provides:

```txt
durable log
record append/read
materialized projections
stream-first client observation
```

## Level 1: Intent and Runtime Operators

A Level 1 implementation adds:

```txt
intent records
claimed work operators
runtime side effects
terminal records
restart-safe replay
```

## Level 2: Agent Sessions

A Level 2 implementation adds:

```txt
agent protocol adapters
session lifecycle
prompt dispatch
streaming updates
session projections
```

## Level 3: Durable Workflow Primitives

A Level 3 implementation adds:

```txt
durable promises / awaitables
required actions / approvals
timers
subscriber operators
```

## Level 4: Provider and Resource Plane

A Level 4 implementation adds:

```txt
provider provisioning
sandbox resources
local stdio agents
network agents
resource projections
```

## Level 5: Conductor / Middleware Plane

A Level 5 implementation adds:

```txt
proxy chains
middleware/policy components
protocol-aware conductor routing
trace/mutation/proxy semantics
```

A system may be useful at any level.

## 29.6 Conformance Tests

This RFC defines test families, not a required test harness. A conforming implementation **SHOULD** publish which tests it passes at each compliance level.

Prototype-tier tests for Level 0 and Level 1:

| Test family | Required assertion |
| --- | --- |
| Log append/read | Appended records are acknowledged with stable cursors and replay in order. |
| Envelope shape | Records expose type, key/subject, value, and headers with schema and producer identity. |
| EOF/live tail | A reader can catch up to EOF, then observe a later append without losing records. |
| Idempotent append | Duplicate producer append returns original result or documented conflict. |
| Projection rebuild | Projection rows rebuild from the retained log to the same logical state. |
| Snapshot-first wait | A wait resolves from snapshot if the terminal row already exists, otherwise subscribes after the snapshot cursor. |
| Terminal winner | First valid terminal in append order wins; later conflicting terminals are surfaced as conflicts without changing projection state. |
| Projection sink authority | SQL/search/archive sink lag or schema change does not change log replay results. |
| Prompt intent ordering | Prompt intent is durably appended before any adapter dispatch side effect. |
| Claim before side effect | Prompt dispatch appends/observes a winning claim before sending through any agent adapter. |
| Claim row shape | Claim records include work_key, claim_id, owner_id, claimed_at, attempt_number, and lease/heartbeat fields when used. |
| Replay no side effects | Restart replay does not send prompts, re-run tools, or provision resources until live boundary rules allow it. |

Prototype-tier tests for Level 2:

| Test family | Required assertion |
| --- | --- |
| Protocol-neutral prompt | The same substrate prompt flow works through at least one adapter without client access to the agent wire transport. |
| Durable chunks | Agent updates are appended as durable chunk/update rows, not only emitted as transient UI events. |
| Chunk ordering | Chunk projection order follows append/projection cursor rather than timestamp fields. |
| Terminal prompt snapshot | A client started after prompt completion resolves from projection snapshot before live subscription. |
| Restart loses live ownership | After runtime restart, an existing durable session row is not treated as promptable until reattached/reloaded/reprovisioned. |
| Reattach profile | Each adapter declares no-reattach, protocol-load, replacement, or supervised-reattach policy. |
| Stale session not promptable | A duplicate idempotent launch that resolves to not-live does not permit a new prompt intent for the stale session id. |
| Adapter capability negotiation | Adapter capabilities are negotiated and represented before client-visible use. |
| Adapter metadata boundary | Durable/queryable state is in log records; adapter-private metadata is not the only recovery record. |
| ACP adapter, if implemented | `session/new`, `session/load` or equivalent load support, `session/prompt`, `session/update`, `session/request_permission`, and `session/cancel` mappings follow the ACP reference pages cited in §37; `session/prompt` is not sent until prompt intent and claim requirements pass. |

Prototype-tier tests for Level 3:

| Test family | Required assertion |
| --- | --- |
| Permission pending row | A required action appends a pending `permission.requested` row with permission/session/scope identity. |
| Permission resolution | A resolution row resumes the waiting operation through projection/log observation, not a private callback. |
| Permission timeout | Timeout appends a durable terminal permission state through the documented timeout appender and the waiter observes it. |
| Permission denial behavior | Denial maps to documented adapter behavior: tool failure, prompt failure, or continued prompt execution with denial result. |
| Restarted suspension | Pending approval/tool/prompt wait after runtime restart receives a durable reattach/recovered or terminal record according to adapter profile. |
| Durable promise replay | A waiter reconstructed after completion resolves from existing completion records. |
| Timer restart | An overdue timer after restart fires once and does not refire if a fired/completion record already exists. |
| Choreography trace | `sleep`, `wait_for`, `spawn`, `spawn_all`, `schedule_me`, and `execute` invocations emit durable trace/session records. |
| Choreography semantics | Choreography tools lower to timer, wait, child-session, prompt-schedule, or sandbox-tool records as specified in §6.3. |
| Agent introspection | An agent can read its own authorized choreography/tool trace history through a stream-derived query/tool surface. |

Prototype-tier tests for Level 4 and Level 5:

| Test family | Required assertion |
| --- | --- |
| Provider lifecycle | Provision, ready, stop, cleanup, and failed states are durable and rebuildable. |
| Provider handle boundary | A provider handle is not treated as durable promptability proof after restart. |
| Tool descriptor freeze | Tool descriptors are validated/frozen before session initialization and remain stable for session lifetime. |
| Tool descriptor shape | Agent-visible descriptors contain `name`, `description`, and `inputSchema` only; transport and credential references are host-side capability plumbing. |
| Tool attachment collision | Same-name tool attachments resolve by a deterministic replay-stable policy, such as first-valid-attach-wins. |
| Tool attachment boundary | Tool publication is a topology/conductor component behavior; provider or transport execution is not exposed as the descriptor. |
| Middleware lowering | A serializable approval middleware spec lowers to a named `approval_gate` topology/runtime component. |
| Middleware purity | Middleware authoring performs no IO/service-discovery/secret resolution and preserves declared order. |
| Deterministic topology ids | Replaying the same topology spec produces stable distinct component ids, including repeated middleware kinds. |
| Middleware durability | The approval gate appends required-action records and observes resolution records through the log/projection. |
| Conductor ordering | A conductor chain routes through deterministic predecessor/successor ordering. |
| Capability mutation audit | Application-visible capability changes are projected or auditable. |

Model-to-code and architecture-drift guard tests:

| Test family | Required assertion |
| --- | --- |
| Model replay equivalence | Executable models or semantic tests cover append dedupe, replay suffix equivalence, and first-resolution stability. |
| Runtime trace validation | Representative runtime traces are accepted by the matching model or semantic oracle when the implementation claims that guard. |
| No side effects during replay | Architecture tests or model-to-code checks prove replay paths do not execute prompt/tool/provider side effects. |
| Retired public surfaces | Forbidden-token or import-boundary guards prevent direct use of retired/bypassed public surfaces. |
| Boundary drift | Changes to adapter, provider, middleware, or client boundaries update the relevant guard or explicitly defer it. |
| Tool component guard | Static guards prevent owned tool components from bypassing the shared topology publication path. |
| Protocol-clean adapter stream | Stdio-like adapters keep protocol output free of logs/traces and export observability on a separate channel. |

Implementations SHOULD also run negative tests for the anti-patterns in §33, especially direct client transport use, side effects during replay, unclaimed multi-worker side effects, hidden mailbox-to-session prompt injection, and missing restart decisions for pending suspensions.

---
