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
- `not-active` means historical proof source remains in the repo as a porting
  reference, but it is not in the active runner registry because its product
  surface has moved or retired.

Rows with `registered`, `ci-compile`, or `not-active` evidence are traceable but
remain automation gaps. They must not be cited as fully conformance-green until
the listed evidence is promoted to `ci-green` or accompanied by a recorded green
run.

Evidence driver:

- `upstream-sdk` means the proof drives S2 through `@s2-dev/streamstore` as an
  external substrate oracle. It proves the S2 guarantee, not Firegrid client
  conformance.
- `foundation-fsharp` means the proof drives the F# Foundation proof runner
  emitted through Fable.
- `firegrid-log` is reserved for proofs that drive Firegrid's idiomatic TS
  facade over the Fable-emitted `Firegrid.Log` client.
- `ts-package` means the proof drives a TS-zone package's public surface directly
  (no external substrate): a deterministic in-process property over adapter-facing
  data types, folds, and lowerings (the MS-C6 vocabulary and harness-adapter
  contract). These proofs assert on workload results rather than trace evidence.

The intended substrate end state is differential evidence: `upstream-sdk`
proves the S2 oracle behavior, while `firegrid-log` proves Firegrid's client
facade faithfully exposes that behavior. A divergence between the two should be
a red proof run, not a code-review-only observation.

| Invariant | Requirement | Proof evidence | Evidence driver | Evidence status |
| --- | --- | --- | --- | --- |
| INV-001 | An acknowledged append is visible to later tail/read operations in append order. | `effect-s2.capability-a.read-after-append`; `firegrid-log.capability-a.read-after-append` | `upstream-sdk`; `firegrid-log` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-002 | Restart from a persisted cursor folds exactly the durable suffix after that cursor. | `effect-s2.capability-a.cursor-fold`; `firegrid-log.capability-a.cursor-fold`; `foundation.subject-history` | `upstream-sdk`; `firegrid-log`; `foundation-fsharp` | `ci-green` in `apps/proofs` and `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-003 | Two writers appending at the same expected tail cannot both commit. | `effect-s2.capability-b.match-seq-num-contention`; `firegrid-log.capability-b.match-seq-num-contention`; `foundation.subject-history` | `upstream-sdk`; `firegrid-log`; `foundation-fsharp` | `ci-green` in `apps/proofs` and `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-004 | Fencing is explicit and cooperative: a deposed owner must be unable to commit after takeover. | `effect-s2.capability-b.fence-semantics`; `firegrid-log.capability-b.fence-semantics` | `upstream-sdk`; `firegrid-log` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. Historical `store.object-live-fencing` and `store.object-stale-owner` files are `not-active` until rehomed to current surfaces. |
| INV-005 | Workflow event appends use expected-index CAS; stale writers surface conflict without changing committed order. | `store.event-log-cas`, `store.run-lifecycle` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-006 | Runtime replay reconstructs committed workflow state from durable events without reissuing completed effects. | `store.runtime-end-to-end`, `store.awakeable`, `store.runtime-approval` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-007 | Timers and schedules are durable: overdue work is swept once after restart and does not refire after completion. | `store.timers-signals`, `store.runtime-timer-sweep`, `store.runtime-schedule-sweep`, `store.workflow-schedule` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-008 | Object state is materialized from durable facts and can be replayed/reconstructed by a new owner. | `store.object-state`, `store.object-replay-state`, `store.object-serialization` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-009 | Object waits resolve from durable state/index facts rather than private callbacks. | `store.object-state-wait`, `store.object-index-wait` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-010 | Host recovery can resume due work after crash/restart using durable facts, not process memory. | `store.host-crash-restart`, `store.host-tick`, `store.object-cross-host` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-011 | Delayed sends are represented as durable starts and are drained by host/object recovery. | `store.object-delayed-send`, `store.service-delayed-send` | no active driver | `not-active`; historical TS proof files remain as porting references and are not in the active CI registry. |
| INV-012 | Subject-scoped history uses exclusive tail versions: expected-version append returns the new tail, stale append reports the actual tail and winning record, and fold-to-target applies records through that target tail in append order. | `foundation.subject-history` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-013 | A `StateView` strong read observes all records through the checked tail, while an eventual read exposes only the local applied snapshot and pump failures terminalize later reads. | `foundation.state-view` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-014 | `KvStore` put/delete operations append durable facts, strong reads catch up through the view, and an append acknowledgment is not hidden by a later local projection failure. | `foundation.kv-store` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-015 | Turn attach contract: attach replays the durable prefix from `Seq 0`, then the live tail, then the terminal; a reader attached mid-flight observes a byte-identical chunk prefix and the same terminal as a reader attached from the start. | `session.turn-attach` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-016 | Turn terminal/closure semantics: a producer deposed by a takeover cannot append after the takeover (its stale output never enters the log), and a recovery host drives a crashed turn to a durable terminal that an attached reader observes rather than hanging. | `session.turn-crash-terminal` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Extends the `store.object-live-fencing` live-deposed-owner technique to turns (two S2 clients over one `s2Lite`; the killed producer kept live). |
| INV-017 | Turn-stream naming + idempotent create: a turn is addressed deterministically from session/turn ids; a same-identity create retry re-attaches to the one stream (never forks), and a different-identity create takes over under a new epoch, deposing prior producers. | `session.turn-idempotent-create` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-018 | A checkpointed fold reconstructs identical `(state, version)` from `latest snapshot + suffix replay` as a full fold from `Seq 0`, including across a host restart — a cold fold with no resident memory rebuilds identical state. | `state.checkpoint-rebuild-equivalence` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-019 | Two checkpointers committing at the same observed sidecar tail resolve to exactly one committed snapshot (open-CAS single-winner via the I5 `Authority.admit` Open regime); the loser is rejected `Raced`, never interleaved, and a stale-state commit (`AsOf <= latest`) is rejected `Regressed` (monotonic snapshots). | `state.checkpoint-race` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-020 | `trim` never advances past the latest committed snapshot's `AsOf` (`AheadOfCheckpoint` otherwise); a reader starting at the trim floor rebuilds equivalent state, the trim marker on the source being skipped rather than folded. | `state.trim-safety` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-021 | L1 observation vocabulary (I2) is an ACP `session/update` superset: base variants decode faithfully, `firegrid/` extensions and unrecognized `sessionUpdate` values are ignorable-by-default (the base fold is invariant to stripping them), the schema is versioned, decoding is JSON round-trip stable, and subagent output folds under its parent tool call rather than into top-level turn text. | `l1-vocabulary.schema-conformance` | `ts-package` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-022 | Harness-adapter determinism: replaying a recorded transcript through the adapter's pure lowering and its `drive` shell reconstructs an L1 record sequence and a folded state identical to the recorded fixture, deterministically across runs; a mutated transcript is detected as divergent. | `harness.fixture-replay` | `ts-package` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-023 | Harness-adapter fact-level resume-suppression: driving with a `ResumePoint` emits exactly the suffix at Version >= the exclusive-upper-bound `observedThrough`, re-emitting no already-durable fact; `observedThrough = 0` emits the whole turn and `= length` emits nothing. The side-effect-non-re-execution half of resume requires a live gateable harness and is deferred to the WP D3 adapter proofs. | `harness.resume-suppression` | `ts-package` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-024 | Turn single-writer via composition (MS-C5): two concurrent starts on one session yield exactly one live turn — the session `Authority.claim` fences the loser (its stale append fails `Deposed`) and the `AlreadyLive` policy rejects a start observing a different live turn; the one turn stream carries both hosts' committed chunks and never forks. | `session.lifecycle-single-writer` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Lifecycle is a session-actor policy over I5 + I1 — no second authority. |
| INV-025 | Durable cancel is a mailbox send (MS-C5): a `cancel` appended by a process that is not the producer (holding no session authority) is admitted on the holder's next `drive` and seals the turn to a durable `TurnTerminal.Cancelled` (with `TurnEnded Cancelled` recorded as the L2 cause on the session log); a resend — same `(source, sourceSeq)` — folds once, so there is no second terminal. | `session.lifecycle-durable-cancel` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. |
| INV-026 | Deposed producer cannot append (MS-C5): after a takeover `start` rotates the session/turn fence, the prior `LiveTurn`'s `append`/`complete` fails `Deposed` — it computes but cannot commit — while recovery drives the turn to a durable terminal an attached reader observes rather than hanging; the deposed producer's stale write never enters the log. | `session.lifecycle-deposed-producer` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Extends the `store.object-live-fencing` / `session.turn-crash-terminal` live-deposed-owner technique (two S2 clients over one `s2Lite`) to the lifecycle surface. |
| INV-027 | Subagent scoping: a Claude Agent SDK event with a non-null `parent_tool_use_id` lowers its output to `tool_call_update` content on the parent tool call plus an ignorable `firegrid/subagent` attribution, so the base fold attributes subagent work to its parent tool call and never interleaves it into top-level turn text; the fold is invariant to stripping `firegrid/subagent`. | `harness.subagent-scoping` | `ts-package` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-028 | Claude adapter determinism: replaying a recorded Claude Agent SDK transcript through the pure lowering and the D2 `drive` shell reconstructs valid, identical L1 records across runs, consistently between lowering and shell; a mutated transcript is detected as divergent. | `harness.claude.fixture-replay` | `ts-package` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-029 | Usage/cost facts: the Claude Agent SDK `result` message's token usage and `total_cost_usd` lower to an ignorable `firegrid/usage` L1 extension; the base fold is invariant to stripping it. | `harness.claude.usage-facts` | `ts-package` | `ci-green` in `apps/proofs`, executed by `pnpm run check`. |
| INV-030 | Fenced native-resume-artifact store (MS-C5): the harness resume artifact (e.g. a Claude session id) is written to a per-session register under an `Authority` fence (last-store-under-fence-wins); a writer deposed by a takeover `openWriter` fails `store` with `Deposed` — it computes but cannot commit — closing agent-ui's last-writer-wins session-store race, while an authority-free `read` returns the latest artifact (a bare fence takeover never shadows it). The **claim-then-read** convention is proven: a resuming holder that `openWriter`s (rotating the epoch, deposing any stale writer) before its re-hydration `read` cannot then observe a stale writer's late `store` — that store is fenced `Deposed` — so the stale-store-after-new-holder-read-without-claim fork is impossible; the contrast is proven positively (an authority-free read alone does not rotate the epoch). | `session.resume-artifact-fenced` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Domain binding of I5 `Authority` (FencedOwner) — no second authority. Extends the `store.object-live-fencing` live-deposed-owner technique (two S2 clients over one `s2Lite`) to the resume register. |
| INV-031 | Wake tail-latency (MS-C3): an appended wake reaches its claimed handler within a recorded bound, measured from trace evidence — the C1 router's `openCursorWithWait` poll window plus overhead, pinned at `<=3000ms`. | `wake.tail-latency` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Driven through the public `WakeShard`/`WakeRouter` surface; the injected `Drive` seam records the dispatch time. |
| INV-032 | Wake single-claim (MS-C3): two routers tailing one shard — exactly one advances the durable cursor via `Authority.claim` (FencedOwner); the loser's fenced cursor commit fails `Deposed` (cannot advance the cursor). Dispatch is at-least-once with an idempotent drive, so a not-yet-aware deposed holder's at-most-one redundant re-drive is harmless (the target's own claim makes it a no-op tick). | `wake.single-claim` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Extends the `store.object-live-fencing` two-S2-client technique to the shard cursor; architect-approved wording (Option A, PR #110). |
| INV-033 | Wake timer exactly-once (MS-C3): a due timer fires effectively exactly once across a router restart (fenced durable cursor); a not-yet-due timer survives unfired; an undecodable poison record is consumed and skipped — the cursor passes it, later wakes still dispatch, a restart re-dispatches nothing and does not re-wedge; a wake at the committed cursor survives a restart undropped and a poison at that boundary recovers without wedging (the last-scanned+1 exclusive-upper-bound cursor). | `wake.timer-exactly-once` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Folded `TimerIndex` posts `TimerFired` wakes through the public `WakeShard` surface. |
| INV-034 | Session-state read model (MS-C4): a **strong** `StateReads` read (`readLatest`, or `readThrough` a committed version) observes every commit acknowledged before the read — including a *second host's* acknowledged append — while an **eventual** read is a monotonic prefix that may lag, never ahead of a strong read, and catches up once the fold has applied through the committed tail. Consumes the P2 `StateView` unchanged. | `state.stateview-strong-read` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Two S2 clients over one `s2Lite`: the second host's append is observed by the reader's strong read. |
| INV-035 | Session history fold (MS-C4): the L2 turn index folded from B3's session log (I6, `SessionLifecycle.LifecycleFact` on `sessions/{s}/log`) rebuilds deterministically — `SessionHistory.rebuild` (latest checkpoint + suffix, I4) equals a fold-from-zero, including across a host restart; checkpointing does not change the result. `EndCause` is preserved losslessly: an idle-timeout folds to `Ended IdleTimeout`, never collapsed to `Cancelled`. History entries are pointers to turns (L2), not L1 content. | `session.history-fold` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. Driven end-to-end through `SessionLifecycle`'s public API (`start`/`cancel`/`complete`/`drive` producing the real lifecycle log); `Checkpoint.rebuild` ignores the fenced log's command records. |
| INV-036 | Session projection lag (MS-C4): a `SessionHistory` reader (A3 `StateReads`, seeded from the latest checkpoint) exposes projection staleness as data — an eventual read's `AppliedTail` is a monotonic prefix never ahead of a strong read, and a strong read observes a turn appended after the reader started. Projections are rebuildable, never alternate truth. | `session.projection-lag-observable` | `foundation-fsharp` | `ci-green` in `Firegrid.Foundation.Proofs`, executed by `pnpm run check`. The reader is seeded past the session log's seq-0 fence via the checkpoint. |

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
