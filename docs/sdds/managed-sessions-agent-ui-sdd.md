# SDD: Managed Sessions — agent-ui as First Production User

Doc-Class: SDD
Status: active
Date: 2026-07-06
Owner: Firegrid Architecture
Substrate: S2

## Status

This SDD tracks the path from the current state — replay-model substrate proven,
TanStack scaffolding running, agent-ui on hand-rolled infrastructure — to the
target state: a managed-session (reconstruction-model) kernel exposed as a
production API, with agent-ui in `home-observability-stack` running on it in
production.

Two workstreams, one binding rule:

- **Capabilities** (platform side, this repo): no capability is done without a
  green proof in `apps/proofs`.
- **Milestones** (consumer side, agent-ui): no milestone is done without deleted
  agent-ui code and passing acceptance evidence.

Day-to-day execution is tracked in
[`../execution/managed-sessions-lanes.md`](../execution/managed-sessions-lanes.md).
This SDD is the design authority; the lanes doc is the work ledger.

## Objective

agent-ui's `app/api/chat/` ends up containing three things: ingress, attach, and
a harness adapter. Resumable turn streams, turn lifecycle (claims, fencing,
cancel, timeouts), history and thread projections, and session metadata are all
consumed from this repo's packages. The reconstruction execution model described
in [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md)
exists as a proven production surface, not only as canon.

## Document Relationships

| Document | Relationship |
| --- | --- |
| [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md) | Replay vs. reconstruction contract this SDD implements the reconstruction half of. |
| [`../canon/architecture/fluent/s2-substrate.md`](../canon/architecture/fluent/s2-substrate.md) | Primitive semantics (conditional append, cooperative fencing, read sessions, trim) all capabilities lower to. |
| [`../canon/architecture/fluent/harness-io.md`](../canon/architecture/fluent/harness-io.md) | Role map for the C6 adapter contract. |
| [`../requirements/agent-ui-stress-test.md`](../requirements/agent-ui-stress-test.md) | The consumer requirements this SDD schedules. Requirement gaps found during milestones flow back there. |
| [`../rfc/agent-substrate/operating/conformance.md`](../rfc/agent-substrate/operating/conformance.md) | Neutral invariant statements. Each capability lists the invariants it adds or proves. |
| [`fluent-firegrid-finish-line-sdd.md`](./fluent-firegrid-finish-line-sdd.md) | Authored-procedure ergonomics and native-kernel direction. Explicitly not this SDD's scope; shared kernel modules are coordinated through the lanes doc. |
| [`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md) | Frozen scaffolding. This SDD adds no features to the TanStack path. |

## Current State

**Proven** (green in `apps/proofs`):

- Substrate capabilities: read-after-append visibility, cursor-fold restart,
  matchSeqNum contention, cooperative fence semantics.
- Coordination: leases (claim / live-reject / stale-takeover), live deposed-owner
  fencing across two processes, host crash-restart recovery.
- Temporal: durable timers and schedules with sweep semantics; racing sweepers
  start exactly one run.
- Execution (replay model): journaled steps not re-executed, awakeables resolved
  by a recreated host, object state replay without double-apply, per-key
  serialization across hosts, delayed sends, send handles with cross-host attach.

**Running unproven** (agent-ui, e2e-verified 2026-07-05 but outside any proof
harness): deterministic per-turn stream ids; mid-stream client-disconnect
survival; server-side turn materialization; resume/replay of in-flight turns;
cancel-by-thread; session-id persistence per thread.

**Absent**: checkpoint + trim; turn streams as a first-class primitive;
tail-driven wakes (everything temporal is sweep/poll); harness adapter contract
and any concrete adapter; session projections; the managed-session kernel
itself.

## Vocabulary

Aligned to [`../rfc/agent-substrate/concepts/terminology.md`](../rfc/agent-substrate/concepts/terminology.md):

- **Session** — durable conversation identity (agent-ui thread).
- **Turn** — one prompt→terminal unit of agent work within a session.
- **Turn stream** — the per-turn S2 stream carrying the live output record
  sequence; closed at terminal.
- **Attach** — replay a turn stream from zero plus live-tail to terminal.
- **Materialize** — fold a finalized turn into session history.
- **Wake shard** — the S2 stream a kernel host tails to learn an entity needs
  driving.
- **Harness adapter** — protocol lowering between one agent harness and the L1
  observation vocabulary.
- **Native resume artifact** — harness-owned resume state (e.g. Claude session
  id) durably kept by the kernel, fenced per session.
- **L1 observation / L2 coordination fact** — per
  [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md).

## Non-Goals

- Authored-procedure ergonomics (finish-line SDD).
- Human-in-the-loop approvals and `wait_until` self-prompts as product features
  (next SDD; C3 wake path is their prerequisite and *is* in scope).
- Multi-tenant security hardening beyond scoped S2 access tokens.
- More than one additional harness beyond the MS-M5 smoke criterion.
- Blob/attachment claim-check implementation (C2 makes the decision; the
  implementation is deferred unless MS-M3 forces it).
- ACP conductor (editor-facing) role.

## Capability Ladder

Every capability entry has: production surface, proof obligations (falsifiable,
named), RFC invariants touched, and the consumer milestone it unblocks. Proof
names follow the house registry pattern in `apps/proofs`.

**Target Surface convention (gate G6).** Each capability carries a Target
Surface subsection — module placement, exported types and signatures, and the
laws the surface obeys. Surface language follows the two-zone rule in
[`../canon/architecture/fluent/language-and-targets.md`](../canon/architecture/fluent/language-and-targets.md):
platform capabilities (C1–C5) are F# signatures (modules, DU-typed errors,
sans-IO core), while TS-facing surfaces (the C6 adapter contract and the client
seam) use Effect shapes per `LLMS.md`. The surface is written and
architect-approved *before* proofs or implementation; proofs import only the
public surface. Signatures below are directional — the implementing WP refines
them under G6, and the merged PR updates this document to match. MS-C2 is
written out as the exemplar (in TS notation for readability; WP B1 re-expresses
it in F# — `DurableLog` is eff-firegrid's proven `SubjectHistory` plus
seal/terminal and fenced-producer semantics, so B1 extends the P2 port rather
than starting fresh); the first WP of each other capability supplies its
section at the same altitude.

### MS-C1 — Checkpoint + Trim

The deferred state-story completion: snapshot records carrying
`{state, asOfSeqNum}`, rebuild = latest snapshot + suffix replay, trim behind
committed snapshots, checkpoint races resolved under fencing.

Production surface: checkpointed fold in the substrate layer
(`src/Firegrid.Store` / `packages` seam per the package-boundary principle).

Proof obligations:

- `state.checkpoint-rebuild-equivalence` — fold-from-checkpoint equals
  fold-from-zero for the same stream, including across a host restart.
- `state.checkpoint-race` — two racing checkpointers commit exactly one snapshot
  at a given `asOfSeqNum`; the loser's snapshot is rejected, not interleaved.
- `state.trim-safety` — trim never advances past the latest committed snapshot;
  a reader starting at the trim point still rebuilds equivalent state.

RFC invariants: extends durable-log requirements with checkpoint/trim contract.
Unblocks: MS-M3.

### MS-C2 — Turn Streams

The managed-session storage primitive. Per-turn S2 stream with deterministic
naming derived from ids the client already holds
(`sessions/{sessionId}/turns/{turnId}` shape, matching agent-ui's existing
`streamIdFor`); producer-fenced appends; a terminal record plus stream closure
as EOF; attach as replay-from-zero + live tail + terminal status.

Decision (made here): the turn stream carries the token-level output record
sequence — the same wire chunks the UI consumes — so attach is byte-faithful
and folds stay off the token hot path. Block-level L1 observation facts land on
the session stream (C6), not the turn stream. Alternative (L1 facts at token
granularity) rejected for fold cost and append rate.

Decision (made here): attachments and other large payloads are stored by
reference (claim-check); turn and session streams never carry inline blobs
larger than one S2 record comfortably allows. Implementation deferred; the
record schema reserves the reference shape now.

Target Surface (exemplar — refined by WP B1 under gate G6):

The generic primitive is the *stream half* of the table/stream duality — a
sealed, single-writer, schema-coded durable log. Turn is a domain **binding**
of it (an address scheme plus chunk/terminal schemas), not a new API. The
*table half* is Lane A's checkpointed fold/`StateView` (prior art:
firegrid's `effect-durable-operators` `DurableTable` — collections with
`insert/get/query/subscribe` over a changelog); predicate-waits over tables,
already proven on the object side (`store-object-state-wait`,
`store-object-index-wait`), bridge the two halves.

```ts
// packages/fluent, module: durable/log — generic, domain-free

interface LogAddress { readonly segments: ReadonlyArray<string> } // derived, never random

class DurableLog extends Context.Service<DurableLog>()("firegrid/DurableLog", ...) {
  // Idempotent by address: a retried create attaches to the existing log.
  create: <C, T>(address: LogAddress, codec: LogCodec<C, T>) =>
    Effect<LogProducer<C, T>, LogAlreadyLiveError | SubstrateError>
  attach: <C, T>(address: LogAddress, codec: LogCodec<C, T>) =>
    Effect<LogAttachment<C, T>, LogNotFoundError | SubstrateError>
}

interface LogProducer<C, T> {
  readonly append: (chunk: C) => Effect<void, ProducerDeposedError | SubstrateError>
  readonly seal: (terminal: T) => Effect<void, ProducerDeposedError | SubstrateError>
}

interface LogAttachment<C, T> {
  readonly chunks: Stream<C, SubstrateError>      // replay-from-zero + live tail
  readonly terminal: Effect<T, SubstrateError>    // resolves at seal
}

// Turn: a binding, zero new methods. If turn work appears to need an
// operation the generic surface lacks, that is gate G1/G6, not a TurnStreams method.
const turnLog = (s: SessionId, t: TurnId) =>
  ({ address: turnAddress(s, t), codec: LogCodec.make(TurnChunk, TurnTerminal) })
```

Laws (stated and proven at the generic level): `attach` after `seal` replays
the full prefix then yields the terminal; `append`/`seal` after `seal` fail; a
deposed producer's `append` fails (fenced); two `create` calls for one address
yield one durable log. The consumer never sees S2 stream names, fencing
tokens, or sequence numbers.

Scope guard (both directions): domain semantics stay out of the generic layer,
*and* the generic layer does not grow a KStreams operator algebra (joins,
windowing, repartitioning) ahead of a consumer that demands it. The duality
surface is: log (`append/seal/attach`), table (`fold/get/query/subscribe`,
Lane A), predicate-wait (the bridge).

Proof obligations:

- `session.turn-attach` — a reader attaching mid-flight observes a
  byte-identical prefix to a reader attached from the start, then the same live
  tail and terminal status.
- `session.turn-crash-terminal` — kill -9 the producer mid-turn; recovery drives
  the turn to a durable terminal state and closes the stream; an attached reader
  observes the terminal rather than hanging.
- `session.turn-idempotent-create` — duplicate turn creation with the same
  client-supplied ids yields one stream and one producer; the retry attaches
  rather than forking.

RFC invariants: turn-stream naming, terminal/closure semantics, attach contract.
Unblocks: MS-M1.

### MS-C3 — Wake Path

Tail-driven wakes replacing sweep/poll for latency-sensitive work: per-shard
wake streams, a `readSession`-tailed router with a durable cursor, and a folded
timer index in front of the existing sweep semantics. Sweeps remain as the
degraded-mode contract and their proofs stay green.

Proof obligations:

- `wake.tail-latency` — an appended wake reaches its claimed handler within a
  bound asserted from trace evidence (bound recorded in the proof, not prose).
- `wake.single-claim` — two routers tailing the same shard: exactly one claims
  a given wake; the loser observes the claim, not the work.
- `wake.timer-exactly-once` — a due timer fires exactly once across a router
  restart; a not-yet-due timer survives the restart unfired.

RFC invariants: wake-delivery liveness statement (at least one live host per
shard; no substrate push).
Unblocks: MS-M5 timer/approval-dependent features; not on the MS-M1–M4 path.

### MS-C4 — Session State + Projections

TypeScript-surface `StateView` per the KV-demo pattern (fold + `AppliedTail`;
eventual reads local; strong reads via check-tail barrier), session history as a
C1-checkpointed fold, and the thread-index projection.

Proof obligations:

- `state.stateview-strong-read` — a strong read issued after a second host's
  acknowledged append observes that append; an eventual read may not.
- `session.history-fold` — session history materialized from L1/L2 facts equals
  the same fold replayed from zero; checkpointing does not change the result.
- `session.projection-lag-observable` — projection staleness is exposed as data
  (applied tail vs. checked tail), not hidden.

RFC invariants: projection/read-model contract (projections are rebuildable,
never alternate truth).
Unblocks: MS-M3.

### MS-C5 — Turn Lifecycle Authority

Claim, fence, cancel, and idle/max-duration timeouts as a durable protocol over
C2 + C3, replacing agent-ui's in-memory `activeRuns` map. Cancel is a durable
control fact any process can append; the producer observes it and terminates the
turn; timeouts are kernel obligations, not process-local timers.

Proof obligations:

- `session.lifecycle-single-writer` — two concurrent starts for the same
  session: exactly one live turn; the second is rejected or queued by policy,
  never a second producer on the same session.
- `session.lifecycle-durable-cancel` — cancel appended by a process that is not
  the producer terminates the turn to a durable cancelled state; duplicate
  cancel is idempotent.
- `session.lifecycle-deposed-producer` — extends `store.object-live-fencing` to
  turns: a live deposed producer cannot append turn output after takeover.
- `session.resume-artifact-fenced` — the native resume artifact (e.g. Claude
  session id) is written under the session fence; a stale owner's write is
  rejected (closes agent-ui's last-writer-wins session-store race).

RFC invariants: turn single-writer, durable cancel, resume-artifact fencing.
Unblocks: MS-M2.

### MS-C6 — Harness Adapter Contract + Claude Agent SDK Adapter

The L1 observation vocabulary and the adapter interface: `drive` (prompt in),
observe → L1 facts, native resume artifact production, and a declared
**interception capability** (gateable vs. observe-only — the durability
guarantees differ and the contract must say so). First concrete adapter: Claude
Agent SDK, including subagent scoping (`parent_tool_use_id`) and usage/cost
facts.

Decision required at C6 start (architect gate): L1 vocabulary base. Default
position: a superset of ACP session-update semantics, so future ACP harnesses
lower trivially and the UI folds one format. Record the decision in this
section when made.

Proof obligations:

- `harness.fixture-replay` — replaying a recorded harness transcript fixture
  through the adapter reconstructs an identical L1 fact sequence and identical
  folded message state (adapter determinism).
- `harness.resume-suppression` — resuming a session does not re-emit
  already-observed L1 facts and does not re-execute observed side effects; new
  facts append after the recorded prefix.
- `harness.subagent-scoping` — subagent output is attributed to its parent tool
  call, never interleaved into the top-level turn text.

RFC invariants: L1 vocabulary schema; interception-capability declaration;
resume-suppression contract.
Unblocks: MS-M4.

## Consumer Milestones

Consumer repo: `home-observability-stack` (`apps/agent-ui`). Every milestone
ships flag-gated with a one-deploy revert path (this is a production home
system), links back to this SDD's ids, and treats the 2026-07-05 e2e list as the
standing regression suite:

> deterministic streamId; server materializes the assistant turn; full content +
> terminal status survive mid-stream client disconnect; user-turn backfill;
> resume during streaming; replay after finalize; active-stream cleared on
> finalize; assistant-history writes rejected; cancel reaches a terminal state.

### MS-M1 — Attach replaces the resumable store

Consumes: MS-C2. Deletes: `resumable-store.ts`, most of `stream-runtime.ts`
stream plumbing. Evidence: regression list green over the C2 attach path; no
`assistant-stream/resumable` S2 store writes remain.

### MS-M2 — Turn lifecycle on kernel authority

Consumes: MS-C5. Deletes: in-memory `activeRuns`, cancel plumbing,
session-store write path. Evidence: regression list green; double-POST produces
one live turn in production; cancel works against a production build from a
process other than the producer; the dev-mode cancel gotcha is obsolete.

### MS-M3 — History and threads as projections

Consumes: MS-C1 + MS-C4. Deletes: `history-store` event vocabulary,
`turn-writer.ts`; materialization becomes a kernel obligation (closes the
orphaned-turn crash gap: kill the container mid-turn, the turn still reaches
history). Evidence: regression list green; history load cost bounded by
checkpoint, verified on the largest real thread.

### MS-M4 — Event loop becomes an adapter

Consumes: MS-C6. Deletes: the `switch (m.type)` lowering in `route.ts`; the UI
folds the L1 vocabulary. Evidence: regression list green; usage/cost surfaced
per turn; subagent output correctly scoped in the live UI.

### MS-M5 — Exit criterion

One week of production traffic on the full stack, plus the second-harness
smoke: a codex-CLI or raw-SDK adapter drives one turn behind the unchanged UI.
Evidence: zero data-loss incidents; the smoke turn renders correctly with no UI
changes.

## Build Order

C2 → M1 → C5 → M2 → (C1 + C4) → M3 → C6 → M4 → C3 → M5.

Each milestone de-risks the next capability with production feedback. C3 gates
only M5-era temporal features and can proceed in parallel from any point.
Capability pairs in parentheses are independent of each other.

## Implemented Assets (living)

Update this section as work lands: proof green / module shipped / agent-ui PR
merged / LOC deleted.

- (none yet)

## Acceptance Criteria

1. All MS-C1–C6 proof obligations green in CI.
2. agent-ui's chat API is ingress + attach + adapter; the five bespoke modules
   named in MS-M1–M4 are deleted.
3. The regression list passes against the new stack in production.
4. The orphaned-turn and double-POST defects are impossible-by-proof
   (`session.turn-crash-terminal`, `session.lifecycle-single-writer`), not
   merely unobserved.
5. Second-harness smoke passes with zero UI changes.
6. Every RFC invariant added by this SDD appears in
   `rfc/agent-substrate/operating/conformance.md` mapped to a green proof.

## Risks / Open Questions

- **S2 append/read latency envelope on the chat hot path** — C2's design keeps
  folds off the token path, but the turn stream itself must sustain chat-rate
  appends with acceptable attach latency; measure early in C2, not at M1.
- **L1 vocabulary base** — the C6 architect gate; wrong choice here is the most
  expensive to unwind.
- **Wake liveness statement** — no substrate push means at least one live host
  per shard; the RFC must say this plainly (same operational posture as any
  partition-leader system, but it forecloses "fully serverless" readings).
- **`fireline` profile naming** — C6 cites RFC profile pages; rename or
  re-document the suffix before C6 lands.
- **Fable/TS seam placement** — active substrate is `src/Firegrid.Log` /
  `src/Firegrid.Store` (F#→JS) with the product surface in `packages/`; each
  capability's module placement follows the package-boundary principle (stable
  seams first, packages later) and is proposed per work packet in the lanes doc.
