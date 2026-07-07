# Execution Lanes: Managed Sessions → agent-ui Production

Doc-Class: execution
Status: active
Date: 2026-07-06
Owner: Firegrid Architecture
Substrate: S2

This is the work ledger for
[`../sdds/managed-sessions-agent-ui-sdd.md`](../sdds/managed-sessions-agent-ui-sdd.md)
(the design authority — read it first). Lanes are independent streams of work an
agent can own without coordinating beyond the interfaces below. Work packets
(WPs) are the unit of assignment: one WP ≈ one PR.

## Agent Onboarding

Read in this order before starting a WP:

1. This document (claim protocol, your lane, cross-lane interfaces).
2. The SDD section your WP implements (capability or milestone id).
3. The canon pages your WP cites — at minimum
   [`../canon/architecture/fluent/s2-substrate.md`](../canon/architecture/fluent/s2-substrate.md)
   and, for session work,
   [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md).
4. House proof style: `apps/proofs/README.md`, then two existing proofs as
   templates — `store-object-live-fencing.ts` (multi-process + fault injection)
   and `effect-s2-substrate-proofs.ts` (substrate property style).

Ground rules:

- **Surface-first, then proof-first.** A capability WP's deliverable is the
  Target Surface (SDD section, gate G6) *and* the proof that exercises it.
  Proofs drive production modules through their public API and verify via
  workload results plus trace evidence (`traceSql`), never via test-only
  shortcuts, deep imports, or proof-only branches in production code.
- **Stay in lane.** Do not modify another lane's modules or proofs. Shared
  contracts change only through an architect gate (below).
- **Module placement and language** follow the two-zone rule in
  [`../canon/architecture/fluent/language-and-targets.md`](../canon/architecture/fluent/language-and-targets.md):
  lanes P/A/B/C are F# in `src/` (sans-IO core rule; Target Surfaces are F#
  signatures with DU-typed errors); lanes D/E are TS (Effect shapes per
  `LLMS.md`). Cross zones only via the kernel protocol or the single
  Fable-emitted package seam. Within a zone, stable seams inside existing
  packages first; promotion later.
- **Docs travel with code.** If your WP changes a contract, update the SDD's
  Implemented Assets section and (Lane F) the conformance page in the same PR.
- **Frozen paths stay frozen.** No new features on the TanStack lowering.
- Verify with the repo root `pnpm preflight` plus the proof runner for your new
  proofs.

## Claim Protocol

Claim a WP by editing the ledger row (Owner + Status: `in-progress`) in a small
standalone commit before starting. Statuses: `open` → `in-progress` → `in-review`
→ `done` (merged, proofs green). Blocked? Set `blocked:<reason>` and move on.
One WP per agent at a time.

## Architect Gates

Escalate to the architect (do not proceed) when a WP requires:

- G1 — Changing a cross-lane interface (below) or any record schema another
  lane consumes.
- G2 — **Decided 2026-07-06**: L1 vocabulary is an ACP session-update
  superset — see the decision record in the SDD's MS-C6 section. Deviations
  from it re-open the gate.
- G3 — Adding or weakening an RFC invariant.
- G4 — Any agent-ui production deploy (Lane E milestones; flag-gated with a
  revert path, per SDD).
- G5 — Deviating from an SDD proof obligation (renaming is fine; weakening what
  it asserts is not).
- G6 — Surface sign-off: before writing proof or implementation code for a
  capability WP, the Target Surface section in the SDD must exist and be
  architect-approved (write or update it as your WP's first commit if it is
  missing or your work changes it). Proofs then import only that public
  surface. See "Proof-Driven Development" in the repo `AGENTS.md`. F#-zone
  surfaces additionally pass the Validation Gates in
  [`../sdds/fsharp-fable-effsharp-evaluation-sdd.md`](../sdds/fsharp-fable-effsharp-evaluation-sdd.md)
  (ergonomic sample, emitted-TS sample, works-without-EffSharp proof or
  documented necessity, Fable build check, package-level EffSharp decision).

Everything else is lane-owner discretion.

## Ledger

| WP | Lane | Title | SDD ref | Deps | Status | Owner | PR |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P1 | P | Port eff-firegrid `src/S2` (later rev); supersede `Firegrid.Log/S2` scaffold | canon: language-and-targets | — | done | Codex | #81 |
| P2 | P | Port `SubjectHistory`/`StateView`/`KvStore` + their F# proofs | canon: language-and-targets | P1 | done | Codex | #82 |
| P3 | P | Port `Foundation/Durable` kernel + F# proofs; audit sans-IO core/shell split | canon: language-and-targets | P2 | open | — | — |
| A1 | A | Checkpointed fold: snapshot record + rebuild | MS-C1 | P2 | open | — | — |
| A2 | A | Checkpoint-race + trim-safety proofs | MS-C1 | A1 | open | — | — |
| A3 | A | StateView strong/eventual reads exposed at the seam + proof | MS-C4 | P2 | open | — | — |
| A4 | A | Session history fold + thread-index projection + proofs | MS-C4 | A1, B1 | open | — | — |
| B1 | B | Turn stream module: naming, fenced append, terminal+close, attach | MS-C2 | P2 | open | — | — |
| B2 | B | Turn attach / crash-terminal / idempotent-create proofs | MS-C2 | B1 | open | — | — |
| B3 | B | Lifecycle authority: claim, durable cancel, timeouts + proofs | MS-C5 | B1 | open | — | — |
| B4 | B | Fenced native-resume-artifact store + proof | MS-C5 | B1 | open | — | — |
| C1 | C | Shard wake stream + tailed router with durable cursor | MS-C3 | P1 | open | — | — |
| C2 | C | Folded timer index; latency + single-claim + exactly-once proofs | MS-C3 | C1 | open | — | — |
| D1 | D | L1 vocabulary decision record + schema (**gate G2**) | MS-C6 | — | open | — | — |
| D2 | D | Adapter contract + fixture-replay proof harness | MS-C6 | D1 | open | — | — |
| D3 | D | Claude Agent SDK adapter (subagent scoping, usage facts) + proofs | MS-C6 | D2 | open | — | — |
| E1 | E | agent-ui M1: attach replaces resumable store (**gate G4**) | MS-M1 | B2 | open | — | — |
| E2 | E | agent-ui M2: lifecycle on kernel authority (**gate G4**) | MS-M2 | B3, B4, E1 | open | — | — |
| E3 | E | agent-ui M3: history/threads as projections (**gate G4**) | MS-M3 | A2, A4, E2 | open | — | — |
| E4 | E | agent-ui M4: event loop becomes adapter (**gate G4**) | MS-M4 | D3, E3 | open | — | — |
| E5 | E | agent-ui M5: soak week + second-harness smoke | MS-M5 | E4, C2 | open | — | — |
| F1 | F | Conformance bridge: number existing invariants ↔ green proofs | RFC | — | done | Codex | #83 |
| F2 | F | Per-capability invariant additions (rolling, one PR per capability) | RFC | F1 | open | — | — |
| F3 | F | Resolve `fireline` profile-suffix naming before D-lane cites it | RFC | — | open | — | — |
| F4 | F | CI runs both proof suites as blocking checks (`apps/proofs` TS + `Firegrid.Foundation.Proofs` F#); conformance evidence flips to ci-green | SDD binding rule | — | in-review | Codex | #86 |

## Lanes

### Lane P — Ports (language-and-targets decision)

Port the proven eff-firegrid F# assets into `src/` per the dispositions table
in the decision record: S2 client (later rev), `SubjectHistory`/`StateView`/
`KvStore`, then the `Foundation/Durable` kernel, each with its F# proofs. P3
includes the sans-IO audit: pure semantics separated from I/O shells, ambient
clock/randomness lifted to parameters. The `durable {}` CE port follows the
F# API doctrine's CE rules (Delay/Run program-as-data, no arbitrary task
binds, explicit `Workflow.local`, Result-shaped timeouts). Ports are refactors of proven code —
behavior changes are out of scope; anything that looks like a redesign
escalates (G1/G6).

### Lane A — State Kernel (MS-C1, MS-C4)

Checkpoint/trim and StateView/projections. Pure substrate + fold work; no
session semantics. A1/A3 are independent starts. Templates: the KV-demo pattern
as specified in the SDD's MS-C4 and the eff-firegrid foundational SDD's
`SubjectHistory`/`StateView` shapes (version = exclusive upper bound —
non-negotiable convention).

### Lane B — Turn Streams + Lifecycle (MS-C2, MS-C5)

The managed-session storage and authority primitives. B1 defines the
turn-stream record schema and naming — this is cross-lane interface I1, so its
PR needs architect sign-off (G1) before B2–B4 and Lane E build on it. The
deposed-producer proof should extend the technique in
`store-object-live-fencing.ts` (two process hosts, real kill).

### Lane C — Wake Path (MS-C3)

Tail-driven wake router + timer index. Independent of A/B/D until integration;
sweeps and their proofs remain green as the degraded mode. The latency bound in
`wake.tail-latency` is asserted from trace evidence — pick and record the bound
in the proof.

### Lane D — Harness Adapter (MS-C6)

Blocked at start by gate G2 (L1 vocabulary decision). D2's fixture-replay
harness is the lane's main artifact: recorded harness transcripts as fixtures,
deterministic adapter reconstruction as the proof. D3 must cover subagent
scoping (`parent_tool_use_id`) and usage/cost facts — both are current agent-ui
defects, so fixtures should include them.

### Lane E — agent-ui Integration (MS-M1–M5)

Lives in `home-observability-stack` (`apps/agent-ui`); tracked here. Each WP:
flag-gated, one-deploy revert, regression list from the SDD run against the new
path, links its PR back to this ledger. Friction with a platform API is
reported as a requirements delta
([`../requirements/agent-ui-stress-test.md`](../requirements/agent-ui-stress-test.md))
or a new proof obligation — never worked around locally.

### Lane F — Spec/Conformance Upkeep (RFC)

Keeps [`../rfc/agent-substrate/operating/conformance.md`](../rfc/agent-substrate/operating/conformance.md)
truthful: every invariant maps to a named green proof; aspirational pages say
so. F1 is high-leverage and unblocked today.

## Cross-Lane Interfaces

Changes to any of these require gate G1:

- **I1 — Generic `DurableLog` surface + Turn binding schema** (B1). The
  generic sealed-log API and the turn address/chunk/terminal schemas bound to
  it. Consumed by Lanes A (history fold), D (adapter emits into turns), E
  (attach). Domain methods on the binding are a G1 violation.
- **I2 — L1 observation vocabulary** (D1). Consumed by A4 (history fold) and E4
  (UI fold).
- **I3 — Wake record shape + shard naming** (C1). Consumed by B3 (lifecycle
  wakes) and future temporal features.
- **I4 — Checkpoint record shape** (A1). Consumed by A4 and any long-lived fold.

## Suggested Parallel Start

Start with the ports: P1 immediately, P2 behind it (they are refactors of
proven code — the safest warm-up for the F# zone), with F1 and D1's decision
record (gate G2) in parallel. A1/A3/B1 open when P2 lands; C1 when P1 lands;
E1 when B2 is green. B1's schema gets early architect review — it unblocks the
most.
