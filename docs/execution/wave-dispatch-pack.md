# Wave Dispatch Pack — Managed Sessions

Doc-Class: execution
Status: active
Date: 2026-07-07
Owner: Firegrid Architecture

Part A briefs the wave coordinator (an Opus session). Part B is the prompt
library the coordinator hands to worker agents. The ledger in
[`managed-sessions-lanes.md`](./managed-sessions-lanes.md) remains the single
source of truth; this pack is operating procedure around it.

---

## Part A — Coordinator Brief

### Mission

Run closed-loop build waves over the managed-sessions ledger: dispatch worker
agents, verify their definition-of-done, merge, dispatch dependents — repeat
until every non-E-lane work packet is done. You coordinate; you never
implement, and you never modify contract docs (canon/SDD/RFC) yourself.

### Setup (first actions, in order)

1. Load the `cmux` skill and read its doc before orchestrating anything.
2. Working repo: `~/gurdasnijor/dev/fluent-firegrid` (github
   `gurdasnijor/fluent-firegrid`). Pull main.
3. Read, in order: `docs/execution/managed-sessions-lanes.md` (ledger, gates,
   claim protocol) → `docs/sdds/managed-sessions-agent-ui-sdd.md` (capabilities
   and proof obligations) → `docs/canon/architecture/fluent/authority-and-actors.md`
   (the design target for all F#-zone work) → `AGENTS.md` (zones,
   proof-driven-development rules).
4. Worktree convention: one per WP —
   `git worktree add ~/gurdasnijor/firegrid-worktrees/<wp-slug> -b <branch>`
   from current main.

### Dispatch mechanics (cmux)

- One cmux tab per **lane** (B, A, C, D, F), named by lane. In each tab run
  `claude` in that lane's current WP worktree and paste the matching prompt
  from Part B.
- **Reuse lane tabs across waves** — when a lane's WP completes, dispatch the
  next WP in the same lane into the same session (warm context: the B agent
  that wrote the B1 surface implements B1 and then writes B2's proofs).
- Keep ≤ 6 worker agents active. Track tab ↔ lane ↔ WP ↔ PR in a scratch note.
- Monitor by checking tab output and `gh pr checks` / `gh pr view`; workers
  post handoff summaries on their PRs.

### State at handoff (2026-07-07)

| Item | Action for coordinator |
| --- | --- |
| PR #91 (P3 impl, CI-green, architect-approved) | Merge immediately |
| PR #92 (B1 surface, approved w/ 2 required changes in architect comment) | Dispatch B1-IMPL prompt to the B lane (worktree `b1-authority-durablelog-surface` still exists); verify the two fixes are folded, then merge and continue |
| PR #93 (A1 surface, approved w/ 1 required change) | Same pattern — A lane |
| PR #94 (F3 rename, in-review) | Verify docs-only + CI-green → merge |
| C1, D1 | Unclaimed — dispatch fresh |
| PRs #72, #73, #68 | Stale pre-commitment codex work — propose closure to the human; do not close unilaterally |

### Per-WP loop

dispatch → monitor → on handoff verify DoD → merge → flip ledger row if the
worker didn't → consult the wave map → dispatch whatever became unblocked.

DoD = CI-green on GitHub (never local-only) · ledger row updated in the PR ·
gates honored · proof names match the SDD's obligations · handoff summary on
the PR.

### Standing rulings (architect, 2026-07-07)

1. **Ledger rows mean the whole WP.** A surface-only merge sets
   `surface-approved`, never `done`. Reconcile any row that drifted.
2. **Stale PRs #72/#73/#68: closure authorized** — close with a one-line
   comment linking the superseding artifact (#73 → the restored F# API
   doctrine SDD; #72 → two-zone rule + P-lane ports + EffSharp strip; #68 →
   P3's ported kernel and C-lane timer proofs).
3. **Prompts are templates; ledger + PR state is truth.** Adapt Part B prompts
   to ground truth mechanically; never change scope, gates, or laws.

### Merge authority

You may merge a PR yourself when it is CI-green **and** either (a) the
architect approved it and you have verified the required changes are folded
verbatim, or (b) it is implementation/proofs against an already-ratified
surface with no shape change. Everything else escalates.

### Escalation hatches (route to the human, who loops in the Fable architect)

1. **New Target Surface (G6)** — B3, B4, A3, C1's wake-record shape (I3), D2's
   adapter contract: the worker stops at its surface commit; you post the PR
   link with a one-paragraph summary and pause that lane only.
2. **Shape change to a ratified surface (G1)** — Processor signatures, I1, I4,
   I5, the L1 schema.
3. **G3** (RFC invariants), **G5** (weakening a proof obligation), and
   **G4 — hard stop: never dispatch E-lane / agent-ui work.** E1 requires
   explicit human sign-off; it deploys to a production home system.
4. CI red twice on one WP; cross-lane conflicts workers can't rebase; a worker
   asserting canon is wrong.

Escalation format: one message — WP, PR link, the question in one paragraph,
your recommendation.

### Wave map (remaining WPs and dependencies)

| WP | Needs | Surface review by architect? | Notes |
| --- | --- | --- | --- |
| B1-IMPL | #92 fixes | No (ratified once fixes folded) | Wraps P3's fence mechanics — never a parallel impl |
| B2 | B1-IMPL | No — proofs only | Obligations verbatim from SDD MS-C2 |
| B3 | B1-IMPL | **Yes — surface stop** | Lifecycle = session-actor policy; cancel is a mailbox send |
| B4 | B1-IMPL | **Yes — surface stop** | Fenced resume-artifact store |
| A1-IMPL | #93 fix | No | Monotonicity law folded |
| A2 | A1-IMPL | No — proofs only | Drive commit through `Authority.admit` once I5 lands |
| A3 | A1-IMPL | **Yes — light** | StateView reads at the P4 seam |
| A4 | A1, B1 | **Yes — surface stop** | Session-history fold + thread index |
| C1 | — | **Yes — I3 records** | Router mechanics; leadership via I5 |
| C2 | C1 | No — proofs only | Latency bound asserted in-trace |
| D1 | — | No (G2 already decided) | Deviations re-open G2 → escalate |
| D2 | D1 | **Yes — surface stop** | Adapter contract + fixture harness (MS-C6) |
| D3 | D2 | No | Claude Agent SDK adapter over ratified contract |
| F2 | rolling | No | Conformance rows travel with each capability PR |
| E1–E5 | various | **G4 — human only** | Never dispatch |

---

## Part B — Worker Prompt Library

### Common preamble (prepend to every prompt verbatim)

> You own WP **<ID>** in `gurdasnijor/fluent-firegrid`. Work in
> `~/gurdasnijor/firegrid-worktrees/<wp-slug>` on a branch off current main.
> Read in order: `docs/execution/managed-sessions-lanes.md` (your lane, gates
> G1–G6, claim protocol), the SDD section your WP cites in
> `docs/sdds/managed-sessions-agent-ui-sdd.md`,
> `docs/canon/architecture/fluent/authority-and-actors.md` (the design target),
> and `AGENTS.md` (language zones — F# zone is EffSharp-free, sans-IO;
> Proof-Driven Development rules). Claim your ledger row (`in-progress`, your
> name) as a standalone commit before other work. Surface-first: if your WP
> creates or changes a public surface, your first commit is the Target Surface
> in the SDD/canon and you STOP for architect review. Proofs import public
> surfaces only — no deep imports, no proof-only branches. One WP, one PR,
> ledger updated in it. Done = CI-green on GitHub via `pnpm run check`, not
> local-only. Local NuGet 401: use the documented workaround in the lanes doc.
> Post a handoff summary on your PR: what shipped, validation, next
> recommended action. If you hit any gate G1–G6, stop and report — do not
> proceed.

### B1-IMPL — Authority + DurableLog implementation (warm B session)

> Your B1 surface (PR #92) is architect-approved with two required changes —
> fold them into the surface first, exactly as specified in the architect's PR
> comment: (1) `Authority.claim` takes a `HolderId`; same holder on the current
> epoch returns the same `Holder` (idempotent), a different holder rotates to
> epoch+1 (takeover). (2) State the create-on-live-address law: same holder →
> idempotent re-attach; different holder → takeover under a new epoch;
> AlreadyLive rejection is MS-C5 lifecycle policy, never log mechanism. Also
> consider the two suggestions (pin `next`'s blocking/tail semantics — 
> recommended: block with wait per `openCursorWithWait`; carry Basin/Codec in
> `Holder`/`Producer`). Then implement in `src/Firegrid.Store/Foundation/`:
> `Authority` wraps the fence/claim mechanics the P3 port landed (PR #91) —
> compose, never duplicate. `DurableLog` = SubjectHistory + Authority + seal;
> `Turn` binding with zero methods. F#-native, EffSharp-free, Fable-safe.
> No proofs in this PR — B2 follows in this same session. Ledger: B1 →
> in-review with your PR, then done on merge.

### B2 — Turn-stream proofs (same B session, after B1-IMPL merges)

> Write the three MS-C2 proof obligations, named exactly:
> `session.turn-attach` (mid-flight attach observes a byte-identical prefix,
> then the same live tail and terminal), `session.turn-crash-terminal` (kill -9
> the producer; recovery drives the turn to a durable terminal; an attached
> reader observes it rather than hanging — extend the two-process technique in
> `apps/proofs/proofs/store-object-live-fencing.ts` or its F# equivalent), and
> `session.turn-idempotent-create` (same holder retry attaches; different
> holder takes over under a new epoch, per the ratified law). Drive everything
> through the public `DurableLog`/`Turn` surface. Add conformance rows mapping
> each proof to its invariant (F2 travels with your PR).

### A1-IMPL — Checkpoint implementation (warm A session)

> Your A1 surface (PR #93) is architect-approved with one required change —
> fold it first: the monotonicity law plus a `Regressed` case in
> `CommitFailure` (`commit` rejects `snapshot.AsOf <= latest.AsOf`). Note the
> two suggestions (record that sidecar compaction is deferred; A2 must drive
> commit through `Authority.admit` once I5 lands). Then implement
> `Firegrid.Foundation.Checkpoint` in `src/Firegrid.Store/Foundation/` per the
> ratified surface, and add the `state.checkpoint-rebuild-equivalence` proof
> (fold-from-checkpoint ≡ fold-from-zero, including across a host restart).
> A2 follows in this same session.

### A2 — Checkpoint race + trim proofs (same A session)

> Write `state.checkpoint-race` (two racing checkpointers: exactly one snapshot
> commits at a given AsOf; loser observes `Raced`; regression attempts observe
> `Regressed`) and `state.trim-safety` (trim never passes the latest committed
> snapshot; a reader from the trim floor rebuilds equivalent state). If B1's
> `Authority` has merged, drive the election through `Authority.admit` — not a
> private CAS path. Conformance rows travel with the PR.

### C1 — Wake shard + tailed router (fresh agent)

> MS-C3, first slice. Your surface commit defines the wake-record schema and
> shard-stream naming — this is cross-lane interface **I3**, so STOP after the
> surface commit for architect review. Design constraints: wake records are
> pointers (subject address + reason), not payloads; the router is an actor per
> `authority-and-actors.md` (its mailbox IS the shard stream); leadership =
> `Authority` fenced-owner regime (I5, PR #92) — do not invent a third fence
> idiom; durable cursor per the fenced-checkpoint pattern; sweeps stay green as
> degraded mode. After approval: implement the tail-driven router. C2's
> latency/single-claim/exactly-once proofs come later — do not write them in
> this WP.

### D1 — L1 vocabulary schema (fresh agent)

> MS-C6, first slice. The G2 decision is already made and recorded in the
> SDD's MS-C6 section — implement it, do not re-debate it: base vocabulary is
> an ACP session-update superset (message chunks, thought chunks, tool_call +
> tool_call_update, plan); Firegrid extensions namespaced and additive
> (`firegrid/usage`, `firegrid/subagent`, `firegrid/native`); every extension
> ignorable-by-default (never load-bearing for the base fold); schema
> versioned. Deliverables: the schema (TS zone — this is adapter-facing;
> Effect-free data types), a decision-record page, and an initial fixture set
> for D2's replay harness. Any deviation from the recorded decision re-opens
> gate G2 — stop and escalate.

### F2 — rolling conformance (no dedicated agent)

> Not a standalone dispatch: every capability PR carries its own conformance
> rows (docs travel with code). Coordinator verifies this at merge time.
