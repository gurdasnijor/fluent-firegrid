# Dispatch Brief: Packet 0.3b — consolidate the 28 foundation proofs into invariant families

Doc-Class: dispatch-brief
Date: 2026-07-08
Packet: PHASE 0.3b (serial queue head; P0.3a merged as #125, main green)
Branch: `p0/foundation-consolidation` · Draft PR immediately after first commit
Architect: this session — target deletes/renames below are architect-authorized; anything beyond them is a GATE
Merge gate: architect review

## Mission

Collapse the 28 foundation proofs (now in `apps/proofs`, suite
`foundation`) into ~14 invariant-family properties built on THREE shared
templates, per the ratified consolidation map in
[`docs/proofs-inventory.md`](../proofs-inventory.md) (levers L3). This
kills the estate's worst anti-pattern: the same invariant restated
bespoke per module (fencing ×7, crash-window ×4, rebuild ×5). Update the
inventory doc's dispositions as part of the packet.

## The one inviolable rule

**Consolidation deletes RESTATEMENTS, never ASSERTIONS.** Every check in
every retired proof must map to a surviving check (in its family
instantiation or a kept single). The PR body MUST carry a
28-row correspondence table: old id → new home (family/instantiation or
kept id) → assertion mapping → delta (`none` / `ADDITIVE` /
`MERGED-INTO <id>`). A row that can't honestly map is a GATE.

## The target shape (architect-designed; execute, don't redesign)

**Template 1 — `FencingLaw`**: parameterized over a surface record
{setup; ownerAct; supersede; staleAttempt; observe}. Asserts: exactly
one winner commits; the loser fails typed (Deposed/Regressed/rejected)
having committed nothing; post-state is consistent; trace-op evidence.
One ProofSpec `foundation.fencing` with SIX property instantiations:
1. checkpoint-commit (from `state.checkpoint-race`, incl. the
   stale-state→Regressed case)
2. turn-takeover (from `session.turn-idempotent-create` — same-identity
   re-attach never forks + new-identity deposes priors)
3. turn-crash-terminal (from `session.turn-crash-terminal` — deposed
   producer can't commit; recovery drives to observed terminal)
4. lifecycle-single-writer (from `session.lifecycle-single-writer` —
   racing start fenced + AlreadyLive rejection)
5. lifecycle-deposed-producer (from `session.lifecycle-deposed-producer`)
6. resume-artifact (from `session.resume-artifact-fenced`) and
   wake-claim (from `wake.single-claim`) — yes, that is seven sources;
   fold wake-claim in as instantiation 7 if the surface record fits, or
   keep it a 7th instantiation explicitly. Retires 7 proofs.

**Template 2 — `CrashWindowLaw`**: parameterized over {seed a
committed-but-not-dispatched window; crash/abandon; recover; observe}.
Asserts: the effect lands exactly-once-effective; nothing lost, nothing
duplicated; redundant recovery is idempotent; trace evidence. One
ProofSpec `foundation.crash-window` with FOUR instantiations:
continue-as-new, child-result, one-way-send (from the three
`durable.*` K1-debt proofs) and parallel-batch (from
`durable.parallel-kill-window`). Retires 4 proofs.

**Template 3 — `RebuildEquivalenceLaw`**: parameterized over {writer
ops; checkpoint/trim policy; rebuild; reference fold-from-zero; poison
variant}. Asserts: rebuild ≡ reference fold (incl. never-checkpointed
and across-restart cases); trim never crosses a committed checkpoint and
floor-rebuild is equivalent; decode/apply poison fails closed
permanently. One ProofSpec `foundation.rebuild-equivalence` with FOUR
instantiations: checkpoint-trim (merges
`state.checkpoint-rebuild-equivalence` + `state.trim-safety`),
session-history (from `session.history-fold`), state-view (from
`foundation.state-view` incl. poisoned-decode), kv-store (from
`foundation.kv-store` incl. poisoned-apply). Retires 5 proofs.

**Merge without template** — `foundation.read-lag`: one property merging
`state.stateview-strong-read` + `session.projection-lag-observable`
(strong linearizable vs eventual monotonic lagging prefix, lag bounded
and observable). Retires 2 proofs.

**Kept as-is (10 ids, bodies untouched)**:
`foundation.subject-history` (substrate OCC/cursor primitive),
`foundation.durable-replay`, `foundation.durable-mailbox`,
`foundation.durable-processor` (replay-core trio),
`durable.parallel-overlap`, `durable.parallel-fault-isolation`,
`wake.tail-latency`, `wake.timer-exactly-once`,
`session.lifecycle-durable-cancel`, and `session.turn-attach` — the last
annotated in code + inventory as SUNSET: retire when
`t1.log-attach-byte-faithful` greens (it duplicates that red law; we do
not delete coverage for a law that is not yet green).

**Resulting manifest**: foundation suite = 14 ids
(fencing, crash-window, rebuild-equivalence, read-lag + the 10 kept).
Full scoreboard: **39 registered — 30 green, 9 expected-red, 0 errors**
(t0: 2, canary: 1, t1: 22, foundation: 14). The 28→14 delete/rename of
foundation targets is architect-authorized HERE; any other manifest
change is a GATE.

**Negative controls (mandatory for the 3 templates)**: each template
gets one negative control on one instantiation — a known-bad variant
that must fail for the expected reason (e.g. fencing with the stale
attempt allowed to commit; crash-window with the recovery skipping the
dedup guard; rebuild with the poison variant silently swallowed). The
template shape makes these cheap; report.json must show
failed-as-expected.

## Milestones (push after FIRST commit; draft PR immediately; push per milestone)

- **M1** — FencingLaw template + `foundation.fencing` (7 sources
  retired, files deleted, registry + targets updated: −7 ids +1).
  Verified via `proof run foundation.fencing`.
- **M2** — CrashWindowLaw + RebuildEquivalenceLaw + read-lag merge
  (11 sources retired: −11 ids +3). Note `FoundationDurableDebtsProof.fs`
  and `FoundationParallelActivitiesProof.fs` each contain both retired
  and kept properties — split carefully; kept property bodies stay
  byte-identical.
- **M3** — negative controls ×3 + inventory-doc disposition updates
  (section B rows marked DONE with their new homes; sunset note for
  turn-attach).
- **M4** — full `pnpm run check` **run as a blocking foreground command
  and completed in-session — do NOT background it and stop** (expect
  39/30/9/0). PR body: 28-row correspondence table + scoreboard tail +
  negative-control verdicts + deviations. Mark ready.

## Freezes and scope guards

- All `src/` product code: FROZEN (GATE). Templates drive the same
  public/module surfaces the source proofs drove.
- The 22 corpus laws, `p0.harness-kill-demo`, t0, `apps/proofs-legacy/`:
  untouched.
- Kept-single proof bodies: byte-identical (move-within-file/registry
  hookup only where file splits force it).
- Harness infra: only additive template modules (e.g.
  `FencingLaw.fs`, `CrashWindowLaw.fs`, `RebuildLaw.fs`); no changes to
  Runner/Property semantics.

## Operating rules

Same as P0.1–P0.3a (fresh worktree, `git fetch`,
`SKIP_SIMPLE_GIT_HOOKS=1`, never `git add -A`, Fable traps). Known
flake: the parked-signal law flakes intermittently — re-run before
blaming your change.

## Exit criteria

1. `pnpm run check` green: 39/30/9/0.
2. Foundation suite = 14 ids; 3 template modules exist; 17 bespoke
   proofs retired with correspondence rows; kept bodies byte-identical.
3. 3 template negative controls failing-as-expected in report.json.
4. `docs/proofs-inventory.md` dispositions updated in the same PR.
5. PR ready, NOT merged.
