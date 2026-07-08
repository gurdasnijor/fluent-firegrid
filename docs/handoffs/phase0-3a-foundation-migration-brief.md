# Dispatch Brief: Packet 0.3a — migrate the 28 foundation proofs 1:1 into apps/proofs

Doc-Class: dispatch-brief
Date: 2026-07-08
Packet: PHASE 0.3a (serial queue head; P0.2 merged as #124, main green)
Branch: `p0/foundation-migration` · Draft PR immediately after first commit
Architect: this session — escalate on anything marked GATE
Merge gate: architect review (no human ratification needed — foundation
proofs are not ratified-frozen law bodies)

## Mission

Move all 28 foundation proofs (15 `Foundation*.fs` files in
`src/Firegrid.Foundation.Proofs/`) into the `apps/proofs` harness **1:1 —
zero verify-logic changes** — register them in the ratchet as suite
`foundation`, and delete the `src/` project. Consolidation (~28→~13) is
explicitly NOT this packet — it is 0.3b, after this lands. Read:
[`docs/proofs-inventory.md`](../proofs-inventory.md) section B (the 28
rows), [`targets-README.md`](../../targets-README.md), and PR #124's
shape (atomic-swap precedent).

## The one inviolable rule

**This is a pure move.** Proof bodies transfer content-identical modulo
mechanical adaptation only: file location, registry hookup, module
opens/refs, suite tagging. Any change to a workload, a check, an
expected value, or a proof id is a GATE — stop and escalate. The review
will be a move-diff; make it diff-clean.

## Rulings

- Proof files move to `apps/proofs/` keeping their filenames; the 28
  existing proof ids are UNCHANGED (`foundation.subject-history`,
  `state.checkpoint-rebuild-equivalence`, `durable.continue-as-new`,
  `session.lifecycle-single-writer`, `wake.timer-exactly-once`, etc.).
- Keep namespace `Firegrid.Foundation.Proofs` (final rename is Packet
  0.5; do not add churn here).
- Registry: tag all 28 with suite `foundation`; `proof targets
  foundation` (generalized in 0.2 M0) runs them and emits one
  `{id, pass}` line each.
- `targets.json`: add suite `foundation` (command
  `node apps/proofs/dist/Main.js proof targets foundation`) + 28 target
  entries, `wp: "P0"`, `status: "green"` — they pass today and already
  block CI via `pnpm run proofs`, so green-on-entry is honest (t0
  precedent; no red→green ceremony applies).
- **No double-running**: in the SAME commit that registers the suite,
  delete `src/Firegrid.Foundation.Proofs/` (project, package.json,
  Program.fs) and drop the foundation leg from the root `proofs` script
  (`pnpm run proofs` keeps only the legacy TS leg until 0.4). slnx and
  `fable:build` updated. Atomic-swap pattern exactly as PR #124.
- Untouched: the 22 migrated corpus laws, `p0.harness-kill-demo`,
  `apps/proofs-legacy/`, all `src/` product code (GATE).

## Milestones (push after FIRST commit; draft PR immediately; push per milestone)

- **M1** — move the state/foundation cluster (~7 files: SubjectHistory,
  StateView, StateReads, SessionHistory, KvStore, Checkpoint×3) +
  registry entries; verify each via `proof run <id>`.
- **M2** — move the durable/session/wake cluster (~8 files:
  DurableKernel, DurableDebts, ParallelActivities, TurnStream,
  SessionLifecycle, ResumeArtifact, WakePath) + registry entries;
  verify each via `proof run <id>`.
- **M3** — atomic swap commit: targets.json suite + 28 green targets +
  `src/Firegrid.Foundation.Proofs/` deletion + root-script/slnx/lockfile
  cleanup.
- **M4** — full `pnpm run check` completed in-session. Expected
  scoreboard: **53 registered, 53 reported — 44 green, 9 expected-red,
  0 errors**. PR body: file move map, the complete list of mechanical
  adaptations (and nothing else), scoreboard tail. Mark ready.

## Operating rules

Same as P0.1/P0.2 (fresh worktree, `git fetch` first,
`SKIP_SIMPLE_GIT_HOOKS=1`, never `git add -A`, Fable traps, wait for
checks in-session — do not stop with a run in flight).

## Exit criteria

1. `pnpm run check` green: 53/44/9/0.
2. `src/Firegrid.Foundation.Proofs/` no longer exists; `apps/proofs`
   serves suite `foundation`; 28 ids preserved.
3. Move-diff cleanliness: proof bodies content-identical modulo the
   listed mechanical adaptations.
4. PR ready, NOT merged.
