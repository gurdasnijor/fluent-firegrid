# Dispatch Brief: Packet B1 — cancellation rides the inbox (kernel)

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE B item pulled forward (human-surfaced fragility; queued
after C4 #132 lands, before C5)
Branch: NEW `b1/cancel-inbox` from post-#132 main
Architect: this session — escalate on anything marked GATE
Merge gate: architect review

## Mission

Make the contract's ratified wording literally true: "cancellation
lands at the NEXT BIND BOUNDARY as a catchable value." Today the kernel
delivers cancel only at WAIT boundaries; a turn that never waits never
observes it. L3 compensates with `observeCancel` — a 150ms durable
sleep before every move whose correctness depends on tick ordering
(empirically tuned: 0ms provably loses the race). This packet moves
pending-cancel into the SAME lane every other durable input already
rides — the FIFO inbox fold that the drive loop runs before every
dispatch — and DELETES the L3 workaround in the same PR.

## The design (unification, not invention)

- The drive loop already folds admitted inbox records before
  dispatching the next node (`foundation.durable-mailbox` /
  `durable-processor`: admission before execution, commit precedes
  dispatch). Extend that fold to consult a pending cancel BEFORE
  dispatching any next effect — not only when entering/leaving a wait.
  On observation: raise `DurableCancelled` exactly as the wait-boundary
  path does today (same catchable value, same typed terminal — zero
  contract change).
- Delivery remains journaled and replay-stable: a replay that observed
  the cancel at boundary N observes it at N again (the observation is
  a journal fact, not a race).
- No API change of any kind: contract signatures untouched, no new
  primitives. Kernel file(s): the drive/dispatch path in
  `src/Firegrid.Durable/Internal.fs` only. If the change genuinely
  cannot stay within that file, GATE.

## Deletions (same PR)

- `observeCancel` and its call sites in
  `src/Firegrid.Grid/InternalSessions.fs` (the 150ms per-move tax).
- The PR body records before/after per-turn latency for a
  representative law (e.g. converse-across-crashes wall clock) — the
  tax refund, measured.

## Freezes and regression duty (this is a KERNEL packet — maximum guard)

- All law bodies FROZEN. All contract signatures FROZEN. Everything
  outside the drive-path change + the L3 workaround deletion FROZEN.
- `targets.json`: ZERO changes.
- Regression mandate: full `pnpm run check` (expect the pre-packet
  scoreboard unchanged: 49 — 44 green, 5 expected-red, 0 errors) PLUS
  `proof run` 3× each on the two cancellation laws
  (`t1.recoverable-cancellation`, `t2.cancel-live-turn`) and 1× each on
  every kill-window-sensitive t1 law (replay-determinism, saga,
  signal-to-parked, timer-across-restart) — cancel observation touches
  the hot dispatch path; treat ANY behavioral drift as your bug.

## Milestones

- **M1** — kernel change + the two cancellation laws green 3×
  (push + draft PR immediately; title: "B1: cancellation rides the
  inbox — next-bind-boundary delivery, L3 workaround deleted").
- **M2** — workaround deletion + kill-window law sweep + full check
  (blocking foreground) + latency before/after in the PR body. Ready.

## Operating rules

Standard set (fresh worktree, `git fetch`, `SKIP_SIMPLE_GIT_HOOKS=1`,
no `git add -A`, Fable traps, parked-signal flake re-run, BLOCKING
FOREGROUND checks, never stop mid-run).

## Exit criteria

1. Full check: scoreboard unchanged (49/44/5/0), t1 22/22.
2. `observeCancel` gone; cancellation observed at bind boundaries
   without timers; both cancellation laws green 3×.
3. PR ready. NOT merged.
