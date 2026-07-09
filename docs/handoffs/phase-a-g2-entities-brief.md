# Dispatch Brief: Phase A / G2 — land the entities packet (3 laws green)

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE A item 2 (G3 merged as #121 → 33 green; queue: G2 → G4)
Branch: EXISTING `g2/entities` (PR #122, draft — take it over)
Architect: this session — escalate on anything marked GATE
Merge gate: architect review

## Mission

Finish and land the halted G2 entities implementation: green
`t1.entity-exclusive-serialization`, `t1.entity-zombie-fenced`,
`t1.entity-shared-read-nonblocking`, plus the ruled reserved-segment
admission validation (user-supplied ids may not contain the reserved
`/gen/` and `/child/` segments — K1's generation/child id scheme).
Product code per the packet file discipline: `InternalEntity.fs` +
entity contract-section bodies. Target scoreboard: **39 registered —
36 green, 3 expected-red, 0 errors** (remaining reds: t0.wiring-red +
G4's child-spawn + eternal-continueasnew).

## Context you must absorb first

1. [`docs/handoffs/platform-greenmaking-handoff.md`](platform-greenmaking-handoff.md)
   Phase A item 2. The old agent was on a final full-corpus stability
   pass at the halt — the implementation is believed near-complete.
2. PR #122's body and the `g2/entities` branch.
3. **The verifier got STRONGER since your branch was written.** The
   migrated entity laws (`apps/proofs/EntityLawProofs.fs`, FROZEN) now:
   - `entity-exclusive-serialization`: 40 op-tagged concurrent calls
     whose replies must form exactly ONE hash-chained linear history
     (FNV-1a chain carried in the law's own entity fold — user-space;
     your product code needs no chain awareness, real serialization
     makes it pass) + the original reply-set and final-state checks +
     40 operation spans as trace evidence.
   - `entity-zombie-fenced`: the pause choreography now runs through
     the harness FaultController (`PauseHost`/`ResumeHost`, SIGSTOP/
     SIGCONT) against a declared processHost.
   - `entity-shared-read-nonblocking`: as migrated.
4. Memory/doctrine that governs the implementation:
   **object exclusivity = admission control** — the single-writer
   guarantee MUST be a durable FIFO inbox on the owner stream, never an
   in-process lock (a lock loses updates across crash). If the branch's
   implementation used a lock anywhere on the exclusive path, that is a
   defect to fix, not to land.

## The work

1. Rebase `g2/entities` onto current main (post-G3). Expect
   `targets.json` conflicts (mechanical — take main's registry, re-flip
   yours at the end) and possibly contract-file section adjacency
   conflicts with G3's landed sections.
2. Reconcile the implementation with the stronger laws: run each entity
   law via `proof run <id>` and fix product code until green. The law
   bodies are FROZEN — if you believe a law is wrong, GATE.
3. Reserved-segment admission: user-supplied entity/workflow ids
   containing `/gen/` or `/child/` must be rejected with a typed
   admission error at the public surface (the ruled validation).
   If the migrated laws don't pin this (they don't — it was ruled
   post-corpus), implement it and state in the PR body that it lands
   law-unpinned; DO NOT write a new law for it (law count is frozen;
   the T2 corpus can pin it later).
4. One full `pnpm run check` (blocking foreground, ~4.5 min). Promotion
   protocol: implementation + exactly 3 `targets.json` flips in the
   same PR. No ledger row exists for G2 (same as G3) — say so.
5. Force-push to `g2/entities`, update PR #122 body (rebase summary,
   stronger-law reconciliation notes, admission validation), mark ready.

## Freezes and scope guards

- All law/proof bodies in `apps/proofs/`: FROZEN. Harness infra:
  untouched (serial Registry tag with evidence is the only allowed
  harness edit, if an entity law proves starvation-flaky in the pool).
- Contract SIGNATURES frozen; only the packet's own section bodies.
- G3's landed code and all other product areas: untouched.
- `targets.json`: exactly 3 status flips.

## Operating rules

Fresh worktree onto the EXISTING branch; `git fetch` first;
`SKIP_SIMPLE_GIT_HOOKS=1`; never `git add -A`; Fable traps; known
parked-signal flake → re-run before blaming; full checks BLOCKING
FOREGROUND; push early (rebase force-push counts) and per milestone.

## Exit criteria

1. Full check green: 39 — 36 green, 3 expected-red, 0 errors.
2. Three entity laws green via the frozen (history-checked) bodies;
   reserved-segment admission implemented; exclusive path is durable
   admission (no in-process lock).
3. PR #122 ready with rebase + reconciliation + scoreboard evidence.
   NOT merged.
