# Dispatch Brief: Packet 0.1 — rebuild `apps/proofs` as the F#/Fable verification harness

Doc-Class: dispatch-brief
Date: 2026-07-08
Packet: PHASE 0.1 (serial queue head; nothing else is in flight)
Branch: `p0/proof-harness` · Draft PR immediately after first commit
Architect: this session — escalate, don't improvise, on anything marked GATE

## Mission

One harness, one authoring shape. Rebuild `apps/proofs` as the single
F#/Fable proof harness for this repo, per
[`docs/handoffs/platform-greenmaking-handoff.md`](platform-greenmaking-handoff.md)
Phase 0.1 and the guideline SDD
`~/gurdasnijor/eff-firegrid/docs/proof-runner-proposal.md`. The estate
inventory and consolidation map is
[`docs/proofs-inventory.md`](../proofs-inventory.md) — read it; it scopes
what you do NOT migrate (that's 0.2–0.4, later packets).

## Verified starting state (do not re-derive)

The harness infrastructure is ALREADY in this repo, Fable-adapted and
running in CI — `src/Firegrid.Foundation.Proofs/` contains line-for-line
ports of the eff-firegrid MVP's infra: `Proof.fs` (incl. FaultController +
NegativeControl types), `ProofBuilder.fs`, `Property.fs` (incl.
negativeControl + KillHosts plumbing), `ProofOperation.fs`, `Expect.fs`,
`TraceSql.fs`, `TraceProof.fs`, `TraceExpect.fs`, `Verification.fs`,
`Reports.fs` (incl. replayCommand round-trip), `S2Lite.fs`, and a
`Program.fs` CLI (`proof list | proof run`). 28 foundation proofs run
through it today via `pnpm run proofs`.

What is genuinely missing versus the SDD and the eff-firegrid MVP
(`~/gurdasnijor/eff-firegrid/src/Proofs/`):

1. **`ProcessHost.fs`** — no processHost resource exists here; nothing can
   declare, readiness-probe, or kill a runner-owned host. NOTE:
   eff-firegrid's ProcessHost.fs is declared-but-never-exercised there
   (0/25 proofs use it) — treat it as a shape reference, NOT proven code.
   The battle-tested spawn/kill mechanics in this repo live in
   `src/Firegrid.Durable.Corpus/Harness.fs` + `Node.fs` (SIGKILL/SIGSTOP
   child hosts, s2-lite supervision — proven by 14 green kill-heavy laws).
   Marry the two: eff-firegrid's spec/lifecycle shape, corpus-proven Node
   mechanics.
2. **`Runner.fs` / `Registry.fs` split + `proof replay <report.json>`** —
   this repo's Program.fs has list/run only. Port from eff-firegrid
   (105 + 29 lines) and add replay per the SDD (reuse recorded trial id,
   preserve trial dir, re-enter compiled runner).
3. **Targets-suite mode** — a `--targets` (or `proof targets <suite>`)
   mode speaking the ratchet's JSONL protocol exactly per
   [`targets-README.md`](../../targets-README.md): one
   `{ "id": ..., "pass": bool }` line per test on stdout, diagnostics to
   stderr, exit 0 when the suite ran.
4. **The harness lives in `src/`** — Phase 0.5 makes `src/` product-only;
   0.1 rehomes the harness to `apps/proofs`.

## Rulings (architect decisions — follow, don't re-litigate)

- **Layout**: the new harness is an F#/Fable package AT `apps/proofs`
  (project e.g. `Firegrid.Proofs.Harness.fsproj`, npm name
  `@firegrid/proofs`). Infra files MOVE from
  `src/Firegrid.Foundation.Proofs/` into it;
  `Firegrid.Foundation.Proofs` gains a project reference to the harness
  and keeps its 28 proof bodies exactly where they are, running unchanged
  (their migration is Packet 0.3, not yours). Zero edits to proof bodies.
- **Legacy TS estate**: move today's `apps/proofs` wholesale to
  `apps/proofs-legacy`, rename its package to `@firegrid/proofs-legacy`,
  repoint the root `proofs` script; its 16 wired proofs must keep running
  green until Packet 0.4 dispositions them. Do not delete anything.
- **Demonstration proof (the point of the packet)**: one harness
  self-proof exercising the full missing surface end-to-end —
  `processHost` (a tiny apps/proofs-owned Node host script with a
  readiness probe) + `ctx.Faults.KillHost`/`WorkloadContext.killHost` +
  dual evidence (Expect on the workload result AND a chdb TraceSql/
  TraceProof query over the trial's `spans.jsonl`, incl.
  `verification.host.kill` with accepted-flag) + one negative control
  that fails for the expected reason (e.g. the no-kill variant fails the
  "host stopped after kill" check) + `report.json` with a working replay
  command.
- **Ratchet registration**: register suite `p0-harness` in `targets.json`
  running the new runner in targets mode, with one target
  `p0.harness-kill-demo`, status `green`, in the same PR — this is
  runner-canary class, same precedent as `t0.wiring-green`
  (targets-README "Proof of wiring"), not a ratified product law, so it
  enters green directly and stays as a permanent canary.
- **slnx/CI**: add the new fsproj to `Firegrid.Fable.slnx`; extend
  `fable:build` to build the new package. `pnpm run check` must remain
  the single verdict command and must end green.

## Milestones (push after the FIRST commit; draft PR immediately; push per milestone)

- **M0** — legacy move: `apps/proofs` → `apps/proofs-legacy`, package
  rename, root-script repoint; `pnpm run proofs` green.
- **M1** — rehome: infra files move to `apps/proofs` harness project;
  Foundation.Proofs references it; `pnpm run check` green (proofs still
  run, corpus untouched).
- **M2** — runner: Runner/Registry split, `proof replay`, targets mode;
  `p0-harness` suite + canary target registered (initially may be a
  trivial pass to prove wiring).
- **M3** — ProcessHost resource + KillHost fault path implemented on
  corpus-proven Node mechanics; readiness, env injection
  (`EFF_TRIAL_ID`-style vars per the SDD's Runner Order), lifecycle spans,
  report-level fault events.
- **M4** — demonstration proof green end-to-end (kill + negative control
  + dual evidence + replay), canary target flipped to it; full
  `pnpm run check` green ≈ includes the ~9-min corpus run.

## Freezes and scope guards

- `src/Firegrid.Durable/Firegrid.Durable.fs` signatures FROZEN (GATE).
- The 22 corpus law bodies FROZEN (GATE); their migration is 0.2.
- The 28 foundation proof BODIES: unmodified this packet (0.3). Only the
  infra files move.
- Legacy TS proofs: moved, never edited/deleted (0.4).
- No product code changes in `src/` beyond the fsproj/reference wiring
  the rehoming requires. If the rehoming forces a product-code touch,
  STOP and escalate (GATE).

## Operating rules

- Fresh worktree; commit with `SKIP_SIMPLE_GIT_HOOKS=1`; NEVER
  `git add -A` anywhere near `.claude/worktrees/`; always `git fetch`
  before branching (local main goes stale).
- Known Fable traps (cost G1 real time): inline `typeof` in generic
  contexts, attribute erasure (use `CompiledName`), rec-namespace
  monomorphization. Precedent PRs for ceremony/body shape: #119, #121.
- PR body: what moved vs what's new, the check output tail
  (targets scoreboard), and the demo proof's report.json summary.
- If a milestone stalls >1 focused attempt on the same error, record the
  error verbatim in the PR body and continue on what's unblocked; flag it.

## Exit criteria (all of them)

1. `pnpm run check` green from repo root (fable build + legacy TS proofs
   + foundation proofs through the rehomed harness + ratchet incl. the
   new `p0-harness` suite).
2. `apps/proofs` is the harness; `src/Firegrid.Foundation.Proofs/`
   contains only proof bodies + Program entry, no harness infra.
3. Demo proof: kill accepted + spans queried through chdb + negative
   control fails-as-expected + `report.json` replay command reproduces.
4. Draft PR marked ready with the ceremony above; NOT merged — architect
   reviews and merges.
