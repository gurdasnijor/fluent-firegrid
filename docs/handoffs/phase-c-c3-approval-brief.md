# Dispatch Brief: Phase C / C3 — the days-long approval gate

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE C green-making #2 (C2 #130 merged → 41→44 green incl.
flips; session spine live)
Branch: NEW `c3/approval-gate` from post-#130 main
Architect: this session — escalate on anything marked GATE
Merge gate: architect review

## Mission

Green `t2.days-long-approval` (FROZEN body in
`apps/proofs/GridLawProofs.fs` — the spec): an agent calls a gated
tool → the turn parks on a typed approval signal; the park pins no
process (worker stopped, nothing anywhere); an approve arrives from a
FRESH connection (`Session.Approve token true`); the turn wakes; the
gated tool executes exactly once, and only after approval. The
ratified `token=` mechanic in the parked-approval event is part of the
frozen law.

Target scoreboard: **49 registered — 42 green, 7 expected-red, 0
errors.**

## Lowerings (per the contract annotations)

- `Tool.gated approvalPrompt tool` → wraps the tool's step call: emit
  the `WaitingFor` AgentEvent carrying the `token=` mechanic, then park
  on a typed approval signal ([→ Signal.Await]; L2 signal-to-parked is
  green). On approved=true → execute the underlying journaled step
  exactly once; on approved=false → the law body defines the required
  behavior — read it, don't guess.
- `Session.Approve token approved` → [→ run.Signal on the approval
  signal] resolved via the session entity's live-turn (fresh
  `Grid.connect` must work — no in-process state).

New code in `src/Firegrid.Grid/InternalApproval.fs` (packet file).
Touching the C2 tool-call path in `InternalSessions.fs` is permitted
ONLY where the call path must branch on gated tools — keep it to the
minimal seam and list every such edit in the PR body.

## Freezes and scope guards

- All law bodies FROZEN. `Firegrid.Grid.fs` signatures FROZEN (section
  bodies only). Platform FROZEN — this law lowers to green L2
  capabilities (signals, steps, entities); a genuine gap is a GATE
  report with specifics, never an edit. Consumer purity: single
  ProjectReference (GATE).
- `targets.json`: exactly 1 flip. t1 stays 22/22; the three C2 t2 laws
  stay green (you're editing their file's seam — re-run them).

## Milestones

- **M1** — gated wrap + park + WaitingFor/token emission; law
  progresses past the park (push + draft PR immediately).
- **M2** — Approve path end-to-end from a fresh connection; law green
  via `proof run`; C2 laws re-verified green.
- **M3** — flip + full `pnpm run check` (blocking foreground; expect
  49/42/7/0) + PR body (lowering summary, seam edits list, scoreboard
  tail). Mark ready.

## Operating rules

Standard set: fresh worktree, `git fetch` first,
`SKIP_SIMPLE_GIT_HOOKS=1`, never `git add -A`, Fable traps,
parked-signal flake → re-run before blaming, full checks BLOCKING
FOREGROUND, never stop with a run in flight.

## Exit criteria

1. Full check 49 — 42 green, 7 expected-red, 0 errors (t1 22/22, C2
   laws green).
2. The law green via its frozen body; approval park pins no process;
   fresh-connection approve works.
3. PR ready. NOT merged.
