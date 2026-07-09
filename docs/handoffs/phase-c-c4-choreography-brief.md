# Dispatch Brief: Phase C / C4 — choreography + ingress (implements L2 Services)

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE C green-making #3 (dispatches after C3 #131 merges)
Branch: NEW `c4/choreography-ingress` from post-#131 main
Architect: this session — escalate on anything marked GATE
Merge gate: architect review

## Mission

Green TWO frozen laws — `t2.researcher-writer-choreography` (park on a
topic; a publish wakes a NEW turn carrying findings + self-prompt;
sessions never meet) and `t2.webhook-ingress` (publish acked durably
while no worker runs; the waiter wakes once with payload +
self-prompt). Target scoreboard: **49 — 44 green, 5 expected-red, 0
errors.**

## The authorized platform carve-out (READ CAREFULLY)

These laws lower through L2 **Services** (`Service.define` / `.Call` /
`.CallIdempotent`), which are contract-only `notYet` stubs with zero T1
coverage (GAP-2 from C1). Human-ratified ruling: **the T2 laws are the
pin** — the T1 corpus stays closed. Therefore, UNIQUELY in this packet:

- You ARE authorized to implement the **Services section BODIES** in
  `src/Firegrid.Durable` (and `Workflow.local` if a lowering genuinely
  needs it). Contract SIGNATURES remain frozen. Implementation goes in
  the packet's platform file `src/Firegrid.Durable/InternalServices.fs`
  (+ minimal registration hooks in existing files, each listed in the
  PR body).
- Service semantics come from the contract doc comments (read them —
  they are the ratified spec): durable admission before execution,
  `.CallIdempotent` deduplication, worker-hosted execution.
- Every OTHER platform file/area stays FROZEN. The t1 suite (22/22) is
  your regression guard — any t1 wobble is your bug.

## Lowerings (per the contract annotations)

- **Topic subscription + wake**: `wait_for` = topics entity `.Call
  (Subscribe (runId, match))` + park on typed signal; the model's
  wait_for move comes through the GridScripted seam. Wake = a NEW turn
  on the session carrying the event + self-prompt as input (the law
  pins new-turn semantics, not inline resume — read the frozen body).
- **`Grid.Publish` / `Session.Deliver`** → the Publish service: topic
  entity lists subscribers → `Run.Signal` each (fan-out). Durable ack
  before any worker involvement (webhook law stops all workers first).
- New Grid code in `src/Firegrid.Grid/InternalTopics.fs`; seam edits in
  InternalSessions.fs limited to the wait_for move branch + new-turn
  wake path, all listed in the PR body.

## Freezes and scope guards

- All law bodies FROZEN; Firegrid.Grid.fs + Firegrid.Durable.fs
  SIGNATURES frozen (section bodies only, per the carve-out); harness
  infra untouched; consumer purity intact (single ProjectReference for
  Firegrid.Grid).
- `targets.json`: exactly 2 flips.
- Regression duty: t1 22/22 AND the four green t2 laws re-verified
  (`proof run` each) — you are touching both their platform and their
  seam.

## Milestones

- **M1** — L2 Services implemented + smoke-verified via a corpus-side
  probe (push + draft PR immediately; title: "C4: choreography +
  ingress — L2 Services implemented, 2 laws green").
- **M2** — topic entity + Publish service + Grid.Publish/Deliver;
  webhook law green via `proof run`.
- **M3** — wait_for subscribe/park + new-turn wake; choreography law
  green.
- **M4** — 2 flips + regressions (t1 + 4 green t2) + full check
  (blocking foreground; expect 49/44/5/0) + PR body (Services
  implementation summary, hook list, seam list, scoreboard). Ready.

## Operating rules

Standard set (fresh worktree, `git fetch`, `SKIP_SIMPLE_GIT_HOOKS=1`,
no `git add -A`, Fable traps, parked-signal flake re-run, BLOCKING
FOREGROUND checks, never stop mid-run).

## Exit criteria

1. Full check 49 — 44 green, 5 expected-red, 0 errors (t1 22/22, all
   prior t2 greens hold).
2. Both laws green via frozen bodies; Services implemented per contract
   doc comments; sessions demonstrably never address each other (the
   law proves it — coordination is topic-mediated).
3. PR ready. NOT merged.
