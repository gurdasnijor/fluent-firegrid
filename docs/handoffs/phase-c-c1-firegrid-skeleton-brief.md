# Dispatch Brief: Phase C / C1 — firegrid layer skeleton + red T2 corpus

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE C head (dispatches once G4 #128 merges → t1 22/22)
Branch: NEW `c1/firegrid-skeleton` from post-#128 main
Architect: this session — escalate on anything marked GATE
Merge gate: architect review AND **human corpus-ratification read**
(exact ceremony of T1's PR #117: skeleton + red corpus merge together,
laws frozen from ratification onward)

## Mission

Begin the point of it all: the firegrid agent substrate, built as a TRUE
consumer of `Firegrid.Durable`. This packet delivers (1) the compiling
skeleton of the ratified end-user API and (2) the red T2 scenario
corpus that will drive its green-making — no implementation beyond
what compiling requires. Precedent: PR #117 (T1 skeleton + 22-law red
corpus).

## Sources of truth

1. `examples/firegrid/Firegrid.fs` — the RATIFIED end-user API
   (Tool/Agent/Session/Grid/TurnHandle + wait_for / wait_until / spawn /
   publish / execute), every member annotated with its platform
   lowering. Signatures FROZEN.
2. `examples/firegrid/GridExamples.fs` — the eight ratified scenarios.
3. [`docs/handoffs/platform-greenmaking-handoff.md`](platform-greenmaking-handoff.md)
   Phase C.
4. [`docs/sdds/api-layering-sdd.md`](../sdds/api-layering-sdd.md) — the
   layering doctrine (the firegrid layer consumes L2 only).

## Rulings

- **Layout**: new product project `src/Firegrid.Grid/` with contract
  file `Firegrid.Grid.fs` PROMOTED from `examples/firegrid/Firegrid.fs`
  (signature-preserving move; `GridExamples.fs` moves beside it as the
  examples file, exactly the `Firegrid.Durable`/`Examples.fs` pattern).
  `examples/firegrid/` then contains a pointer README to the promoted
  location (the ratified text lives on in git history; do not fork it).
  Bodies: `notYet` stubs except where compilation forces minimal
  plumbing.
- **Consumer purity (the doctrine)**: `Firegrid.Grid` references
  `Firegrid.Durable` ONLY — no `Firegrid.Store`, no `Firegrid.Log`, no
  kernel internals. If a lowering annotation cannot be expressed
  against the public L2 surface, that is a PLATFORM GAP — GATE:
  record the exact member and missing capability, continue with the
  rest. (These gap reports are the packet's most valuable output.)
- **Corpus**: eight scenario laws from `GridExamples.fs`, one per
  scenario (converse-across-crashes; days-long approval;
  researcher/writer choreography where sessions never meet; scheduled
  self-prompts; spawn_all fan-out; webhook ingress; live watch +
  trace-as-schedule ops; cancel), authored black-box in
  `apps/proofs/GridLawProofs.fs` against the `Firegrid.Grid` public
  surface, suite `t2-firegrid`, ALL red, registered in the same PR.
  Law ids `t2.<scenario-kebab>`.
- **Two additional laws** (pin known open semantics — draft them, the
  ratification read freezes them):
  - `t2.reserved-segment-admission` — pins G2's law-unpinned typed
    rejection of `/gen/` and `/child/` in user ids, at the Grid surface.
  - The `spawn_all` fan-out law MUST pin eager start and concurrent
    child execution (G4's recorded deltas: un-awaited spawn currently
    never starts; child fan-out is sequential — the T2 law pins what
    the ratified API annotation promises, and its green-making will
    force the platform work).
  Total: 10 laws, all red.
- **Model step**: scripted `ModelSays` stub in corpus support (a
  deterministic scripted sequence per scenario) — real Claude-adapter
  integration is T3, out of scope. The stub lives in the corpus, not in
  product code.
- **Red discipline**: every law must RUN and fail as a law
  (`pass:false`, diagnostics, no crash) — same bar the 8 T1 red laws
  met. A law that cannot even run against the skeleton is a defect in
  the law or skeleton, not an acceptable state.
- Expected scoreboard: **49 registered — 38 green, 11 expected-red, 0
  errors** (t0.wiring-red + 10 t2). t1 stays 22/22 green.

## Freezes and scope guards

- `Firegrid.Grid.fs` signatures = the ratified examples surface —
  promoting must not alter a signature (GATE). Annotations/comments
  travel with it.
- All existing law bodies, harness infra, `src/Firegrid.Durable`
  (contract AND implementation), kernel: FROZEN. This packet adds; it
  does not modify the platform. A platform gap is a GATE report, never
  a platform edit.
- `targets.json`: additive only — new suite + 10 red targets.

## Milestones (push after FIRST commit; draft PR immediately; push per milestone)

- **M1** — promotion move + `src/Firegrid.Grid` project compiling in
  the slnx/fable build (stub bodies), `pnpm run check` still green
  (existing 39 unaffected).
- **M2** — corpus support (ModelSays scripting, grid trial helpers) +
  first 4 scenario laws running red properly.
- **M3** — remaining 6 laws running red properly; suite + targets
  registered (additive).
- **M4** — full `pnpm run check` blocking foreground (expect 49/38/11/0);
  PR body: per-law one-paragraph intent summaries (the ratification
  read reads THESE first), platform-gap reports if any, scoreboard
  tail. Mark ready — architect review then HUMAN ratification gate the
  merge.

## Operating rules

Fresh worktree; `git fetch` first; `SKIP_SIMPLE_GIT_HOOKS=1`; never
`git add -A`; Fable traps; full checks BLOCKING FOREGROUND; never stop
with a run in flight.

## Exit criteria

1. Full check green: 49 — 38 green, 11 expected-red, 0 errors.
2. `Firegrid.Grid` compiles as an L2-only consumer; signature-identical
   promotion; gap reports for anything inexpressible.
3. 10 red laws run red properly; ids/status registered.
4. PR ready with ratification-oriented body. NOT merged.
