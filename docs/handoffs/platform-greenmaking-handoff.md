# Handoff: Firegrid Platform Green-Making → Firegrid Agent Substrate

Doc-Class: handoff
Date: 2026-07-08
From: architect session (Fable), context-full
Status: development HALTED by the human mid-Phase-A — resume only on their go

## Who you are and what governs you

You are the **architect**. You author contracts and corpora, rule on
escalations, review and merge; workers implement. The governing documents,
in read order:

1. [`docs/sdds/api-layering-sdd.md`](../sdds/api-layering-sdd.md) — the
   ratified platform contract (layers L0–L3, doctrine, the red/green
   execution model, migration map). **Status: active/ratified.**
2. [`docs/sdds/api-layering-decisions.md`](../sdds/api-layering-decisions.md)
   — every rationale and rejected alternative (12 decisions). Read before
   re-litigating anything.
3. [`targets-README.md`](../../targets-README.md) — the ratchet: manifest
   schema, strict rules, promotion protocol.
4. The **contract itself is code**: `src/Firegrid.Durable/Firegrid.Durable.fs`
   (+ `Examples.fs`, `examples/firegrid/` — the end-user firegrid API with
   per-member platform lowerings). Signatures are FROZEN; changing one is an
   architect gate. The 22 corpus laws in `src/Firegrid.Durable.Corpus/` are
   FROZEN; editing a law body is an architect gate.

## Exact current state (verified, not narrated)

**`main` = 14/22 laws green, CI-verified** (`targets.json` is the scoreboard;
`pnpm run check` prints it; a full run ≈ 9 min because green laws really
execute kill/replay against s2-lite).

Merged, in order: #115 (ratified contract + examples + firegrid reference
API) · #116 (T0 ratchet) · #117 (T1 skeleton + 22-law red corpus, incl. 3
ruled contract amendments: `DurableLog.Seal`, `Append→Async<Version>`,
`Step.terminal`) · #118 (K1 kernel primitives: ContinueAsNew rollover, child
workflows, one-way send — all proofed) · #119 (G1 core runtime, 13 laws) ·
#120 (K2 concurrent activity batches) · direct promotion commit
(`t1.andbang-teaching`, forced by the ratchet's unexpected-pass rule).

**Three latent kernel bugs were found and fixed by the laws** (record for
morale and for the retro): unpaginated `readLogText` truncating >1000-record
journals; shared-source highwater dedupe silently wedging out-of-order
parallel completions; adapters' mutual re-checkpointing counted as progress
so `runUntilIdle` never idles (worked around in L2 — kernel cleanup still
owed, see Phase B).

## HALTED in flight — resume exactly here (Phase A: finish 22/22)

The human stopped all agents. Their branches are pushed and intact. The old
session's agent ids are dead — **dispatch fresh agents (worktree isolation)
onto the existing branches**; old worktrees under `.claude/worktrees/` can
be pruned (`git worktree prune` + delete dirs; the directory is gitignored).

**EXECUTION MODE IS NOW SERIAL — human directive (2026-07-08).** One packet
in flight at a time, ever: dispatch → watch → review → merge → verify main
CI → only then dispatch the next. No fan-out, regardless of how parallel
the work looks. The order below is the queue, not a menu.

1. **G3 — streams (`g3/streams`, draft PR #121): 3 laws green locally**
   (`log-attach-byte-faithful`, `three-read-grades`, `cel-wait`).
   An architect ruling was delivered and was MID-IMPLEMENTATION at the halt:
   rebase onto current main; add the one-line `CelWatch.noteBasin` hook in
   `Wiring.runWorker` (Internal.fs — main's file now) so worker-only
   processes evaluate CEL waits (the interim client-surface seeding is NOT
   mergeable); retire that caveat in the CelWatch module comment + PR body;
   re-run check (expect 17/22 + t0 green); mark ready. Then review + merge
   (design note already approved: header-tagged L2 log justified by
   byte-faithfulness).
2. **G2 — entities (`g2/entities`, branch pushed, PR may not exist yet): 3
   laws green locally** (`entity-exclusive-serialization`, `zombie-fenced`,
   `shared-read-nonblocking`) + the ruled reserved-segment admission
   validation (`/gen/`, `/child/`). Agent was on a final full-corpus
   stability pass at the halt. Resume: rebase onto main (after G3 —
   `targets.json` will conflict mechanically), verify, open/ready the PR,
   review + merge.
3. **G4 — children/eternal (killed at start; branch `g4/children-eternal`
   may be empty): redispatch.** Scope: `t1.child-spawn` (CallChild /
   SpawnChild / ChildHandle.Await over K1's PerformChild + adapters) and
   `t1.eternal-continueasnew` (defineEternal / Eternal<'state> over K1's
   rollover; `Run.Result`/`Client.attach` must FOLLOW generation chains) +
   the ruled adoption of K1's one-way send for `Step.Send` (true one-way per
   the contract doc; already-green laws are the regression guard). New code
   in `InternalChildren.fs`; ceremony precedents in PR #119/#121 bodies.

Serial queue: G3 (finish ruling → merge, 17/22) → G2 (rebase → merge,
20/22) → G4 (redispatch → merge, **22/22**) → **A-final: repo-layout move
(human-ruled 2026-07-08)**: proof/corpus code does NOT belong in `src/` —
relocate `src/Firegrid.Durable.Corpus/` AND `src/Firegrid.Foundation.Proofs/`
to a root-level `proofs/` tree (`src/` = product only). Mechanical but
wide-touching (slnx, fsproj refs, CI paths, `targets.json` suite commands →
new dist paths), which is why it is sequenced AFTER all in-flight branches
merge — doing it earlier conflicts with every open packet. Each step fully
lands — merged, main CI green — before the next begins.

### Operating rules that were learned the hard way (follow them)

- **CI-green-before-merge, guarded in one command**:
  `[ "$(gh pr checks N --json bucket --jq 'all(.[]; .bucket == "pass")')" = "true" ] && gh pr merge N --merge …`
  (No branch protection exists; `gh` will happily merge on pending. Twice
  nearly bitten. RECOMMEND the human enable branch protection.)
- **Every dispatch brief mandates: push after the FIRST commit, draft PR
  immediately, push per milestone.** Silent two-hour agents are a process
  failure even when the work is good.
- **Messages to running agents deliver only at stop boundaries.** The
  working checkpoint lever is `TaskStop` → `SendMessage` (resumes from
  transcript with your directive first).
- Liveness checks without context overflow: worktree file mtimes
  (`find <worktree>/src -mmin -5`) and a bounded
  `tail -c 20000 <output> | grep -o '"timestamp":"2026[^"]*"' | tail -1`.
- **Ratchet unexpected-pass turns main red on purpose** — that is a demand
  for an explicit promotion commit, not a failure to debug.
- **Serial execution (standing human directive):** one packet in flight at
  a time; next dispatch only after the previous is merged and main CI is
  green. The per-packet file discipline stays (one `Internal<Area>.fs` per
  packet; contract-file touches only in the packet's own section bodies) —
  it keeps history reviewable even without parallelism.
- Commit with `SKIP_SIMPLE_GIT_HOOKS=1`; never `git add -A` near
  `.claude/worktrees/`.

## Phase B — platform hardening (small, recorded, post-22/22)

All architect-acknowledged follow-ups; none blocks Phase C but do them
before calling the platform "1.0":

1. Kernel `runUntilIdle` real-progress predicate (G1 worked around
   adapter-checkpoint churn in L2; fix belongs kernel-side).
2. Post `WakeReason.ChildTerminal` through the C1 wake path (liveness
   accelerator for parent wake-up; correctness floor already inbox+sweep).
3. **OpId-indexed history** (spike finding, still unscheduled): kernel
   history is a list scan, O(n²) per plan — fine for corpus scale, wrong for
   long agent sessions. Schedule before real workloads.
4. `Worker.run` currently ignores `ns` for routing (G1 delta, recorded):
   decide namespace semantics (ns in stream prefix?) — architect design
   note, small kernel/L2 change.
5. The `t1.bounded-loop` law pins journal length, not wall-clock — keep it
   that way; perf work hangs off item 3.

## Phase C — T2: build the firegrid ON the platform (the point of it all)

`examples/firegrid/Firegrid.fs` + `GridExamples.fs` are the ratified
end-user API (Tool/Agent/Session/Grid/TurnHandle + the model's choreography
vocabulary: wait_for / wait_until / spawn / publish / execute — every member
annotated with its platform lowering). T2 = implement it **as a true
consumer of `Firegrid.Durable` only** — anywhere it must reach around L2 is
a platform gap → surface amendment through the architect gate.

Shape it as the corpus pattern again: a T2 scenario corpus from
`GridExamples.fs`'s eight scenarios (converse-across-crashes; days-long
approval; researcher/writer choreography where sessions never meet;
scheduled self-prompts; spawn_all fan-out; webhook ingress; live watch +
trace-as-schedule ops; cancel), registered in `targets.json` as a `t2-*`
suite, red first, ratified by the human, then greened — serially, one packet at a time. Note: the harness
step (`agent/model`) should be stubbed/fake for T2 (a scripted ModelSays
sequence) — real Claude-adapter integration is T3 territory.

**T2 doubles as the ergonomics verdict**: the biggest open question the
machine does NOT answer is whether the API is *pleasant* (the ratchet proves
correctness only). T2 is the first real consumer; friction found there is
surface-defect evidence. After T2, run the deferred **blind-consumer test**
(fresh agent, zero context, one-page quickstart, novel task, mistake
catalog for error-message quality) — designed earlier, deprioritized in
favor of build-what-we-want, now meaningful against a working library.

## Phase D — productization & conformance

- README/quickstart for `Firegrid.Durable` (write it from `Examples.fs`;
  if it can't fit a page, that's a surface finding).
- Conformance sweep: tie the 22 laws + T2 laws back to the SDD's promises
  (the old wave's F2 pattern); every claim in the layering SDD should point
  at a green law or be edited.
- De-Effect the harness-adapter contract (T3 from the SDD — plain-TS
  contract + red fixture-replay corpus; the D-lane packages still carry
  `effect@4.0.0-beta.87`).
- Close out the managed-sessions-era docs: `managed-sessions-lanes.md` is a
  finished ledger (all non-E rows done); mark it historical.

## Phase E — parked with the human (do NOT self-start)

1. **T4: TS emission** (`@firegrid/durable` plain-TS wrapper via Fable) —
   unblocks agent-ui; human schedules. The Fable-green gate has kept it a
   build decision.
2. **fluent/TanStack retirement** at parity — checklist source: the two
   fluent SDDs (state-materialization, finish-line). Several parity items
   (awakeables-as-signals, delayed sends, table waits) are already covered
   by green laws; audit then propose.
3. **Workflow-authoring SDD** (the second consumer branch over the same
   kernel) — future design cycle.
4. E-lane / agent-ui production integration — human-gated (G4 in the old
   ledger's terms).

## The north star (so the ladder has a top)

The final API surface is already written and ratified — it is not a future
artifact: `Firegrid.Durable.fs` (platform) and `examples/firegrid/`
(agent substrate). Everything above ladders to making those two files TRUE:
Phase A makes the platform's promises machine-verified (22/22); Phase C
makes the firegrid layer real on top of it; Phase D proves a stranger can
use it. Elegance was ratified by the human on consumer code and is
preserved by the signature freeze — implementation pressure flows into the
kernel, never into the API. When someone asks "is it done," the answer is
`pnpm run check` — never a summary, including this one.
