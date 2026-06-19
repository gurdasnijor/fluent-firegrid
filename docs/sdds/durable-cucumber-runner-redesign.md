# Redesign: Durable Cucumber Runner (corrected)

This supersedes the first attempt (PR #42). It records what was wrong with that
attempt and the corrected design, for agreement **before** any further code.

## What the first attempt got wrong

1. **The worker wasn't durable.** `worker.runScenario` ran every step in-process
   and returned envelopes — it journaled nothing. On recovery the engine re-runs
   a handler body top-to-bottom and expects `run(...)`/`state(...)` facts to
   replay; with no journaled steps, a crashed scenario would re-execute every
   step (including proof reads and side effects). It was an `object(...)` in
   shape only.
2. **Per-run handler definitions broke recovery.** `makeCoordinator(support)` /
   `makeWorker(support)` minted fresh `service`/`object` definitions per
   `runFeatures` call, closing over the support module. The engine registers
   handlers **by name** and re-drives them by name on boot (`recoverExecution`,
   `objectBootRecover`); a recovered process can't reconstruct a per-call
   closure, and a new runtime/layer per call is not how the engine is driven.
3. **The durable path was never exercised.** The CCK test gated
   `runFeaturesLocal`, an in-process shortcut that bypasses
   `client(coordinator) → objectClient(worker) → S2`. Message shaping was
   validated; the actual durable machinery was not.
4. **It added a runner beside the harness instead of replacing it.** The real
   "spec harness" is `cucumber.mjs` + `runtime.ts` + `trace-formatter.ts`
   running `features/**/*.feature` on `@cucumber/cucumber`, with `@sql:`
   trace-proofs over chDB spans. The goal is to run those specs *on* the durable
   runner and retire that machinery.

## Corrected model

### Handler lifecycle (stable, name-addressable)

`coordinator` (a `service`) and `scenario` (an `object`) are defined **once at
module scope**, registered through one `serviceLayer(coordinator, scenario)`
provided at the application entry. No per-run definitions, no per-run runtime.
Recovery re-drives them by name; determinism comes from journaled facts, not
from captured closures.

### Support-code threading

Step/hook bodies are closures — they cannot cross the durable boundary and must
not be captured per-run. They are **registered at module load** (the same shape
cucumber-js uses: importing the support modules registers their steps) into a
process-level support library that a module-level handler reads. On a fresh
process, boot re-imports the support modules, so the library is re-established
before recovery re-drives anything. This is recovery-safe and matches the
existing harness, which already relies on `features/**/*.ts` being imported.

> Note on guardrails: the no-module-registry guardrail targets *durable-authority*
> caches (runs/completions/claims/event planes). A support **library** is code,
> not durable run state, and is re-derived from imports on every boot. I'll name
> and shape it so that intent is clear (and confirm with you it's acceptable
> rather than smuggling it past the linter).

### Per-step durability (the boundary you chose)

The `scenario` object handler executes one scenario attempt (key =
`${testCaseId}:${attempt}`):

- **Scenario-local state** lives in `state(Table)` (durable, journaled to the
  owner stream).
- **Each effectful step** runs its side-effecting work inside `run(...)`. Proof
  steps (`Then the trace should satisfy: …`) run the chDB query inside
  `run(queryProofRows(sql), { name, output })` so a read that decides pass/fail
  is journaled and replayed, never re-issued.
- The handler's own loop (current step, accumulated statuses) is **derived**:
  on recovery the body re-runs, `run`/`state` facts replay, and the loop
  recomputes deterministically. Step order must therefore be a function of input
  + journaled results only.

### Coordinator = message authority, scenario = execution authority

To keep Cucumber message ids consistent without coordinating ids across the
durable boundary, split responsibilities:

- **Coordinator (`service`)**: parse features, build support + test plan, own
  **all** id-bearing envelopes (`meta`, discovery, support, `testRunStarted`,
  `testCase`, `testStepStarted/Finished`, `testCaseStarted/Finished`,
  `testRunFinished`). For each scenario it issues a durable child call to the
  `scenario` object (deterministic call id → dedups on replay) and maps the
  returned per-step outcomes onto envelopes using its own ids.
- **Scenario (`object`)**: given a **serializable** pickle (+ tags), reconstruct
  the runnable steps from the process-level support library (deterministic
  match — no closures crossing, no id coordination), execute each step durably
  (`run`/`state`), and return ordered per-step **outcomes** (status,
  attachments, error). It mints no Cucumber ids.

Boundary payloads are plain data: the coordinator sends the `Pickle` message
(and options); the scenario returns `{ steps: [{ status, attachments?, error? }], ... }`.
Mapping is positional — both sides derive the same step order from `(pickle, support)`.

### Trace-proofs

The `@sql:`/`scenario_spans` proof mechanism in `runtime.ts`'s `After` hook
becomes ordinary durable steps:

- The `scenario` object provides `WorldServices` (chDB/S2/OTel) via a layer
  composed **once** at the entry and wraps step execution in the
  `firegrid.scenario` span (carrying `firegrid.scenario.id`).
- A proof step body issues the chDB query inside `run(...)`; pass/fail is a
  normal step result. This replaces the bespoke `After`-hook proof loop and the
  `trace-formatter.ts` reporting (CCK-message consumers can report instead).

## Replacement plan (sequencing)

1. **Durable core, exercised.** Module-level `coordinator` + `scenario`; per-step
   `run`/`state`; drive through the real engine; keep CCK (`minimal`,
   `attachments`) as the message-compliance gate — gated through
   `client(coordinator).run`, not a local shortcut. Delete `runFeaturesLocal`.
2. **Port fluent-firegrid specs.** Re-express the `features/effect-s2-durable/*`
   specs (and the `@sql` trace-proofs) as feature files + support code on the new
   runner.
3. **Retire the old harness.** Remove `runtime.ts`, `trace-formatter.ts`,
   `cucumber.mjs`, `cucumber-tsx-register.mjs`, and `@cucumber/cucumber`; point
   the spec scripts at the durable runner.

## Open questions for you

1. **Support-code threading** — is module-level registration (re-established at
   boot) the intended mechanism, or do you have a different one in mind (e.g. a
   support library handed to the entry layer)?
2. **Coordinator/scenario split** — agree with coordinator-as-message-authority /
   scenario-as-execution-authority? Or should a run be a single `object` /
   `workflow` instead of a `service` routing to per-scenario objects?
3. **Test execution locally** — the durable path needs S2 (+ chDB for proofs),
   neither of which is in this environment. Is there an intended in-memory/test
   S2 (the `effect-s2/testing` alias points at a `TestS2.ts` that doesn't exist
   yet), or is the durable runner exercised only in CI with the `s2` binary?
4. **Scope of this branch** — land step 1 here and do 2–3 as follow-ups, or
   bigger?
