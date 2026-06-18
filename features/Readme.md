---
name: spec-altitude
description: How to write executable specs (Gherkin/Cucumber features and systems-design specs) at the correct altitude for this repo. Load when writing or reviewing .feature files, acceptance criteria, trace proofs, or design specs for a package/subsystem.
license: MIT
metadata:
  focus: executable-specs
  format: gherkin + trace-proof
  version: cucumber-12
---

# Writing specs at the correct altitude

This repo proves behavior, not prose. A spec earns its place only if a machine
can run it and watch it fail. Everything below follows from one rule.

> **The one rule:** A scenario must be something the harness can execute and
> fail. If it cannot fail, it is not a scenario — it is documentation, a
> constraint, or an unwritten test. Put it where it belongs (see
> [Anti-pattern: the requirements ledger](#anti-pattern-the-requirements-ledger)).

There is exactly **one** legitimate shape for a `.feature` scenario here: an
**executable trace proof**. It drives a real public API, the production code
path emits OpenTelemetry spans, and a sibling SQL proof asserts the *shape of
the execution trace* (which spans fired, how many, in what order). See
`packages/spec-harness/README.md` for the mechanism; this guide is about getting
the **altitude** right.

---

## What "altitude" means

Altitude is the level of abstraction a step describes behavior at. The domain
here is systems software, so the stakeholder is an API consumer or operator —
not a retail customer. "Correct altitude" does **not** mean "talk like a
business user." It means: describe the **capability** in durable-execution
domain language (executions, signals, recovery, exclusivity, idempotency,
journaling), in terms that stay true when the SDK is refactored — never the
method names you happen to call.

| Altitude | Looks like | Problem |
| --- | --- | --- |
| **Too low** (implementation) | `When I call sendClient(Calculator).double through the durable send client` / `Then observed direct 42, attached 14, and deferred "ok"` | Names the SDK surface, breaks on any rename, tests several things at once. |
| **Correct** (capability) | `When a caller submits "double 7" without waiting` / `Then attaching to that execution id resolves to 14` | Names the durable-execution capability; survives SDK refactors; one behavior. |
| **Too high** (vacuous) | `When the system runs` / `Then it works` | Cannot fail meaningfully; proves nothing. |

The give-away for *too low* is a scenario title that **enumerates primitives**
(`"supports client, sendClient, attach, run, and deferred"`). That is an
inventory of the implementation, not a description of a behavior.

---

## The five rules for an executable scenario

### 1. One behavior per scenario — exactly one `When`

A scenario proves one capability. If you wrote four `When`/`And` action steps,
you have four scenarios. The trace proof depends on this: each scenario is
isolated by `firegrid.scenario.id`, and the `.sql` proof asserts the span shape
for *that one behavior*. Cramming four behaviors into one scenario makes the
proof a meaningless aggregate.

```gherkin
# ❌ four actions, one scenario — the proof can only assert a blurry sum
Scenario: stateless services support client, sendClient, attach, run, and deferred primitives
  When I call Calculator.double with 21 through the durable client
  And I send Calculator.double with 7 through the durable send client
  And I attach the sent Calculator execution
  And I resolve Calculator.deferredEcho with "ok"
  Then Calculator observed direct 42, attached 14, and deferred "ok"

# ✓ one capability per scenario — each proof asserts one trace shape
@sql:service_call
Scenario: A durable call returns its handler result and leaves a replayable journal
  When a caller doubles 21 as a durable call
  Then the call returns 42

@sql:service_send_attach
Scenario: A fire-and-forget submission can be awaited later by its execution id
  Given a caller submitted "double 7" without waiting
  When the caller attaches to that execution id
  Then it resolves to 14
```

### 2. The name is a capability, not an API inventory

The title is the behavior a reader should be able to trust. Write the
*guarantee*, not the call list.

- ❌ `objectClient and objectSendClient route to a child object`
- ✓ `A handler can call another keyed object and observe its updated state`
- ❌ `signal/resolveSignal resume a waiting service`
- ✓ `An externally resolved signal resumes a parked execution after a crash`

### 3. Steps speak the domain; data lives in `Given`

`When` is the action in capability language. Parameters and setup belong in
`Given`. Avoid SDK nouns (`durable send client`, `shared handler`) in step text
— say what the caller is *doing* (submitting without waiting, reading shared
state, approving the run).

### 4. The proof must witness the *production* path

The whole point of trace proofs is that evidence spans come from real product
code (`S2.append`, `effect-s2-durable.object.admit/drain/shared`,
`effect-s2-stream-db.commit`), never from validation-only instrumentation a test
added to make itself pass. If your scenario passes without exercising the code
path you claim, it is at the wrong altitude. (This is the same discipline
`production-readiness` calls "evidence spans must come from production code
paths.")

### 5. Recovery is proven by re-entering the runtime, not by a flag

Durability claims (a parked execution resumes after a crash) must be proven by
crossing a real runtime boundary over the same S2/S2Lite streams — see the
`object_recovery_trace` scenario, whose proof asserts a `boot-recover` span
fired and that the signal resolved exactly once. Don't simulate recovery with a
boolean.

---

## Anti-pattern: the requirements ledger

Most `.feature` files in this repo (`public-api.feature`,
`production-readiness.feature`, the `Rule:` blocks in `storage-primitives.feature`)
currently contain scenarios shaped like this:

```gherkin
Scenario: There is no public ctx object and no public Actor namespace
  Then There is no public ctx object and no public Actor namespace.
```

**This is an anti-pattern.** The `Then` restates the title verbatim, there is no
step definition that matches it, and the `proofs` profile filters to `@sql:*`
tags — so these scenarios are **never executed and cannot fail**. They are a
markdown checklist wearing a `.feature` costume. Worse than untested: they read
as covered in the Cucumber summary while asserting nothing.

The kernel they're reaching for is real — **traceability**: every public surface
should have a proof. But Gherkin scenarios are the wrong container. Each ledger
line is really one of three things, and each has a home that *actually fails the
build* — machinery this repo already runs:

| A ledger line that is really… | Example | Move it to |
| --- | --- | --- |
| A **negative / architectural constraint** | "no public `ctx`", "no parallel `Actor` surface", "schema-derived identity only", "callers must not hand-build stream paths" | **eslint / dep-cruiser / knip / a type-level test.** These break CI. (`lint`, `lint:deps`, `lint:dead` already run.) |
| A **positive behavior** | "upsert appends insert for an absent key and update for a live key", "defining a table without a primary key fails at definition time" | A **real test**: a `@sql` trace proof (S2-backed behavior) or a pure package unit test (pure/type-level). |
| A **design record / inventory** | the curated `Rule:` groupings themselves | A **markdown design spec** that links each requirement to the check that covers it. Prose, not a green scenario. |

### Migration recipe (per feature file)

1. **Sort** each `Scenario:` line into the table above.
2. **Constraints → guardrails.** Encode as an eslint `no-restricted-syntax`
   rule, a dep-cruiser rule, or a `*.type-test.ts` that fails to compile when
   violated. Delete the scenario.
3. **Behaviors → tests.** If S2-backed, write a `@sql` trace proof (rules
   above). If pure, write a package unit test. Delete the scenario.
4. **Inventory → markdown.** Move the curated prose into a design doc
   (`docs/<package>-contract.md` or the package README) where each line carries
   a **proof obligation**: a link to the eslint rule / test / trace proof that
   discharges it.
5. A requirement with no discharging check is a coverage gap — track it as one
   (an open task), not as a green scenario.

The existing ledger files are left in place until migrated; do not add new
ledger-style scenarios.

---

## Systems-design specs (before the code exists)

The guide is also for laying out a subsystem's spec at design time. Same rule
applies — a design spec is not exempt from being checkable; it just hasn't been
discharged yet.

A design spec is **prose in markdown**, structured as:

- **Capabilities** — what a caller can observably do. Each becomes a future
  `@sql` trace proof. Write them at capability altitude now so the scenario
  titles fall out for free later.
- **Invariants** — what must always/never be true (single-writer exclusivity,
  schema-derived identity, no parallel facade). Each becomes a guardrail
  (lint/type/dep rule) or a recovery trace proof.
- **Non-goals / v1 limits** — explicitly out of scope (e.g. "single-batch
  checkpoints only; framed checkpointing is a follow-up"). These are *not*
  scenarios; they're scope statements that keep a future reviewer from filing
  the gap as a bug.

Each capability and invariant carries a **proof obligation** column: how it will
be discharged (trace proof name, test, or guardrail). When the obligation is
filled, link it. The design doc and the proofs stay traceable to each other
without any scenario ever asserting nothing.

> Rule of thumb: if a design-spec line can only ever be checked by a human
> reading it, it's an invariant that needs a guardrail or a behavior that needs
> a proof — find which, and name the obligation.

---

## Worked example: `effect-s2-durable/durable-executions/durable-executions.feature`

**Before** — one scenario, four actions, names = SDK methods, `Then`
re-states internal mechanics:

```gherkin
@sql:service_trace
Scenario: stateless services support client, sendClient, attach, run, and deferred primitives
  When I call Calculator.double with 21 through the durable client
  And I send Calculator.double with 7 through the durable send client
  And I attach the sent Calculator execution
  And I resolve Calculator.deferredEcho with "ok"
  Then Calculator observed direct 42, attached 14, and deferred "ok"
```

**After** — one capability per scenario, capability-language names, each
keeping a focused `@sql` proof of its production trace shape:

```gherkin
@sql:service_call
Scenario: A service execution completes with its durable step result
  Given a service execution will double 21
  When the service execution starts
  Then the service execution result is 42

@sql:service_send_attach
Scenario: A submitted service execution keeps a stable id for later attachment
  Given a service execution was submitted without waiting for input 7
  When the caller attaches to the submitted service execution
  Then the submitted service execution result is 14

@sql:service_deferred
Scenario: An invocation-local promise resumes the same service execution
  Given a service execution has local promise payload "ok"
  When the service execution resolves its local promise
  Then the local promise result is "ok"
```

Splitting one proof into three means each `.sql` block now asserts the trace
shape for a single behavior (e.g. `service_call` asserts the
`createStream → append → commit` sequence for exactly one call), instead of a
blurry "≥3 appends across four mixed actions." That is the altitude win: the
proof becomes specific enough to actually catch a regression in one capability.

---

## Authoring & run reference

- **Write a behavioral scenario**: tag it `@sql:<name>`, add the matching
  step definitions in the feature's `*.steps.ts`, drive the **public** API
  (never internal `Actor.admit`/`drain` or validation facades), and use
  `scenarioKey(world, …)` for idempotency keys so reruns stay deterministic.
- **Write the proof**: add a `-- name: <name>` block to the sibling `.sql` file.
  It must be a single read-only `SELECT`/`WITH` whose first column (or an `ok`
  column) is truthy when the trace is correct. Use the `scenario_spans` macro to
  scope to the current scenario's spans. Details:
  `packages/spec-harness/README.md`.
- **Run the proofs**: `pnpm --filter @firegrid/spec-harness spec`
  (the `proofs` profile; runs only `@sql:*` scenarios).
- **Inventory steps** (no execution): `pnpm --filter @firegrid/spec-harness spec:inventory`.

## Checklist before committing a scenario

- [ ] It has a step definition and a `@sql:` proof — it *can* fail.
- [ ] Exactly one `When` action; one capability.
- [ ] Title names the capability/guarantee, not a list of SDK methods.
- [ ] Step text is domain language; parameters live in `Given`.
- [ ] The proof witnesses production spans, not test-only instrumentation.
- [ ] Durability/recovery claims cross a real runtime boundary.
- [ ] It is **not** a ledger entry (a `Then` that restates the title). If it is,
      route it via the [migration recipe](#migration-recipe-per-feature-file).
