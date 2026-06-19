# SDD: `cucumber-effect` - Durable Firegrid Cucumber Runner

**Status:** proposed concrete build plan.
**Pin:** `@cucumber/core` <-> `@cucumber/messages 33.0.2` <-> `@cucumber/compatibility-kit 29.2.2`.

---

## 0. Thesis

Use the programming model that already exists in `effect-s2-durable`:

```txt
Coordinator = stateless service(...)
Worker      = stateful object(...)
```

Do not introduce a generic executor layer, event bus, output queue, or outbox in the first implementation. The Cucumber-compliant runner can be built as a durable service routing work to durable keyed workers.

First goal:

```ts
runFeatures(paths, options): Stream<Envelope>
```

implemented by:

```txt
client(coordinator).run({ paths, options })
  -> returns ordered Envelope[]
  -> Stream.fromIterable(envelopes)
```

This is enough for CCK validation and for running fluent-firegrid's executable specs on the new Effect Cucumber system. Live streaming and owner-stream projection can be added later after the compliant runner exists.

---

## 1. Reuse Boundaries

Reuse Cucumber packages:

- `gherkin-streams` for feature parsing envelopes.
- `@cucumber/core` for support-code assembly, `makeTestPlan`, matching, `prepare()`, `DataTable`, undefined/ambiguous structure, and `testCase` envelopes.
- `@cucumber/messages` for all message types and id generation.
- `@cucumber/message-streams` for ndjson at the CLI/test boundary.
- `@cucumber/compatibility-kit@29.2.2` as the conformance oracle.

Reuse durable Firegrid:

- `service(...)` for the stateless coordinator.
- `object(...)` for keyed stateful workers.
- `client(...)` / `sendClient(...)` / `attach(...)` for calls.
- `run(...)` for replay-sensitive step/proof/external-effect boundaries.
- `state(...)`, `signal(...)`, `awakeable(...)`, and recovery semantics as the worker model matures.

---

## 2. Concrete Runtime Shape

### 2.1 Coordinator Service

The coordinator is a stateless durable service. It is the top-level Cucumber runner.

Coordinator responsibilities:

- own Cucumber run ordering;
- create and thread the run id / testRunStarted id;
- parse feature files;
- build support and test plan with `@cucumber/core`;
- emit/collect run-level envelopes;
- call workers for scenario attempts;
- fold statuses;
- return the final ordered `Envelope[]`.

In this repo, the first implementation lands in `packages/spec-harness/src/durable/`:

```txt
packages/spec-harness/src/durable/
  coordinator.ts      service(...) top-level runner
  worker.ts           object(...) keyed scenario actor + scenario executor
  assembly.ts         gherkin + @cucumber/core plan assembly
  messages.ts         message constructors and result mapping
  support.ts          support-code registry / DSL lowering
  run.ts              shared run shaping + in-process local runner
  runtime.ts          runFeaturesDurable public entry
  world.ts            scenario World (attach/log/link)
  cck.ts              CCK comparison helpers (normalize/reorder/strip)
  types.ts            serializable boundary types
```

The existing `packages/spec-harness/src/runtime.ts` Cucumber hook harness stays alive while this lands. There is no Firelab compatibility layer.

### 2.2 Worker Object

The worker is a keyed stateful object. Its key is the scenario attempt id `${testCaseId}:${attempt}`. Retry is a new worker key.

### 2.3 Public API

The first implementation is batch-returning: `client(coordinator).run` collects the ordered `Envelope[]` and `runFeaturesDurable` hands it back as a `Stream`. Live projection is a later optimization over the same handlers.

---

## 3. Coordinator Algorithm

The coordinator returns the canonical ordered Cucumber Messages output:

```txt
meta
-> source / gherkinDocument / pickle (discovery)
-> stepDefinition / hook / parameterType (support)
-> testRunStarted
-> testCase (one per assembled case)
-> [BeforeAll hooks]
-> per test case: testCaseStarted, (testStepStarted, [attachment], testStepFinished)*, testCaseFinished
-> [AfterAll hooks]
-> testRunFinished
```

CCK mode is the same coordinator with `scenarioConcurrency: 1` and deterministic (incrementing) ids.

## 4. Worker Algorithm

The worker executes one scenario attempt: append `testCaseStarted`, reduce the assembled `testSteps` with a run/skip mode, mapping each step's outcome to a Cucumber result (undefined/ambiguous from `@cucumber/core`, attachments captured from the World), then append `testCaseFinished`.

## 5. Support DSL

A Cucumber-shaped DSL (`Given/When/Then/Before/After/BeforeAll/AfterAll/ParameterType`) lowered onto `@cucumber/core`'s `buildSupportCode`. `@cucumber/core` owns matching and `PreparedStep`. Step bodies may be sync, return a Promise, or return an Effect (so they can reach for durable primitives); a returned generator is surfaced loudly as a failure, not a pass.

## 6. Durable Step Boundaries

Replay-sensitive work uses `run(...)` inside the step body. Durability comes from the body running inside the durable worker object, not from the execution boundary.

## 7. fluent-firegrid Spec Authoring

Target normal Cucumber feature files + support code expressing the same product behaviors and trace proofs. `defineValidation` is not preserved as a compatibility API.

## 8. CCK Gate

Per sample: run its feature + support code, capture `Envelope[]`, normalize ids/timestamps/etc. (mirroring cucumber-js's `cck_spec` `ignorableKeys` + run-hook reorder), and deep-compare against the expected `.ndjson`. Initial targets: `minimal`, `attachments`.

## 9. Milestones

1. Coordinator service + worker object; minimal CCK passes.
2. Attachments.
3. DSL correctness (generator lift, fail-loud returns, DataTable/docString, undefined/ambiguous).
4. Hooks and retry.
5. Full CCK green.
6. fluent-firegrid specs.
7. Recovery proof.

## 10. Net

```txt
coordinator = service(...)  // stateless Cucumber router
worker      = object(...)   // keyed scenario actor
```

---

## Implementation Notes (landed)

The first cut (milestones 1 and 2) is implemented in `packages/spec-harness/src/durable/` and gated by `packages/spec-harness/test/cck.test.ts` (CCK `minimal` and `attachments` pass). Deviations from the plan above, all deliberate:

- **No module-scope run cache.** Passing the live `AssembledTestCase` (with its `prepare()`/`fn` closures) across the durable call boundary is impossible — those are not serializable — and the repo guardrails forbid module-scope registries/caches. Instead, assembly (`assembleRun`) is a **pure, deterministic function of `(sources, support)`**: it threads one `IdGenerator.incrementing()` through gherkin parse -> support build -> testRunStarted -> `makeTestPlan`, so the coordinator and each worker re-assemble independently and still agree on every id. The support module (step-body closures) is captured in the per-run coordinator/worker closures (`makeCoordinator` / `makeWorker`), never serialized.
- **`@cucumber/messages 32.3.1`, not 33.0.2.** `@cucumber/core@0.9.0` is compatible with the `messages@32.3.1` / `query@13.6.0` already pinned for the existing `@cucumber/cucumber@12` harness, so the durable runner adds `@cucumber/core`, `@cucumber/gherkin`, `@cucumber/cucumber-expressions`, `@cucumber/tag-expressions` without disturbing `trace-formatter.ts` / `runtime.ts`.
- **`@cucumber/gherkin`'s `generateMessages`**, not `gherkin-streams` — the synchronous, `newId`-threaded parser is the cleaner Effect fit and yields the same source/gherkinDocument/pickle envelopes.
- **In-process local runner.** `runFeaturesLocal` runs the same assembly + scenario execution + envelope ordering as the durable coordinator, without an S2 backend. It is the CCK message gate (S2 is not available in every environment) and shares all logic with the durable path via `run.ts`.
- **Run-level `BeforeAll`/`AfterAll`** are not executed yet (no current CCK target uses them); scenario-level `Before`/`After` are assembled into each test case and run by the worker. Retry (milestone 4), full CCK (5), fluent-firegrid specs (6), and recovery (7) remain.
