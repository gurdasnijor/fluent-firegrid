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

```ts
export const coordinator = service({
  name: "cucumber-effect/coordinator",
  handlers: {
    *run(input: RunInput) {
      // parse features
      // build support code
      // make test plan
      // run beforeAll hooks
      // route each test case to a worker object
      // run afterAll hooks
      // return ordered envelopes + final status
    },
  },
  schemas: {
    run: { input: RunInput, output: RunResult },
  },
})
```

Coordinator responsibilities:

- own Cucumber run ordering;
- create and thread the run id / testRunStarted id;
- parse feature files with `gherkin-streams`;
- build support and test plan with `@cucumber/core`;
- emit/collect run-level envelopes;
- call workers for scenario attempts;
- fold statuses;
- return the final ordered `Envelope[]`.

The coordinator should stay stateless with respect to scenario execution. It can be retried/idempotent by durable service id, but it should not become a stateful actor.

In this repo, land the first implementation in `packages/spec-harness`:

```txt
packages/spec-harness/src/durable/
  coordinator.ts      service(...) top-level runner
  worker.ts           object(...) keyed scenario actor
  assembly.ts         gherkin-streams + @cucumber/core plan assembly
  messages.ts         message constructors and result mapping
  support.ts          support-code registry / DSL lowering
  runtime.ts          runFeaturesDurable public entry
  cck.ts              CCK fixture runner helpers
```

Keep the existing `packages/spec-harness/src/runtime.ts` Cucumber hook harness alive while this lands. The durable runner replaces it after it can run the same specs. There is no Firelab compatibility layer.

### 2.2 Worker Object

The worker is a keyed stateful object. Its key is the scenario attempt id.

```ts
export const worker = object({
  name: "cucumber-effect/worker",
  handlers: {
    *runScenario(input: ScenarioAttemptInput) {
      // execute exactly one AssembledTestCase attempt
      // return scenario envelopes + statuses
    },
  },
  schemas: {
    runScenario: { input: ScenarioAttemptInput, output: ScenarioAttemptResult },
  },
})
```

Worker responsibilities:

- own scenario-local state;
- provide `WorldServices`;
- execute assembled test steps;
- capture attachments/evidence;
- return scenario-level envelopes and statuses;
- use `run(...)` for replay-sensitive proof/external effects.

The worker key is:

```ts
const attemptKey = `${testCaseId}:${attempt}`
```

Retry is a new worker key. Attempt 2 must not reuse attempt 1's key.

### 2.3 Public API

The first implementation does not need live streaming internally.

```ts
export const runFeatures = (paths, options) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const runId = options.runId ?? stableRunId(paths, options)
      const result = yield* client(coordinator).run({ runId, paths, options })
      return Stream.fromIterable(result.envelopes)
    }),
  )
```

The CLI can pipe this stream to `MessageToNdjsonStream`. Tests can collect it with `Stream.runCollect`.

Later, if long-running product runs need live output, add a `shared` read handler or owner-stream projection. Do not design that before the CCK runner works.

Concrete first API in `packages/spec-harness/src/durable/runtime.ts`:

```ts
import { Stream } from "effect"
import { client, serviceLayer } from "effect-s2-durable"
import type { Envelope } from "@cucumber/messages"
import { coordinator } from "./coordinator.ts"
import { worker } from "./worker.ts"

export const DurableCucumberLive = serviceLayer(coordinator, worker)

export const runFeaturesDurable = (
  paths: ReadonlyArray<string>,
  options: RunOptions,
): Stream.Stream<Envelope, RunnerError, RunnerServices> =>
  Stream.unwrap(
    client(coordinator).run({ paths, options }).pipe(
      Effect.map((result) => Stream.fromIterable(result.envelopes)),
      Effect.provide(DurableCucumberLive),
    ),
  )
```

This is intentionally batch-returning. A compliant runner that returns correct messages is more valuable than a live-streaming design that is not compliant yet.

---

## 3. Coordinator Algorithm

The coordinator returns the canonical ordered Cucumber Messages output.

```txt
1. create nextId policy
2. create meta/testRunStarted envelopes
3. parse feature files with gherkin-streams
4. append source/gherkinDocument/pickle envelopes
5. build support library with @cucumber/core
6. append support-code envelopes
7. make test plan with @cucumber/core
8. append testCase envelopes
9. execute BeforeAll hooks and append run-hook envelopes
10. if BeforeAll succeeded:
      for each test case in order:
        call client(worker, attemptKey).runScenario(...)
        append returned scenario envelopes
        fold returned statuses
11. execute AfterAll hooks and append run-hook envelopes
12. append testRunFinished exactly once
13. return { envelopes, statuses, success }
```

CCK mode is the same coordinator with deterministic options:

```ts
{
  scenarioConcurrency: 1,
  idPolicy: "incrementing",
}
```

No separate serial runner.

Illustrative coordinator shape:

```ts
import { Effect, Schema } from "effect"
import { client, service } from "effect-s2-durable"
import { IdGenerator, type Envelope } from "@cucumber/messages"
import { worker } from "./worker.ts"
import { assembleRun } from "./assembly.ts"
import { finishTestRun, metaEnvelope, testRunStarted } from "./messages.ts"

export const coordinator = service({
  name: "firegrid-cucumber/coordinator",
  handlers: {
    *run(input: RunInput) {
      const newId = IdGenerator.incrementing()
      const testRunStartedId = newId()
      const envelopes: Envelope[] = [
        metaEnvelope(),
        testRunStarted(testRunStartedId),
      ]

      const assembled = yield* assembleRun({
        paths: input.paths,
        options: input.options,
        newId,
        testRunStartedId,
      })

      envelopes.push(...assembled.discoveryEnvelopes)
      envelopes.push(...assembled.supportEnvelopes)
      envelopes.push(...assembled.testCaseEnvelopes)

      const beforeAll = yield* runBeforeAllHooks(assembled.support, testRunStartedId)
      envelopes.push(...beforeAll.envelopes)

      const scenarioResults = beforeAll.success
        ? yield* Effect.forEach(
            assembled.testCases,
            (testCase) => {
              const attemptKey = `${testCase.id}:0`
              return client(worker, attemptKey).runScenario({
                testRunStartedId,
                testCase,
                options: input.options,
              })
            },
            { concurrency: input.options.scenarioConcurrency ?? 1 },
          )
        : []

      for (const result of scenarioResults) envelopes.push(...result.envelopes)

      const afterAll = yield* runAfterAllHooks(assembled.support, testRunStartedId)
      envelopes.push(...afterAll.envelopes)

      const statuses = [
        ...beforeAll.statuses,
        ...scenarioResults.flatMap((result) => result.statuses),
        ...afterAll.statuses,
      ]
      envelopes.push(finishTestRun({ testRunStartedId, statuses }))
      return { envelopes, statuses, success: testRunSuccess(statuses) }
    },
  },
  schemas: {
    run: { input: RunInput, output: RunResult },
  },
})
```

This example uses local arrays deliberately. They are return values inside a single durable handler, not a public event model. Once the runner is compliant, the same handler can persist/provide live message projection.

---

## 4. Worker Algorithm

The worker executes one scenario attempt.

```txt
1. append testCaseStarted
2. build scenario-local WorldServices
3. reduce assembled testSteps with mode = "run" | "skip"
4. for each step:
     if mode is skip and !step.always:
       append skipped testStepFinished
       continue
     append testStepStarted
     prepared = step.prepare()
     if undefined/ambiguous:
       map to Cucumber result
     if prepared:
       decode arguments / DataTable / docString
       invoke step body inside WorldServices
       collect attachments/evidence
       map outcome to Cucumber result
     append attachments
     append testStepFinished
     update mode
5. append testCaseFinished
6. return { envelopes, statuses }
```

The worker object is the right place for mutable scenario state because `object(...)` is keyed, stateful, and serial per key.

Illustrative worker shape:

```ts
import { Effect, Exit, Schema } from "effect"
import { object, run, state } from "effect-s2-durable"
import type { Envelope, TestStepResultStatus } from "@cucumber/messages"
import { DataTable } from "@cucumber/core"

export const worker = object({
  name: "firegrid-cucumber/worker",
  handlers: {
    *runScenario(input: ScenarioAttemptInput) {
      const envelopes: Envelope[] = []
      const statuses: TestStepResultStatus[] = []
      const scenario = state(ScenarioState)

      const testCaseStartedId = input.testCaseStartedId
      yield* scenario.set({ id: "active", testCaseStartedId })
      envelopes.push(testCaseStartedEnvelope(input.testCase, testCaseStartedId))

      let mode: "run" | "skip" = "run"
      for (const step of input.testCase.testSteps) {
        if (mode === "skip" && !step.always) {
          const skipped = skippedResult()
          envelopes.push(testStepFinishedEnvelope(step, testCaseStartedId, skipped))
          statuses.push(skipped.status)
          continue
        }

        envelopes.push(testStepStartedEnvelope(step, testCaseStartedId))
        const prepared = step.prepare()
        const result = yield* executePreparedStep(prepared, input.worldConfig)
        envelopes.push(...result.attachments)
        envelopes.push(testStepFinishedEnvelope(step, testCaseStartedId, result.result))
        statuses.push(result.result.status)
        mode = result.result.status === "PASSED" ? "run" : "skip"
      }

      envelopes.push(testCaseFinishedEnvelope(input.testCase, testCaseStartedId, statuses))
      return { envelopes, statuses }
    },
  },
  schemas: {
    runScenario: { input: ScenarioAttemptInput, output: ScenarioAttemptResult },
  },
})
```

Proof steps use `run(...)` inside the invoked step body:

```ts
Then("the trace should satisfy:", function* (sql: string) {
  const scenario = yield* ScenarioId
  const rows = yield* run(
    queryProofRows(sql, scenario.id),
    { name: `proof:${hashSql(sql)}`, output: ProofRows },
  )
  if (!proofRowsPass(rows)) {
    return yield* Effect.fail(new TraceProofFailed({ sql, rows }))
  }
})
```

The worker does not need to know this is a proof step. It just provides `WorldServices`; the step body decides which effects need durable journaling.

---

## 5. Support DSL

Expose a Cucumber-shaped DSL:

```ts
defineSupport(({ Given, When, Then, Before, After, BeforeAll, AfterAll, ParameterType }) => {
  Given("...", function* (...) {
    // Effect generator body
  })
})
```

Rules:

- Use `@cucumber/core` as the support-code substrate.
- Keep the one `SupportCodeFunction` cast at the lowering boundary.
- Lift generator bodies with `Effect.fn(...)`.
- `invokeStep` must fail loudly on unexpected returns. A returned generator is a bug, not a pass.
- Do not hand-roll Cucumber message/match types that packages already export.

Lowering sketch:

```ts
const defineStep = (
  keyword: "Given" | "When" | "Then",
  pattern: string | RegExp,
  body: EffectStepBody,
) => {
  const fn = isGeneratorFunction(body)
    ? Effect.fn(String(pattern))(body)
    : body

  builder.step({
    keyword,
    pattern,
    fn: ((...args: unknown[]) => fn(...args)) as SupportCodeFunction,
    sourceReference: callerLocation(),
  })
}
```

Worker invocation boundary:

- `@cucumber/core` owns matching and `PreparedStep` structure.
- The worker executes the prepared step through one small boundary that follows the installed `@cucumber/core` API exactly.
- That boundary does not implement durability. Durability comes from the step body running inside the durable worker object and using `effect-s2-durable` primitives such as `run(...)`, `state(...)`, `signal(...)`, and `awakeable(...)`.
- The boundary must fail loudly if a step returns an unsupported value, because a missed generator lift should not become a silent pass.

Do not copy/paste pseudo-code here. Implement this only after reading the exact `PreparedStep` / argument API in the pinned `@cucumber/core` package.

---

## 6. Durable Step Boundaries

Most CCK steps are pure and can just execute inside the worker.

Replay-sensitive work must use `run(...)`:

```ts
const rows = yield* run(queryChdb(sql), {
  name: `proof:${proofName}`,
  output: ProofRows,
})
```

Branch on journaled results, not on bare external reads. This is required for Firegrid proof steps and any step where an external read affects pass/fail.

---

## 7. fluent-firegrid Spec Authoring

Some historical validation files are useful only as examples of the behavior/evidence pattern we want to express in Cucumber. They are not a compatibility target.

Old shape:

```txt
defineValidation({
  backend,
  component,
  requirements: [
    { id, description, evidence, claim }
  ]
})
```

Effect Cucumber mapping:

- `backend` -> run/scenario layer setup.
- `component({ key, keyFor })` -> fixture setup in `Given`.
- `claim` -> ordinary step execution.
- `evidence` -> SQL proof over spans.
- `requirement.id` -> tag or scenario id.
- `description` -> feature/scenario prose.

Do not preserve `defineValidation` as a public compatibility API. There is no requirement that old Firelab validation modules keep running. The target is normal Cucumber feature files and support code that express the same product behaviors and trace proofs.

First fluent-firegrid specs to author against this runner:

- `effect-s2-durable-object-call`
- `effect-s2-durable-object-recovery`
- `effect-s2-durable-workflow`
- `effect-s2-durable-service-recovery`
- `effect-s2-stream-db-basic`
- `effect-s2-stream-db-storage-primitives`

---

## 8. CCK Gate

For each CCK sample:

1. Run only that feature and its support code.
2. Capture produced `Envelope[]`.
3. Load expected `.ndjson` with `NdjsonToMessageStream`.
4. Normalize ids/timestamps/durations/stack traces while preserving id references.
5. Compare parsed objects.

Initial target:

- `minimal`
- `attachments`

Then add hooks, data tables, doc strings, parameter types, scenario outlines, rules, retry, pending/skipped/ambiguous.

---

## 9. Milestones

1. **Coordinator service + worker object.** `runFeatures` calls `client(coordinator).run`, coordinator calls `client(worker, attemptKey).runScenario`, minimal CCK passes.
2. **Attachments.** Worker captures attachments/evidence and returns attachment envelopes in correct order.
3. **DSL correctness.** Generator lift, fail-loud returns, DataTable/docString support, undefined/ambiguous from `@cucumber/core`.
4. **Hooks and retry.** Run hooks in coordinator; scenario hooks in worker; retry uses fresh worker keys.
5. **Full CCK green.**
6. **fluent-firegrid specs.** Rewrite object-call and workflow validations as normal Cucumber specs first.
7. **Recovery proof.** Crash/resume a durable run or worker and prove scenario attempt recovery.

---

## 10. Net

The architecture is:

```txt
coordinator = service(...)  // stateless Cucumber router
worker      = object(...)   // keyed scenario actor
```

Build the Cucumber-compliant runner on that. Keep streaming, live projection, and owner-stream tailing as later optimizations after the runner produces correct Cucumber Messages.
