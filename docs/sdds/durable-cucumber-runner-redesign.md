# Redesign: Durable Cucumber Runner (corrected)

This supersedes the first attempt (PR #42). It records what was wrong with that
attempt and the corrected design.

**Status:** the wire-shaped topology (final section) is now drafted in
`packages/spec-harness/src/durable/` — `runner` (service), `world` (object),
support bundles by name. Verified locally by typecheck, lint, Effect diagnostics
(0 errors), and an engine-free gate over the static message layer
(`minimal`/`attachments`). The full S2-backed durable gate is wired but skipped
where the `s2` binary is absent (this environment), so the runner↔world durable
path is **CI-verified only** for now. The "Concrete code (earlier)" section below
predates the wire reframing and is kept only for history.

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

---

## Concrete code

All examples use the verified primitive signatures
(`run(name, action, { output })`, `state(Table)`, `objectClient(def, key)`,
`service`/`object`). They are illustrative, not final.

### The load-bearing question: how services reach a handler

`service()`/`object()` compile a handler body to `Handler<…, never, never>` (the
`R` is erased by a cast). The engine only injects `ActiveInvocation` +
`DurableExecutionRuntime` into the body (`runExecution`/`runObjectBody` in
`Runtime.ts`). So a step body that reads `ChdbClient` (proofs) or a
`SupportLibrary` service must get them **some other way**. Two options — I need
your call on which matches the engine's real behavior:

**Option A — provide world + support to the runtime layer; bodies inherit it.**
The engine forks handler bodies with `Effect.forkIn(body, engineScope)`, which
inherits the *fiber context* in which `DurableExecutionRuntime.layer` was built.
If the entry provides those services to that layer, bodies can read them
ambiently:

```ts
// entry (composition root) — built once, not per run
const DurableCucumberLive = serviceLayer(coordinator, scenario).pipe(
  Layer.provide(Layer.mergeAll(SupportLibrary.Live, WorldServices.Live, S2LiteLive)),
)
```

This is the clean authoring story (proof steps just `yield* run(query…)`), but it
leans on context inheritance through `forkIn` + the `R`-erasure cast. **Does the
engine guarantee bodies see services provided to its layer?**

**Option B — no inheritance; provide explicitly per step.** Each external effect
in a step body carries its own `Effect.provide(WorldServices.Live)` (memoized).
Safe regardless of engine internals, noisier for authors. Falls back to a
module-level pre-built `Context` if per-call layer build is too costly.

Everything below assumes **A**; if it's **B** the only change is where
`Effect.provide` sits.

### Support code as a service (no module-scope registry)

`defineSupport` stays a pure description; the entry folds the imported modules
into a `SupportLibrary` **service**, so handlers read it from context rather than
a mutable global:

```ts
// support.ts
export type SupportModule = (api: SupportApi) => void
export const defineSupport = (register: SupportModule): SupportModule => register

export class SupportLibrary extends Context.Service<SupportLibrary, {
  readonly forText: (text: string) => ReadonlyArray<MatchedStep>   // @cucumber/core matching
  readonly beforeHooks: (tags: ReadonlyArray<string>) => ReadonlyArray<DefinedTestCaseHook>
  readonly afterHooks: (tags: ReadonlyArray<string>) => ReadonlyArray<DefinedTestCaseHook>
  readonly toEnvelopes: () => ReadonlyArray<Envelope>
}>()("cucumber-effect/SupportLibrary") {
  static fromModules = (modules: ReadonlyArray<SupportModule>, newId: IdGenerator.NewId) =>
    Layer.sync(SupportLibrary, () => {
      const lib = buildSupportLibrary(modules, newId)   // wraps @cucumber/core buildSupportCode
      return SupportLibrary.of({ /* … delegate to lib … */ })
    })
}
```

### Scenario object — per-step journaling

The worker is a **stable, module-level** `object`. One call runs one attempt;
scenario-local state is `state(Table)`; effectful work inside step bodies is
journaled by `run(...)`. It returns positional outcomes and mints no ids.

```ts
// scenario.ts
class ScenarioFacts extends Table<ScenarioFacts>("scenarioFacts")({
  id: Schema.String.pipe(primaryKey),
  json: Schema.String,
}) {}

export const scenario = object({
  name: "cucumber-effect/scenario",
  handlers: {
    // input.pickle is a serializable @cucumber/messages Pickle
    *runScenario(input: { readonly pickle: Pickle; readonly attempt: number }) {
      const support = yield* SupportLibrary
      const steps = planSteps(input.pickle, support)   // deterministic: (pickle, support)
      const facts = state(ScenarioFacts)

      // Sequential fold; the loop is *derived* — on replay it recomputes from the
      // journaled run/state facts. No Effect.reduce in lib code → manual chain.
      let mode: "run" | "skip" = "run"
      const outcomes: Array<StepOutcome> = []
      for (const step of steps) {            // (lib guardrail: replace with a fold helper)
        const outcome = mode === "skip" && !step.always
          ? skipped()
          : yield* runOneStep(step, facts)   // step body uses run(...)/state(...) inside
        outcomes.push(outcome)
        mode = outcome.status === "PASSED" ? mode : "skip"
      }
      return { steps: outcomes }
    },
  },
  schemas: { runScenario: { input: Schema.Unknown, output: Schema.Unknown } },
})
```

### A proof step body — the durable boundary in practice

Proof reads (the whole point of fluent-firegrid) run **inside `run(...)`** so a
read that decides pass/fail is journaled and replayed, never re-issued:

```ts
// authored support code (a feature's *.ts), via the DSL
defineSupport(({ Then }) => {
  Then("the trace should satisfy:", function* (sql: string) {
    const scenarioId = yield* ScenarioId            // from WorldServices (Option A)
    const rows = yield* run(
      `proof:${hashSql(sql)}`,
      queryProofRows(sql, scenarioId),               // requires ChdbClient (ambient)
      { output: ProofRows },
    )
    if (!proofRowsPass(rows)) {
      return yield* Effect.fail(new TraceProofFailed({ sql }))
    }
  })
})
```

This replaces `runtime.ts`'s `After`-hook proof loop: a proof is now an ordinary
step whose result is journaled.

### Coordinator service — message/id authority

```ts
// coordinator.ts
export const coordinator = service({
  name: "cucumber-effect/coordinator",
  handlers: {
    *run(input: { readonly sources: ReadonlyArray<SourceInput>; readonly options: RunOptions }) {
      const support = yield* SupportLibrary
      const assembled = assembleRun(input.sources, support)   // owns all ids/envelopes

      const scenarioOutcomes = yield* Effect.forEach(
        assembled.testCases,
        (tc) =>
          objectClient(scenario, `${tc.id}:0`).runScenario({   // deterministic child id → dedups on replay
            pickle: assembled.pickleFor(tc),
            attempt: 0,
          }),
        { concurrency: input.options.scenarioConcurrency ?? 1 },
      )

      // map positional outcomes → envelopes using the coordinator's own ids
      const envelopes = buildEnvelopes(assembled, scenarioOutcomes)
      return { envelopes, success: foldSuccess(scenarioOutcomes) }
    },
  },
  schemas: { run: { input: Schema.Unknown, output: Schema.Unknown } },
})
```

### Public entry + CCK gate through the real engine

```ts
// runtime.ts
export const DurableCucumberLive = (modules: ReadonlyArray<SupportModule>) =>
  serviceLayer(coordinator, scenario).pipe(
    Layer.provide(Layer.mergeAll(SupportLibrary.fromModules(modules, IdGenerator.incrementing()), WorldServices.Live)),
  )

export const runFeaturesDurable = (paths, options) =>
  Stream.unwrap(
    readSources(paths).pipe(
      Effect.flatMap((sources) => client(coordinator).run({ sources, options })),
      Effect.map((r) => Stream.fromIterable(r.envelopes)),
      Effect.provide(DurableCucumberLive(options.support)),
    ),
  )
```

```ts
// cck.test.ts — gates the REAL durable path (no local shortcut), S2-backed
it("minimal", () =>
  runFeaturesDurable([cckFeature("minimal")], { support: [minimalSupport] }).pipe(
    Stream.runCollect,
    Effect.map((envs) => expect(normalize(envs)).toEqual(normalize(expected("minimal")))),
    Effect.provide(S2LiteLive),   // CI (s2 binary); or a TestS2 layer if one exists
    Effect.runPromise,
  ))
```

---

## Revision: wire-protocol-shaped topology (supersedes the split above)

Reading `cucumber-ruby-wire` reframes this. The Wire protocol decouples the
**runner** from a **step-definition host** over a connection; the runner never
imports step code, it drives the host with a tiny message set and builds Cucumber
messages from the host's responses:

| Wire message | Request | Response |
|---|---|---|
| `step_matches` | `{ name_to_match }` | `["success", [{ id, args:[{val,pos}], source }]]`; `[]`=undefined, >1=ambiguous |
| `begin_scenario` | `{ tags? }` | `["success"]` (Before hooks / world setup) |
| `invoke` | `{ id, args, table? }` | `["success"]` / `["fail", {message,exception,backtrace}]` / `["pending", msg]` |
| `end_scenario` | `{ tags? }` | `["success"]` (After hooks / teardown) |
| `snippet_text` | `{ step_keyword, step_name, multiline_arg_class }` | `["success", snippet]` |

### Why this is the right shape here

The "wire" is **durable RPC**. The step host is a **durable endpoint that owns
its own support code + WorldServices** (it provides its own layer). That collapses
the problems above:

- **The services question disappears.** The coordinator (runner) needs no
  `ChdbClient`, no `SupportLibrary`, no inheritance hack — step code and proof
  reads live in the host, which provides its own layer like any normal durable
  service. Option A vs B is moot.
- **`invoke` *is* the per-step durable boundary.** Each step invocation is a
  durable call into the host; its result is journaled, so on replay a completed
  step returns its recorded outcome and the chDB proof read never re-runs. That
  is exactly the per-step journaling you chose, with no bespoke wrapping.
- **The World is a keyed object.** A virtual object keyed by scenario-attempt id
  holds per-scenario `state(...)` between `begin_scenario` and `end_scenario` —
  the durable analogue of the wire connection's per-scenario world.

### Topology

- **`runner` (service)** — parses gherkin, owns ordering and *all* Cucumber
  ids/envelopes. For each scenario: `objectClient(world, attemptId).beginScenario`,
  then per step `stepMatches` → `invoke`, then `endScenario`; it maps each
  response onto `testStep*`/`testCase*` envelopes. No step code, no world services.
- **`world` (object, key = `${testCaseId}:${attempt}`)** — the step-definition
  host + World. Owns support code + WorldServices; runs Before/After hooks and
  step bodies; holds scenario state in `state(...)`.
- **`support` (service or shared handlers)** — `stepMatches` / `supportEnvelopes`
  / `snippetText`: support-library queries the runner needs for matching and for
  `stepDefinition`/`testCase` envelopes.

### Concrete shape

```ts
// world.ts — the step-definition host (owns support code + WorldServices)
export const world = object({
  name: "cucumber-effect/world",
  handlers: {
    *beginScenario(input: { readonly scenarioId: string; readonly tags: ReadonlyArray<string> }) {
      // Before hooks; open the firegrid.scenario span / seed scenario state
    },
    *invoke(input: {
      readonly stepDefinitionId: string
      readonly args: ReadonlyArray<unknown>
      readonly docString?: string
      readonly dataTable?: PickleTable
    }) {
      const fn = yield* SupportLibrary.pipe(Effect.map((l) => l.fnFor(input.stepDefinitionId)))
      // step body runs here; proof reads use run(...) INSIDE it (journaled).
      // returns the wire-shaped outcome the runner maps to a TestStepResult:
      return yield* invokeStep(fn, input)   // { status } | { status:"FAILED", error } | { status:"PENDING" }
    },
    *endScenario(input: { readonly tags: ReadonlyArray<string> }) {
      // After hooks; teardown
    },
  },
  schemas: { /* opaque JSON in/out */ },
})

// runner.ts — pure orchestration + Cucumber message authority (no step code)
export const runner = service({
  name: "cucumber-effect/runner",
  handlers: {
    *run(input: { readonly sources: ReadonlyArray<SourceInput> }) {
      const support = yield* objectClient(world, "support").supportEnvelopes() // or a support service
      const assembled = assembleRun(input.sources, support)   // gherkin parse + ids/envelopes
      // for each scenario: beginScenario → (stepMatches → invoke)* → endScenario,
      // mapping each durable response onto testStep*/testCase* envelopes.
      ...
    },
  },
})
```

### Open questions (revised)

1. **Granularity** — drive the wire conversation as separate durable calls
   (`beginScenario`, one `invoke` per step, `endScenario`) so every step is its
   own journaled boundary (closest to the wire, maximal recovery)? Or one
   `runScenario` call that loops internally and journals each step with `run(...)`?
   (Per-step calls match your "per-step journaling" most literally.)
2. **World-as-keyed-object** — agree the World/step-host is a virtual object keyed
   by scenario-attempt id, with `runner` as a `service` driving it? Or a different
   split (e.g. host as a `service`, world state passed explicitly)?
3. **`stepMatches` location** — matching lives in the host (it owns
   `@cucumber/core` support); the runner builds `stepDefinition`/`testCase`
   envelopes from `step_matches`/`supportEnvelopes` responses. Agree, or should
   the runner own a copy of the support library for assembly?
4. **Literal wire vs. internal protocol** — model the *architecture* on the wire
   protocol (durable RPC, our own message shapes), or actually speak the Cucumber
   wire JSON so non-Effect step hosts could connect too?


