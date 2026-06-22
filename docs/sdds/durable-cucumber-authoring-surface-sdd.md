# SDD: Durable Cucumber Authoring Surface

## 1. Purpose

Redesign the *authoring surface* of `@firegrid/durable-cucumber` (the package
introduced in PR #42) toward the cucumber-rs model: a **typed, user-owned World
as the per-scenario state container**, **typed captured arguments**, and a
**declarative entry derived from the World type**.

This is a surface re-skin, **not** an engine rewrite. The durable engine —
the per-scenario `object` (`begin`/`invoke`/`end`), step-outcome journaling, the
pure `runner-core` protocol, `step-host` matching, the CCK gate, and the
firegrid `@sql:` trace-proof harness — is sound and CCK-green and stays as is.
Only how a spec *author* declares steps, state, hooks, and proofs changes.

## 2. Motivation

The current surface is cucumber-js-shaped: `defineSteps(({Given,When,Then}) =>
…)` with step bodies typed `(this: any, ...args: any[]) => unknown`. Two
concrete defects fall out of that:

1. **No place for per-scenario state.** The World is a fixed
   `{ attach, log, link }` interface, so every steps file invents a
   `WeakMap<SpecWorld, State>` side-channel to carry `db`/`key` across steps
   (see `features/effect-s2-stream-db/storage-primitives.steps.ts`).
2. **Captured arguments are untyped.** `(this: SpecWorld, id: string, value:
   number)` is a hand-maintained fiction; `bindArguments` actually hands the
   body `unknown[]`. The compiler verifies nothing.

cucumber-rs fixes both with one move: steps receive a typed World as their state
container, and captured groups are parsed into typed arguments.

## 3. The Layering (mental model)

Two layers that the prior design conflated under the word "World":

1. **The durable engine** — the `object`/`service`, `begin`/`invoke`/`end`,
   step-outcome journaling, CCK gate. The *mechanism*. Authors never write this.
   Its `invoke` handler's contract is: "given a matched step, run its body."
2. **The authoring surface** — the step *vocabulary* (`"I insert item
   {string}"` → a body) plus the *per-scenario state*. The only thing an author
   writes.

The scenario `object` is the **vessel**; the World is the **typed state the
engine places inside that vessel for one scenario**. They are distinct: the
object is the durable runtime vessel; the World is the author's typed state.

## 4. Load-Bearing Decisions

1. **The World is the user's typed state container.** Per-scenario, mutable,
   constructed fresh per scenario via an `init` effect. It replaces both the
   `this: any` existential and the `WeakMap` side-channel.
2. **The World is transient, not durably serialized.** Durability lives in the
   product defs steps call, which are replay-idempotent via `scenarioKey(...)`
   (e.g. `StorageDb.open(scenarioKey(...))` re-opens the same durable stream on
   replay). The World holds live, non-serializable handles; it is not journaled.
   A Schema-backed *durable* World that survives a mid-scenario crash is a
   heavier future variant and is **explicitly deferred** (§9).
3. **Captured arguments are typed.** Built-in parameter types (`{int}`,
   `{float}`, `{string}`, `{word}`) infer to `number`/`string`; custom parameter
   types declare an Effect `Schema` that supplies both the runtime transform and
   the TS type. Wrong arity/type is a compile error.
4. **The authoring surface is a value, never a global registry.** Whatever the
   declaration syntax, it compiles to the existing `SupportBundle` *value*,
   captured in the handler closures and rebuilt on recovery (Restate-style
   deployment dependency). No module-global mutated by import side effects — that
   is the footgun PR #42 removed and it also breaks replay.
5. **Trace proofs are scenario-scoped.** A `@sql:` proof asserts over the trace
   of an entire scenario, not a single step. It binds at the feature/scenario
   level (tag + sibling `.sql`, or a class/feature-level declaration), never per
   step.
6. **The engine is unchanged.** `runner-core`, `step-host`, `step-exec` outcome
   journaling, the scenario `object`, the CCK gate, and the firegrid `@sql:`
   harness are not modified beyond the dispatch glue in §6.

## 5. Two Candidate Syntaxes

Both compile to the same `SupportBundle` value and wire onto the identical
engine. The choice is house-style (explicit-value vs. ergonomic-magic), not
capability.

### 5.1 Decorated World class (preferred for ergonomics)

`this` *is* the typed World; method signatures *are* the typed args.

```ts
class StorageWorld extends World<StorageWorld>() {
  db?: StorageDbInstance
  key?: string

  @given("an open storage db with infinite retention at key {string}")
  openDb(key: string) {
    return StorageDb.open(scenarioKey(this, key), { config: { retentionPolicy: { infinite: {} } } })
      .pipe(Effect.map((db) => { this.db = db }))
  }

  @when("I insert item {string} value {int}")
  insert(id: string, value: number) {
    return this.db!.items.insert({ id, value })
  }

  @then("item {string} has value {int}")
  expect(id: string, value: number) {
    return this.db!.items.get(id).pipe(Effect.map((v) => assert.equal(v.value, value)))
  }

  @after()
  teardown(outcome: ScenarioOutcome) { return Effect.void }
}

export const storage = feature(StorageWorld)        // reads class metadata -> SupportBundle value
storage.run("features/effect-s2-stream-db/storage-primitives.feature")
```

**Hard constraint:** decorators must emit *metadata on the class* (Stage-3
`context.metadata`), and `feature(World)` reads it back to build the bundle.
Decorators that `push` into a module-global at load time are prohibited
(Decision 4). Stage-3 has no parameter decorators — args come from the method
signature positionally, like cucumber-rs. Heavily magic; in mild tension with
the repo's explicit-value Effect idiom (LLMS.md).

### 5.2 Value-first `feature(World)(register)` (preferred for idiom)

Fully Effect-idiomatic; slightly more ceremony. The World is passed as the first
parameter; reporting (`attach`/`log`/`link`/`scenarioId`) is an ambient
`Scenario` service rather than `this`.

```ts
export const storage = feature(StorageWorld)(({ given, when, then, before, after }) => {
  before((w) => StorageDb.open(scenarioKeyOf("storage"), …).pipe(Effect.map((db) => { w.db = db })))
  when("I insert item {string} value {int}", (w, id, value) => w.db!.items.insert({ id, value }))
  then("item {string} has value {int}", (w, id, value) =>
    w.db!.items.get(id).pipe(Effect.map((v) => assert.equal(v.value, value))))
  after((w, outcome) => outcome.status === "FAILED" ? Effect.logError("failed") : Effect.void)
})
```

Typed args in this form rely on template-literal-type inference over the pattern
string:

```ts
type ParamType<S extends string> =
  S extends "int" | "float" | "biginteger" ? number :
  S extends "string" | "word" ? string : unknown
type Args<P extends string> =
  P extends `${string}{${infer N}}${infer Rest}` ? [ParamType<N>, ...Args<Rest>] : []
```

## 6. Engine Dispatch Glue (the only engine-adjacent change)

- **`begin`** runs the World `init` and holds the constructed typed World for the
  scenario execution (transient, per Decision 2).
- **`invoke`** resolves the matched step (unchanged `step-host`) and runs its
  body bound to the per-scenario World instance (`this` for 5.1, first arg for
  5.2), passing typed captured args. The journaled `StepOutcome` contract is
  unchanged.
- **`end`** runs the `after` hook with `(world, outcome)`, exposing
  pass/fail/skip (cucumber-rs's `ScenarioFinished`).
- Reporting capabilities (`attach`/`log`/`link`/`scenarioId`) move off the World
  type into a `Scenario` Effect service available to step bodies (which already
  run with `WorldServices` ambient).

## 7. Run Policy on the Runner, Not the Surface

Executor choice (durable vs. direct), CCK serial scheduling, retry, and the
firegrid `@sql:` proof layer are runner config, separate from step authoring
(cucumber-rs's `World::cucumber().before().after().run()` split):

```ts
storage.run("features/.../storage-primitives.feature").pipe(
  Cucumber.withExecutor(durable),
  Cucumber.withProofs,
)
```

## 8. What Changes vs PR #42

| File | Change |
| --- | --- |
| `support.ts` | `SupportApi` becomes World-parameterized and arg-typed; `StepBody` stops being `(this:any,...args:any)`. Still produces a `SupportBundle` value. |
| `world.ts` (currently empty) | Gains the `World` base class + `init`. |
| `scenario.ts` | `begin` runs `World.init` and holds the typed World; `invoke` binds the body to the instance with typed args; `end` runs `after(world, outcome)`. |
| `proofs.ts` / firegrid | `scenarioId` + attach/log/link move into the `Scenario` service; product state moves into the user's World; per-feature `WeakMap` deleted. |
| `runner-core`, `step-host`, `step-exec`, CCK gate, `@sql` harness | **Unchanged.** |

## 9. Non-Goals / Deferred

- **Durable (Schema-backed) World** surviving a mid-scenario crash. The
  transient-World + idempotent-product-def model (Decision 2) is sufficient for
  the trace-proof use case; a serialized World is a separate, heavier variant.
- **Cross-feature step sharing** beyond class inheritance / multiple bundles.
- **Inlining ClickHouse SQL in decorators.** Proofs stay in sibling `.sql` files
  (syntax highlighting, standalone editing) unless a later pass shows co-location
  is worth the loss.

## 10. Open Decision

**§5.1 decorated class vs. §5.2 value-first `register`.** Both wire onto the
same engine; the decision is whether the repo accepts decorator/metadata magic
(best ergonomics, `this` = World) or holds the explicit-value Effect line
(slightly more ceremony, `Scenario` service). Resolve before building.

## 11. Build Plan

1. Land the `World` base + `feature(World)` value-builder and the engine
   dispatch glue (§6) behind the existing direct executor.
2. Migrate `storage-primitives.steps.ts` as the first consumer; delete its
   `WeakMap`. Prove typed args + typed World compile.
3. Re-point the durable executor and the firegrid `@sql:` layer at the new
   surface; confirm the CCK gate and existing trace-proof scenarios still pass.
4. Only after the value-first surface is proven, optionally add the §5.1
   decorator front-end as metadata over the same builder (if §10 lands that way).

## 12. References

- PR #42 — `Add durable Cucumber runner (coordinator + worker)` (the engine).
- cucumber-rs — <https://github.com/cucumber-rs/cucumber> (typed World, typed
  args, `World::run`).
- `docs/sdds/cucumber-executable-spec-runtime-design.md` — prior runtime design.
- `docs/sdds/durable-cucumber-runner-sdd.md` — the engine SDD #42 implements.
