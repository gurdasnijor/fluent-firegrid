# Design Note: Cucumber Executable Spec Runtime

**Status:** proposed corrective design  
**Scope:** `packages/spec-harness`, `features/support`, and executable
`.steps.ts` files.  
**Supersedes in practice:** fixture registries, feature-specific wrapper APIs,
and per-scenario infrastructure setup introduced during the first executable
spec cutover attempt.

## Problem

The executable spec harness has drifted toward hand-rolled Cucumber
infrastructure:

- parameter-driven fixture registries;
- feature-specific wrapper objects hanging off `World`;
- step files delegating to wrapper methods instead of exercising product APIs;
- per-scenario runtime construction mixed into `World` setup.

That is not the intended model. Cucumber should be the authoring and execution
surface. Feature files should use natural Given/When/Then steps; the backing
World should provide only the shared scenario context those steps need.

## Design

### Runtime Lifecycle

The harness has run/worker-scoped infrastructure and scenario-scoped proof
evaluation.

Run/worker setup:

- start `S2LiteLive`;
- create a shared in-memory chDB session/client for trace proof queries;
- install the OpenTelemetry Node SDK through `@effect/opentelemetry/NodeSdk`;
- attach `ChdbSpanExporter` through a `BatchSpanProcessor`;
- build one managed Effect runtime from those composed layers.

Scenario setup:

- record the Cucumber scenario id on `World`;
- reset the scenario's proof list;
- do not create S2 lite, chDB, or OTel infrastructure.

Scenario teardown:

- force-flush the span processor;
- query chDB for span summaries filtered by `firegrid.scenario.id`;
- run each SQL proof block filtered by that scenario id;
- record formatter data.

Run/worker teardown:

- dispose the managed Effect runtime;
- shut down the OpenTelemetry processor/SDK through the layer finalizers.

Cucumber `BeforeAll` / `AfterAll` hooks own run/worker setup and teardown.
Those hooks do not use a scenario `World`. Cucumber `Before` / `After` hooks
own per-scenario metadata and proof checks.

### Effect Layer Shape

Harness infrastructure should be built as an Effect layer graph. Cucumber hooks
only acquire and dispose that graph.

Conceptually:

```ts
const ChdbLive = ChdbLayer({})

const OtelLive = Layer.unwrap(
  Effect.gen(function*() {
    const session = yield* ChdbSession
    const processor = new BatchSpanProcessor(
      new ChdbSpanExporter({ session, table: "otel_traces" }),
    )

    return Layer.mergeAll(
      HarnessTraceProcessor.layer(processor),
      NodeSdk.layer(() => ({
        resource: { serviceName: "firegrid-cucumber" },
        spanProcessor: [processor],
      })),
    )
  }),
)

const HarnessLive = Layer.mergeAll(S2LiteLive, OtelLive).pipe(
  Layer.provideMerge(ChdbLive),
)
```

The concrete names can differ, but the dependency direction should not:

- `ChdbSpanExporter` consumes the shared `ChdbSession`;
- `ChdbClient` consumes the same shared `ChdbSession`;
- `NodeSdk.layer` installs tracing;
- `S2LiteLive` provides the S2 client;
- the harness composes these services, rather than storing raw services on
  Cucumber `World`.

### World Responsibilities

`World` should be thin per-scenario context, not a product fixture container or
a second step-definition API.

It may own:

- `scenarioId`;
- collected SQL proof blocks;
- a deterministic `scenarioKey(seed)` helper;
- small scenario context objects used by step definitions, such as the
  storage-primitives test model and the currently opened instance.

It should not own:

- S2 lite lifecycle;
- chDB session lifecycle;
- OpenTelemetry setup;
- wrapper methods that simply mirror Gherkin steps;

### Storage Primitives World

The storage-primitives scenarios need a plain Cucumber World context:

```ts
interface StoragePrimitivesWorld {
  readonly Item: typeof Item
  readonly Note: typeof Note
  readonly StorageDb: typeof StorageDb
  db?: StorageDbInstance
  key?: string
}
```

This is data, not a command surface. Step definitions call the public
`StreamDb` APIs directly and return `Effect` values through the harness step
adapter. The adapter owns Effect-to-Promise execution in the run-scoped harness
runtime and attaches the scenario trace attributes.

### Step Authoring

Executable steps should remain ordinary Cucumber step definitions. They may be
fine-grained when that makes the Gherkin scenario readable.

Example:

```gherkin
Scenario: checkpoint snapshots the live set and reopens from the compacted stream
  Given an open storage db with infinite retention at key "cart"
  When I insert item "a" value 1
  And I upsert item "a" value 2
  And I delete item "a"
  And I checkpoint
  Then reopening, item "a" is absent
```

The implementation should stay direct:

```ts
WhenEffect("I insert item {string} value {int}", function(id, value) {
  return storageDbFor(this).items.insert({ id, value })
})
```

Do not introduce wrapper methods such as
`this.storagePrimitives.insertItem(...)`; that only recreates a second step API.

### Product Test Definitions

Small product definitions used only by executable specs, such as a test
`Table`, `StreamDb`, service, object, or workflow definition, may live in the
World context or a co-located support module when reuse demands it.

They should not be hidden behind:

- Cucumber parameter types;
- fixture registries;
- `World` wrapper APIs;
- reflective package lookup from feature text.

The feature text should describe behavior. The step body should drive the
public product API needed to prove that behavior.

### SQL Proofs

SQL proof DocStrings remain the trace evidence mechanism. A proof step only
records SQL on the current scenario. The scenario `After` hook runs proofs
after forcing OTel flush.

Proof SQL is scoped with:

```sql
WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
```

This makes a run/worker-scoped trace database safe across multiple scenarios.

## Consequences

- `features/support/world.ts` may define small scenario contexts, but should
  not grow feature-specific command wrappers.
- `features/support/fixtures.ts` is not needed for the current design.
- Step definitions should call product APIs directly, not wrapper methods.
- `packages/spec-harness/src/world.ts` should be renamed or split if needed:
  one module for Cucumber `World`, one module for hook/runtime layer lifecycle.
- Per-scenario runtime creation should be removed once run/worker-scoped hooks
  are in place.

## Migration Checklist

1. Move harness runtime acquisition to `BeforeAll`.
2. Move runtime disposal to `AfterAll`.
3. Keep `Before` limited to scenario id/proof reset.
4. Keep `After` limited to force-flush, proof queries, and formatter report
   state.
5. Remove feature-specific `World` wrapper methods.
6. Define the storage-primitives World as plain data/context.
7. Keep storage-primitives steps as direct public API calls over that context.
8. Port additional executable coverage by writing Cucumber scenarios first,
   then direct step bodies plus SQL proof DocStrings.

## References

- Cucumber support hooks:
  <https://github.com/cucumber/cucumber-js/blob/main/docs/support_files/hooks.md>
- Effect OpenTelemetry layer composition example:
  <https://github.com/Effect-TS/effect/blob/7b57f41f85a19d4c531e0a1b5573ff017cfa699e/packages/opentelemetry/examples/index.ts#L4>
