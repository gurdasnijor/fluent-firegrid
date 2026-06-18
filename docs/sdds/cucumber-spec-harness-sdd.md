# SDD: Cucumber + chDB Spec Harness

**Status:** accepted direction; implementation in progress  
**Scope:** replace Firelab with a Cucumber authoring/runtime loop backed by an
in-memory chDB proof engine for OpenTelemetry trace assertions.  
**Supersedes:** Firelab validation definitions, Firelab proof joins,
CEL-based trace claims, JSONL trace files, and Firelab validation CLIs.

## Summary

Cucumber is the behavior authoring and execution surface. Feature files describe
public behavior using ordinary Gherkin and may include ClickHouse SQL proof
queries over OpenTelemetry spans. Step definitions are normal Cucumber step
definitions; the harness must not introduce a second authoring API such as
`this.run`, `this.action`, or `this.assertion`.

Trace proofing is done in an embedded ClickHouse database:

- one real `chdb.Session` is created as a scoped Effect service;
- `ChdbSpanExporter` writes ended OTel spans into `otel_traces`;
- `ChdbClient` queries the same session using Effect-native operations;
- scenario `After` hooks force flush OTel, run SQL proof blocks, and fail the
  scenario if any proof is false.

This keeps the proof model powerful and inspectable without maintaining a
custom CEL language.

## Goals

- Make `.feature` files the vertical behavior-first spec surface.
- Prove behavior with trace data emitted by the real system under test.
- Replace Firelab/CEL/JSONL with Cucumber plus in-memory chDB.
- Use stock Cucumber concepts: features, steps, hooks, formatters, messages.
- Keep chDB and OTel wiring Effect-native and layer-composable.
- Keep typed authoring support through parameter types and explicit fixtures.

## Non-Goals

- No Firelab compatibility layer.
- No long-lived ClickHouse server.
- No custom wrapper API around every step.
- No reflective package parsing for fixtures.
- No feature-YAML proof join in this harness; Cucumber scenarios, tags,
  locations, and reports are the executable spec identity.
- No generic SQL builder before real usage demands one.

Unit/property tests remain in Vitest. This harness is the real-S2,
span-evidenced vertical proof layer.

## Architecture

### Runner

Use Cucumber as the runner. `@cucumber/cucumber` is the stable baseline. A
future spike may switch to `@cucumber/node` only if it preserves the same model:
TypeScript loading, scenario identity in hooks, standard Cucumber messages or
equivalent reporter events, and custom formatter/report support.

The runner shape is:

1. build a worker/scenario scoped Effect runtime;
2. run ordinary Cucumber steps;
3. export ended spans into chDB;
4. run SQL proof blocks after the scenario;
5. render scenario output from Cucumber messages plus chDB queries.

### Shared chDB Session

The chDB session is a real `chdb.Session` owned by an Effect layer. It is not
owned by Cucumber `World`, not exposed as a field on `ChdbClient`, and not
represented by a fake structural interface.

One scoped session is provided to both:

- `ChdbClient`, which exposes Effect-native query/command/insert/stream
  operations;
- `ChdbSpanExporter`, which writes OTel spans into `otel_traces`.

Required package boundary:

```ts
export type ChdbSession = Session
export const ChdbSession = Context.Service<ChdbSession>("@chdb/Session")

export const sessionLayer:
  (config: ChdbClientConfig) => Layer.Layer<ChdbSession, SqlError>

export const layerFromSession:
  (config: ChdbClientConfig) =>
    Layer.Layer<ChdbClient | SqlClient, SqlError, ChdbSession>

export const layer:
  (config: ChdbClientConfig) =>
    Layer.Layer<ChdbSession | ChdbClient | SqlClient, SqlError>
```

If the harness needs a chDB capability, add it to `ChdbClient` as an
Effect-returning method typed from the real `chdb` API. Do not leak the raw
session through the client object.

### OpenTelemetry Wiring

Wire OTel through `@effect/opentelemetry` the same way Effect examples do:

```ts
const OtelLive = Layer.unwrap(
  Effect.gen(function*() {
    const session = yield* ChdbSession
    const processor = new BatchSpanProcessor(
      new ChdbSpanExporter({ session, table: "otel_traces" }),
    )
    return NodeSdk.layer(() => ({
      resource: { serviceName: "firegrid-cucumber" },
      spanProcessor: [processor],
    }))
  }),
)
```

`ChdbClient` must not import or construct `ChdbSpanExporter`. The dependency
direction is layer composition: both services consume the shared session.

### chDB Trace Store

`ChdbSpanExporter` owns the `otel_traces` table and uses the OpenTelemetry
ClickHouse schema:

- `TraceId`, `SpanId`, `ParentSpanId`, `SpanName`, `SpanKind`, `ServiceName`;
- `ResourceAttributes` and `SpanAttributes` as `Map(String, String)`;
- `Timestamp` as `DateTime64(9)`;
- `Duration`, `StatusCode`, `StatusMessage`;
- nested `Events` and `Links`.

The baseline proof SQL queries `otel_traces` directly. Do not install ad hoc
views from the harness. If ergonomic trace views become necessary, add them
deliberately in `packages/observability` and keep the exporter/client boundary
clean.

### ChdbClient

`ChdbClient` is the Effect-facing query client over the shared session. It may
provide:

- the Effect SQL tag path for normal row queries;
- schema-decoded query helpers;
- encoded insert helpers;
- `asCommand` for DDL/commands;
- native wrappers for real chDB capabilities such as `query`, `queryBind`,
  `queryAsync`, `queryBindAsync`, `insert`, and `queryStream`.

Rules:

- wrap chDB calls in `Effect.try` / `Effect.tryPromise`;
- classify chDB errors into the existing SQL error model;
- keep query parameter support aligned with `chdb-node`'s real `queryBind`
  API;
- do not add `queryJsonEachRow` or similar one-off helpers when the Effect SQL
  tag or native formatted query already covers the need;
- do not expose the raw `Session` on `ChdbClient`;
- do not import OTel/exporter code from the client.

## Authoring Model

### Parameter Types

Cucumber parameter types should decode feature text through production codecs
where possible. Parameter transformers are pure text-to-value boundaries;
runtime liveness checks stay in step definitions.

Examples:

- `{streamDbFixture}` resolves a fixture name through the fixture registry;
- future `{objectCallId}` should decode through the durable call-id codec;
- future `{duration}` should decode through a shared duration codec.

### Fixture Registry

Fixtures are explicit TypeScript registry entries, not reflective package
parsing. They construct the component under test and may carry reporting
metadata. They must not hide assertions or proof queries.

Example shape:

```ts
export interface Fixture<A> {
  readonly name: string
  readonly make: (ctx: { readonly key: string }) =>
    Effect.Effect<A, unknown, S2Client>
}
```

Step definitions stay ordinary Cucumber:

```ts
Given("an open {streamDbFixture} at key {string}", async function(fixture, key) {
  this.streamDb = await runtime.runPromise(
    fixture.make({ key: scenarioKey(this, key) }),
  )
})
```

### SQL Proof Blocks

Feature files may include one or more proof steps whose DocString is arbitrary
ClickHouse SQL. Each query runs after OTel flushes.

Baseline shape:

```gherkin
Then the trace should satisfy:
  """
  SELECT
    countIf(SpanName = 'effect-s2-stream-db.checkpoint') > 0
    AND countIf(SpanName = 'S2.append') > 0
    AND countIf(SpanName = 'S2.readBatch') > 0 AS ok
  FROM otel_traces
  WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
  """
```

Rules:

- the query must return one row with a truthy `ok` column, or a truthy first
  column if `ok` is absent;
- `{scenario_id:String}` is bound by the harness;
- multiple proof blocks are conjunctive;
- proof SQL can use normal ClickHouse features, including windows, maps,
  `sequenceMatch`, and `sequenceCount`;
- repeated SQL can later be factored into explicit helpers, but the baseline is
  direct SQL in the feature file.

### Step Definitions

Step definitions should use stock Cucumber concepts. A small internal helper to
run an Effect inside the managed runtime is acceptable, but it must not become
a visible authoring model or a proof/action/assertion wrapper.

Behavior assertions can use normal test/assertion libraries inside steps. Trace
assertions belong in SQL proof blocks.

## Lifecycle

Per worker or scenario scope:

1. provide `ChdbSession`;
2. provide `ChdbClient` from the same session;
3. build `OtelLive` using `ChdbSpanExporter` and that session;
4. provide `S2LiteLive` and package layers under test;
5. dispose the runtime and let scoped finalizers close chDB/S2 resources.

Per scenario:

1. derive a stable scenario id from Cucumber's pickle id;
2. derive fresh S2/object keys from that id;
3. run steps;
4. force flush the span processor;
5. execute SQL proof blocks;
6. record/report span counts, failed SQL, trace tree, and slow spans.

Scenario attribution must be applied at the runtime boundary so authors do not
have to remember to annotate every step by hand.

## Reporting

Use Cucumber's standard reporting as the base:

- `summary` for local console output;
- Cucumber Messages for structured artifacts;
- optional HTML output through Cucumber tooling;
- `usage` to flag unused step definitions.

Add a custom formatter built on `@cucumber/query` for trace/proof context. It
should consume Cucumber message envelopes for scenario/step metadata and query
chDB for:

- SQL proof result and failed SQL;
- trace tree for the scenario;
- span name counts;
- slow spans and idle gaps;
- fixture names and selected decoded I/O recorded by steps.

If CI needs post-processable trace artifacts later, add explicit export from
chDB. Do not reintroduce Firelab JSONL as the primary execution model.

## Cutover Plan

1. Keep `packages/observability` boundaries clean:
   - scoped `ChdbSession`;
   - `ChdbClient` over that session;
   - `ChdbSpanExporter` taking real `chdb.Session`;
   - no fake session interface and no client/exporter import cycle.
2. Get one executable `effect-s2-stream-db` feature green end-to-end.
3. Add Cucumber formatter/reporting using `@cucumber/query`.
4. Convert feature-YAML inventories to Gherkin `.feature` files.
5. Keep unimplemented requirement inventory scenarios tagged `@spec-only` until
   step definitions make them executable.
6. Convert remaining stream-db and durable scenarios from requirement inventory
   to executable Cucumber proofs incrementally.
7. Keep semantic inventory in Gherkin only where still useful as non-executable
   design/spec inventory; do not make the harness depend on Firelab proof joins.

## Risks

- **chDB native support:** CI must run on platforms supported by `chdb-node`.
- **Exporter flush timing:** `After` must force flush before querying.
- **Scenario attribution:** missing scenario id makes trace proofs too broad.
- **SQL injection:** bind scenario-generated values where chDB supports it;
  otherwise escape only harness-generated values.
- **Formatter drift:** pin Cucumber packages together and verify formatter
  loading in CI.
- **Fixture sprawl:** fixtures should remain construction helpers, not hidden
  mini-runners.

## Worked Example

```gherkin
@product:effect-s2-stream-db @feature:storage-primitives
Feature: Storage primitives

  Scenario: checkpoint snapshots the live set and reopens from the compacted stream
    Given an open stream-db:retained at key "cart"
    When I insert item "a" value 1
    And I checkpoint
    Then reopening, item "a" is 1
    And the trace should satisfy:
      """
      SELECT
        countIf(SpanName = 'effect-s2-stream-db.checkpoint') > 0
        AND countIf(SpanName = 'S2.append') > 0
        AND countIf(SpanName = 'S2.readBatch') > 0 AS ok
      FROM otel_traces
      WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
      """
```
