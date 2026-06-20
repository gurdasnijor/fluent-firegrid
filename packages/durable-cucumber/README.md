# @firegrid/durable-cucumber

The Cucumber runtime + **trace-proof** engine behind the executable specs in
`features/`. It runs a scenario, lets the real product code emit OpenTelemetry
spans into an embedded chDB (ClickHouse) table, then runs a SQL query that
asserts the *shape of the execution trace*. The assertion is not "the function
returned 42" — it is "the production path appended to S2 in this order."

For how to write specs at the right **altitude**, see `features/Readme.md`. This
README documents the **mechanism**.

## The pieces

The runner is built from scratch on `@cucumber/{gherkin,messages,cucumber-expressions}`
(no `@cucumber/cucumber`): a pure protocol core that emits a cucumber-messages
`Envelope` stream, with step execution injected as an `Executor`.

| Piece | Where | Role |
| --- | --- | --- |
| `src/durable/runner-core.ts` | here | The cucumber protocol: parse → assemble → emit the `Envelope` stream in order; step execution is the injected `Executor`. |
| `src/durable/step-host.ts` | here | Owns step-definition identity + matching (cucumber-wire `step_matches`), over `@cucumber/cucumber-expressions`. |
| `src/durable/{scenario,runner,runtime}.ts` | here | The durable path: per-scenario virtual object (`begin`/`invoke`/`end` commands) + the run coordinator. |
| `src/firegrid/runtime.ts` | here | The `WorldServices` layer: OTel → chDB span export, s2-lite S2, chDB, FS/Path, and a span-flush handle. |
| `src/firegrid/proofs.ts` | here | Loads `@sql:` proofs from the sibling `.sql`, evaluates them against chDB; `SpecWorld` + `scenarioKey`. |
| `src/firegrid/run.ts` | here | `firegridExec` (shares one World across a scenario's steps, wraps each in the `firegrid.scenario` span, runs proofs at scenario end) + `runFiregrid`. |
| `src/s2lite.ts` | here | In-process S2 implementation so scenarios get real append/read semantics without a server. |
| `<feature>.steps.ts` | `features/**` | A `defineSteps(...)` bundle; drives the **public** API and returns effects (run with `WorldServices` ambient). |
| `<feature>.sql` | `features/**` | Named trace-proof queries, one per `@sql:` tag. |

## How a proof runs (end to end)

1. A scenario is tagged `@sql:service_trace`. `runFiregrid` runs only
   `@sql:`-tagged scenarios (the old `proofs` profile).
2. At `beginScenario`, `firegridExec` reads the sibling `<feature>.sql`, parses
   `-- name:` blocks, and loads the block(s) named by the scenario's `@sql:`
   tags. It also creates one `SpecWorld` shared across the scenario's steps.
3. Each step's returned effect is run wrapped in a `firegrid.scenario` span
   carrying `firegrid.scenario.id = <pickle id>`. Every product span emitted
   underneath (e.g. `S2.append`, `effect-s2-durable.object.admit`) inherits that
   trace.
4. Spans flow OTel → `BatchSpanProcessor` → `ChdbSpanExporter` → the
   `otel_traces` table in chDB.
5. At `endScenario`, `firegridExec` flushes the span processor (`SpecTracing`),
   then runs each loaded proof query against `otel_traces`, scoped to this
   scenario's spans.
6. The proof **passes** iff the first row's `ok` column (or its first column, if
   there is no `ok`) is truthy; a failing proof is reported as a failed result
   for the scenario.

## Writing a proof query

Add a named block to the feature's sibling `.sql` file:

```sql
-- name: service_trace
WITH ordered AS (
  SELECT toUInt64(toUnixTimestamp64Nano(Timestamp)) AS ts, SpanName
  FROM scenario_spans
)
SELECT
  countIf(SpanName = 'S2.append') >= 1
  AND sequenceMatch('(?1).*(?2).*(?3)')(
    ts,
    SpanName = 'S2.createStream',
    SpanName = 'S2.append',
    SpanName = 'effect-s2-stream-db.commit'
  ) AS ok
FROM ordered
```

Rules enforced by `normalizeProofSql`:

- **Single read-only statement.** Must start with `SELECT` or `WITH`; no `;`
  (no multiple statements). Trailing `;` is stripped.
- **Truthiness contract.** Name the verdict column `ok`, or make the first
  selected column the verdict. `truthy()` treats `0`, `''`, `'0'`, `'false'`,
  and `NULL`/absent as false.
- **No rows = fail** (`"query returned no rows"`), so aggregate to a single
  boolean row rather than relying on row presence.

### The `scenario_spans` macro

Write `scenario_spans` and the harness expands it to "all spans in any trace
touched by this scenario":

```sql
(SELECT * FROM otel_traces
 WHERE TraceId IN (
   SELECT TraceId FROM otel_traces
   WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}))
```

`{scenario_id:String}` is bound to the running scenario's id at execution time.
Use `scenario_spans` for every proof — querying `otel_traces` directly will leak
spans from other scenarios.

### Useful chDB idioms

- **Count a span:** `countIf(SpanName = 'effect-s2-durable.object.admit') >= 4`
- **Assert ordering:** `sequenceMatch('(?1).*(?2)')(ts, cond1, cond2)`
- **Parent/child relationship:** self-join `scenario_spans` on
  `child.ParentSpanId = parent.SpanId` (see `workflow_trace` in
  `features/effect-s2-durable/durable-executions/durable-executions.sql`).

## Adding a new behavioral scenario

1. Tag the scenario `@sql:<name>` in the `.feature` (one behavior, one `When` —
   see `features/Readme.md`).
2. Add step definitions to the feature's `defineSteps(...)` bundle in
   `<feature>.steps.ts`. Drive the **public** API only; return effects (they run
   with `WorldServices` ambient). Use `scenarioKey(this, key)` for idempotency
   keys so reruns are deterministic.
3. Add a `-- name: <name>` block to `<feature>.sql`.
4. Run `pnpm --filter @firegrid/durable-cucumber spec`.

`runFiregrid` selects `@sql:*` scenarios automatically; a scenario tagged
`@sql:foo` with no `-- name: foo` block fails with a clear "SQL proof foo not
found" error.

## Commands

```bash
pnpm --filter @firegrid/durable-cucumber spec   # run the @sql:* specs + their trace proofs
pnpm --filter @firegrid/durable-cucumber test   # the full vitest suite (CCK + durable + firegrid)
```

## Design notes

- There are no module-level runtime singletons: `WorldServicesLive` is an Effect
  layer (one chDB + OTel + s2-lite per run), per-scenario state lives on the
  `SpecWorld` shared across a scenario's steps, and trace isolation is by
  `firegrid.scenario.id`.
- Evidence spans must come from **production** code paths. A proof that passes
  against validation-only instrumentation is proving the test, not the system —
  see the `production-readiness` requirement of the same name.
