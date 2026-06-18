# @firegrid/spec-harness

The Cucumber runtime + **trace-proof** engine behind the executable specs in
`features/`. It runs a scenario, lets the real product code emit OpenTelemetry
spans into an embedded chDB (ClickHouse) table, then runs a SQL query that
asserts the *shape of the execution trace*. The assertion is not "the function
returned 42" — it is "the production path appended to S2 in this order."

For how to write specs at the right **altitude**, see `features/Readme.md`. This
README documents the **mechanism**.

## The pieces

| Piece | Where | Role |
| --- | --- | --- |
| `src/runtime.ts` | here | Cucumber `Before`/`After`/`BeforeAll` hooks: builds the Effect runtime, flushes spans, runs the SQL proofs. |
| `src/s2lite.ts` | here | In-process S2 implementation so scenarios get real append/read semantics without a server. |
| `src/trace-formatter.ts` | here | Cucumber formatter that prints per-scenario span/trace coverage. |
| `src/report-state.ts` | here | Accumulates per-scenario proof/span/coverage counts for the report. |
| `cucumber.mjs` (repo root) | root | Config + the `proofs` profile, which tags-filters to `@sql:*` scenarios. |
| `<feature>.steps.ts` | `features/**` | Step definitions; drive the **public** API and run effects through `runSpecEffect`. |
| `<feature>.sql` | `features/**` | Named trace-proof queries, one per `@sql:` tag. |

## How a proof runs (end to end)

1. A scenario is tagged `@sql:service_trace`.
2. `Before` reads the sibling `<feature>.sql`, parses `-- name:` blocks, and
   loads the block(s) named by the scenario's `@sql:` tags into scenario state.
3. Step definitions run effects via `runSpecEffect(world, effect)`, which wraps
   them in a `firegrid.scenario` span carrying
   `firegrid.scenario.id = <pickle id>`. Every product span emitted underneath
   (e.g. `S2.append`, `effect-s2-durable.object.admit`) inherits that trace.
4. Spans flow OTel → `BatchSpanProcessor` → `ChdbSpanExporter` → the
   `otel_traces` table in chDB.
5. `After` calls `processor.forceFlush()`, then runs each loaded proof query
   against `otel_traces`, scoped to this scenario's spans.
6. The proof **passes** iff the first row's `ok` column (or its first column, if
   there is no `ok`) is truthy. Otherwise the scenario fails with the query, the
   reason, and an observed-span summary.

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
2. Add step definitions in `<feature>.steps.ts`. Drive the **public** API only;
   run effects through `runSpecEffect`. Use `scenarioKey(world, key)` for
   idempotency keys so reruns are deterministic.
3. Add a `-- name: <name>` block to `<feature>.sql`.
4. Run `pnpm --filter @firegrid/spec-harness spec`.

`cucumber.mjs` discovers `@sql:*` tags automatically, so no profile edit is
needed; a scenario tagged `@sql:foo` with no `-- name: foo` block fails with a
clear "SQL proof foo not found" error.

## Commands

```bash
pnpm --filter @firegrid/spec-harness spec            # run the proofs profile (@sql:* only)
pnpm --filter @firegrid/spec-harness spec:inventory  # dry-run: list steps, execute nothing
```

## Design notes

- The run-scoped `runtime` and `processor` are deliberately module-level
  singletons (one chDB + Effect runtime per Cucumber run); per-scenario state is
  isolated in a `WeakMap<IWorld, …>`, and trace isolation is by
  `firegrid.scenario.id`, not by tearing down the runtime.
- Evidence spans must come from **production** code paths. A proof that passes
  against validation-only instrumentation is proving the test, not the system —
  see the `production-readiness` requirement of the same name.
