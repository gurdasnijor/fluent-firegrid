# Firelab

Firelab runs executable spec-compliance validations against the current
`effect-s2-*` packages and records OpenTelemetry evidence for each requirement.

It is not a general test runner and it is no longer a host/client simulation
harness. A Firelab validation is a small executable contract:

1. map to a feature file such as
   `features/effect-s2-stream-db/storage-primitives.feature.yaml`;
2. run one isolated claim per feature requirement;
3. assert the behavioral result in code;
4. require corroborating OTel spans from the system under test.

The verdict is computed from both trusted observations and trace evidence. A
claim that only passes in memory, without the expected production-path spans,
does not count as covered.

## Requirements

- Run from the repo root unless otherwise noted.
- `pnpm install` has been run.
- The `s2` CLI is available on `PATH`; validations that use `S2LiteLive` launch
  `s2 lite` automatically.

## Commands

```bash
# List executable validations
pnpm --filter firelab validate:list

# Run a validation by id
pnpm --filter firelab validate:run effect-s2-stream-db-storage-primitives --timeout-ms 120000

# Run every discovered validation
pnpm --filter firelab validate:all --timeout-ms 120000

# List recorded runs
pnpm --filter firelab validate:runs

# Render a run's trace tree; omit the run id to use latest
pnpm --filter firelab validate:show
pnpm --filter firelab validate:show 2026-06-16T22-38-35-781Z__effect-s2-stream-db-storage-primitives

# Summarize span timing for a run
pnpm --filter firelab validate:perf 2026-06-16T22-38-35-781Z__effect-s2-stream-db-storage-primitives

# Show instrumentation coverage gaps for a run
pnpm --filter firelab validate:gaps 2026-06-16T22-38-35-781Z__effect-s2-stream-db-storage-primitives

# Re-judge a saved run against the current validation definition
pnpm --filter firelab validate:seams effect-s2-stream-db-storage-primitives 2026-06-16T22-38-35-781Z__effect-s2-stream-db-storage-primitives

# Check proof wiring for a feature while it is still under construction
pnpm --filter firelab validate:proofs check effect-s2-stream-db/storage-primitives --allow-missing

# Strictly require every feature requirement to have a well-formed proof
pnpm --filter firelab validate:proofs check effect-s2-stream-db/storage-primitives

# Bootstrap a validation module from an existing feature spec
pnpm --filter firelab validate:proofs init effect-s2/resource-spec

# Generate requirement proof stubs for missing feature requirements
pnpm --filter firelab validate:proofs scaffold effect-s2-stream-db/storage-primitives

# Package checks
pnpm --filter firelab typecheck
pnpm --filter firelab diagnostics
```

`validate:run` writes a run directory and exits non-zero when any gate fails.
The raw CLI also works:

```bash
pnpm --filter firelab validate run effect-s2-stream-db-basic --timeout-ms 120000
```

## Validation Layout

Validations live under `src/validations/<id>/index.ts` and default-export a
`defineValidation(...)` value.

```ts
import { Effect, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import { assertNone, assertSome } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

class TestDb extends StreamDb<TestDb>("example-validation")({ items: Item }) {}

export default defineValidation({
  id: "example-validation",
  description: "Validates one storage requirement against s2 lite.",
  feature: {
    product: "effect-s2-stream-db",
    name: "storage-primitives",
  },
  backend: S2LiteLive,
  component: ({ key }) =>
    Effect.gen(function*() {
      const db = yield* TestDb.open(key)
      return {
        db,
        reopen: () => TestDb.open(key),
      }
    }),
  requirements: [
    {
      id: "CHECKPOINT.1",
      description: "compact snapshots live state and reopens from the compacted stream",
      evidence:
        'spans.exists(s, named(s, "effect-s2-stream-db.compact"))'
        + ' && spans.exists(s, named(s, "S2.readBatch"))',
      claim: ({ db, reopen }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "a", value: 1 })
          yield* db.compact
          const reopened = yield* reopen()
          assertSome(yield* reopened.items.get("a"), { id: "a", value: 1 })
          assertNone(yield* reopened.items.get("missing"))
        }),
    },
  ],
})
```

## Component Context

Firelab builds a fresh component for each requirement claim. The component
factory receives:

- `validationId`
- `runId`
- `feature`
- `requirementId`, for example `storage-primitives.CHECKPOINT.1`
- `requirementLocalId`, for example `CHECKPOINT.1`
- `requirementDescription`
- `key`, a fresh stable key for this run and requirement
- `keyFor(suffix)`, additional keys under the same requirement scope

Prefer using `key` in the component factory and returning ergonomic handles
such as `db`, `reopen`, `client`, or `drain`. Requirement claims should read
like the behavior under validation, not like fixture plumbing.

## Requirements And Gates

Each entry in `requirements` maps to one feature requirement:

```ts
{
  id: "PROJECTION.1",
  description: "latest-value materialized reads remain available for state",
  evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.table.query"))',
  claim: ({ db }) =>
    Effect.gen(function*() {
      // Drive the system and assert the observable behavior.
    }),
}
```

Firelab compiles this into a coverage gate with id
`<feature.name>.<requirement.id>`, for example
`storage-primitives.PROJECTION.1`.

The `claim` is the executable behavior check. It passes when it returns `true`
or completes without throwing. Use `packages/firelab/src/assertions.ts` helpers
for assertion-style claims; they are intentionally CLI-safe and mirror the
small value-level assertion style used in Effect tests.

The `evidence` expression is CEL over scoped OTel spans. It must reference
evidence spans emitted by the system under test or Firelab runtime, such as:

- `S2.append`
- `S2.checkTail`
- `S2.createStream`
- `S2.readBatch`
- `effect-s2-stream-db.open`
- `effect-s2-stream-db.table.get`
- `effect-s2-stream-db.compact`
- `firelab.s2lite.ready`
- `firegrid.validation.run`

The coverage oracle rejects vacuous gates: a gate that passes without firing
any referenced evidence span does not prove production-path coverage.

## Feature Proof Workflow

Feature files are the normative requirement list. Firelab validations are the
executable proof layer over those requirements.

```text
features/<product>/<name>.feature.yaml
        │
        ├─ requirement ids: GROUP.1, GROUP.2, ...
        │
        ▼
packages/firelab/src/validations/<validation-id>/index.ts
        │
        ├─ defineValidation({ feature: { product, name }, requirements: [...] })
        │
        ▼
firelab proofs check
```

Use `proofs check` during authoring:

```bash
# Draft mode: validates existing proofs and lists missing requirements.
pnpm --filter firelab validate:proofs check effect-s2-stream-db/storage-primitives --allow-missing

# Strict mode: exits non-zero if any feature requirement has no proof.
pnpm --filter firelab validate:proofs check effect-s2-stream-db/storage-primitives

# Check one validation module against its declared feature.
pnpm --filter firelab validate:proofs check effect-s2-stream-db-storage-primitives --allow-missing

# Check every validation-backed feature.
pnpm --filter firelab validate:proofs check --all --allow-missing
```

Use `proofs init` when a feature has no validation module yet:

```bash
# Print the file that would be generated.
pnpm --filter firelab validate:proofs init effect-s2/resource-spec --dry-run

# Create packages/firelab/src/validations/effect-s2-resource-spec/index.ts.
pnpm --filter firelab validate:proofs init effect-s2/resource-spec
```

`proofs init` parses the feature YAML, infers the validation id from
`<product>-<feature.name>`, and writes a `defineValidation(...)` module with a
placeholder component plus one proof stub per requirement. It refuses to
overwrite an existing validation unless `--force` is passed.

CI runs `pnpm --filter firelab validate:all --timeout-ms 120000`, so every
validation under `packages/firelab/src/validations/<id>/index.ts` is a merge
gate once committed.

A well-formed proof has:

- a local feature requirement id such as `CHECKPOINT.1`;
- a non-empty description;
- a non-empty CEL evidence expression;
- at least one named production evidence span;
- no references to non-evidence driver spans;
- a claim function.

Use `proofs scaffold` to generate requirement entries for the missing
requirements in a feature:

```bash
pnpm --filter firelab validate:proofs scaffold effect-s2-stream-db/storage-primitives
```

The scaffold output is intentionally not considered complete proof work. Paste
the entries into a validation, replace the `TODO` evidence span with real
production-path evidence, and replace the failing claim with the executable
behavior check.

## Artifacts

Each run writes artifacts under `packages/firelab/.simulate/runs/<run-id>/`.
The directory name is:

```text
<timestamp>__<validation-id>
```

Important files:

- `trace.jsonl`: one JSON object per completed OTel span
- `observations.json`: per-requirement claim outcomes

`packages/firelab/.simulate/latest.json` points at the newest run with a trace.
The `.simulate` directory name is historical; treat it as Firelab's local run
artifact directory.

## Trace Analysis

`validate:show` renders the parent/child span tree with timings.

`validate:perf` summarizes slow spans and idle gaps.

`validate:gaps` prints the instrumentation map: observed span names,
evidence-span classification, and evidence named by the validation but not
observed in that run.

`validate:seams` re-runs the coverage oracle over a saved `trace.jsonl` and
`observations.json`. This is useful when tightening evidence gates without
re-running s2 lite.

## Current Validations

- `effect-s2-stream-db-basic`
- `effect-s2-stream-db-storage-primitives`

Both launch `s2 lite` through `S2LiteLive`, open real `effect-s2-stream-db`
instances, drive storage behavior, and require OTel evidence from `effect-s2`
and `effect-s2-stream-db`.

## Development Notes

- Keep validation ids stable; run directories and rejudge commands use them.
- Keep each requirement self-contained. Shared setup belongs in `component`;
  requirement-specific behavior belongs in `claim`.
- Use `key`/`keyFor()` for fresh storage resources. Avoid hard-coded stream keys.
- Add instrumentation to the package under test, not to validation claims, when
  trace evidence is missing.
- Do not use TypeSpec/OpenAPI commands as part of Firelab verification; Firelab
  validates runtime behavior and trace evidence.
