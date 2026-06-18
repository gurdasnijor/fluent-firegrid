# Handoff: Cucumber + chDB Spec Harness

**Branch:** `codex/stricter-effect-linting`  
**State:** implementation in progress; build cleanup still required.  
**Primary SDD:** [cucumber-spec-harness-sdd.md](./cucumber-spec-harness-sdd.md)

## Goal

Replace Firelab with a Cucumber-based executable spec harness using in-memory
chDB for OpenTelemetry trace proofs. The desired end state is:

- Cucumber `.feature` files drive public behavior;
- step definitions are stock Cucumber code;
- OTel spans are exported into `otel_traces` in chDB;
- feature-authored SQL proof blocks are evaluated after each scenario;
- Firelab is removed only after equivalent Cucumber coverage is green in CI.

## Design Guardrails

- Do not reintroduce Firelab compatibility.
- Do not add `this.action`, `this.assertion`, or another authoring wrapper API.
- Do not let Cucumber `World` own a chDB `Session`.
- Do not expose the raw chDB `Session` as a field on `ChdbClient`.
- Do not make `ChdbClient` import or construct `ChdbSpanExporter`.
- Do not add fake structural session interfaces.
- Do not add one-off query helpers like `queryJsonEachRow` when the Effect SQL
  tag or native chDB wrappers cover the need.
- Do not install ad hoc trace views from the harness; baseline proof SQL queries
  `otel_traces` directly.

The intended boundary is:

- `ChdbSession`: scoped Effect service wrapping the real `chdb.Session`;
- `ChdbClient`: Effect-native query client over that session;
- `ChdbSpanExporter`: OTel exporter taking the real `Session` type;
- `spec-harness`: composes those services in layers and runs Cucumber.

## Current Files To Review

- `docs/sdds/cucumber-spec-harness-sdd.md`
- `packages/observability/src/ChdbClient.ts`
- `packages/observability/src/ChdbExporter.ts`
- `packages/observability/src/index.ts`
- `packages/spec-harness/src/world.ts`
- `packages/spec-harness/src/s2lite.ts`
- `packages/spec-harness/src/trace-formatter.ts`
- `features/effect-s2-stream-db/storage-primitives.feature`
- `features/support/fixtures.ts`
- `features/step_definitions/stream_db_steps.ts`

## Current Implementation Notes

`packages/observability/src/ChdbClient.ts` currently contains the rough shape
needed for the shared session:

- `export type ChdbSession = Session`
- `export const ChdbSession = Context.Service<ChdbSession>("@chdb/Session")`
- `sessionLayer(config)`
- `layerFromSession(config)`
- `layer(config)` composing session + client

`packages/observability/src/ChdbExporter.ts` should take the real `Session`
type from `chdb`, not a local fake interface.

`packages/spec-harness/src/world.ts` currently tries to import
`ChdbSession` from `@firegrid/observability`, create `ChdbSpanExporter` from
that session inside `NodeSdk.layer`, and run Cucumber proof SQL after flushing
the span processor. This is directionally correct, but the next agent should
clean the build/type/lint details rather than preserving the current code as
final.

## Known Cleanup Needed

- The branch is behind `origin/codex/stricter-effect-linting` by one commit.
  Fast-forward before finalizing if no local changes conflict.
- `packages/spec-harness` diagnostics/build wiring needs cleanup. Do not add a
  shadow `tsconfig.diagnostics.json`; use the real package config/layout.
- Confirm `@firegrid/observability` exports `ChdbSession`, `ChdbClient`,
  `ChdbSpanExporter`, and the layer helpers from `src/index.ts`.
- Confirm `spec-harness` imports resolve against local workspace source, not a
  stale package surface.
- Remove any leftover fake session interfaces or client/exporter coupling.
- Check whether Firelab deletion is premature relative to CI coverage. The SDD
  says delete Firelab only after equivalent Cucumber coverage is green.
- Keep Cucumber support layout conventional:
  - `features/support/*.ts` for support/fixtures/parameter types;
  - `features/step_definitions/*.ts` for step definitions.
- Keep proof SQL in feature DocStrings for now.

## Suggested Verification

Run the narrow checks first:

```sh
pnpm --filter @firegrid/observability typecheck
pnpm --filter @firegrid/spec-harness typecheck
pnpm --filter @firegrid/spec-harness spec
```

Then run broader checks relevant to this branch:

```sh
pnpm --filter @firegrid/observability diagnostics
pnpm --filter @firegrid/spec-harness diagnostics
pnpm lint
pnpm typecheck
```

If `effect-language-service diagnostics` fails with `No Project`, fix the
package layout/script against the real `tsconfig.json`. Do not add a separate
diagnostics config as a workaround.

## Open Build Questions

- Should the spec harness own only Cucumber runtime code while fixtures/steps
  live under top-level `features/`, or should executable support code move into
  a package-local test/support area? Current user preference is conventional
  Cucumber layout under `features/support` and `features/step_definitions`.
- Should Firelab be deleted in this PR or only after parity is proven? The SDD
  now says deletion should wait for equivalent Cucumber CI coverage.
- How should Cucumber reports persist trace context in CI after the in-memory
  chDB session closes? Baseline can print/report from the formatter; explicit
  artifact export can be added later if needed.

## Minimal Next Step

Stabilize the observability/spec-harness build without changing the design:

1. Fast-forward the branch.
2. Ensure `ChdbSession` is exported from `@firegrid/observability`.
3. Ensure `ChdbClient.layer({})` provides both session and client.
4. Ensure `world.ts` composes `ChdbLayer({})`, `OtelLive`, and `S2LiteLive`
   without `World` owning the session.
5. Run the narrow checks above.
6. Only then decide whether Firelab deletion should remain in this branch.
