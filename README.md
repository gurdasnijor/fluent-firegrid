# fluent-firegrid

Effect-native durable execution and fluent authoring over S2.

This repo no longer carries legacy durable-runtime experiments in `packages/*`.
Production work should land in one of the package lanes below. If a package is
not listed here, do not add it without also updating this map and the relevant
SDD.

## Production Packages

- `effect-s2` — Effect wrapper around the S2 client/substrate.
- `@tanstack/workflow-core` — vendored TanStack Workflow core compatibility
  source.
- `@tanstack/workflow-runtime` — vendored TanStack Workflow runtime/store
  compatibility source.
- `@firegrid/tanstack-workflow-s2` — S2-backed TanStack
  `WorkflowExecutionStore` plus host recovery/sweep helpers.
- `@firegrid/fluent-firegrid` — Restate-like Effect-native authoring surface:
  definitions, descriptor contracts, clients, durable primitives, and virtual
  object state authoring.
- `@firegrid/fluent-firegrid-s2` — S2-backed fluent object/state runtime
  binding.
- `@firegrid/fluent-firegrid-http` — framework-neutral HTTP
  `Request -> Response` transport binding for fluent definitions.

## Support Packages

- `@firegrid/verification` — real-substrate proof harness and proof registry.
- `@firegrid/observability` — tracing/export support used by verification and
  runtime processes.
- `@firegrid/fluent-acp-process` — ACP process adapter work; separate from the
  durable execution core.

## Cleanup Policy

Legacy experiments should not remain as workspace packages. If a direction is
replaced, delete the package and its static-tooling registrations instead of
leaving a README that looks like an active surface.

Historical architecture material can live in docs only when it directly explains
the current production path. Superseded implementation plans should be deleted
or moved out of the active repo before they start steering agents.
