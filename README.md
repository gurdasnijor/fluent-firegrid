# fluent-firegrid

Effect-native durable execution and fluent authoring over S2.

This repo no longer carries legacy durable-runtime experiments in `packages/*`.
Production work should land in one of the source lanes below. If a lane is
not listed here, do not add it without also updating this map and the relevant
SDD.

## Production Lanes

- `Firegrid.Log` — EffSharp wrapper around the S2 client/substrate.
- `@firegrid/core` — shared durable workflow contract, primitive types, and
  run-store interfaces.
- `@firegrid/runtime` — durable workflow runtime, schedule materialization, and
  in-memory store implementation.
- `@firegrid/store` / `Firegrid.Store` — S2-backed workflow execution store,
  host recovery/sweep helpers, and fluent object/state runtime binding.
- `@firegrid/fluent` — Restate-like Effect-native authoring surface:
  definitions, descriptor contracts, clients, durable primitives, and virtual
  object state authoring.
- `@firegrid/fluent/http` — framework-neutral HTTP
  `Request -> Response` transport binding for fluent definitions.

## Support Packages

- `@firegrid/proofs` — real-substrate proof harness and proof registry.
- `@firegrid/trace` — tracing/export support used by verification and
  runtime processes.
- `@firegrid/acp-process` — ACP process adapter work; separate from the
  durable execution core.

## Verification

Run the production proof registry with:

```sh
pnpm run proofs
```

This starts real `s2 lite` and process-host fixtures, then verifies the current
log, store, runtime, and fluent object surfaces from workload results plus
OTel/chDB trace evidence.

## Cleanup Policy

Legacy experiments should not remain as workspace packages. If a direction is
replaced, delete the package and its static-tooling registrations instead of
leaving a README that looks like an active surface.

Historical architecture material can live in docs only when it directly explains
the current production path. Superseded implementation plans should be deleted
or moved out of the active repo before they start steering agents.
