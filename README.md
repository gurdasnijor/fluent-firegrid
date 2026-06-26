# fluent-firegrid

Effect-native durable execution and fluent authoring over S2.

This repo no longer carries legacy durable-runtime experiments in `packages/*`.
Production work should land in one of the package lanes below. If a package is
not listed here, do not add it without also updating this map and the relevant
SDD.

## Production Packages

- `@firegrid/log` — Effect wrapper around the S2 client/substrate.
- `@firegrid/core` — shared durable workflow contract, primitive types, and
  run-store interfaces.
- `@firegrid/fluent` — Restate-like Effect-native authoring surface:
  definitions, descriptor contracts, clients, durable primitives, virtual
  object state authoring, runtime driving, S2 adapters, testing helpers, and
  HTTP transport bindings.

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
`@firegrid/log` and `@firegrid/fluent` runtime/object surfaces from workload
results plus OTel/chDB trace evidence.

## Cleanup Policy

Legacy experiments should not remain as workspace packages. If a direction is
replaced, delete the package and its static-tooling registrations instead of
leaving a README that looks like an active surface.

Historical architecture material can live in docs only when it directly explains
the current production path. Superseded implementation plans should be deleted
or moved out of the active repo before they start steering agents.
