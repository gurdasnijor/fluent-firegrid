# SDD Index

These are the active design documents for this repository.

## Active Path

- [`effect-s2.md`](./effect-s2.md) — Effect-native S2 client/substrate.
- [`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md) —
  S2-backed TanStack Workflow runtime store and host substrate.
- [`fluent-firegrid-authoring-surface-sdd.md`](./fluent-firegrid-authoring-surface-sdd.md) —
  Restate-like Effect-native definitions, clients, and transport-neutral
  bindings.
- [`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md) —
  virtual object state and table/materialization semantics.
- [`fluent-firegrid-finish-line-sdd.md`](./fluent-firegrid-finish-line-sdd.md) —
  remaining Restate-like ergonomics and product-surface gaps above the proven
  substrate.
- [`package-boundary-consolidation-sdd.md`](./package-boundary-consolidation-sdd.md) —
  package export inventory and consolidation recommendation for the current
  premature package boundaries.

## Support Package Docs

- [`../../apps/proofs/README.md`](../../apps/proofs/README.md) —
  trace-native proof harness and current proof registry.

## Policy

SDDs in this directory should describe production surfaces that exist in
`packages/*` or the next layer directly above them. Superseded runtime plans for
deleted packages should be removed instead of kept as active-looking reference
material.
