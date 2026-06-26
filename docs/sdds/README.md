# SDD Index

These are the active design documents for this repository.

## Active Path

- [`effect-s2.md`](./effect-s2.md) — Effect-native S2 client/substrate.
- [`store-store-sdd.md`](./store-store-sdd.md) —
  S2-backed TanStack Workflow runtime store and host substrate.
- [`fluent-authoring-surface-sdd.md`](./fluent-authoring-surface-sdd.md) —
  Restate-like Effect-native definitions, clients, and transport-neutral
  bindings.
- [`fluent-state-materialization-sdd.md`](./fluent-state-materialization-sdd.md) —
  virtual object state and table/materialization semantics.
- [`fluent-finish-line-sdd.md`](./fluent-finish-line-sdd.md) —
  remaining Restate-like ergonomics and product-surface gaps above the proven
  substrate.

## Support Package Docs

- [`../../apps/proofs/README.md`](../../apps/proofs/README.md) —
  trace-native proof harness and current proof registry.

## Policy

SDDs in this directory should describe production surfaces that exist in
`packages/*` or the next layer directly above them. Superseded runtime plans for
deleted packages should be removed instead of kept as active-looking reference
material.
