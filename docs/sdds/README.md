# SDD Index

Doc-Class: SDD
Status: active
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: neutral

These are the design documents for this repository. SDDs are implementation
plans, not stable contracts; re-status them when the package or substrate target
changes.

## Active Path

- [`fluent-firegrid-authoring-surface-sdd.md`](./fluent-firegrid-authoring-surface-sdd.md) —
  Restate-like Effect-native definitions, clients, and transport-neutral
  bindings.
- [`fluent-firegrid-finish-line-sdd.md`](./fluent-firegrid-finish-line-sdd.md) —
  remaining Restate-like ergonomics, native-kernel direction, and
  product-surface gaps above the proven S2 substrate.
- [`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md) —
  virtual object state and table/materialization semantics; implemented for the
  current TypeScript surface and subject to EffSharp package-boundary updates.

## Frozen / Historical Scaffolding

- [`effect-s2.md`](./effect-s2.md) — original TypeScript `@firegrid/log`
  implementation plan. The active implementation now lives in
  `src/Firegrid.Log`.
- [`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md) —
  accurate record of the TanStack/S2 store scaffolding, but not the build target.
- [`package-boundary-consolidation-sdd.md`](./package-boundary-consolidation-sdd.md) —
  package export inventory from the pre-EffSharp consolidation.

## Support Package Docs

- [`../../apps/proofs/README.md`](../../apps/proofs/README.md) —
  trace-native proof harness and current proof registry.

## Policy

SDDs in this directory should describe production surfaces that exist in
`packages/*`, `src/Firegrid.*`, or the next layer directly above them.
Superseded runtime plans for deleted packages should be removed or explicitly
marked frozen/superseded instead of kept as active-looking reference material.
