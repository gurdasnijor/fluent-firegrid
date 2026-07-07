/**
 * `@firegrid/l1-vocabulary` — the L1 observation vocabulary (cross-lane
 * interface I2): an ACP `session/update` superset with additive, ignorable
 * `firegrid/` extensions, plus its Effect-free decoder and canonical base fold.
 *
 * See `docs/canon/architecture/fluent/l1-observation-vocabulary.md` for the G2
 * decision record and the full contract.
 */

export * from "./decode.ts"
export * from "./fixtures.ts"
export * from "./fold.ts"
export * from "./vocabulary.ts"
