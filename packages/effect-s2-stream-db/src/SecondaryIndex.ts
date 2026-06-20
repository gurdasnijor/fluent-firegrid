import type { Effect } from "effect"
import type { S2StreamDbError } from "./errors.ts"
import type { TableFacade } from "./StreamDb.ts"

/**
 * A durable secondary index over a table: look rows up by a projected key
 * instead of the primary key. This first cut is an on-read index — it folds the
 * table's materialized rows (`query`) and groups by `project`, so it stays
 * exactly consistent with the table and needs no separate maintained state.
 *
 * The projected key must be a primitive (compared with `===`); ids/brands fit.
 * A maintained (incrementally-updated) index is a later slice, declared with the
 * fluent `toTable(..., { indexes })` surface; this accessor is its read shape.
 */
export interface SecondaryIndex<Row, K> {
  /** All rows whose projected key equals `key`, in table (insertion) order. */
  readonly get: (key: K) => Effect.Effect<ReadonlyArray<Row>, S2StreamDbError>
  /** The distinct index keys currently present. */
  readonly keys: () => Effect.Effect<ReadonlyArray<K>, S2StreamDbError>
}

/** Build a secondary index over a table facade, keyed by `project(row)`. */
export const secondaryIndex = <Row, K>(
  facade: Pick<TableFacade<Row>, "query">,
  project: (row: Row) => K,
): SecondaryIndex<Row, K> => ({
  get: (key) => facade.query((rows) => rows.filter((row) => project(row) === key)),
  keys: () => facade.query((rows) => Array.from(new Set(rows.map(project)))),
})
