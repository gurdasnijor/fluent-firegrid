import type { PickleTable } from "@cucumber/messages"

/**
 * The data-table argument handed to a step body as its trailing parameter,
 * replacing `@cucumber/core`'s `DataTable`. A thin read-only view over the
 * pickle table's raw string cells with the canonical Cucumber accessors.
 */
export class DataTable {
  private readonly cells: ReadonlyArray<ReadonlyArray<string>>

  constructor(rows: ReadonlyArray<ReadonlyArray<string>>) {
    this.cells = rows
  }

  static from(table: PickleTable): DataTable {
    return new DataTable(table.rows.map((row) => row.cells.map((cell) => cell.value)))
  }

  /** Every row, header included. */
  raw(): ReadonlyArray<ReadonlyArray<string>> {
    return this.cells.map((row) => [...row])
  }

  /** Every row except the header. */
  rows(): ReadonlyArray<ReadonlyArray<string>> {
    return this.cells.slice(1).map((row) => [...row])
  }

  /** Body rows as objects keyed by the header row. */
  hashes(): ReadonlyArray<Record<string, string>> {
    const [header, ...body] = this.cells
    if (header === undefined) return []
    return body.map((row) => Object.fromEntries(header.map((key, index) => [key, row[index] ?? ""])))
  }

  /** A two-column table as a single key→value record. */
  rowsHash(): Record<string, string> {
    return Object.fromEntries(this.cells.map((row) => [row[0] ?? "", row[1] ?? ""]))
  }

  /** Rows transposed (columns become rows). */
  transpose(): DataTable {
    const width = this.cells.reduce((max, row) => Math.max(max, row.length), 0)
    return new DataTable(Array.from({ length: width }, (_, col) => this.cells.map((row) => row[col] ?? "")))
  }
}
