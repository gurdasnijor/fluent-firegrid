/**
 * Tests for the chDB (embedded ClickHouse) Effect SQL driver.
 *
 * Two layers, matching the driver's own split:
 *   - PURE: the `Ch` literal builders and their composition — no native chdb.
 *   - INTEGRATION: a real chdb-node Session via `ChdbClient.layer`, exercising
 *     the `sql` tag (literal inlining, since chDB has no bind channel), typed
 *     `param`, Schema-decoded `query`, `insertQuery`, `asCommand`, and error
 *     classification.
 */
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { Ch, ChdbClient, layer } from "../src/ChdbClient.ts"

// ── pure: Ch literal builders ─────────────────────────────────────────────────

describe("Ch literal builders", () => {
  it("renders scalars with their ClickHouse type", () => {
    expect(Ch.String.lit("hi")).toBe("'hi'")
    expect(Ch.String.lit("a'b\\c")).toBe("'a\\'b\\\\c'") // ' and \ are escaped
    expect(Ch.Int64.lit(5)).toBe("5")
    expect(Ch.Int64.lit(5.9)).toBe("5") // truncated, not rounded
    expect(Ch.Int64.lit(9007199254740993n)).toBe("9007199254740993") // bigint is lossless
    expect(Ch.UInt64.type).toBe("UInt64")
    expect(Ch.Float64.lit(1.5)).toBe("1.5")
    expect(Ch.Bool.lit(true)).toBe("true")
    expect(Ch.Bool.lit(false)).toBe("false")
    expect(Ch.UUID.lit("0000-0000")).toBe("'0000-0000'")
    expect(Ch.DateTime64Nanos.lit(1_700_000_000_000_000_000n)).toBe(
      "fromUnixTimestamp64Nano(1700000000000000000)"
    )
  })

  it("composes array / nullable / map by function composition", () => {
    const arr = Ch.array(Ch.Int64)
    expect(arr.type).toBe("Array(Int64)")
    expect(arr.lit([1, 2, 3])).toBe("[1, 2, 3]")

    const nul = Ch.nullable(Ch.String)
    expect(nul.type).toBe("Nullable(String)")
    expect(nul.lit(null)).toBe("NULL")
    expect(nul.lit(undefined)).toBe("NULL")
    expect(nul.lit("x")).toBe("'x'")

    const map = Ch.map(Ch.String, Ch.Int64)
    expect(map.type).toBe("Map(String, Int64)")
    expect(map.lit({ a: 1, b: 2 })).toBe("map('a', 1, 'b', 2)")
    expect(map.lit(new Map([["a", 1]]))).toBe("map('a', 1)")

    // nesting builds both the renderer and the type with no stringly-typed dispatch
    const nested = Ch.array(Ch.nullable(Ch.Int64))
    expect(nested.type).toBe("Array(Nullable(Int64))")
    expect(nested.lit([1, null, 3])).toBe("[1, NULL, 3]")
  })
})

// ── integration: a real chdb-node Session ─────────────────────────────────────

class Event extends Schema.Class<Event>("Event")({
  id: Schema.String,
  n: Schema.Number,
  tags: Schema.Array(Schema.String)
}) {}

const provide = <A, E, R>(effect: Effect.Effect<A, E, R | ChdbClient>) => Effect.provide(effect, layer({}))

describe("ChdbClient", () => {
  it.effect("inlines interpolated values as literals (no bind channel)", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      const rows = yield* sql<{ n: number }>`SELECT ${42} AS n`
      expect(rows).toEqual([{ n: 42 }])
    })))

  it.effect("inlines a typed param fragment", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      const rows = yield* sql<{ n: number }>`SELECT ${sql.param(Ch.Int64, 100n)} AS n`
      expect(rows).toEqual([{ n: 100 }])
    })))

  it.effect("round-trips insert -> select, decoding rows through a Schema", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      yield* sql`CREATE TABLE events (id String, n Int64, tags Array(String)) ENGINE = MergeTree ORDER BY id`

      const written = yield* sql.insertQuery({
        table: "events",
        schema: Event,
        values: [
          new Event({ id: "a", n: 1, tags: ["x"] }),
          new Event({ id: "b", n: 2, tags: ["y", "z"] })
        ]
      })
      expect(written).toEqual({ written: 2 })

      const events = yield* sql.query(Event, sql`SELECT id, n, tags FROM events ORDER BY id`)
      expect(events).toEqual([
        new Event({ id: "a", n: 1, tags: ["x"] }),
        new Event({ id: "b", n: 2, tags: ["y", "z"] })
      ])
    })))

  it.effect("filters by a typed param literal", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      yield* sql`CREATE TABLE nums (n Int64) ENGINE = MergeTree ORDER BY n`
      yield* sql.insertQuery({ table: "nums", values: [{ n: 1 }, { n: 2 }, { n: 3 }] })

      const rows = yield* sql<{ n: number }>`SELECT n FROM nums WHERE n >= ${sql.param(Ch.Int64, 2n)} ORDER BY n`
      expect(rows).toEqual([{ n: 2 }, { n: 3 }])
    })))

  it.effect("asCommand executes for effect and yields no rows", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      yield* sql`CREATE TABLE c (n Int64) ENGINE = MergeTree ORDER BY n`
      const result = yield* sql.asCommand(sql`INSERT INTO c VALUES (${1}), (${2})`)
      expect(result).toEqual([])

      const count = yield* sql<{ c: number }>`SELECT count() AS c FROM c`
      expect(count).toEqual([{ c: 2 }])
    })))

  it.effect("classifies a syntax error as SqlSyntaxError", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      const error = yield* Effect.flip(sql`SELEC nonsense`)
      expect(error._tag).toBe("SqlError")
      expect(error.reason._tag).toBe("SqlSyntaxError")
    })))

  it.effect("classifies an unknown table as SqlSyntaxError", () =>
    provide(Effect.gen(function*() {
      const sql = yield* ChdbClient
      const error = yield* Effect.flip(sql`SELECT * FROM does_not_exist`)
      expect(error.reason._tag).toBe("SqlSyntaxError")
    })))
})
