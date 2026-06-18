import { defineParameterType } from "@cucumber/cucumber"
import { Schema } from "effect"
import type { Effect } from "effect"
import type { S2Client } from "effect-s2"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import type { StreamDbInstance } from "effect-s2-stream-db"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

class Note extends Table<Note>("notes")({
  key: Schema.String.pipe(primaryKey),
  text: Schema.String,
}) {}

class TestDb extends StreamDb<TestDb>("cucumber-stream-db")({ items: Item, notes: Note }) {}

export type StreamDbFixtureInstance = StreamDbInstance<{ readonly items: typeof Item; readonly notes: typeof Note }>

export interface Fixture<A> {
  readonly name: string
  readonly make: (ctx: { readonly key: string }) => Effect.Effect<A, unknown, S2Client>
}

export const STREAM_DB_FIXTURES = {
  "stream-db": {
    name: "stream-db",
    make: ({ key }) => TestDb.open(key),
  },
  "stream-db:retained": {
    name: "stream-db:retained",
    make: ({ key }) => TestDb.open(key, { config: { retentionPolicy: { infinite: {} } } }),
  },
} satisfies Record<string, Fixture<StreamDbFixtureInstance>>

export const StreamDbFixtureName = Schema.Literals(
  Object.keys(STREAM_DB_FIXTURES) as [keyof typeof STREAM_DB_FIXTURES, ...(keyof typeof STREAM_DB_FIXTURES)[]],
)

const escapedAlternation = Object.keys(STREAM_DB_FIXTURES)
  .map((key) => key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
  .join("|")

defineParameterType({
  name: "streamDbFixture",
  regexp: new RegExp(escapedAlternation),
  transformer(raw: string): Fixture<StreamDbFixtureInstance> {
    const name = Schema.decodeUnknownSync(StreamDbFixtureName)(raw)
    return STREAM_DB_FIXTURES[name]
  },
})

export { TestDb }
