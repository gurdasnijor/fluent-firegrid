import { strict as assert } from "node:assert"
import { Effect, Option, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import type { StreamDbInstance } from "effect-s2-stream-db"
import { defineSteps } from "../../packages/durable-cucumber/src/durable/support.ts"
import { scenarioKey, type SpecWorld } from "../../packages/durable-cucumber/src/firegrid/proofs.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

class Note extends Table<Note>("notes")({
  key: Schema.String.pipe(primaryKey),
  text: Schema.String,
}) {}

class StorageDb extends StreamDb<StorageDb>("cucumber-storage-primitives")({ items: Item, notes: Note }) {}

type StorageDbInstance = StreamDbInstance<{ readonly items: typeof Item; readonly notes: typeof Note }>

interface StorageState {
  db?: StorageDbInstance
  key?: string
}

const storageStates = new WeakMap<SpecWorld, StorageState>()

const storageStateFor = (world: SpecWorld): StorageState => {
  let state = storageStates.get(world)
  if (state === undefined) {
    state = {}
    storageStates.set(world, state)
  }
  return state
}

const storageDbFor = (world: SpecWorld) => {
  const db = storageStateFor(world).db
  if (db === undefined) {
    throw new Error("storage db is not open")
  }
  return db
}

const storageKeyFor = (world: SpecWorld): string => {
  const key = storageStateFor(world).key
  if (key === undefined) {
    throw new Error("storage db key is not set")
  }
  return key
}

export const storagePrimitivesSteps = defineSteps(({ Given, When, Then }) => {
  Given("an open storage db with infinite retention at key {string}", function(
    this: SpecWorld,
    key: string,
  ) {
    const state = storageStateFor(this)
    const actualKey = scenarioKey(this, key)
    state.key = actualKey
    return StorageDb.open(
      actualKey,
      { config: { retentionPolicy: { infinite: {} } } },
    ).pipe(
      Effect.tap((db) =>
        Effect.sync(() => {
          state.db = db
        })),
    )
  })

  When("I insert item {string} value {int}", function(this: SpecWorld, id: string, value: number) {
    return storageDbFor(this).items.insert({ id, value })
  })

  When("I upsert item {string} value {int}", function(this: SpecWorld, id: string, value: number) {
    return storageDbFor(this).items.upsert({ id, value })
  })

  When("I delete item {string}", function(this: SpecWorld, id: string) {
    return storageDbFor(this).items.delete(id)
  })

  When("I checkpoint", function(this: SpecWorld) {
    return storageDbFor(this).checkpoint
  })

  Then("reopening, item {string} is {int}", function(this: SpecWorld, id: string, expected: number) {
    const key = storageKeyFor(this)
    return Effect.gen(function*() {
      const db = yield* StorageDb.open(key)
      const actual = yield* db.items.get(id)
      assert.deepEqual(Option.getOrThrow(actual), { id, value: expected })
    })
  })

  Then("reopening, item {string} is absent", function(this: SpecWorld, id: string) {
    const key = storageKeyFor(this)
    return Effect.gen(function*() {
      const db = yield* StorageDb.open(key)
      const actual = yield* db.items.get(id)
      assert.equal(Option.isNone(actual), true)
    })
  })
})
