import { setWorldConstructor } from "@cucumber/cucumber"
import { strict as assert } from "node:assert"
import { Effect, Option, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import type { StreamDbInstance } from "effect-s2-stream-db"
import { FiregridWorld as HarnessWorld, runScenarioEffect } from "../../packages/spec-harness/src/world.ts"

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

interface StreamDbState {
  readonly db: StorageDbInstance
  readonly key: string
}

class StoragePrimitivesWorld {
  private state?: StreamDbState

  constructor(private readonly world: HarnessWorld) {}

  async openWithInfiniteRetention(key: string): Promise<void> {
    const actualKey = this.world.scenarioKey(key)
    const db = await this.world.run(
      `open storage db ${key}`,
      StorageDb.open(actualKey, { config: { retentionPolicy: { infinite: {} } } }),
    )
    this.state = { db, key: actualKey }
  }

  async insertItem(id: string, value: number): Promise<void> {
    await this.world.run(`insert item ${id}`, this.current().db.items.insert({ id, value }))
  }

  async upsertItem(id: string, value: number): Promise<void> {
    await this.world.run(`upsert item ${id}`, this.current().db.items.upsert({ id, value }))
  }

  async deleteItem(id: string): Promise<void> {
    await this.world.run(`delete item ${id}`, this.current().db.items.delete(id))
  }

  async checkpoint(): Promise<void> {
    await this.world.run("checkpoint", this.current().db.checkpoint)
  }

  async assertReopenedItem(id: string, expected: number): Promise<void> {
    const { key } = this.current()
    const actual = await this.world.run(
      `reopen item ${id}`,
      Effect.gen(function*() {
        const db = yield* StorageDb.open(key)
        return yield* db.items.get(id)
      }),
    )
    assert.deepEqual(Option.getOrThrow(actual), { id, value: expected })
  }

  async assertReopenedItemAbsent(id: string): Promise<void> {
    const { key } = this.current()
    const actual = await this.world.run(
      `reopen missing item ${id}`,
      Effect.gen(function*() {
        const db = yield* StorageDb.open(key)
        return yield* db.items.get(id)
      }),
    )
    assert.equal(Option.isNone(actual), true)
  }

  private current(): StreamDbState {
    if (this.state === undefined) {
      throw new Error("storage db is not open")
    }
    return this.state
  }
}

export class FiregridWorld extends HarnessWorld {
  readonly storagePrimitives = new StoragePrimitivesWorld(this)
}

setWorldConstructor(FiregridWorld)

export { runScenarioEffect }
