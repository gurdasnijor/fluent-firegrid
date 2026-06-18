import { setWorldConstructor } from "@cucumber/cucumber"
import { Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import type { StreamDbInstance } from "effect-s2-stream-db"
import { FiregridWorld as HarnessWorld } from "../../packages/spec-harness/src/world.ts"

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

export interface StoragePrimitivesWorld {
  readonly Item: typeof Item
  readonly Note: typeof Note
  readonly StorageDb: typeof StorageDb
  db?: StorageDbInstance
  key?: string
}

export class FiregridWorld extends HarnessWorld {
  readonly storagePrimitives: StoragePrimitivesWorld = {
    Item,
    Note,
    StorageDb,
  }
}

setWorldConstructor(FiregridWorld)
