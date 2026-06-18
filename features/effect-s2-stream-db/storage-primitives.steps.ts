import { Then } from "@cucumber/cucumber"
import { strict as assert } from "node:assert"
import { Effect, Option } from "effect"
import { GivenEffect, ThenEffect, WhenEffect } from "../support/effect_steps.ts"
import { defineInventoryStep } from "../support/spec_inventory.ts"
import type { FiregridWorld } from "../support/world.ts"

defineInventoryStep("the storage-primitives contract includes:")

GivenEffect("an open storage db with infinite retention at key {string}", function(
  this: FiregridWorld,
  key: string,
) {
  const actualKey = this.scenarioKey(key)
  this.storagePrimitives.key = actualKey
  return this.storagePrimitives.StorageDb.open(
    actualKey,
    { config: { retentionPolicy: { infinite: {} } } },
  ).pipe(
    Effect.tap((db) =>
      Effect.sync(() => {
        this.storagePrimitives.db = db
      })),
  )
})

WhenEffect("I insert item {string} value {int}", function(this: FiregridWorld, id: string, value: number) {
  return storageDbFor(this).items.insert({ id, value })
})

WhenEffect("I upsert item {string} value {int}", function(this: FiregridWorld, id: string, value: number) {
  return storageDbFor(this).items.upsert({ id, value })
})

WhenEffect("I delete item {string}", function(this: FiregridWorld, id: string) {
  return storageDbFor(this).items.delete(id)
})

WhenEffect("I checkpoint", function(this: FiregridWorld) {
  return storageDbFor(this).checkpoint
})

ThenEffect("reopening, item {string} is {int}", function(this: FiregridWorld, id: string, expected: number) {
  const key = storageKeyFor(this)
  const { StorageDb } = this.storagePrimitives
  return Effect.gen(function*() {
    const db = yield* StorageDb.open(key)
    const actual = yield* db.items.get(id)
    assert.deepEqual(Option.getOrThrow(actual), { id, value: expected })
  })
})

ThenEffect("reopening, item {string} is absent", function(this: FiregridWorld, id: string) {
  const key = storageKeyFor(this)
  const { StorageDb } = this.storagePrimitives
  return Effect.gen(function*() {
    const db = yield* StorageDb.open(key)
    const actual = yield* db.items.get(id)
    assert.equal(Option.isNone(actual), true)
  })
})

Then("the trace should satisfy:", function(this: FiregridWorld, sql: string) {
  this.addTraceProof(sql)
})

const storageDbFor = (world: FiregridWorld) => {
  const db = world.storagePrimitives.db
  if (db === undefined) {
    throw new Error("storage db is not open")
  }
  return db
}

const storageKeyFor = (world: FiregridWorld): string => {
  const key = world.storagePrimitives.key
  if (key === undefined) {
    throw new Error("storage db key is not set")
  }
  return key
}
