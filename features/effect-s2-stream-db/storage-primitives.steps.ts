import { Given, Then, When } from "@cucumber/cucumber"
import { strict as assert } from "node:assert"
import { Effect, Option } from "effect"
import { defineInventoryStep } from "../support/spec_inventory.ts"
import type { FiregridWorld } from "../support/world.ts"

defineInventoryStep("the storage-primitives contract includes:")

Given("an open storage db with infinite retention at key {string}", async function(
  this: FiregridWorld,
  key: string,
) {
  const actualKey = this.scenarioKey(key)
  this.storagePrimitives.key = actualKey
  this.storagePrimitives.db = await this.run(
    `open storage db ${key}`,
    this.storagePrimitives.StorageDb.open(actualKey, { config: { retentionPolicy: { infinite: {} } } }),
  )
})

When("I insert item {string} value {int}", async function(this: FiregridWorld, id: string, value: number) {
  await this.run(`insert item ${id}`, storageDbFor(this).items.insert({ id, value }))
})

When("I upsert item {string} value {int}", async function(this: FiregridWorld, id: string, value: number) {
  await this.run(`upsert item ${id}`, storageDbFor(this).items.upsert({ id, value }))
})

When("I delete item {string}", async function(this: FiregridWorld, id: string) {
  await this.run(`delete item ${id}`, storageDbFor(this).items.delete(id))
})

When("I checkpoint", async function(this: FiregridWorld) {
  await this.run("checkpoint", storageDbFor(this).checkpoint)
})

Then("reopening, item {string} is {int}", async function(this: FiregridWorld, id: string, expected: number) {
  const key = storageKeyFor(this)
  const { StorageDb } = this.storagePrimitives
  const actual = await this.run(
    `reopen item ${id}`,
    Effect.gen(function*() {
      const db = yield* StorageDb.open(key)
      return yield* db.items.get(id)
    }),
  )
  assert.deepEqual(Option.getOrThrow(actual), { id, value: expected })
})

Then("reopening, item {string} is absent", async function(this: FiregridWorld, id: string) {
  const key = storageKeyFor(this)
  const { StorageDb } = this.storagePrimitives
  const actual = await this.run(
    `reopen missing item ${id}`,
    Effect.gen(function*() {
      const db = yield* StorageDb.open(key)
      return yield* db.items.get(id)
    }),
  )
  assert.equal(Option.isNone(actual), true)
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
