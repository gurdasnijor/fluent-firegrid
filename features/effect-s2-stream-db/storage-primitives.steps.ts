import { Given, Then, When } from "@cucumber/cucumber"
import { strict as assert } from "node:assert"
import { Effect, Option } from "effect"
import type { Fixture, StreamDbFixtureInstance } from "../support/fixtures.ts"
import { TestDb } from "../support/fixtures.ts"
import { defineInventoryStep } from "../support/spec_inventory.ts"
import type { FiregridWorld } from "../support/world.ts"
import { runScenarioEffect } from "../support/world.ts"

defineInventoryStep("the storage-primitives contract includes:")

type StoragePrimitivesWorld = FiregridWorld & {
  streamDb?: unknown
  streamDbKey?: string
}

const dbFor = (world: StoragePrimitivesWorld): StreamDbFixtureInstance => {
  if (world.streamDb === undefined) {
    throw new Error("stream-db is not open")
  }
  return world.streamDb as StreamDbFixtureInstance
}

Given("an open {streamDbFixture} at key {string}", async function(
  this: StoragePrimitivesWorld,
  fixture: Fixture<StreamDbFixtureInstance>,
  key: string,
) {
  const actualKey = `${this.scenarioId.replace(/[^A-Za-z0-9_.-]/g, "-")}-${key}`
  this.streamDbKey = actualKey
  this.streamDb = await runScenarioEffect(
    this,
    `open ${fixture.name} ${key}`,
    fixture.make({ key: actualKey }),
  )
})

When("I insert item {string} value {int}", async function(this: StoragePrimitivesWorld, id: string, value: number) {
  await runScenarioEffect(
    this,
    `insert item ${id}`,
    dbFor(this).items.insert({ id, value }),
  )
})

When("I upsert item {string} value {int}", async function(this: StoragePrimitivesWorld, id: string, value: number) {
  await runScenarioEffect(
    this,
    `upsert item ${id}`,
    dbFor(this).items.upsert({ id, value }),
  )
})

When("I delete item {string}", async function(this: StoragePrimitivesWorld, id: string) {
  await runScenarioEffect(this, `delete item ${id}`, dbFor(this).items.delete(id))
})

When("I checkpoint", async function(this: StoragePrimitivesWorld) {
  await runScenarioEffect(this, "checkpoint", dbFor(this).checkpoint)
})

Then("reopening, item {string} is {int}", async function(this: StoragePrimitivesWorld, id: string, expected: number) {
  if (this.streamDbKey === undefined) {
    throw new Error("stream-db key is not set")
  }
  const key = this.streamDbKey
  const actual = await runScenarioEffect(
    this,
    `reopen item ${id}`,
    Effect.gen(function*() {
      const db = yield* TestDb.open(key)
      return yield* db.items.get(id)
    }),
  )
  assert.deepEqual(Option.getOrThrow(actual), { id, value: expected })
})

Then("reopening, item {string} is absent", async function(this: StoragePrimitivesWorld, id: string) {
  if (this.streamDbKey === undefined) {
    throw new Error("stream-db key is not set")
  }
  const key = this.streamDbKey
  const actual = await runScenarioEffect(
    this,
    `reopen missing item ${id}`,
    Effect.gen(function*() {
      const db = yield* TestDb.open(key)
      return yield* db.items.get(id)
    }),
  )
  assert.equal(Option.isNone(actual), true)
})

Then("the trace should satisfy:", function(this: StoragePrimitivesWorld, sql: string) {
  this.proofs.push({ sql })
})
