import { Given, Then, When } from "@cucumber/cucumber"
import { defineInventoryStep } from "../support/spec_inventory.ts"
import type { FiregridWorld } from "../support/world.ts"

defineInventoryStep("the storage-primitives contract includes:")

Given("an open storage db with infinite retention at key {string}", async function(
  this: FiregridWorld,
  key: string,
) {
  await this.storagePrimitives.openWithInfiniteRetention(key)
})

When("I insert item {string} value {int}", async function(this: FiregridWorld, id: string, value: number) {
  await this.storagePrimitives.insertItem(id, value)
})

When("I upsert item {string} value {int}", async function(this: FiregridWorld, id: string, value: number) {
  await this.storagePrimitives.upsertItem(id, value)
})

When("I delete item {string}", async function(this: FiregridWorld, id: string) {
  await this.storagePrimitives.deleteItem(id)
})

When("I checkpoint", async function(this: FiregridWorld) {
  await this.storagePrimitives.checkpoint()
})

Then("reopening, item {string} is {int}", async function(this: FiregridWorld, id: string, expected: number) {
  await this.storagePrimitives.assertReopenedItem(id, expected)
})

Then("reopening, item {string} is absent", async function(this: FiregridWorld, id: string) {
  await this.storagePrimitives.assertReopenedItemAbsent(id)
})

Then("the trace should satisfy:", function(this: FiregridWorld, sql: string) {
  this.addTraceProof(sql)
})
