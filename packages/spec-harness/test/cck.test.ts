import { createReadStream } from "node:fs"
import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { normalizeEnvelopes } from "../src/durable/cck.ts"
import { runFeaturesLocal } from "../src/durable/run.ts"
import { cckExpectedEnvelopes, cckSamplePath, loadCckSources } from "./cck-support.ts"
import { defineSupport, type SupportModule } from "../src/durable/support.ts"

// Support code for the targeted CCK samples, authored against the durable
// runner's Cucumber-shaped DSL. These mirror the kit's reference step
// definitions (`@cucumber/fake-cucumber`) one-for-one.

const minimalSupport = defineSupport(({ Given }) => {
  Given("I have {int} cukes in my belly", (_cukeCount: number) => {
    // no-op
  })
})

const attachmentsSupport = defineSupport(({ When }) => {
  When("the string {string} is attached as {string}", async function(text: string, mediaType: string) {
    await (this as { attach: (data: unknown, options: unknown) => Promise<void> }).attach(text, mediaType)
  })

  When("the string {string} is logged", async function(text: string) {
    await (this as { log: (text: string) => Promise<void> }).log(text)
  })

  When("text with ANSI escapes is logged", async function() {
    await (this as { log: (text: string) => Promise<void> }).log(
      "This displays a \x1b[31mr\x1b[0m\x1b[91ma\x1b[0m\x1b[33mi\x1b[0m\x1b[32mn\x1b[0m\x1b[34mb\x1b[0m\x1b[95mo\x1b[0m\x1b[35mw\x1b[0m",
    )
  })

  When("the following string is attached as {string}:", async function(mediaType: string, text: string) {
    await (this as { attach: (data: unknown, options: unknown) => Promise<void> }).attach(text, mediaType)
  })

  When("an array with {int} bytes is attached as {string}", async function(size: number, mediaType: string) {
    const data = [...Array(size).keys()]
    await (this as { attach: (data: unknown, options: unknown) => Promise<void> }).attach(Buffer.from(data), mediaType)
  })

  When("a PDF document is attached and renamed", async function() {
    await (this as { attach: (data: unknown, options: unknown) => Promise<void> }).attach(
      createReadStream(cckSamplePath("attachments", "document.pdf")),
      { mediaType: "application/pdf", fileName: "renamed.pdf" },
    )
  })

  When("a link to {string} is attached", async function(uri: string) {
    await (this as { link: (uri: string) => Promise<void> }).link(uri)
  })

  When(
    "the string {string} is attached as {string} before a failure",
    async function(text: string, mediaType: string) {
      await (this as { attach: (data: unknown, options: unknown) => Promise<void> }).attach(text, mediaType)
      throw new Error("whoops")
    },
  )
})

const gate = (sample: string, support: SupportModule) =>
  Effect.gen(function*() {
    const result = yield* runFeaturesLocal(loadCckSources(sample), support, {})
    expect(normalizeEnvelopes(result.envelopes)).toEqual(normalizeEnvelopes(cckExpectedEnvelopes(sample)))
  })

describe("Cucumber Compatibility Kit", () => {
  it("minimal", () => Effect.runPromise(gate("minimal", minimalSupport)))
  it("attachments", () => Effect.runPromise(gate("attachments", attachmentsSupport)))
})
