import { execSync } from "node:child_process"
import { createReadStream } from "node:fs"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import type { Envelope } from "@cucumber/messages"
import { Effect, Layer, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { normalizeEnvelopes } from "../src/durable/cck.ts"
import { directExec, runFeatures } from "../src/durable/runner-core.ts"
import { runFeaturesDurable } from "../src/durable/runtime.ts"
import { stepHost } from "../src/durable/step-host.ts"
import { defineSteps, type SupportBundle } from "../src/durable/support.ts"
import { S2LiteLive } from "../src/s2lite.ts"
import { cckExpectedEnvelopes, cckSamplePath, loadCckSources } from "./cck-support.ts"

// Support bundles for the targeted CCK samples, as plain values (the bundle is a
// deployment dependency, not a global registry). These mirror the kit's
// reference step definitions.

const minimal = defineSteps(({ Given }) => {
  Given("I have {int} cukes in my belly", (_cukeCount: number) => {
    // no-op
  })
})

const attachments = defineSteps(({ When }) => {
  type Attacher = { attach: (data: unknown, options: unknown) => Promise<void>; log: (t: string) => Promise<void>; link: (u: string) => Promise<void> }
  When("the string {string} is attached as {string}", async function(text: string, mediaType: string) {
    await (this as Attacher).attach(text, mediaType)
  })
  When("the string {string} is logged", async function(text: string) {
    await (this as Attacher).log(text)
  })
  When("text with ANSI escapes is logged", async function() {
    await (this as Attacher).log(
      "This displays a \x1b[31mr\x1b[0m\x1b[91ma\x1b[0m\x1b[33mi\x1b[0m\x1b[32mn\x1b[0m\x1b[34mb\x1b[0m\x1b[95mo\x1b[0m\x1b[35mw\x1b[0m",
    )
  })
  When("the following string is attached as {string}:", async function(mediaType: string, text: string) {
    await (this as Attacher).attach(text, mediaType)
  })
  When("an array with {int} bytes is attached as {string}", async function(size: number, mediaType: string) {
    await (this as Attacher).attach(Buffer.from([...Array(size).keys()]), mediaType)
  })
  When("a PDF document is attached and renamed", async function() {
    await (this as Attacher).attach(createReadStream(cckSamplePath("attachments", "document.pdf")), {
      mediaType: "application/pdf",
      fileName: "renamed.pdf",
    })
  })
  When("a link to {string} is attached", async function(uri: string) {
    await (this as Attacher).link(uri)
  })
  When("the string {string} is attached as {string} before a failure", async function(text: string, mediaType: string) {
    await (this as Attacher).attach(text, mediaType)
    throw new Error("whoops")
  })
})

const bundles: Record<string, SupportBundle> = { minimal, attachments }

// Pure gate: run the cucumber core in-process (directExec) and compare the FULL
// envelope stream — discovery, execution, attachments, results — to the kit's
// expected ndjson. No durable engine, no S2; the same core the durable path runs.
const runPure = (sample: string): Promise<ReadonlyArray<Envelope>> => {
  const host = stepHost(bundles[sample]!)
  return runFeatures(loadCckSources(sample), host, directExec(host)).pipe(
    Stream.runCollect,
    Effect.map((chunk) => Array.from(chunk) as ReadonlyArray<Envelope>),
    Effect.runPromise,
  )
}

describe("CCK — pure runner core (in-process, engine-free)", () => {
  it("minimal", async () => {
    expect(normalizeEnvelopes(await runPure("minimal"))).toEqual(normalizeEnvelopes(cckExpectedEnvelopes("minimal")))
  })
  it("attachments", async () => {
    expect(normalizeEnvelopes(await runPure("attachments"))).toEqual(normalizeEnvelopes(cckExpectedEnvelopes("attachments")))
  })
})

// Full durable gate: drives the REAL runner -> scenario path over S2. Runs only
// where the `s2` binary is available (CI); skipped otherwise. Proves the durable
// engine path produces the identical stream the pure core does.
const hasS2 = (): boolean => {
  try {
    execSync("command -v s2", { stdio: "ignore" })
    return true
  } catch {
    return false
  }
}

const runDurable = (sample: string): Promise<ReadonlyArray<Envelope>> =>
  runFeaturesDurable([cckSamplePath(sample, `${sample}.feature`)], { runId: `${sample}-${Date.now()}`, support: bundles[sample]! }).pipe(
    Stream.runCollect,
    Effect.map((chunk) => Array.from(chunk) as ReadonlyArray<Envelope>),
    Effect.provide(Layer.mergeAll(S2LiteLive, NodeFileSystem.layer)),
    Effect.scoped,
    Effect.runPromise,
  )

describe.skipIf(!hasS2())("CCK — durable engine path (S2-backed)", () => {
  it("minimal", async () => {
    expect(normalizeEnvelopes(await runDurable("minimal"))).toEqual(normalizeEnvelopes(cckExpectedEnvelopes("minimal")))
  })
  it("attachments", async () => {
    expect(normalizeEnvelopes(await runDurable("attachments"))).toEqual(normalizeEnvelopes(cckExpectedEnvelopes("attachments")))
  })
})
