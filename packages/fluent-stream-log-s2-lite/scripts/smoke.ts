/* eslint-disable no-restricted-syntax */
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect } from "effect"
import { make } from "../src/index.ts"
import { startS2Lite } from "./s2-lite-process.ts"
import { BeginningOffset, makeOffset, type StreamPath } from "@firegrid/fluent-stream-log"

const main = async () => {
  const root = await mkdtemp(join(tmpdir(), "fluent-s2-lite-smoke-"))
  const s2 = await startS2Lite(root)
  try {
    const log = await Effect.runPromise(make({
      endpoint: s2.endpoint,
      basin: process.env["S2_LITE_BASIN"] ?? "fluent-firegrid",
      streamPrefix: `smoke-${Date.now()}-`,
      ...(process.env["S2_LITE_TOKEN"] !== undefined && { token: process.env["S2_LITE_TOKEN"] }),
    }))
    const path = "orders" as StreamPath
    const created = await Effect.runPromise(log.create({ path, contentType: "text/plain" }))
    if (created._tag !== "Created") {
      throw new Error(`expected Created, got ${created._tag}`)
    }
    const appended = await Effect.runPromise(log.append({
      path,
      contentType: "text/plain",
      messages: [Buffer.from("hello")],
    }))
    if (appended._tag !== "Appended" || appended.metadata.tailOffset !== makeOffset(1)) {
      throw new Error(`unexpected append result ${JSON.stringify(appended)}`)
    }
    const read = await Effect.runPromise(log.read({ path, offset: BeginningOffset }))
    const bodies = read.records.map((record) => Buffer.from(record.bytes).toString("utf8"))
    if (JSON.stringify(bodies) !== JSON.stringify(["hello"])) {
      throw new Error(`unexpected read bodies ${JSON.stringify(bodies)}`)
    }
    console.log(`S2 Lite smoke passed against ${s2.endpoint}`)
  } finally {
    await s2.close()
    await rm(root, { recursive: true, force: true })
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
