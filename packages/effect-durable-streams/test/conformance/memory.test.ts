/**
 * Server conformance harness for the Effect memory store (effect-server
 * CONFORMANCE.1): boots the Effect server and runs the shared
 * `@durable-streams/conformance-tests/server` suite against it.
 *
 * This memory-store slice implements basic create/append/head/catch-up-read and
 * the producer decision matrix. The full suite also covers long-poll, SSE,
 * fork, TTL, JSON edge cases, property tests, and subscriptions, which are OUT
 * OF SCOPE for this slice and WILL fail — so the harness is skipped by default
 * and run explicitly:
 *
 *   RUN_CONFORMANCE=1 pnpm --filter effect-durable-streams exec vitest run \
 *     test/conformance/memory.test.ts -t "Basic Stream Operations"
 */
import { runConformanceTests } from "@durable-streams/conformance-tests/server"
import { afterAll, beforeAll, describe } from "vitest"
import { startServer } from "../support/start-server.ts"
import type { Running } from "../support/start-server.ts"

const options = { baseUrl: "", subscriptions: false }
let server: Running

const suite = process.env.RUN_CONFORMANCE === "1" ? describe : describe.skip

suite("effect server conformance (memory)", () => {
  beforeAll(async () => {
    server = await startServer()
    options.baseUrl = server.baseUrl
  })
  afterAll(async () => {
    await server.close()
  })

  runConformanceTests(options)
})
