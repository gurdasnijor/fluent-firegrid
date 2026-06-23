import * as NodeServices from "@effect/platform-node/NodeServices"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import { describe, expect, it } from "vitest"
import { spawnAcpProcess } from "../src/process-owner.ts"

/**
 * Real ACP process smoke test of the owner's surface: spawn a real ACP harness
 * and confirm the exposed `acp.Stream` carries a working ACP handshake (we send
 * `initialize`, the agent replies). The FULL binding acceptance (FiregridAcpClient
 * + fluent runtime, Layer 1/2, resume, cancel) is a separate fluent lane.
 *
 *   ACP_RUN_REAL=1 pnpm test
 *   ACP_RUN_REAL=1 ACP_AGENT=codex pnpm test
 */
const runReal = process.env.ACP_RUN_REAL === "1"
const maybe = runReal ? describe : describe.skip
const AGENT = process.env.ACP_AGENT ?? "claude"

maybe(`spawnAcpProcess (real ${AGENT})`, () => {
  it(
    "produces an acp.Stream that completes an initialize handshake",
    () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function*() {
            const handle = yield* spawnAcpProcess({ agent: AGENT, cwd: "." })

            const writer = handle.stream.writable.getWriter()
            yield* Effect.promise(() =>
              writer.write({
                jsonrpc: "2.0",
                id: 1,
                method: "initialize",
                params: { protocolVersion: 1, clientCapabilities: {} }
              })
            )

            const reader = handle.stream.readable.getReader()
            let response: { id?: number; result?: unknown } | undefined
            const started = yield* Clock.currentTimeMillis
            const deadline = started + 60_000
            let now = started
            while (now < deadline && response === undefined) {
              const next = yield* Effect.promise(() => reader.read())
              if (next.done) break
              const msg = next.value as { id?: number; result?: unknown }
              if (msg.id === 1 && msg.result !== undefined) response = msg
              now = yield* Clock.currentTimeMillis
            }

            expect(response?.result).toBeDefined()
            reader.releaseLock()
          })
        ).pipe(Effect.provide(NodeServices.layer))
      ),
    90_000
  )
})
