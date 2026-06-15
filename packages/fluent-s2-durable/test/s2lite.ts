import { spawn } from "node:child_process"
import { createServer } from "node:net"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Duration, Effect, Layer, Schedule, type Scope } from "effect"
import { S2, S2Live, type S2Service } from "../src/index.ts"

/**
 * Boots a real `s2 lite` server (in-memory object store) as a Scope-managed
 * resource and exposes the live `S2` service pointed at it. No fakes — every
 * test runs against the actual S2 SDK + server, which is the whole point of the
 * spike (S2 is our Bifrost; emulating it would emulate the thing under test).
 */

const BASIN = "durablewf" // S2 requires basin names ≥ 8 bytes

const freePort: Effect.Effect<number> = Effect.callback<number>((resume) => {
  const server = createServer()
  server.listen(0, () => {
    const address = server.address()
    const port = typeof address === "object" && address !== null ? address.port : 0
    server.close(() => resume(Effect.succeed(port)))
  })
})

const writeInitFile = (): string => {
  const dir = mkdtempSync(join(tmpdir(), "s2lite-"))
  const path = join(dir, "init.json")
  writeFileSync(
    path,
    JSON.stringify({
      basins: [{ name: BASIN, config: { create_stream_on_append: true }, streams: [] }],
    }),
  )
  return path
}

const spawnServer = (
  port: number,
  initFile: string,
): Effect.Effect<void, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.sync(() =>
      spawn("s2", ["lite", "--port", String(port)], {
        env: { ...process.env, S2LITE_INIT_FILE: initFile },
        stdio: ["ignore", "ignore", process.env.S2LITE_DEBUG === "1" ? "inherit" : "ignore"],
      }),
    ),
    (proc) => Effect.sync(() => void proc.kill("SIGKILL")),
  ).pipe(Effect.asVoid)

const make: Effect.Effect<S2Service, never, Scope.Scope> = Effect.gen(function* () {
  const port = yield* freePort
  yield* spawnServer(port, writeInitFile())
  const service = yield* S2Live.make({ endpoint: `http://127.0.0.1:${port}`, basin: BASIN })
  // Readiness: a 404 (stream-not-found) means the server is up and answering.
  yield* service.checkTail("ready-probe").pipe(
    Effect.retry(Schedule.spaced(Duration.millis(100))),
    Effect.timeout(Duration.seconds(15)),
    Effect.orDie,
  )
  return service
})

/** Scoped layer providing the live `S2` service backed by a fresh s2-lite server. */
export const S2LiteLive: Layer.Layer<S2> = Layer.effect(S2, make)
