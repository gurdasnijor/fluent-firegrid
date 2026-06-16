import * as NodeChildProcessSpawner from "@effect/platform-node/NodeChildProcessSpawner"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer"
import { Duration, Effect, FileSystem, Layer, Path, Schedule } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { S2Client, S2NotFound } from "effect-s2"

/**
 * Boots a real `s2 lite` server (in-memory, real S2 SDK protocol) as a
 * Scope-managed resource and provides the `effect-s2` `S2Client` pointed at it.
 * No fakes, no in-memory S2 emulation — the tests exercise the actual server.
 *
 * Everything goes through the Effect Node platform: the server is spawned with
 * `ChildProcess` (terminated on scope close), the free port comes from a
 * `NodeSocketServer` bound on port 0, and the init file via `FileSystem`.
 */

const BASIN = "streamdbtest" // S2 requires basin names ≥ 8 bytes
const debug = process.env.S2LITE_DEBUG === "1"

/** An ephemeral free TCP port: bind a socket server on port 0, read its address. */
const freePort = Effect.scoped(
  Effect.gen(function*() {
    const server = yield* NodeSocketServer.make({ port: 0, host: "127.0.0.1" })
    return server.address._tag === "TcpAddress" ? server.address.port : 0
  }),
).pipe(Effect.orDie)

/** Pre-create the basin (with `create_stream_on_append`) via the lite init file. */
const writeInitFile = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "s2lite-" })
  const file = path.join(dir, "init.json")
  yield* fs.writeFileString(
    file,
    JSON.stringify({
      basins: [{ name: BASIN, config: { create_stream_on_append: true }, streams: [] }],
    }),
  )
  return file
})

const fsAndPath = Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)
const PlatformLive = Layer.mergeAll(
  fsAndPath,
  NodeChildProcessSpawner.layer.pipe(Layer.provide(fsAndPath)),
)

/** Live `S2Client` backed by a fresh `s2 lite` server (one per layer scope). */
export const S2LiteLive: Layer.Layer<S2Client> = Layer.unwrap(
  Effect.gen(function*() {
    const port = yield* freePort
    const initFile = yield* writeInitFile

    // spawn `s2 lite --port <port>`; killed on scope close.
    yield* ChildProcess.make("s2", ["lite", "--port", String(port)], {
      env: { ...process.env, S2LITE_INIT_FILE: initFile },
      stdout: debug ? "inherit" : "ignore",
      stderr: debug ? "inherit" : "ignore",
    })

    // `effect-s2` reads its endpoints/token/basin from the environment.
    yield* Effect.sync(() => {
      process.env.S2_ACCESS_TOKEN = "s2lite-test"
      process.env.S2_ACCOUNT_ENDPOINT = `http://127.0.0.1:${port}`
      process.env.S2_BASIN_ENDPOINT = `http://127.0.0.1:${port}`
      process.env.S2_BASIN = BASIN
    })

    // readiness: retry until the server answers — a 404 (stream-not-found) counts as up.
    yield* S2Client.checkTail("readyprobe").pipe(
      Effect.catch((cause) => (cause instanceof S2NotFound ? Effect.void : Effect.fail(cause))),
      Effect.retry(Schedule.spaced(Duration.millis(150))),
      Effect.timeout(Duration.seconds(20)),
      Effect.provide(S2Client.layerConfig),
      Effect.orDie,
    )

    return Layer.orDie(S2Client.layerConfig)
  }).pipe(Effect.orDie),
).pipe(Layer.provide(PlatformLive))
