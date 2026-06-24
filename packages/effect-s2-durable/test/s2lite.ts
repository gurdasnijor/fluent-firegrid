import * as NodeChildProcessSpawner from "@effect/platform-node/NodeChildProcessSpawner"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer"
import { type BasinConfig as SdkBasinConfig, S2Client, type S2ClientError } from "effect-s2"
import * as Config from "effect/Config"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Path from "effect/Path"
import * as Redacted from "effect/Redacted"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as ChildProcess from "effect/unstable/process/ChildProcess"

// A real S2 backend for the engine's S2-backed integration tests: spawn `s2 lite`
// on a free port, seed a basin, and hand back an `S2Client` layer pointed at it.
// The engine package ships no S2 backend of its own (its unit tests are pure
// actor-core); this is test infrastructure only.

const BASIN = "effect-s2-durable-test"
const debugConfig = Config.boolean("S2LITE_DEBUG").pipe(Config.withDefault(false))

const BasinConfig = Schema.Struct({
  createStreamOnAppend: Schema.optionalKey(Schema.Boolean),
  createStreamOnRead: Schema.optionalKey(Schema.Boolean),
  defaultStreamConfig: Schema.Unknown.pipe(Schema.NullOr, Schema.optionalKey),
  streamCipher: Schema.Unknown.pipe(Schema.NullOr, Schema.optionalKey)
}).pipe(Schema.encodeKeys({
  createStreamOnAppend: "create_stream_on_append",
  createStreamOnRead: "create_stream_on_read",
  defaultStreamConfig: "default_stream_config",
  streamCipher: "stream_cipher"
}))

const S2LiteInitFile = Schema.fromJsonString(Schema.Struct({
  basins: Schema.Array(Schema.Struct({
    name: Schema.String,
    config: BasinConfig,
    streams: Schema.Array(Schema.Unknown)
  }))
}))

const initFilePayload = Schema.encodeSync(S2LiteInitFile)({
  basins: [
    {
      name: BASIN,
      config: { createStreamOnAppend: true } satisfies SdkBasinConfig,
      streams: []
    }
  ]
})

const freePort = Effect.scoped(
  Effect.gen(function*() {
    const server = yield* NodeSocketServer.make({ port: 0, host: "127.0.0.1" })
    return server.address._tag === "TcpAddress" ? server.address.port : 0
  })
).pipe(Effect.orDie)

const writeInitFile = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "effect-s2-durable-s2lite-" })
  const file = path.join(dir, "init.json")
  yield* fs.writeFileString(file, initFilePayload)
  return file
})

const fsAndPath = Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)
const PlatformLive = Layer.mergeAll(
  fsAndPath,
  NodeChildProcessSpawner.layer.pipe(Layer.provide(fsAndPath))
)

const isNotFound = (cause: S2ClientError): boolean => cause.status === 404

export const S2LiteLive: Layer.Layer<S2Client> = Layer.unwrap(
  Effect.gen(function*() {
    const port = yield* freePort.pipe(Effect.withSpan("test.s2lite.allocate-port"))
    const debug = yield* debugConfig
    const initFile = yield* writeInitFile.pipe(
      Effect.withSpan("test.s2lite.write-init-file", {
        attributes: { "test.s2lite.basin": BASIN }
      })
    )

    yield* ChildProcess.make("s2", ["lite", "--port", String(port)], {
      env: { S2LITE_INIT_FILE: initFile },
      extendEnv: true,
      stdout: debug === true ? "inherit" : "ignore",
      stderr: debug === true ? "inherit" : "ignore"
    }).pipe(
      Effect.withSpan("test.s2lite.spawn", {
        attributes: {
          "test.s2lite.port": port,
          "test.s2lite.init_file": initFile
        }
      })
    )

    const endpoint = `http://127.0.0.1:${port}`
    const clientLayer = S2Client.layer({
      accessToken: Redacted.make("s2lite-effect-s2-durable"),
      basinName: BASIN,
      endpoints: {
        account: endpoint,
        basin: endpoint
      }
    })

    yield* S2Client.checkTail("readyprobe").pipe(
      Effect.catch((cause) => isNotFound(cause) ? Effect.void : Effect.fail(cause)),
      Effect.retry(Schedule.spaced(Duration.millis(150))),
      Effect.timeout(Duration.seconds(20)),
      Effect.provide(clientLayer),
      Effect.orDie,
      Effect.withSpan("test.s2lite.ready", {
        attributes: {
          "test.s2lite.port": port,
          "test.s2lite.basin": BASIN
        }
      })
    )

    return Layer.orDie(clientLayer)
  }).pipe(
    Effect.withSpan("test.s2lite.layer"),
    Effect.orDie
  )
).pipe(Layer.provide(PlatformLive))
