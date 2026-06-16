import * as NodeChildProcessSpawner from "@effect/platform-node/NodeChildProcessSpawner"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer"
import { Config, Duration, Effect, FileSystem, Layer, Path, Redacted, Schedule, Schema } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { S2Client } from "effect-s2"
import type { BasinConfig as SdkBasinConfig, S2ClientError } from "effect-s2"

const BASIN = "firelab-s2"
const debugConfig = Config.boolean("S2LITE_DEBUG").pipe(Config.withDefault(false))

const BasinConfig = Schema.Struct({
  createStreamOnAppend: Schema.optionalKey(Schema.Boolean),
  createStreamOnRead: Schema.optionalKey(Schema.Boolean),
  defaultStreamConfig: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
  streamCipher: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
}).pipe(Schema.encodeKeys({
  createStreamOnAppend: "create_stream_on_append",
  createStreamOnRead: "create_stream_on_read",
  defaultStreamConfig: "default_stream_config",
  streamCipher: "stream_cipher",
}))

const S2LiteInitFile = Schema.fromJsonString(Schema.Struct({
  basins: Schema.Array(Schema.Struct({
    name: Schema.String,
    config: BasinConfig,
    streams: Schema.Array(Schema.Unknown),
  })),
}))

const encodeInitFile = Schema.encodeSync(S2LiteInitFile)

const s2LiteBasinConfig = {
  createStreamOnAppend: true,
} satisfies SdkBasinConfig

const initFilePayload = encodeInitFile({
  basins: [
    {
      name: BASIN,
      config: s2LiteBasinConfig,
      streams: [],
    },
  ],
})

const freePort = Effect.scoped(
  Effect.gen(function*() {
    const server = yield* NodeSocketServer.make({ port: 0, host: "127.0.0.1" })
    return server.address._tag === "TcpAddress" ? server.address.port : 0
  }),
).pipe(Effect.orDie)

const writeInitFile = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "firelab-s2lite-" })
  const file = path.join(dir, "init.json")
  yield* fs.writeFileString(file, initFilePayload)
  return file
})

const fsAndPath = Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)
const PlatformLive = Layer.mergeAll(
  fsAndPath,
  NodeChildProcessSpawner.layer.pipe(Layer.provide(fsAndPath)),
)

const isNotFound = (cause: S2ClientError): boolean =>
  cause.status === 404

export const S2LiteLive: Layer.Layer<S2Client> = Layer.unwrap(
  Effect.gen(function*() {
    const port = yield* freePort.pipe(
      Effect.withSpan("firelab.s2lite.allocate-port"),
    )
    const debug = yield* debugConfig
    const initFile = yield* writeInitFile.pipe(
      Effect.withSpan("firelab.s2lite.write-init-file", {
        attributes: { "firelab.s2lite.basin": BASIN },
      }),
    )

    yield* ChildProcess.make("s2", ["lite", "--port", String(port)], {
      env: { S2LITE_INIT_FILE: initFile },
      extendEnv: true,
      stdout: debug ? "inherit" : "ignore",
      stderr: debug ? "inherit" : "ignore",
    }).pipe(
      Effect.withSpan("firelab.s2lite.spawn", {
        attributes: {
          "firelab.s2lite.port": port,
          "firelab.s2lite.init_file": initFile,
        },
      }),
    )

    const endpoint = `http://127.0.0.1:${port}`
    const clientLayer = S2Client.layer({
      accessToken: Redacted.make("s2lite-firelab"),
      basinName: BASIN,
      endpoints: {
        account: endpoint,
        basin: endpoint,
      },
    })

    yield* S2Client.checkTail("readyprobe").pipe(
      Effect.catch((cause) => isNotFound(cause) ? Effect.void : Effect.fail(cause)),
      Effect.retry(Schedule.spaced(Duration.millis(150))),
      Effect.timeout(Duration.seconds(20)),
      Effect.provide(clientLayer),
      Effect.orDie,
      Effect.withSpan("firelab.s2lite.ready", {
        attributes: {
          "firelab.s2lite.port": port,
          "firelab.s2lite.basin": BASIN,
        },
      }),
    )

    yield* Effect.annotateCurrentSpan({
      "firelab.s2lite.port": port,
      "firelab.s2lite.basin": BASIN,
    })

    return Layer.orDie(clientLayer)
  }).pipe(
    Effect.withSpan("firelab.s2lite.layer"),
    Effect.orDie,
  ),
).pipe(Layer.provide(PlatformLive))
