import * as NodeChildProcessSpawner from "@effect/platform-node/NodeChildProcessSpawner"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer"
import { Config, Duration, Effect, FileSystem, Layer, Path, Redacted, Schedule, Schema } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { S2Client, type BasinConfig as SdkBasinConfig, type S2ClientError } from "effect-s2"

const BASIN = "durable-cucumber-s2"
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

const initFilePayload = Schema.encodeSync(S2LiteInitFile)({
  basins: [
    {
      name: BASIN,
      config: { createStreamOnAppend: true } satisfies SdkBasinConfig,
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
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "firegrid-spec-s2lite-" })
  const file = path.join(dir, "init.json")
  yield* fs.writeFileString(file, initFilePayload)
  return file
})

const fsAndPath = Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)
const PlatformLive = Layer.mergeAll(
  fsAndPath,
  NodeChildProcessSpawner.layer.pipe(Layer.provide(fsAndPath)),
)

const isNotFound = (cause: S2ClientError): boolean => cause.status === 404

export const S2LiteLive: Layer.Layer<S2Client> = Layer.unwrap(
  Effect.gen(function*() {
    const port = yield* freePort.pipe(Effect.withSpan("spec.s2lite.allocate-port"))
    const debug = yield* debugConfig
    const initFile = yield* writeInitFile.pipe(
      Effect.withSpan("spec.s2lite.write-init-file", {
        attributes: { "spec.s2lite.basin": BASIN },
      }),
    )

    yield* ChildProcess.make("s2", ["lite", "--port", String(port)], {
      env: { S2LITE_INIT_FILE: initFile },
      extendEnv: true,
      stdout: debug === true ? "inherit" : "ignore",
      stderr: debug === true ? "inherit" : "ignore",
    }).pipe(
      Effect.withSpan("spec.s2lite.spawn", {
        attributes: {
          "spec.s2lite.port": port,
          "spec.s2lite.init_file": initFile,
        },
      }),
    )

    const endpoint = `http://127.0.0.1:${port}`
    const clientLayer = S2Client.layer({
      accessToken: Redacted.make("s2lite-durable-cucumber"),
      basinName: BASIN,
      endpoints: {
        account: endpoint,
        basin: endpoint,
      },
    })

    yield* S2Client.checkTail("readyprobe").pipe(
      Effect.catch((cause) => isNotFound(cause) ? Effect.void : Effect.fail(cause)),
      // eslint-disable-next-line no-restricted-syntax -- test harness readiness probe for an external s2lite process
      Effect.retry(Schedule.spaced(Duration.millis(150))),
      Effect.timeout(Duration.seconds(20)),
      Effect.provide(clientLayer),
      Effect.orDie,
      Effect.withSpan("spec.s2lite.ready", {
        attributes: {
          "spec.s2lite.port": port,
          "spec.s2lite.basin": BASIN,
        },
      }),
    )

    // eslint-disable-next-line no-restricted-syntax -- spec harness startup is a documented crash boundary
    return Layer.orDie(clientLayer)
  }).pipe(
    Effect.withSpan("spec.s2lite.layer"),
    Effect.orDie,
  ),
).pipe(Layer.provide(PlatformLive))
