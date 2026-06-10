import { createServer } from "node:http"
import {
  HttpApiBuilder,
  HttpApiSwagger,
  HttpMiddleware,
  HttpServer,
} from "@effect/platform"
import { NodeHttpServer } from "@effect/platform-node"
import { Effect, Layer } from "effect"
import * as ApiLive from "./ApiLive.ts"
import * as AppConfig from "./Config.ts"
import * as MemoryStore from "./MemoryStore.ts"
import * as Store from "./Store.ts"
import type * as http from "node:http"

export interface ServerOptions {
  readonly port: number
  /**
   * Node HTTP server factory. Defaults to `createServer`; an embedder/test may
   * inject a pre-created server (e.g. to read the bound port for `port: 0`).
   * This is production construction injection, not a test harness.
   */
  readonly server?: () => http.Server
}

export const layer = (options: ServerOptions) => {
  const ServerLive = NodeHttpServer.layer(
    options.server ?? (() => createServer()),
    { port: options.port },
  )

  return HttpApiBuilder.serve(HttpMiddleware.logger).pipe(
    Layer.provide(HttpApiSwagger.layer()),
    Layer.provide(ApiLive.layer),
    HttpServer.withLogAddress,
    Layer.provide(ServerLive),
  )
}

/** Launch the server, reading the port from `Config`. */
export const launch = Effect.gen(function* () {
  const port = yield* AppConfig.port
  return yield* Layer.launch(
    layer({ port }).pipe(Layer.provide(Store.withTracing(MemoryStore.layer))),
  )
})
