// @effect-diagnostics effect/nodeBuiltinImport:off -- host edge: `node:http` creates the server that `NodeHttpServer.layer` adapts. This module is the single Node-bound seam (SDD §8); the engine core (`.` import graph) stays Node-free.
/**
 * The runnable host surface for `effect-s2-durable` — promotes the engine
 * assembly that previously lived only in `test/ingress-support.ts` into a real,
 * composable layer + entrypoint (SDD §10.1–10.2, build step 1).
 *
 * A single host (N=1) is: boot-recover (the engine does this on layer build) +
 * optionally serve ingress + run forever. Fenced ownership / claim-sweep
 * (multi-host, step 4), the timer driver, and service-path unification (step 5)
 * are explicitly out of scope here.
 *
 * This is the **only** module in the package that imports `@effect/platform-node`
 * (alongside its `bin/` entrypoint). The engine core must stay Node-free.
 */
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as NodePath from "@effect/platform-node/NodePath"
import { S2Client } from "effect-s2"
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as HttpServerError from "effect/unstable/http/HttpServerError"
import { createServer } from "node:http"
import type { AnyDef } from "../authoring/definition.ts"
import { serviceLayer } from "../catalog/layer.ts"
import type { DurableEngine } from "../engine/api.ts"
import type { DurableExecutionError } from "../errors.ts"
import { durableIngress } from "../ingress/server.ts"

/** Configuration for a single durable host. */
export interface DurableHostOptions {
  /** The compile-time catalog of durable definitions this host serves + recovers (Model A). */
  readonly catalog: ReadonlyArray<AnyDef>
  /** The S2 basin this host operates within (§3.4/§3.5: namespace = basin). */
  readonly namespace: string
  /** Serve the HTTP ingress when present; omit for a headless (recover-only) host. */
  readonly ingress?: { readonly port: number }
  /**
   * Override the S2 backing layer. Defaults to `S2Client.layer` built from
   * `namespace` + `S2_ACCESS_TOKEN`. Tests inject an in-process `s2 lite` layer
   * here to dogfood the host composition without real S2 creds.
   */
  readonly s2?: Layer.Layer<S2Client, Config.ConfigError>
}

/**
 * The full host layer for a catalog. Always includes the engine (whose layer
 * build runs boot recovery). When `ingress` is set, also stands up the Node HTTP
 * server serving `durableIngress` over the same catalog; headless hosts omit the
 * server entirely. Self-contained — `S2Client` is built from the explicit
 * `namespace` plus `S2_ACCESS_TOKEN` from config.
 */
export const DurableHostLive = (
  opts: DurableHostOptions
): Layer.Layer<
  DurableEngine,
  DurableExecutionError | Config.ConfigError | HttpServerError.ServeError
> => {
  const s2 = opts.s2 ?? S2Client.layer({
    accessToken: Config.redacted("S2_ACCESS_TOKEN"),
    basinName: opts.namespace
  })
  const engine = serviceLayer(...opts.catalog).pipe(Layer.provide(s2))
  if (opts.ingress === undefined) return engine
  const port = opts.ingress.port
  // Mirror the proven `test/ingress-support.ts` provide order: ingress ← engine
  // ← Node HTTP server ← FileSystem/Path. `provideMerge` keeps the engine in the
  // output so both branches expose the same service.
  return durableIngress(opts.catalog).pipe(
    Layer.provideMerge(engine),
    Layer.provide(NodeHttpServer.layer(() => createServer(), { port })),
    Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer))
  )
}

/**
 * Build a host layer from the environment: namespace from `S2_BASIN` (§3.4/§3.5)
 * and, when `INGRESS_PORT` is set, an HTTP ingress on that port (otherwise
 * headless). The catalog is supplied by the caller (compile-time, Model A).
 */
export const DurableHostFromConfig = (
  catalog: ReadonlyArray<AnyDef>
): Layer.Layer<
  DurableEngine,
  DurableExecutionError | Config.ConfigError | HttpServerError.ServeError
> =>
  Layer.unwrap(
    Effect.gen(function*() {
      const namespace = yield* Config.string("S2_BASIN")
      const ingressPort = yield* Config.port("INGRESS_PORT").pipe(Config.option)
      return DurableHostLive({
        catalog,
        namespace,
        ...(ingressPort._tag === "Some" ? { ingress: { port: ingressPort.value } } : {})
      })
    })
  )

/** Launch a host and run forever (build the layer, then never return). */
export const startHost = (
  opts: DurableHostOptions
): Effect.Effect<
  never,
  DurableExecutionError | Config.ConfigError | HttpServerError.ServeError
> => Layer.launch(DurableHostLive(opts))
