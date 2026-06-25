import * as NodeServices from "@effect/platform-node/NodeServices"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as ChildProcess from "effect/unstable/process/ChildProcess"
import * as HttpClient from "effect/unstable/http/HttpClient"

import { S2LiteError } from "./VerificationError.ts"

export interface S2LiteConfig {
  readonly bin?: string
  readonly args?: (config: S2LiteConfig) => ReadonlyArray<string>
  readonly port: number
  readonly localRoot: string
  readonly initFile?: string
  readonly readiness?: {
    readonly attempts?: number
    readonly interval?: Duration.Input
    readonly path?: string
  }
}

interface S2LiteHandle {
  readonly endpoint: string
  readonly stop: Effect.Effect<void>
  readonly kill: Effect.Effect<void>
}

export class S2LiteSupervisor extends Context.Service<S2LiteSupervisor, {
  readonly endpoint: Effect.Effect<string, S2LiteError>
  readonly start: Effect.Effect<void, S2LiteError, Scope.Scope>
  readonly stop: Effect.Effect<void, S2LiteError>
  readonly kill: Effect.Effect<void, S2LiteError>
  readonly restart: Effect.Effect<void, S2LiteError, Scope.Scope>
}>()("@firegrid/verification/S2LiteSupervisor") {
  static readonly layer = (config: S2LiteConfig): Layer.Layer<S2LiteSupervisor> =>
    Layer.effect(
      this,
      makeSupervisor(config)
    )
}

const makeSupervisor = Effect.fn("S2LiteSupervisor.make")(function*(
  config: S2LiteConfig
) {
  const state = yield* Ref.make<Option.Option<S2LiteHandle>>(Option.none())
  const endpoint = `http://127.0.0.1:${config.port}`

  const waitUntilReady = Effect.fn("S2LiteSupervisor.waitUntilReady")(function*() {
    const attempts = config.readiness?.attempts ?? 400
    const interval = config.readiness?.interval ?? "50 millis"
    const path = config.readiness?.path ?? "/"
    const url = `${endpoint}${path}`

    const loop = (remaining: number): Effect.Effect<void, S2LiteError, HttpClient.HttpClient> =>
      Effect.gen(function*() {
        const ready = yield* Effect.exit(HttpClient.get(url))
        if (ready._tag === "Success") {
          return
        }
        if (remaining <= 0) {
          return yield* new S2LiteError({
            message: `s2 lite did not become ready at ${url}`,
            cause: ready.cause
          })
        }
        yield* Effect.sleep(interval)
        return yield* loop(remaining - 1)
      })

    return yield* loop(attempts)
  })

  const spawn = Effect.fn("S2LiteSupervisor.spawn")(function*() {
    const bin = config.bin ?? "s2"
    const args = config.args?.(config) ?? [
      "lite",
      "--port",
      String(config.port),
      "--local-root",
      config.localRoot,
      ...(config.initFile === undefined ? [] : ["--init-file", config.initFile])
    ]
    const proc = yield* ChildProcess.make(bin, args, {
      stdout: "ignore",
      stderr: "ignore"
    }).pipe(
      Effect.mapError((cause) => new S2LiteError({ message: `failed to start ${bin} lite`, cause }))
    )
    const handle: S2LiteHandle = {
      endpoint,
      stop: proc.kill({ killSignal: "SIGTERM", forceKillAfter: "5 seconds" }).pipe(Effect.ignore),
      kill: proc.kill({ killSignal: "SIGKILL" }).pipe(Effect.ignore)
    }
    yield* waitUntilReady().pipe(
      Effect.catch((cause) => handle.kill.pipe(Effect.andThen(Effect.fail(cause))))
    )
    yield* Ref.set(state, Option.some(handle))
  })

  const release = Effect.fn("S2LiteSupervisor.release")(function*(mode: "stop" | "kill") {
    const current = yield* Ref.get(state)
    if (Option.isSome(current)) {
      if (mode === "kill") {
        yield* current.value.kill
      } else {
        yield* current.value.stop
      }
      yield* Ref.set(state, Option.none())
    }
  })

  const stop = release("stop").pipe(
    Effect.withSpan("S2LiteSupervisor.stop"),
    Effect.mapError((cause) => new S2LiteError({ message: "failed to stop s2 lite", cause }))
  )

  const kill = release("kill").pipe(
    Effect.withSpan("S2LiteSupervisor.kill"),
    Effect.mapError((cause) => new S2LiteError({ message: "failed to kill s2 lite", cause }))
  )
  const spawnLive = spawn().pipe(
    Effect.provide(Layer.mergeAll(NodeServices.layer, NodeHttpClient.layerFetch))
  )

  return S2LiteSupervisor.of({
    endpoint: Ref.get(state).pipe(
      Effect.flatMap((current) =>
        Option.isSome(current)
          ? Effect.succeed(current.value.endpoint)
          : Effect.fail(new S2LiteError({ message: "s2 lite is not started" }))
      )
    ),
    start: Ref.get(state).pipe(
      Effect.flatMap((current) => Option.isSome(current) ? Effect.void : spawnLive)
    ),
    stop,
    kill,
    restart: stop.pipe(Effect.andThen(spawnLive))
  })
})
