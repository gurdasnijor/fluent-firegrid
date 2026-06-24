import * as NodeServices from "@effect/platform-node/NodeServices"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as ChildProcess from "effect/unstable/process/ChildProcess"

import { S2LiteError } from "./VerificationError.ts"

export interface S2LiteConfig {
  readonly bin?: string
  readonly args?: (config: S2LiteConfig) => ReadonlyArray<string>
  readonly port: number
  readonly localRoot: string
  readonly initFile?: string
}

export interface S2LiteHandle {
  readonly endpoint: string
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
      stdout: "pipe",
      stderr: "pipe"
    }).pipe(
      Effect.mapError((cause) => new S2LiteError({ message: `failed to start ${bin} lite`, cause }))
    )
    const handle: S2LiteHandle = {
      endpoint,
      kill: proc.kill().pipe(Effect.ignore)
    }
    yield* Ref.set(state, Option.some(handle))
  })

  const stop = Effect.gen(function*() {
    const current = yield* Ref.get(state)
    if (Option.isSome(current)) {
      yield* current.value.kill
      yield* Ref.set(state, Option.none())
    }
  }).pipe(
    Effect.withSpan("S2LiteSupervisor.stop"),
    Effect.mapError((cause) => new S2LiteError({ message: "failed to stop s2 lite", cause }))
  )
  const spawnLive = spawn().pipe(Effect.provide(NodeServices.layer))

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
    kill: stop,
    restart: stop.pipe(Effect.andThen(spawnLive))
  })
})

export type S2LiteSupervisorRequirements = Scope.Scope
