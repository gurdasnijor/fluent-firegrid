import * as Context from "effect/Context"
import type * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import type { ObjectHandlerSeed, RegisteredHandler, RunningEntry, RunningMap } from "./context.ts"

export interface EngineStateApi {
  readonly engineScope: Scope.Scope
  readonly registry: Map<string, RegisteredHandler>
  readonly objectHandlers: Map<string, RegisteredHandler>
  readonly objectNames: ReadonlyArray<string>
  readonly running: Ref.Ref<RunningMap>
  readonly waiters: Ref.Ref<HashMap.HashMap<string, Deferred.Deferred<void>>>
}

const make = (
  handlers: ReadonlyArray<RegisteredHandler>,
  objectSeeds: ReadonlyArray<ObjectHandlerSeed>
): Effect.Effect<EngineStateApi, never, Scope.Scope> =>
  Effect.gen(function*() {
    const engineScope = yield* Effect.scope
    const running = yield* Ref.make(HashMap.empty<string, RunningEntry>())
    const waiters = yield* Ref.make(HashMap.empty<string, Deferred.Deferred<void>>())
    const registry = new Map<string, RegisteredHandler>(handlers.map((h) => [h.name, h]))
    const objectHandlers = new Map<string, RegisteredHandler>(
      objectSeeds.map((s) => [`${s.object}/${s.method}`, s.handler] as const)
    )
    const objectNames = [...new Set(objectSeeds.map((s) => s.object))]
    return { engineScope, registry, objectHandlers, objectNames, running, waiters }
  })

export class EngineState extends Context.Service<EngineState, EngineStateApi>()(
  "effect-s2-durable/engine/state/EngineState"
) {
  static layer(
    handlers: ReadonlyArray<RegisteredHandler>,
    objectSeeds: ReadonlyArray<ObjectHandlerSeed>
  ): Layer.Layer<EngineState> {
    return Layer.effect(EngineState, make(handlers, objectSeeds))
  }
}
