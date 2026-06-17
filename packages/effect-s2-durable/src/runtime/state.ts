import { Context, type Deferred, Effect, HashMap, Layer, Ref, type Scope } from "effect"
import type { ObjectHandlerSeed, RegisteredHandler, RunningEntry, RunningMap } from "./invocation.ts"

export interface RuntimeStateApi {
  readonly engineScope: Scope.Scope
  readonly registry: Map<string, RegisteredHandler>
  readonly objectHandlers: Map<string, RegisteredHandler>
  readonly objectNames: ReadonlyArray<string>
  readonly running: Ref.Ref<RunningMap>
  readonly waiters: Ref.Ref<HashMap.HashMap<string, Deferred.Deferred<void>>>
}

const make = (
  handlers: ReadonlyArray<RegisteredHandler>,
  objectSeeds: ReadonlyArray<ObjectHandlerSeed>,
): Effect.Effect<RuntimeStateApi, never, Scope.Scope> =>
  Effect.gen(function*() {
    const engineScope = yield* Effect.scope
    const running = yield* Ref.make(HashMap.empty<string, RunningEntry>())
    const waiters = yield* Ref.make(HashMap.empty<string, Deferred.Deferred<void>>())
    const registry = new Map<string, RegisteredHandler>(handlers.map((h) => [h.name, h]))
    const objectHandlers = new Map<string, RegisteredHandler>(
      objectSeeds.map((s) => [`${s.object}/${s.method}`, s.handler] as const),
    )
    const objectNames = [...new Set(objectSeeds.map((s) => s.object))]
    return { engineScope, registry, objectHandlers, objectNames, running, waiters }
  })

export class RuntimeState extends Context.Service<RuntimeState, RuntimeStateApi>()(
  "effect-s2-durable/runtime/RuntimeState",
) {
  static layer(
    handlers: ReadonlyArray<RegisteredHandler>,
    objectSeeds: ReadonlyArray<ObjectHandlerSeed>,
  ): Layer.Layer<RuntimeState> {
    return Layer.effect(RuntimeState, make(handlers, objectSeeds))
  }
}
