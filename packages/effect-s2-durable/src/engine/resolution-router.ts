import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import { objectPartsOption } from "./address.ts"
import type { ExternalResolution } from "./api.ts"
import { DurableStores } from "./durable-stores.ts"
import { encode, fail } from "./helpers.ts"
import { resolveServiceDeferred } from "./service-deferreds.ts"
import { EngineState } from "./state.ts"

export interface ResolutionRouterApi {
  readonly resolveExternal: ExternalResolution
}

const make: Effect.Effect<ResolutionRouterApi, never, EngineState | DurableStores> = Effect.gen(function*() {
  const { running, waiters } = yield* EngineState
  const { objectDriver: store, provideClient } = yield* DurableStores

  const resolveExternal: ExternalResolution = (executionId, name, schema, value) =>
    Effect.gen(function*() {
      const parts = yield* objectPartsOption(executionId)
      if (Option.isSome(parts)) {
        const enc = yield* encode(schema, value)
        return yield* provideClient(store.resolveSignal(executionId, parts.value, name, enc))
      }

      const live = yield* Ref.get(running)
      return yield* Option.match(HashMap.get(live, executionId), {
        onNone: () => fail("resolve", `execution ${executionId} is not running locally`),
        onSome: (entry) =>
          encode(schema, value).pipe(
            Effect.flatMap((enc) => resolveServiceDeferred(waiters, entry.invocation.db, executionId, name, enc))
          )
      })
    })

  return { resolveExternal }
})

export class ResolutionRouter extends Context.Service<ResolutionRouter, ResolutionRouterApi>()(
  "effect-s2-durable/engine/resolution-router/ResolutionRouter"
) {
  static readonly layer: Layer.Layer<ResolutionRouter, never, EngineState | DurableStores> = Layer.effect(
    ResolutionRouter,
    make
  )
}
