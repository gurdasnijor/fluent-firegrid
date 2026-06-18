import { Context, Effect, HashMap, Layer, Option, Ref, type Schema } from "effect"
import { objectPartsOption } from "./address.ts"
import { encode, fail } from "./helpers.ts"
import { resolveServiceDeferred } from "./serviceDeferreds.ts"
import { RuntimeState } from "./state.ts"
import { RuntimeStores } from "./stores.ts"
import type { DurableExecutionError } from "../errors.ts"

export interface IngressRouterApi {
  readonly resolveExternal: <A, I>(
    executionId: string,
    name: string,
    schema: Schema.Codec<A, I, never, never>,
    value: A,
  ) => Effect.Effect<void, DurableExecutionError>
}

const make: Effect.Effect<IngressRouterApi, never, RuntimeState | RuntimeStores> = Effect.gen(function*() {
  const { running, waiters } = yield* RuntimeState
  const { objectStore: store, provideClient } = yield* RuntimeStores

  const resolveExternal = <A, I>(
    executionId: string,
    name: string,
    schema: Schema.Codec<A, I, never, never>,
    value: A,
  ): Effect.Effect<void, DurableExecutionError> =>
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
            Effect.flatMap((enc) => resolveServiceDeferred(waiters, entry.invocation.db, executionId, name, enc)),
          ),
      })
    })

  return { resolveExternal }
})

export class IngressRouter extends Context.Service<IngressRouter, IngressRouterApi>()(
  "effect-s2-durable/runtime/ingress/IngressRouter",
) {
  static readonly layer: Layer.Layer<IngressRouter, never, RuntimeState | RuntimeStores> = Layer.effect(IngressRouter, make)
}
