import { Deferred, Effect, HashMap, Option, Ref } from "effect"
import type { DurableExecutionError } from "../errors.ts"
import { toError } from "./helpers.ts"
import type { EngineStateApi } from "./state.ts"
import type { ServiceInvocation } from "./context.ts"

export const serviceWaiterKey = (executionId: string, name: string) => `${executionId}/${name}`

const pokeServiceDeferred = (
  waiters: EngineStateApi["waiters"],
  executionId: string,
  name: string,
): Effect.Effect<void> =>
  Ref.get(waiters).pipe(Effect.flatMap((map) =>
    Option.match(HashMap.get(map, serviceWaiterKey(executionId, name)), {
      onNone: () => Effect.void,
      onSome: (w) => Effect.asVoid(Deferred.succeed(w, undefined)),
    }),
  ))

export const resolveServiceDeferred = (
  waiters: EngineStateApi["waiters"],
  db: ServiceInvocation["db"],
  executionId: string,
  name: string,
  encoded: unknown,
): Effect.Effect<void, DurableExecutionError> =>
  db.deferreds.insertOrGet({ name, value: encoded }).pipe(
    Effect.mapError(toError("resolve")),
    Effect.andThen(pokeServiceDeferred(waiters, executionId, name)),
  )
