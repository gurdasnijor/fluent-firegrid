import { Context, Effect, Layer } from "effect"
import { S2Client, type S2ClientApi } from "effect-s2"
import { InvocationStore, type InvocationStoreApi } from "../actor/object.ts"
import { type DurableExecutionError } from "../errors.ts"
import { ExecutionId, RosterDb, WorkflowDb } from "../schema.ts"
import { toError } from "./helpers.ts"
import type { WfDb } from "./invocation.ts"

export interface RuntimeStoresApi {
  readonly client: S2ClientApi
  readonly objectStore: InvocationStoreApi
  readonly roster: Effect.Success<ReturnType<typeof RosterDb.open>>["roster"]
  readonly openWf: (executionId: string) => Effect.Effect<WfDb, DurableExecutionError>
  readonly provideClient: <A, Err>(effect: Effect.Effect<A, Err, S2Client>) => Effect.Effect<A, Err>
}

const make = (): Effect.Effect<RuntimeStoresApi, DurableExecutionError, S2Client | InvocationStore> =>
  Effect.gen(function*() {
    const client = yield* S2Client
    const objectStore = yield* InvocationStore
    const provideClient = <A, Err>(effect: Effect.Effect<A, Err, S2Client>): Effect.Effect<A, Err> =>
      Effect.provideService(effect, S2Client, client)
    const roster = (yield* provideClient(RosterDb.open("global")).pipe(Effect.mapError(toError("open-roster")))).roster
    const openWf = (executionId: string): Effect.Effect<WfDb, DurableExecutionError> =>
      provideClient(WorkflowDb.open(ExecutionId.make(executionId))).pipe(Effect.mapError(toError("open-workflow")))
    return { client, objectStore, roster, openWf, provideClient }
  })

export class RuntimeStores extends Context.Service<RuntimeStores, RuntimeStoresApi>()(
  "effect-s2-durable/runtime/RuntimeStores",
) {
  static readonly layer: Layer.Layer<RuntimeStores, DurableExecutionError, S2Client> = Layer.effect(
    RuntimeStores,
    make(),
  ).pipe(Layer.provide(InvocationStore.layer))
}
