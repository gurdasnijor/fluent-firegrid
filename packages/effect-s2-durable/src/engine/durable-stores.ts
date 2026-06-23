import { Context, Effect, Layer } from "effect"
import { S2Client, type S2ClientApi } from "effect-s2"
import { ObjectOwnerDriver, type ObjectOwnerDriverApi } from "../object/owner-driver.ts"
import { type DurableExecutionError } from "../errors.ts"
import { ExecutionId, RosterDb, WorkflowDb } from "../schema.ts"
import { toError } from "./helpers.ts"
import type { WfDb } from "./context.ts"

export interface DurableStoresApi {
  readonly client: S2ClientApi
  readonly objectDriver: ObjectOwnerDriverApi
  readonly roster: Effect.Success<ReturnType<typeof RosterDb.open>>["roster"]
  readonly openWf: (executionId: string) => Effect.Effect<WfDb, DurableExecutionError>
  readonly provideClient: <A, Err>(effect: Effect.Effect<A, Err, S2Client>) => Effect.Effect<A, Err>
}

const make = (): Effect.Effect<DurableStoresApi, DurableExecutionError, S2Client | ObjectOwnerDriver> =>
  Effect.gen(function*() {
    const client = yield* S2Client
    const objectDriver = yield* ObjectOwnerDriver
    const provideClient = <A, Err>(effect: Effect.Effect<A, Err, S2Client>): Effect.Effect<A, Err> =>
      Effect.provideService(effect, S2Client, client)
    const roster = (yield* provideClient(RosterDb.open("global")).pipe(Effect.mapError(toError("open-roster")))).roster
    const openWf = (executionId: string): Effect.Effect<WfDb, DurableExecutionError> =>
      provideClient(WorkflowDb.open(ExecutionId.make(executionId))).pipe(Effect.mapError(toError("open-workflow")))
    return { client, objectDriver, roster, openWf, provideClient }
  })

export class DurableStores extends Context.Service<DurableStores, DurableStoresApi>()(
  "effect-s2-durable/engine/durable-stores/DurableStores",
) {
  static readonly layer: Layer.Layer<DurableStores, DurableExecutionError, S2Client> = Layer.effect(
    DurableStores,
    make(),
  ).pipe(Layer.provide(ObjectOwnerDriver.layer))
}
