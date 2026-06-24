import type { AnyTable, RowOf } from "effect-s2-stream-db"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import type * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import type { MethodCodecs } from "../authoring/definition.ts"
import type { DurablePromiseResolver, Handler, RunStep, StateBinding } from "../authoring/types.ts"
import type { DurableQuery } from "../engine/api.ts"
import type { DurableExecutionError } from "../errors.ts"

export interface ServiceCallTarget {
  readonly service: string
  readonly method: string
}

export interface ObjectCallTarget {
  readonly object: string
  readonly key: string
  readonly method: string
}

interface HandlerRequestAccess {
  readonly input: <A, I>(
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<A, DurableExecutionError>
}

interface StepJournal {
  readonly run: RunStep
}

interface DurableClock {
  readonly sleep: (name: string, duration: Duration.Duration) => Effect.Effect<void, DurableExecutionError>
}

interface DurableStateFactory {
  readonly table: <Tbl extends AnyTable>(table: Tbl) => StateBinding<RowOf<Tbl>>
}

interface Awakeables {
  readonly create: <A, I>(
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<{
    readonly id: string
    readonly promise: Effect.Effect<A, DurableExecutionError>
  }, DurableExecutionError>
}

interface DurablePromises {
  readonly await: <A, I>(
    name: string,
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<A, DurableExecutionError>
  readonly resolve: DurablePromiseResolver
  readonly resolveWorkflow: DurablePromiseResolver
}

interface ServiceCommunication {
  readonly callService: <A, I>(
    handler: Handler<unknown, unknown, never, never>,
    target: ServiceCallTarget,
    input: unknown,
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<A, DurableExecutionError>
  readonly sendService: (
    handler: Handler<unknown, unknown, never, never>,
    target: ServiceCallTarget,
    input: unknown
  ) => Effect.Effect<string, DurableExecutionError>
  readonly callObject: <A, I>(
    target: ObjectCallTarget,
    input: unknown,
    inputSchema: MethodCodecs["input"],
    schema: Schema.Codec<A, I, never, never>
  ) => Effect.Effect<A, DurableExecutionError>
  readonly sendObject: (
    target: ObjectCallTarget,
    input: unknown,
    inputSchema: MethodCodecs["input"]
  ) => Effect.Effect<string, DurableExecutionError>
  readonly sharedObject: DurableQuery
}

export interface InvocationScope {
  readonly request: HandlerRequestAccess
  readonly steps: StepJournal
  readonly clock: DurableClock
  readonly state: DurableStateFactory
  readonly awakeables: Awakeables
  readonly durablePromises: DurablePromises
  readonly calls: ServiceCommunication
}

export class CurrentInvocationScope extends Context.Service<CurrentInvocationScope, InvocationScope>()(
  "effect-s2-durable/invocation/scope/CurrentInvocationScope"
) {}
