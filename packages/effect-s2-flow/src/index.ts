import * as Data from "effect/Data"
import type * as Effect from "effect/Effect"

export class FlowNotImplemented extends Data.TaggedError("FlowNotImplemented")<{
  readonly capability: string
  readonly message: string
}> {}

export interface ServiceDefinition<Handlers extends ServiceHandlers> {
  readonly name: string
  readonly handlers: Handlers
}

export type ServiceHandlers = Record<string, (input: any) => Generator<any, any, any>>

export interface ClientOptions {}

type HandlerInput<Handler> = Handler extends (input: infer Input) => Generator<any, any, any> ? Input : never

type HandlerOutput<Handler> = Handler extends (input: any) => Generator<any, infer Output, any> ? Output : never

export type ServiceClient<Handlers extends ServiceHandlers> = {
  readonly [Name in keyof Handlers]: (
    input: HandlerInput<Handlers[Name]>
  ) => Effect.Effect<HandlerOutput<Handlers[Name]>, FlowNotImplemented>
}

export const service = <Handlers extends ServiceHandlers>(
  definition: ServiceDefinition<Handlers>
): ServiceDefinition<Handlers> => definition

export const client = <Handlers extends ServiceHandlers>(
  definition: ServiceDefinition<Handlers>,
  _options: ClientOptions = {}
): ServiceClient<Handlers> =>
  new Proxy({}, {
    get: (_target, property) =>
      typeof property === "string" && property in definition.handlers
        ? () =>
          new FlowNotImplemented({
            capability: "A",
            message: `effect-s2-flow client call ${definition.name}.${property} is not implemented`
          })
        : undefined
  }) as ServiceClient<Handlers>

export const run = <A, E, R>(
  name: string,
  _effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E | FlowNotImplemented, R> =>
  new FlowNotImplemented({
    capability: "A",
    message: `durable run step ${name} is not implemented`
  })
