import type { Effect } from "effect"
import {
  type Definition,
  type DefinitionHandler,
  type DefinitionKind,
} from "../src/definitions.ts"
import {
  invoke,
  type InputOf,
  type OutputOf,
} from "../src/invocation.ts"
import type { ExecutionContext, FluentRequirements } from "../src/schema.ts"

type BoundTestDefinition<Handlers extends Record<string, DefinitionHandler>> = {
  readonly [Key in keyof Handlers]: (
    input: InputOf<Handlers[Key]>,
  ) => Effect.Effect<OutputOf<Handlers[Key]>, unknown, FluentRequirements>
}

export const bindTestDefinition = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, DefinitionHandler>,
>(
  definition: Definition<Name, Kind, Handlers>,
  ctx: ExecutionContext,
): BoundTestDefinition<Handlers> =>
  new Proxy({}, {
    get: (_target, property) => {
      if (typeof property !== "string") return undefined
      return (input: unknown) => invoke(
        definition,
        property as keyof Handlers,
        input as InputOf<Handlers[keyof Handlers]>,
        ctx,
      )
    },
  }) as BoundTestDefinition<Handlers>
