import { invoke } from "../src/invocation.ts"
import type { InputOf, OutputOf } from "../src/invocation.ts"
import type {
  Definition,
  DefinitionHandler,
  DefinitionKind,
} from "../src/definitions.ts"
import type { Effect } from "effect"
import type {
  DurableExecutionRequirements,
  ExecutionContext,
} from "../src/schema.ts"

type BoundTestDefinition<Handlers extends Record<string, DefinitionHandler>> = {
  readonly [Key in keyof Handlers]: (
    input: InputOf<Handlers[Key]>,
  ) => Effect.Effect<
    OutputOf<Handlers[Key]>,
    unknown,
    DurableExecutionRequirements
  >
}

export const bindTestDefinition = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, DefinitionHandler>,
>(
  definition: Definition<Name, Kind, Handlers>,
  ctx: ExecutionContext,
): BoundTestDefinition<Handlers> =>
  new Proxy(
    {},
    {
      get: (_target, property) => {
        if (typeof property !== "string") return undefined
        return (input: unknown) =>
          invoke(
            definition,
            property as keyof Handlers,
            input as InputOf<Handlers[keyof Handlers]>,
            ctx,
          )
      },
    },
  ) as BoundTestDefinition<Handlers>
