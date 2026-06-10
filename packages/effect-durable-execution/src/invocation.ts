import { Effect } from "effect"
import { DurableExecutionError } from "./error.ts"
import { execute } from "./execute.ts"
import type {
  Definition,
  DefinitionHandler,
  DefinitionKind,
  GeneratorHandler,
  Handler,
  HandlerInput,
  HandlerOutput,
} from "./definitions.ts"
import type {
  DurableExecutionRequirements,
  ExecutionContext,
} from "./schema.ts"

export type InputOf<H> = HandlerInput<H>

export type OutputOf<H> = HandlerOutput<H>

export const invoke = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, DefinitionHandler>,
  Key extends keyof Handlers,
>(
  definition: Definition<Name, Kind, Handlers>,
  handlerName: Key,
  input: InputOf<Handlers[Key]>,
  ctx: ExecutionContext,
): Effect.Effect<
  OutputOf<Handlers[Key]>,
  unknown,
  DurableExecutionRequirements
> =>
  Effect.gen(function* () {
    const handler = definition.handlers[handlerName]
    if (handler === undefined) {
      return yield* new DurableExecutionError({
        message: `Unknown handler ${String(handlerName)} on service ${definition.name}`,
      })
    }
    if (handler.length >= 2) {
      const typedHandler = handler as Handler<
        InputOf<Handlers[Key]>,
        OutputOf<Handlers[Key]>
      >
      return yield* typedHandler(ctx, input)
    }
    const generatorHandler = handler as GeneratorHandler<
      InputOf<Handlers[Key]>,
      OutputOf<Handlers[Key]>
    >
    const effect = Effect.gen(function* () {
      return yield* generatorHandler(input)
    })
    return yield* execute(ctx, effect)
  })
