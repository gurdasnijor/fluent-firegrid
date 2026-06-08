import { Effect } from "effect"
import {
  type Definition,
  type DefinitionHandler,
  type DefinitionKind,
  type GeneratorHandler,
  type Handler,
  type HandlerInput,
  type HandlerOutput,
} from "./definitions.ts"
import { FluentFiregridError } from "./error.ts"
import { execute } from "./execute.ts"
import type { ExecutionContext, FluentRequirements } from "./schema.ts"

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
): Effect.Effect<OutputOf<Handlers[Key]>, unknown, FluentRequirements> =>
  Effect.gen(function* () {
    const handler = definition.handlers[handlerName]
    if (handler === undefined) {
      return yield* new FluentFiregridError({
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
