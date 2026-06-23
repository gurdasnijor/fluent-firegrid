import { Effect } from "effect"
import type { Handler } from "../authoring/types.ts"
import type {
  HandlerFn,
  Handlers,
  MethodCodecs,
  ObjectDefinition,
  ServiceDefinition,
  WorkflowDefinition,
} from "../authoring/definition.ts"
import { WORKFLOW_RUN } from "../authoring/definition.ts"
import { handler } from "../authoring/handler.ts"
import { handlerRequest } from "../authoring/primitives.ts"

export interface CompiledMethod {
  readonly handler: Handler<unknown, unknown, never, never>
  readonly input: MethodCodecs["input"]
  readonly output: MethodCodecs["output"]
}

const compileMethod = (
  owner: string,
  method: string,
  fn: HandlerFn,
  codecs: MethodCodecs,
): CompiledMethod => {
  const body = Effect.fnUntraced(fn)
  const program = handlerRequest(codecs.input).pipe(Effect.flatMap((decoded) => body(decoded)))
  const compiledHandler = handler(`${owner}/${method}`, { input: codecs.input, output: codecs.output })(program) as Handler<
    unknown,
    unknown,
    never,
    never
  >
  return { handler: compiledHandler, input: codecs.input, output: codecs.output }
}

const compileHandlers = (
  owner: string,
  handlers: Handlers,
  codecs: Record<string, MethodCodecs>,
): Record<string, CompiledMethod> =>
  Object.fromEntries(
    Object.entries(handlers).map(([method, fn]) => [method, compileMethod(owner, method, fn, codecs[method] as MethodCodecs)]),
  )

export const compileExclusive = (
  def: ServiceDefinition<string, Handlers> | ObjectDefinition<string, Handlers> | WorkflowDefinition<string, HandlerFn, Handlers>,
): Record<string, CompiledMethod> => {
  if (def.kind === "workflow") {
    return {
      [WORKFLOW_RUN]: compileMethod(def.name, WORKFLOW_RUN, def.run, def.runCodecs),
    }
  }
  return compileHandlers(def.name, def.handlers, def.codecs)
}

export const compileShared = (
  def: ObjectDefinition<string, Handlers> | WorkflowDefinition<string, HandlerFn, Handlers>,
): Record<string, CompiledMethod> =>
  compileHandlers(def.name, def.shared, def.sharedCodecs)

export const compileOne = (
  def: ServiceDefinition<string, Handlers> | ObjectDefinition<string, Handlers> | WorkflowDefinition<string, HandlerFn, Handlers>,
  method: string,
): CompiledMethod | undefined => compileExclusive(def)[method]
