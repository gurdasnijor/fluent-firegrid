import type { S2Client } from "effect-s2"
import type * as Layer from "effect/Layer"
import type {
  HandlerFn,
  Handlers,
  ObjectDefinition,
  ServiceDefinition,
  WorkflowDefinition
} from "../authoring/definition.ts"
import type { DurableEngine } from "../engine/api.ts"
import type { ObjectHandlerSeed, RegisteredHandler } from "../engine/context.ts"
import { DurableEngineLive } from "../engine/live.ts"
import type { DurableExecutionError } from "../errors.ts"
import { compileExclusive } from "./compiler.ts"

/**
 * The engine layer **seeded with these definitions' handlers** so boot recovery can
 * re-drive their running/suspended work after a process restart. Use this whenever
 * work can outlive the process (it parks on `sleep`/`signal`/`awakeable`, or is a
 * pending object/workflow call).
 * Service handlers seed the by-name registry; object/workflow methods seed owner-stream
 * recovery (a workflow's `run` re-drives its pending head exactly like an object call).
 */
export const serviceLayer = (
  ...defs: ReadonlyArray<
    | ServiceDefinition<string, Handlers>
    | ObjectDefinition<string, Handlers>
    | WorkflowDefinition<string, HandlerFn, Handlers>
  >
): Layer.Layer<DurableEngine, DurableExecutionError, S2Client> => {
  const services = defs.filter((def): def is ServiceDefinition<string, Handlers> => def.kind === "service")
  // objects and workflows share owner-stream recovery: seed each exclusive method as
  // `${name}/${method}`. (A workflow's only exclusive method is `run`.)
  const ownerLogged = defs.filter(
    (def): def is ObjectDefinition<string, Handlers> | WorkflowDefinition<string, HandlerFn, Handlers> =>
      def.kind === "object" || def.kind === "workflow"
  )
  return DurableEngineLive(
    services.flatMap((def): ReadonlyArray<RegisteredHandler> =>
      Object.values(compileExclusive(def)).map((c) => c.handler)
    ),
    ownerLogged.flatMap((def): ReadonlyArray<ObjectHandlerSeed> =>
      Object.entries(compileExclusive(def)).map(([method, c]) => ({ object: def.name, method, handler: c.handler }))
    )
  )
}
