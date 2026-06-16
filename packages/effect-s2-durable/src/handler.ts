import type { Effect, Schema } from "effect"
import type { DurableExecutionRuntime } from "./Runtime.ts"
import type { Handler } from "./types.ts"

/**
 * Define a durable handler — the only definition primitive. Stable `name` +
 * discharged input/output schemas + a durable program (plain `Effect.gen` using
 * the free primitives). Curried so the program's `O` is checked against `output`.
 *
 * @example
 * export const reviewRequest = handler("reviewRequest", {
 *   input: PermissionRequest,
 *   output: ApprovalResult,
 * })(Effect.gen(function*() {
 *   const req = yield* handlerRequest(PermissionRequest)
 *   const draft = yield* run("draft", draftResponse(req), { output: DraftResponse })
 *   return yield* run("send", sendResponse(draft), { output: ApprovalResult })
 * }))
 */
export const handler = <Name extends string, I, O>(
  name: Name,
  schemas: {
    readonly input: Schema.Codec<I, unknown, never, never>
    readonly output: Schema.Codec<O, unknown, never, never>
  },
) =>
<E = never, R = never>(program: Effect.Effect<O, E, R | DurableExecutionRuntime>): Handler<I, O, E, R> => ({
  name,
  input: schemas.input,
  output: schemas.output,
  program,
})
