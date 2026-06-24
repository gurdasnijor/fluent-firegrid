import type * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import type { CurrentInvocationScope } from "../invocation/scope.ts"
import type { Handler } from "./types.ts"

/**
 * The low-level definition primitive: stable `name` + discharged input/output
 * schemas + a durable program (`Effect.gen` reading its input via `handlerRequest`).
 * Most code uses `service({ handlers })` (generator methods, input as argument);
 * reach for `handler` only when manually compiling a durable entrypoint.
 *
 * @example
 * export const reviewRequest = handler("reviewRequest", {
 *   input: PermissionRequest,
 *   output: ApprovalResult,
 * })(Effect.gen(function*() {
 *   const req = yield* handlerRequest(PermissionRequest)
 *   const draft = yield* run(draftResponse(req), { name: "draft", output: DraftResponse })
 *   return yield* run(sendResponse(draft), { name: "send", output: ApprovalResult })
 * }))
 */
export const handler = <Name extends string, I, O>(
  name: Name,
  schemas: {
    readonly input: Schema.Codec<I, unknown, never, never>
    readonly output: Schema.Codec<O, unknown, never, never>
  }
) =>
<E = never, R = never>(program: Effect.Effect<O, E, R | CurrentInvocationScope>): Handler<I, O, E, R> => ({
  name,
  input: schemas.input,
  output: schemas.output,
  program
})
