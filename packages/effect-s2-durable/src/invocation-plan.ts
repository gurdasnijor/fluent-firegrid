import { Effect, Random } from "effect"
import type { DurableExecutionError } from "./errors.ts"
import { durableError } from "./errors.ts"
import { encodeObjectCallId, OBJECT_ID_PREFIX } from "./object/machine/index.ts"

export interface InvokeOptions {
  /** Pin the execution id (idempotent invocation). Default: a fresh id per call. */
  readonly idempotencyKey?: string
}

export interface ObjectIdentity {
  readonly name: string
  readonly key: string
}

const freshNonce = Effect.map(
  Effect.all([Random.nextInt, Random.nextInt, Random.nextInt]),
  (parts) => parts.map((part) => Math.abs(part).toString(36)).join("-"),
)

const nonceFor = (options: InvokeOptions | undefined) =>
  options?.idempotencyKey === undefined ? freshNonce : Effect.succeed(options.idempotencyKey)

export const objectIdentity = (
  def: { readonly name: string },
  key: string | undefined,
): ObjectIdentity | undefined => key === undefined ? undefined : { name: def.name, key }

export const planInvocationId = (
  method: string,
  options: InvokeOptions | undefined,
  object: ObjectIdentity | undefined,
): Effect.Effect<string, DurableExecutionError> =>
  object === undefined
    ? nonceFor(options).pipe(
      Effect.flatMap((id) =>
        id.startsWith(OBJECT_ID_PREFIX)
          ? Effect.fail(
            durableError("submit")(
              new Error(`idempotencyKey must not start with the reserved prefix ${JSON.stringify(OBJECT_ID_PREFIX)}`),
            ),
          )
          : Effect.succeed(id),
      ),
    )
    : nonceFor(options).pipe(
      Effect.flatMap((nonce) =>
        encodeObjectCallId({ object: object.name, key: object.key, method, nonce }).pipe(
          Effect.mapError(durableError("object.callId")),
        ),
      ),
    )

export const locateInvocationId = (
  method: string,
  idempotencyKey: string,
  object: ObjectIdentity | undefined,
): Effect.Effect<string, DurableExecutionError> =>
  object === undefined
    ? Effect.succeed(idempotencyKey)
    : encodeObjectCallId({ object: object.name, key: object.key, method, nonce: idempotencyKey }).pipe(
      Effect.mapError(durableError("object.callId")),
    )

export const workflowRunIdFor = (
  workflow: { readonly name: string },
  id: string,
): Effect.Effect<string, DurableExecutionError> =>
  encodeObjectCallId({ object: workflow.name, key: id, method: "run", nonce: id }).pipe(
    Effect.mapError(durableError("workflow.runId")),
  )
