import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import type { DurableExecutionError } from "../errors.ts"
import { decodeObjectCallId, type ObjectCallIdParts } from "../object/address.ts"

export type ExecutionAddress =
  | { readonly _tag: "object"; readonly id: string; readonly parts: ObjectCallIdParts }
  | { readonly _tag: "service"; readonly id: string }

export const decodeExecutionAddress = (id: string): Effect.Effect<ExecutionAddress, DurableExecutionError> =>
  decodeObjectCallId(id).pipe(
    Effect.match({
      onFailure: () => ({ _tag: "service" as const, id }),
      onSuccess: (parts) => ({ _tag: "object" as const, id, parts })
    })
  )

export const objectPartsOption = (id: string): Effect.Effect<Option.Option<ObjectCallIdParts>, DurableExecutionError> =>
  decodeObjectCallId(id).pipe(
    Effect.match({ onFailure: () => Option.none<ObjectCallIdParts>(), onSuccess: Option.some })
  )
