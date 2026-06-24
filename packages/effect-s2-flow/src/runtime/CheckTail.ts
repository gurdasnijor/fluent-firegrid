import * as Effect from "effect/Effect"
import * as S2 from "effect-s2"

import { type FlowError, flowError } from "./FlowError.ts"

export interface CheckTail {
  readonly check: Effect.Effect<number, FlowError, S2.S2Client>
}

export interface CheckTailOptions {
  readonly basin: string
  readonly stream: string
}

export const make = (options: CheckTailOptions): CheckTail => ({
  check: S2.stream(options.basin, options.stream).pipe(
    Effect.flatMap((stream) => stream.checkTail()),
    Effect.map((tail) => tail.tail.seqNum),
    Effect.mapError((cause) => flowError("check-tail", "failed to check S2 stream tail", cause))
  )
})
