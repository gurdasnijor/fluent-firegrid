import type * as Effect from "effect/Effect"

import type { FlowError } from "./FlowError.ts"
import type { StreamStore } from "./StreamStore.ts"

export interface CheckTail {
  readonly check: Effect.Effect<number, FlowError>
}

export const make = <A>(store: StreamStore<A>): CheckTail => ({
  check: store.checkTail
})
