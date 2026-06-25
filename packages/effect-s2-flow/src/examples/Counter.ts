import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"

import { object, state } from "../runtime.ts"

const value = state("counter.value", 0)

interface AddInput {
  readonly amount: number
  readonly delay?: Duration.Input
}

export const counter = object({
  name: "counter",
  handlers: {
    add: (input: AddInput) =>
      Effect.gen(function*() {
        const next = yield* value.update((current) => current + input.amount)
        if (input.delay !== undefined) {
          yield* Effect.sleep(input.delay)
        }
        return next
      }),
    addThenRead: (input: { readonly amount: number }) =>
      Effect.gen(function*() {
        const before = yield* value.get
        yield* value.set(before + input.amount)
        const after = yield* value.get
        return { after, before }
      }),
    value: (_input: {}) => value.get
  }
})
