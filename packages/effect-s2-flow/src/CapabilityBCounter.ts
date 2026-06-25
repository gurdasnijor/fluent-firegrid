import { object, state } from "./runtime.ts"

const value = state("counter.value", 0)

export const counter = object({
  name: "counter",
  handlers: {
    *add(input: { readonly amount: number }) {
      return yield* value.update((current) => current + input.amount)
    },
    *addThenRead(input: { readonly amount: number }) {
      const before = yield* value.get
      yield* value.set(before + input.amount)
      const after = yield* value.get
      return { after, before }
    },
    *value(_input: {}) {
      return yield* value.get
    }
  }
})
