import * as Effect from "effect/Effect"

import { run, service } from "../runtime.ts"

export const greeter = service({
  name: "greeter",
  handlers: {
    process: (input: { readonly name: string }) =>
      Effect.gen(function*() {
        const greeting = yield* run(
          "step-1",
          Effect.sync(() => `Hello, ${input.name}`).pipe(
            Effect.withSpan("effect-s2-flow.example.side-effect", {
              attributes: {
                "effect-s2-flow.step.name": "step-1"
              }
            })
          )
        )
        yield* Effect.sleep("2 seconds")
        return yield* run(
          "step-2",
          Effect.succeed({ greeting: `${greeting}!` })
        )
      })
  }
})
