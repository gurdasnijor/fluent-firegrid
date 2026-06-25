import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"

import { run, service, sleep } from "../runtime.ts"

export const sleeper = service({
  name: "sleeper",
  handlers: {
    nap: (input: { readonly name: string; readonly delay: Duration.Input }) =>
      Effect.gen(function*() {
        yield* sleep("nap", input.delay)
        return yield* run(
          "after-nap",
          Effect.succeed({ woke: input.name })
        )
      })
  }
})
