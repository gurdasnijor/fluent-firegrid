import { describe, expect, it } from "vitest"
import { Effect } from "effect"

import { FluentTimeoutError, orTimeout } from "../src/combinators.ts"

describe("orTimeout", () => {
  it("passes through successful effects", async () => {
    await expect(Effect.runPromise(Effect.succeed("ok").pipe(orTimeout({ seconds: 1 })))).resolves.toBe("ok")
  })

  it("maps Effect timeout failures to FluentTimeoutError", async () => {
    await expect(Effect.runPromise(Effect.never.pipe(orTimeout(1)))).rejects.toBeInstanceOf(FluentTimeoutError)
  })
})
