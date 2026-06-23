import { describe, expect, it } from "@effect/vitest"
import { Context, Duration, Effect, Layer, Schema } from "effect"
import { encodeObjectCallId, pathSegment } from "../src/object/machine/index.ts"
import { openLog } from "../src/object/log.ts"
import { DurableExecutionError } from "../src/errors.ts"
import { DurableEngine } from "../src/engine/api.ts"
import { compileOne } from "../src/authoring/compiler.ts"
import { object } from "../src/authoring/definition.ts"
import { serviceLayer } from "../src/engine/catalog-layer.ts"
import { hasS2 } from "./ingress-support.ts"
import { S2LiteLive } from "./s2lite.ts"

const InfraFailureObject = object({
  name: "infra-failure-object",
  handlers: {
    *boom() {
      return yield* new DurableExecutionError({
        operation: "object.syntheticInfrastructure",
        message: "synthetic infrastructure failure",
        cause: undefined,
      })
    },
  },
  schemas: { boom: { input: Schema.Void, output: Schema.Never } },
})

const engineLayer = serviceLayer(InfraFailureObject)
const boomHandler = compileOne(InfraFailureObject, "boom")!.handler

describe.skipIf(!hasS2())("object infrastructure errors", () => {
  it("do not settle the object call as a user-level Completed failure", async () => {
    const entries = await Effect.gen(function*() {
      const ctx = yield* Layer.build(engineLayer)
      const engine = Context.get(ctx, DurableEngine)
      const callId = yield* encodeObjectCallId({
        object: InfraFailureObject.name,
        key: "k",
        method: "boom",
        nonce: "n1",
      })

      yield* engine.submit(boomHandler, callId, undefined)
      yield* Effect.sleep(Duration.millis(100))

      const stream = `obj/${pathSegment(InfraFailureObject.name)}/${pathSegment("k")}`
      return yield* openLog(stream).read()
    }).pipe(Effect.scoped, Effect.provide(S2LiteLive), Effect.runPromise)

    expect(entries.map((entry) => entry.event._tag)).toContain("Accepted")
    expect(entries.some((entry) => entry.event._tag === "Completed")).toBe(false)
  }, 60_000)
})
