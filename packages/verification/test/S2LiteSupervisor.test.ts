import * as NodeServices from "@effect/platform-node/NodeServices"
import { Effect } from "effect"
import type * as Scope from "effect/Scope"
import { describe, expect, it } from "vitest"

import { S2LiteSupervisor } from "../src/S2LiteSupervisor.ts"

const runScoped = <A, E>(
  effect: Effect.Effect<A, E, NodeServices.NodeServices | Scope.Scope>
): Promise<A> =>
  effect.pipe(
    Effect.scoped,
    Effect.provide(NodeServices.layer),
    Effect.runPromise
  )

describe("S2LiteSupervisor", () => {
  it("launches and stops a managed process", () =>
    runScoped(
      Effect.gen(function*() {
        const supervisor = yield* S2LiteSupervisor

        yield* supervisor.start
        expect(yield* supervisor.endpoint).toBe("http://127.0.0.1:32199")
        yield* supervisor.stop

        const exit = yield* Effect.exit(supervisor.endpoint)
        expect(exit._tag).toBe("Failure")

        yield* supervisor.start
        expect(yield* supervisor.endpoint).toBe("http://127.0.0.1:32199")
        yield* supervisor.kill

        const killedExit = yield* Effect.exit(supervisor.endpoint)
        expect(killedExit._tag).toBe("Failure")
      }).pipe(
        Effect.provide(S2LiteSupervisor.layer({
          bin: "node",
          args: (cfg) => [
            "-e",
            `require("node:http").createServer((_, res) => res.end("ok")).listen(${cfg.port})`
          ],
          port: 32199,
          localRoot: "/tmp/firegrid-verification-test"
        }))
      )
    ))
})
