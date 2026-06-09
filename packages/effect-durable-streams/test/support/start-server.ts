/**
 * Boot the REAL Effect server on an ephemeral port for HTTP-level tests, using
 * the production `Server.layer` (no re-implementing the layer, no importing
 * `router`). A pre-created Node server is injected so the test can read the
 * bound port. Returns the bound `baseUrl` and a `close`.
 */
import { createServer } from "node:http"
import { Effect, Exit, Layer, Scope } from "effect"
import * as MemoryStore from "../../src/MemoryStore.ts"
import * as Server from "../../src/Server.ts"
import * as Store from "../../src/Store.ts"
import type { AddressInfo } from "node:net"

export interface Running {
  readonly baseUrl: string
  readonly close: () => Promise<void>
}

export const startServer = async (): Promise<Running> => {
  const node = createServer()
  const listening = new Promise<number>((resolve) => {
    node.once("listening", () => resolve((node.address() as AddressInfo).port))
  })

  const scope = await Effect.runPromise(Scope.make())
  await Effect.runPromise(
    Layer.buildWithScope(
      Server.layer({ port: 0, server: () => node }).pipe(
        Layer.provide(Store.withTracing(MemoryStore.layer)),
      ),
      scope,
    ).pipe(Effect.asVoid),
  )
  const port = await listening

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
  }
}
