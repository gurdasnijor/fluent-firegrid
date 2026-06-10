import { Effect } from "effect"
import {
  makeTransportMessage,
  type ClientTransport,
  type ServerTransport,
  type TransportMessage,
} from "@firegrid/fluent-transport"
import {
  runClientServerTransportTestSuite,
  type ClientServerTransportTestContext,
  type TransportPair,
} from "@firegrid/fluent-transport/testing"
import * as InMemoryTransport from "../src/inMemoryTransport.ts"

const makeContext = (): Effect.Effect<ClientServerTransportTestContext> =>
  Effect.succeed({
    makeTransportPair: (): TransportPair => {
      let serverInstance: InMemoryTransport.InMemoryServer | undefined

      return {
        makeServer: () =>
          InMemoryTransport.makeInMemoryServer().pipe(
            Effect.tap((server) =>
              Effect.sync(() => {
                serverInstance = server
              }),
            ),
            Effect.map((server): ServerTransport => server),
          ),
        makeClient: () =>
          Effect.sync(() => serverInstance).pipe(
            Effect.flatMap((server) =>
              server === undefined
                ? Effect.fail(new Error("Server must be created before client"))
                : server.connector.connect("memory://contract").pipe(
                    Effect.mapError((cause) => new Error("Failed to connect in-memory client", { cause })),
                  ),
            ),
            Effect.map((client): ClientTransport => client),
          ),
      }
    },
    makeTestMessage: (type: string, payload: unknown): TransportMessage =>
      makeTransportMessage(`test-${type}`, type, JSON.stringify(payload)),
  })

runClientServerTransportTestSuite("In-memory", makeContext)
