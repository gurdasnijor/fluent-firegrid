/* eslint-disable effect/no-runPromise -- Reusable Vitest contract suite runs Effects at test boundaries. */
import { Chunk, Duration, Effect, Stream, type Scope } from "effect"
import { describe, expect, it } from "vitest"
import type { ClientTransport } from "../client.ts"
import type { ClientConnection, ServerTransport } from "../server.ts"
import type { ConnectionState, TransportMessage } from "../shared.ts"

export interface TransportPair {
  readonly makeServer: () => Effect.Effect<ServerTransport, Error, Scope.Scope>
  readonly makeClient: () => Effect.Effect<ClientTransport, Error, Scope.Scope>
}

export interface ClientServerTransportTestContext {
  readonly makeTransportPair: () => TransportPair
  readonly makeTestMessage: (type: string, payload: unknown) => TransportMessage
}

interface StartedTransportPair {
  readonly context: ClientServerTransportTestContext
  readonly pair: TransportPair
  readonly server: ServerTransport
  readonly client: ClientTransport
}

const timeout = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  message: string,
  timeoutMs = 1_000,
): Effect.Effect<A, E | Error, R> =>
  effect.pipe(
    Effect.timeoutFail({
      duration: Duration.millis(timeoutMs),
      onTimeout: () => new Error(message),
    }),
  )

const collectMessages = <E, R>(
  stream: Stream.Stream<TransportMessage, E, R>,
  count: number,
): Effect.Effect<readonly TransportMessage[], E | Error, R> =>
  stream.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map(Chunk.toReadonlyArray),
    (effect) => timeout(effect, `Timed out waiting for ${count} transport message(s)`),
  )

const collectConnections = (
  server: ServerTransport,
  count: number,
): Effect.Effect<readonly ClientConnection[], Error> =>
  server.connections.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map(Chunk.toReadonlyArray),
    (effect) => timeout(effect, `Timed out waiting for ${count} client connection(s)`),
  )

const waitForConnectionState = (
  client: ClientTransport,
  state: ConnectionState,
): Effect.Effect<ConnectionState, Error> =>
  client.connectionState.pipe(
    Stream.filter((current) => current === state),
    Stream.take(1),
    Stream.runCollect,
    Effect.map((states) => Chunk.toReadonlyArray(states)[0]),
    Effect.flatMap((current) =>
      current === undefined ? Effect.fail(new Error(`Connection never reached ${state}`)) : Effect.succeed(current),
    ),
    (effect) => timeout(effect, `Timed out waiting for connection state ${state}`),
  )

const withStartedPair = <A>(
  makeContext: () => Effect.Effect<ClientServerTransportTestContext>,
  use: (pair: StartedTransportPair) => Effect.Effect<A, Error, Scope.Scope>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* makeContext()
      const pair = context.makeTransportPair()
      const server = yield* pair.makeServer()
      const client = yield* pair.makeClient()
      return yield* use({ context, pair, server, client })
    }),
  )

const firstConnection = (server: ServerTransport) =>
  collectConnections(server, 1).pipe(
    Effect.flatMap((connections) => {
      const connection = connections[0]
      return connection === undefined
        ? Effect.fail(new Error("Expected a server-side connection"))
        : Effect.succeed(connection)
    }),
  )

const firstConnectionMessages = (server: ServerTransport) =>
  firstConnection(server).pipe(
    Effect.flatMap((connection) =>
      connection.transport.subscribe().pipe(
        Effect.map((messages) => ({ connection, messages })),
      ),
    ),
  )

export const runClientServerTransportTestSuite = (
  name: string,
  makeContext: () => Effect.Effect<ClientServerTransportTestContext>,
) => {
  describe(`${name} ClientServerTransport`, () => {
    it("establishes client/server connections", async () => {
      const connections = await Effect.runPromise(
        withStartedPair(
          makeContext,
          ({ client, server }) =>
            waitForConnectionState(client, "connected").pipe(
              Effect.zipRight(collectConnections(server, 1)),
            ),
        ),
      )

      expect(connections).toHaveLength(1)
    })

    it("tracks multiple clients on one server", async () => {
      const connections = await Effect.runPromise(
        withStartedPair(
          makeContext,
          ({ client, pair, server }) =>
            pair.makeClient().pipe(
              Effect.flatMap((client2) =>
                Effect.all([
                  waitForConnectionState(client, "connected"),
                  waitForConnectionState(client2, "connected"),
                ]),
              ),
              Effect.zipRight(collectConnections(server, 2)),
            ),
        ),
      )

      expect(connections.map((connection) => connection.clientId)).toHaveLength(2)
    })

    it("delivers client messages to the server-side connection", async () => {
      const received = await Effect.runPromise(
        withStartedPair(
          makeContext,
          ({ client, context, server }) =>
            Effect.gen(function* () {
              const { messages } = yield* firstConnectionMessages(server)
              yield* client.publish(context.makeTestMessage("client.message", { text: "hello" }))
              return yield* collectMessages(messages, 1)
            }),
        ),
      )

      expect(received[0]?.type).toBe("client.message")
      expect(received[0]?.payload).toBe(JSON.stringify({ text: "hello" }))
    })

    it("broadcasts server messages to connected clients", async () => {
      const received = await Effect.runPromise(
        withStartedPair(
          makeContext,
          ({ client: client1, context, pair, server }) =>
            Effect.gen(function* () {
              const client2 = yield* pair.makeClient()
              yield* collectConnections(server, 2)

              const client1Messages = yield* client1.subscribe()
              const client2Messages = yield* client2.subscribe()
              yield* server.broadcast(context.makeTestMessage("server.broadcast", { text: "all" }))
              return yield* Effect.all([
                collectMessages(client1Messages, 1),
                collectMessages(client2Messages, 1),
              ])
            }),
        ),
      )

      expect(received[0][0]?.payload).toBe(JSON.stringify({ text: "all" }))
      expect(received[1][0]?.payload).toBe(JSON.stringify({ text: "all" }))
    })

    it("supports bidirectional request/response flow", async () => {
      const received = await Effect.runPromise(
        withStartedPair(
          makeContext,
          ({ client, context, server }) =>
            Effect.gen(function* () {
              const serverSide = yield* firstConnectionMessages(server)
              const clientMessages = yield* client.subscribe()
              yield* client.publish(context.makeTestMessage("client.request", { query: "ping" }))
              const serverReceived = yield* collectMessages(serverSide.messages, 1)
              yield* serverSide.connection.transport.publish(context.makeTestMessage("server.response", { result: "pong" }))
              const clientReceived = yield* collectMessages(clientMessages, 1)
              return { serverReceived, clientReceived }
            }),
        ),
      )

      expect(received.serverReceived[0]?.payload).toBe(JSON.stringify({ query: "ping" }))
      expect(received.clientReceived[0]?.payload).toBe(JSON.stringify({ result: "pong" }))
    })

    it("filters subscribed messages", async () => {
      const received = await Effect.runPromise(
        withStartedPair(
          makeContext,
          ({ client, context, server }) =>
            Effect.gen(function* () {
              yield* collectConnections(server, 1)

              const messages = yield* client.subscribe((message) => message.type.startsWith("important."))
              yield* server.broadcast(context.makeTestMessage("debug.message", { n: 1 }))
              yield* server.broadcast(context.makeTestMessage("important.alert", { n: 2 }))
              yield* server.broadcast(context.makeTestMessage("important.notice", { n: 3 }))
              return yield* collectMessages(messages, 2)
            }),
        ),
      )

      expect(received.map((message) => message.type)).toEqual(["important.alert", "important.notice"])
    })
  })
}
