import { Effect, HashMap, Layer, PubSub, Queue, Ref, Stream, type Scope } from "effect"
import {
  ServerAcceptorTag,
  TransportError,
  makeClientId,
  type ConnectionError,
  type ClientConnection,
  type ClientConnector,
  type ClientTransport,
  type ConnectionState,
  type ServerAcceptor,
  type ServerTransport,
  type TransportMessage,
} from "@firegrid/fluent-transport"

interface ClientState {
  readonly clientToServer: PubSub.PubSub<TransportMessage>
  readonly serverToClient: PubSub.PubSub<TransportMessage>
  readonly connectionState: Ref.Ref<ConnectionState>
  readonly connectionStateChanges: PubSub.PubSub<ConnectionState>
}

interface RegisteredClient {
  readonly connection: ClientConnection
  readonly state: ClientState
}

interface ServerState {
  readonly connections: Queue.Queue<ClientConnection>
  readonly clients: Ref.Ref<HashMap.HashMap<string, RegisteredClient>>
  readonly nextClientNumber: Ref.Ref<number>
}

export interface InMemoryServer extends ServerTransport {
  readonly connector: ClientConnector
}

const queueCapacity = 256

const stateStream = (state: ClientState): Stream.Stream<ConnectionState> =>
  Stream.unwrap(
    Ref.get(state.connectionState).pipe(
      Effect.map((current) =>
        Stream.succeed(current).pipe(Stream.concat(Stream.fromPubSub(state.connectionStateChanges))),
      ),
    ),
  )

const failIfDisconnected = (
  state: ClientState,
  message: string,
): Effect.Effect<void, TransportError> =>
  Ref.get(state.connectionState).pipe(
    Effect.flatMap((connectionState) =>
      connectionState === "connected"
        ? Effect.void
        : Effect.fail(new TransportError({ message })),
    ),
  )

const streamFromPubSub = (
  pubsub: PubSub.PubSub<TransportMessage>,
  filter?: (message: TransportMessage) => boolean,
) =>
  PubSub.subscribe(pubsub).pipe(
    Effect.map((subscription) => {
      const stream = Stream.fromSubscription(subscription)
      return filter === undefined ? stream : stream.pipe(Stream.filter(filter))
    }),
  )

const clientTransport = (state: ClientState): ClientTransport => ({
  connectionState: stateStream(state),
  publish: (message) =>
    failIfDisconnected(state, "Not connected").pipe(
      Effect.andThen(PubSub.publish(state.clientToServer, message)),
      Effect.asVoid,
    ),
  subscribe: (filter) => streamFromPubSub(state.serverToClient, filter),
})

const serverSideTransport = (state: ClientState): ClientTransport => ({
  connectionState: stateStream(state),
  publish: (message) =>
    failIfDisconnected(state, "Client not connected").pipe(
      Effect.andThen(PubSub.publish(state.serverToClient, message)),
      Effect.asVoid,
    ),
  subscribe: (filter) => streamFromPubSub(state.clientToServer, filter),
})

const disconnectClient = (state: ClientState) =>
  Ref.set(state.connectionState, "disconnected").pipe(
    Effect.andThen(PubSub.publish(state.connectionStateChanges, "disconnected")),
    Effect.asVoid,
  )

const disconnectAll = (serverState: ServerState) =>
  Ref.get(serverState.clients).pipe(
    Effect.flatMap((clients) =>
      Effect.forEach(
        HashMap.values(clients),
        ({ state }) => disconnectClient(state),
        { discard: true },
      ),
    ),
  )

const unregisterClient = (serverState: ServerState, clientId: string, state: ClientState) =>
  disconnectClient(state).pipe(
    Effect.andThen(Ref.update(serverState.clients, HashMap.remove(clientId))),
  )

const nextClientId = (serverState: ServerState): Effect.Effect<string, never> =>
  Ref.modify(serverState.nextClientNumber, (next) => [`client-${next}`, next + 1] as const)

const makeClientState = (): Effect.Effect<ClientState, never> =>
  Effect.all({
    clientToServer: PubSub.bounded<TransportMessage>(queueCapacity),
    serverToClient: PubSub.bounded<TransportMessage>(queueCapacity),
    connectionState: Ref.make<ConnectionState>("connected"),
    connectionStateChanges: PubSub.bounded<ConnectionState>(queueCapacity),
  })

const registerClient = (
  serverState: ServerState,
): Effect.Effect<ClientTransport, ConnectionError, Scope.Scope> =>
  Effect.gen(function* () {
    const clientId = yield* nextClientId(serverState)
    const state = yield* makeClientState()
    const connection: ClientConnection = {
      clientId: makeClientId(clientId),
      transport: serverSideTransport(state),
      metadata: {},
    }
    const registered: RegisteredClient = { connection, state }

    yield* Ref.update(serverState.clients, HashMap.set(clientId, registered))
    yield* Queue.offer(serverState.connections, connection)
    yield* PubSub.publish(state.connectionStateChanges, "connected")
    yield* Effect.addFinalizer(() => unregisterClient(serverState, clientId, state))

    return clientTransport(state)
  })

const makeServerState = (): Effect.Effect<ServerState, never> =>
  Effect.all({
    connections: Queue.unbounded<ClientConnection>(),
    clients: Ref.make(HashMap.empty<string, RegisteredClient>()),
    nextClientNumber: Ref.make(0),
  })

const broadcast = (serverState: ServerState, message: TransportMessage) =>
  Ref.get(serverState.clients).pipe(
    Effect.flatMap((clients) =>
      Effect.forEach(
        HashMap.values(clients),
        ({ state }) => PubSub.publish(state.serverToClient, message),
        { discard: true },
      ),
    ),
    Effect.asVoid,
  )

export const makeInMemoryServer = (): Effect.Effect<InMemoryServer, never, Scope.Scope> =>
  makeServerState().pipe(
    Effect.tap((serverState) => Effect.addFinalizer(() => disconnectAll(serverState))),
    Effect.map((serverState) => ({
      connections: Stream.fromQueue(serverState.connections),
      broadcast: (message) => broadcast(serverState, message),
      connector: {
        connect: () => registerClient(serverState),
      },
    })),
  )

export const makeAcceptor = (): Effect.Effect<ServerAcceptor, never> =>
  Effect.succeed({
    start: makeInMemoryServer,
  })

export const layer: Layer.Layer<ServerAcceptorTag> = Layer.succeed(ServerAcceptorTag, {
  start: makeInMemoryServer,
})
