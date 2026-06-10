import { Context, type Effect, type Scope, type Stream } from "effect"
import type { ConnectionError, ConnectionState, TransportError, TransportMessage } from "./shared.ts"

export interface ClientTransport {
  readonly connectionState: Stream.Stream<ConnectionState>
  readonly publish: (message: TransportMessage) => Effect.Effect<void, TransportError>
  readonly subscribe: (
    filter?: (message: TransportMessage) => boolean,
  ) => Effect.Effect<Stream.Stream<TransportMessage>, TransportError, Scope.Scope>
}

export interface ClientConnector {
  readonly connect: (url: string) => Effect.Effect<ClientTransport, ConnectionError, Scope.Scope>
}

export class ClientConnectorTag extends Context.Tag("@firegrid/fluent-transport/ClientConnector")<
  ClientConnectorTag,
  ClientConnector
>() {}

export class ClientTransportTag extends Context.Tag("@firegrid/fluent-transport/ClientTransport")<
  ClientTransportTag,
  ClientTransport
>() {}
