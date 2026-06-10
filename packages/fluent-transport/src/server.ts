import { Context, type Effect, type Scope, type Stream } from "effect"
import type { ClientTransport } from "./client.ts"
import type { ClientId, ServerStartError, TransportError, TransportMessage } from "./shared.ts"

export interface ClientConnection {
  readonly clientId: ClientId
  readonly transport: ClientTransport
  readonly metadata: Readonly<Record<string, unknown>>
}

export interface ServerTransport {
  readonly connections: Stream.Stream<ClientConnection>
  readonly broadcast: (message: TransportMessage) => Effect.Effect<void, TransportError>
}

export interface ServerAcceptor {
  readonly start: () => Effect.Effect<ServerTransport, ServerStartError, Scope.Scope>
}

export class ServerAcceptorTag extends Context.Service<ServerAcceptorTag, ServerAcceptor>()(
  "@firegrid/fluent-transport/ServerAcceptor",
) {}

export class ServerTransportTag extends Context.Service<ServerTransportTag, ServerTransport>()(
  "@firegrid/fluent-transport/ServerTransport",
) {}
