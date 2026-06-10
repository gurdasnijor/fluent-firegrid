import type { Effect, Scope } from "effect"
import type { ReadonlyMailbox } from "effect/Mailbox"
import * as Protocol from "@firegrid/fluent-protocol"
import { makeProducer, type Producer, type ProducerConfig } from "./Producer.ts"
import type { StreamHandle } from "./StreamHandle.ts"

export interface DurableStreamsClient {
  readonly create: (
    path: Protocol.Create["path"],
    contentType: string,
    options?: { readonly closed?: boolean },
  ) => Effect.Effect<Protocol.CreateResponse, Protocol.TransportError>
  readonly stream: (path: Protocol.Append["path"], contentType: string) => StreamHandle
  readonly append: (
    path: Protocol.Append["path"],
    contentType: string,
    bytes: Uint8Array,
    options?: {
      readonly close?: boolean
      readonly expectedTailOffset?: Protocol.Append["expectedTailOffset"]
    },
  ) => Effect.Effect<Protocol.AppendResponse, Protocol.TransportError>
  readonly read: (
    path: Protocol.Read["path"],
    offset?: Protocol.Read["offset"],
  ) => Effect.Effect<Protocol.ReadResponse, Protocol.TransportError>
  readonly tail: (
    path: Protocol.ReadLive["path"],
    offset?: Protocol.ReadLive["offset"],
  ) => Effect.Effect<ReadonlyMailbox<Protocol.ReadEvent, Protocol.TransportError>, Protocol.TransportError, Scope.Scope>
  readonly head: (path: Protocol.Head["path"]) => Effect.Effect<Protocol.HeadResponse, Protocol.TransportError>
  readonly delete: (path: Protocol.Delete["path"]) => Effect.Effect<Protocol.DeleteResponse, Protocol.TransportError>
  readonly close: (path: Protocol.Close["path"]) => Effect.Effect<Protocol.AppendResponse, Protocol.TransportError>
  readonly producer: (config: ProducerConfig) => Effect.Effect<Producer>
}

export const make = (transport: Protocol.DurableTransportService): DurableStreamsClient => {
  const client: DurableStreamsClient = {
    create: (path, contentType, options) =>
      transport.call(
        new Protocol.Create({
          path,
          contentType,
          closed: options?.closed ?? false,
        }),
      ),
    stream: (path, contentType) => ({
      path,
      append: (bytes, options) => client.append(path, contentType, bytes, options),
      read: (offset) => client.read(path, offset),
    }),
    append: (path, contentType, bytes, options) =>
      transport.call(
        new Protocol.Append({
          path,
          contentType,
          bytes,
          close: options?.close ?? false,
          ...(options?.expectedTailOffset !== undefined && {
            expectedTailOffset: options.expectedTailOffset,
          }),
        }),
      ),
    read: (path, offset = "-1") =>
      transport.call(
        new Protocol.Read({
          path,
          offset,
        }),
      ),
    tail: (path, offset = "now") =>
      transport.stream(
        new Protocol.ReadLive({
          path,
          offset,
        }),
      ),
    head: (path) => transport.call(new Protocol.Head({ path })),
    delete: (path) => transport.call(new Protocol.Delete({ path })),
    close: (path) => transport.call(new Protocol.Close({ path })),
    producer: (config) => makeProducer(transport, config),
  }
  return client
}
