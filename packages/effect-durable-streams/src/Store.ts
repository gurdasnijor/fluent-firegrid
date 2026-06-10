/**
 * Transport-neutral Durable Streams store algebra.
 *
 * The in-process API is this `Store` tag. HTTP routes and optional RPC
 * adapters are peer wire adapters over the same protocol-shaped operations.
 *
 * effect-server.SERVER.1 effect-server.TELEMETRY.2
 */
import { Context, Effect, Layer, Option } from "effect"
import type * as Protocol from "./Protocol.ts"
import type { ProtocolError } from "./ProtocolError.ts"
import * as Telemetry from "./Telemetry.ts"

export interface StoreShape {
  readonly createStream: (
    request: Protocol.CreateRequest,
  ) => Effect.Effect<Protocol.CreateDecision, ProtocolError>

  readonly append: (
    request: Protocol.AppendRequest,
  ) => Effect.Effect<Protocol.AppendResult, ProtocolError>

  readonly read: (
    path: Protocol.StreamPath,
    offset: Protocol.Offset,
  ) => Effect.Effect<Protocol.ReadChunk, ProtocolError>

  readonly head: (
    path: Protocol.StreamPath,
  ) => Effect.Effect<Protocol.StreamTail, ProtocolError>

  readonly deleteStream: (
    path: Protocol.StreamPath,
  ) => Effect.Effect<void, ProtocolError>
}

export class Store extends Context.Tag("@durable-streams/effect-server/Store")<
  Store,
  StoreShape
>() {}

export const traced = (inner: StoreShape): StoreShape => ({
  createStream: (request) =>
    Telemetry.withSpan(
      "stream.create",
      { stream: { path: request.path, closed: request.close } },
      inner.createStream(request).pipe(
        Effect.tap((decision) =>
          Telemetry.annotateCurrentSpan({
            decision: { name: decision._tag },
          }),
        ),
      ),
    ),
  append: (request) =>
    Telemetry.withSpan(
      "stream.append",
      {
        stream: { path: request.path, closed: request.close },
        producer: {
          present: Option.isSome(request.idempotentProducer),
        },
      },
      inner.append(request).pipe(
        Effect.tap((result) =>
          Telemetry.annotateCurrentSpan({
            decision: { name: result.append._tag },
          }),
        ),
      ),
    ),
  read: (path, offset) =>
    Telemetry.withSpan(
      "stream.read",
      { stream: { path, offset } },
      inner.read(path, offset),
    ),
  head: (path) =>
    Telemetry.withSpan(
      "stream.head",
      { stream: { path } },
      inner.head(path),
    ),
  deleteStream: (path) =>
    Telemetry.withSpan(
      "stream.delete",
      { stream: { path } },
      inner.deleteStream(path),
    ),
})

export const withTracing = <E, R>(
  self: Layer.Layer<Store, E, R>,
): Layer.Layer<Store, E, R> =>
  Layer.effect(Store, Effect.map(Store, traced)).pipe(Layer.provide(self))
