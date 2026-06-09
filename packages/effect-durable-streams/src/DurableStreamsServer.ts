/**
 * Optional cross-process RPC adapter over the in-process `Store` service.
 *
 * Public Durable Streams HTTP routes are implemented in `Api.ts`.
 * This group is for typed RPC transports when a composition root chooses to
 * mount `RpcServer.layer(...)` with a concrete protocol.
 *
 * effect-server.SERVER.1
 */
import { Rpc, RpcGroup } from "@effect/rpc"
import { Effect, Schema } from "effect"
import * as Protocol from "./Protocol.ts"
import * as ProtocolError from "./ProtocolError.ts"
import * as Store from "./Store.ts"

export class DurableStreamsRpcs extends RpcGroup.make(
  Rpc.make("CreateStream", {
    payload: Protocol.CreateRequest,
    success: Protocol.CreateDecision,
    error: ProtocolError.Failure,
  }),
  Rpc.make("AppendToStream", {
    payload: Protocol.AppendRequest,
    success: Protocol.AppendResult,
    error: ProtocolError.Failure,
  }),
  Rpc.make("HeadStream", {
    payload: Protocol.HeadRequest,
    success: Protocol.StreamTail,
    error: ProtocolError.Failure,
  }),
  Rpc.make("ReadStream", {
    payload: Protocol.ReadRequest,
    success: Protocol.ReadChunk,
    error: ProtocolError.Failure,
  }),
  Rpc.make("DeleteStream", {
    payload: Protocol.DeleteRequest,
    success: Schema.Void,
    error: ProtocolError.Failure,
  }),
) {}

export const layer = DurableStreamsRpcs.toLayer(
  Effect.gen(function* () {
    const store = yield* Store.Store
    return DurableStreamsRpcs.of({
      CreateStream: store.createStream,
      AppendToStream: store.append,
      HeadStream: (request) => store.head(request.path),
      ReadStream: (request) => store.read(request.path, request.offset),
      DeleteStream: (request) => store.deleteStream(request.path),
    })
  }),
)
