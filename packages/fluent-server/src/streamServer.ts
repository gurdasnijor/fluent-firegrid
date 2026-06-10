import { Context, type Effect, type Stream } from "effect"
import {
  appendBytes,
  beginning,
  type CreateStreamResult,
  type DeleteStreamResult,
  type DurableStreamLog,
  type DurableStreamLogError,
  type AppendResult,
  type Offset,
  type ReadOffset,
  type StreamMetadata,
  type StreamPath,
  type StreamRecord,
} from "@firegrid/fluent-store"

export interface StreamServer {
  readonly create: (
    path: StreamPath,
    contentType: string,
    options?: { readonly closed?: boolean },
  ) => Effect.Effect<CreateStreamResult, DurableStreamLogError>
  readonly append: (
    path: StreamPath,
    contentType: string,
    bytes: Uint8Array,
    options?: {
      readonly close?: boolean
      readonly expectedTailOffset?: Offset
    },
  ) => Effect.Effect<AppendResult, DurableStreamLogError>
  readonly read: (
    path: StreamPath,
    offset?: ReadOffset,
  ) => Effect.Effect<Stream.Stream<StreamRecord, DurableStreamLogError>, DurableStreamLogError>
  readonly head: (path: StreamPath) => Effect.Effect<StreamMetadata, DurableStreamLogError>
  readonly delete: (path: StreamPath) => Effect.Effect<DeleteStreamResult, DurableStreamLogError>
}

export class StreamServerTag extends Context.Tag("@firegrid/fluent-server/StreamServer")<
  StreamServerTag,
  StreamServer
>() {}

export const makeStreamServer = (log: DurableStreamLog): StreamServer => ({
  create: (path, contentType, options) =>
    log.create({
      path,
      contentType,
      ...(options?.closed !== undefined && { closed: options.closed }),
    }),
  append: (path, contentType, bytes, options) =>
    appendBytes(
      log,
      {
        path,
        contentType,
        ...(options?.close !== undefined && { close: options.close }),
        ...(options?.expectedTailOffset !== undefined && {
          expectedTailOffset: options.expectedTailOffset,
        }),
      },
      bytes,
    ),
  read: (path, offset) => log.read(offset === undefined ? beginning(path) : { path, offset }),
  head: log.head,
  delete: log.delete,
})
