import { Chunk, Effect, Stream } from "effect"
import {
  BeginningOffset,
  NowOffset,
  appendBytes,
  decodeOffset,
  decodeStreamPath,
  type DurableStreamLog,
  type ReadOffset,
  type StreamRecord,
} from "@firegrid/fluent-store"
import type { ClientConnection, TransportMessage } from "@firegrid/fluent-transport"
import { decodeProtocolEnvelope, encodeProtocolEnvelope } from "./codec.ts"
import type { DurableStreamsCommand, DurableStreamsResponse, WireStreamRecord } from "./protocol.ts"

const bytesFromWire = (bytes: readonly number[]) => Uint8Array.from(bytes)
const bytesToWire = (bytes: Uint8Array) => Array.from(bytes)

const failure = (error: { readonly _tag: string }): DurableStreamsResponse => ({
  _tag: "Failure" as const,
  reason: error._tag,
  message: error._tag,
})

const readOffset = (offset: string): Effect.Effect<ReadOffset, unknown> => {
  if (offset === BeginningOffset) {
    return Effect.succeed(BeginningOffset)
  }

  if (offset === NowOffset) {
    return Effect.succeed(NowOffset)
  }

  return decodeOffset(offset)
}

const wireRecord = (record: StreamRecord): WireStreamRecord => ({
  path: record.path,
  fromOffset: record.fromOffset,
  nextOffset: record.nextOffset,
  bytes: bytesToWire(record.bytes),
  contentType: record.contentType,
  closed: record.closed,
})

export const handleCommand = (
  log: DurableStreamLog,
  command: DurableStreamsCommand,
): Effect.Effect<DurableStreamsResponse> =>
  Effect.gen(function* () {
    switch (command._tag) {
      case "CreateStream": {
        const path = yield* decodeStreamPath(command.path)
        return yield* log
          .create({
            path,
            contentType: command.contentType,
            ...(command.closed !== undefined && { closed: command.closed }),
          })
          .pipe(
            Effect.match({
              onFailure: failure,
              onSuccess: (result): DurableStreamsResponse => ({
                _tag: result._tag,
                tailOffset: result.metadata.tailOffset,
                closed: result.metadata.closed,
                contentType: result.metadata.contentType,
              }),
            }),
          )
      }
      case "AppendToStream": {
        const path = yield* decodeStreamPath(command.path)
        const expectedTailOffset =
          command.expectedTailOffset === undefined ? undefined : yield* decodeOffset(command.expectedTailOffset)
        return yield* appendBytes(
          log,
          {
            path,
            contentType: command.contentType,
            ...(expectedTailOffset !== undefined && { expectedTailOffset }),
            ...(command.close !== undefined && { close: command.close }),
          },
          bytesFromWire(command.bytes),
        ).pipe(
          Effect.match({
            onFailure: failure,
            onSuccess: (result): DurableStreamsResponse => ({
              _tag: result._tag,
              tailOffset: result.metadata.tailOffset,
              closed: result.metadata.closed,
            }),
          }),
        )
      }
      case "ReadStream": {
        const path = yield* decodeStreamPath(command.path)
        const offset = yield* readOffset(command.offset)
        return yield* log
          .read({ path, offset })
          .pipe(
            Effect.flatMap(Stream.runCollect),
            Effect.map(Chunk.toReadonlyArray),
            Effect.map((items) => items.map(wireRecord)),
            Effect.match({
              onFailure: failure,
              onSuccess: (records): DurableStreamsResponse => ({ _tag: "ReadResult", records }),
            }),
          )
      }
      case "HeadStream": {
        const path = yield* decodeStreamPath(command.path)
        return yield* log.head(path).pipe(
          Effect.match({
            onFailure: failure,
            onSuccess: (metadata): DurableStreamsResponse => ({
              _tag: "HeadResult",
              tailOffset: metadata.tailOffset,
              closed: metadata.closed,
              contentType: metadata.contentType,
            }),
          }),
        )
      }
      case "DeleteStream": {
        const path = yield* decodeStreamPath(command.path)
        return yield* log.delete(path).pipe(
          Effect.match({
            onFailure: failure,
            onSuccess: (result): DurableStreamsResponse =>
              result._tag === "Deleted" ? { _tag: "Deleted" } : { _tag: "NotFound" },
          }),
        )
      }
    }
  }).pipe(
    Effect.catchAll((error) =>
      Effect.succeed({
        _tag: "Failure" as const,
        reason: "ProtocolValidationError",
        message: String(error),
      }),
    ),
  )

const publishResponse = (
  connection: ClientConnection,
  id: string,
  response: DurableStreamsResponse,
) =>
  encodeProtocolEnvelope({
    kind: "response",
    id,
    response,
  }).pipe(
    Effect.flatMap(connection.transport.publish),
    Effect.catchAll(() => Effect.void),
  )

const publishProtocolFailure = (connection: ClientConnection, message: TransportMessage) =>
  publishResponse(connection, message.id, {
    _tag: "Failure",
    reason: "ProtocolValidationError",
    message: "Invalid protocol envelope",
  })

const handleMessage = (log: DurableStreamLog, connection: ClientConnection) => (message: TransportMessage) =>
  decodeProtocolEnvelope(message).pipe(
    Effect.flatMap((envelope) =>
      envelope.kind === "command"
        ? handleCommand(log, envelope.command).pipe(
            Effect.flatMap((response) => publishResponse(connection, envelope.id, response)),
          )
        : Effect.void,
    ),
    Effect.catchAll(() => publishProtocolFailure(connection, message)),
  )

export const serveConnection = (log: DurableStreamLog, connection: ClientConnection) =>
  Effect.gen(function* () {
    const messages = yield* connection.transport.subscribe()
    yield* messages.pipe(
      Stream.runForEach(handleMessage(log, connection)),
      Effect.forkScoped,
    )
  })
