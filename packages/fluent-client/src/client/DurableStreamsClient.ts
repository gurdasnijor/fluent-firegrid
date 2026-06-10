import { Deferred, Duration, Effect, Ref, Stream, type Scope } from "effect"
import * as Protocol from "@firegrid/fluent-protocol"
import type { ClientTransport, TransportMessage } from "@firegrid/fluent-transport"
import { DurableStreamsClientError, DurableStreamsProtocolFailure } from "./Errors.ts"
import type { StreamHandle } from "./StreamHandle.ts"

export interface DurableStreamsClient {
  readonly create: (
    path: string,
    contentType: string,
    options?: { readonly closed?: boolean },
  ) => Effect.Effect<
    {
      readonly path: string
      readonly tailOffset: string
      readonly closed: boolean
      readonly contentType: string
    },
    DurableStreamsClientError | DurableStreamsProtocolFailure
  >
  readonly stream: (path: string, contentType: string) => StreamHandle
  readonly append: (
    path: string,
    contentType: string,
    bytes: Uint8Array,
    options?: {
      readonly close?: boolean
      readonly expectedTailOffset?: string
    },
  ) => Effect.Effect<
    {
      readonly tailOffset: string
      readonly closed: boolean
    },
    DurableStreamsClientError | DurableStreamsProtocolFailure
  >
  readonly read: (
    path: string,
    offset?: string,
  ) => Effect.Effect<
    readonly {
      readonly bytes: Uint8Array
      readonly fromOffset: string
      readonly nextOffset: string
      readonly contentType: string
      readonly closed: boolean
    }[],
    DurableStreamsClientError | DurableStreamsProtocolFailure
  >
  readonly head: (
    path: string,
  ) => Effect.Effect<
    {
      readonly tailOffset: string
      readonly closed: boolean
      readonly contentType: string
    },
    DurableStreamsClientError | DurableStreamsProtocolFailure
  >
  readonly delete: (
    path: string,
  ) => Effect.Effect<"Deleted" | "NotFound", DurableStreamsClientError | DurableStreamsProtocolFailure>
}

const transportError = (operation: string) => (cause: unknown) =>
  new DurableStreamsClientError({
    operation,
    message: "Transport operation failed",
    cause,
  })

const bytesToWire = (bytes: Uint8Array) => Array.from(bytes)
const bytesFromWire = (bytes: readonly number[]) => Uint8Array.from(bytes)

const protocolFailure = (
  response: Extract<Protocol.DurableStreamsResponse, { readonly _tag: "Failure" }>,
) =>
  new DurableStreamsProtocolFailure({
    reason: response.reason,
    message: response.message,
  })

const expectResponse = <A>(
  operation: string,
  response: Protocol.DurableStreamsResponse,
  f: (
    response: Exclude<Protocol.DurableStreamsResponse, { readonly _tag: "Failure" }>,
  ) => Effect.Effect<A, DurableStreamsClientError>,
): Effect.Effect<A, DurableStreamsClientError | DurableStreamsProtocolFailure> =>
  response._tag === "Failure" ? Effect.fail(protocolFailure(response)) : f(response)

const unexpectedResponse = (operation: string, tag: string) =>
  new DurableStreamsClientError({
    operation,
    message: `Unexpected ${operation} response: ${tag}`,
  })

const responseValue = <A>(value: A): Effect.Effect<A, DurableStreamsClientError> =>
  Effect.succeed(value)

const responseError = (operation: string, tag: string): Effect.Effect<never, DurableStreamsClientError> =>
  Effect.fail(unexpectedResponse(operation, tag))

interface PendingResponse {
  readonly operation: string
  readonly deferred: Deferred.Deferred<Protocol.DurableStreamsResponse, DurableStreamsClientError>
}

const completePending =
  (pending: Map<string, PendingResponse>) =>
  (id: string, complete: (entry: PendingResponse) => Effect.Effect<void>) => {
    const entry = pending.get(id)
    if (entry === undefined) {
      return Effect.void
    }
    pending.delete(id)
    return complete(entry)
  }

const handleInbound =
  (pending: Map<string, PendingResponse>) =>
  (message: TransportMessage): Effect.Effect<void> =>
    Protocol.decodeProtocolEnvelope(message).pipe(
      Effect.matchEffect({
        onFailure: (cause) =>
          completePending(pending)(message.id, (entry) =>
            Deferred.fail(entry.deferred, transportError(entry.operation)(cause)),
          ),
        onSuccess: (envelope) =>
          envelope.kind === "response"
            ? completePending(pending)(envelope.id, (entry) =>
                Deferred.succeed(entry.deferred, envelope.response),
              )
            : Effect.void,
      }),
    )

const sendCommand = (
  transport: ClientTransport,
  nextId: Ref.Ref<number>,
  pending: Map<string, PendingResponse>,
  operation: string,
  command: Protocol.DurableStreamsCommand,
): Effect.Effect<Protocol.DurableStreamsResponse, DurableStreamsClientError> => {
  let id: string | undefined
  return Effect.gen(function* () {
    id = yield* Ref.modify(nextId, (current) => [`client-command-${current}`, current + 1] as const)
    const deferred = yield* Deferred.make<Protocol.DurableStreamsResponse, DurableStreamsClientError>()
    pending.set(id, { operation, deferred })
    const message = yield* Protocol.encodeProtocolEnvelope({ kind: "command", id, command }).pipe(
      Effect.mapError(transportError(operation)),
    )
    yield* transport.publish(message).pipe(Effect.mapError(transportError(operation)))
    return yield* Deferred.await(deferred).pipe(
      Effect.timeoutFail({
        duration: Duration.seconds(5),
        onTimeout: () =>
          new DurableStreamsClientError({
            operation,
            message: "Timed out waiting for protocol response",
          }),
      }),
    )
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        if (id !== undefined) {
          pending.delete(id)
        }
      }),
    ),
  )
}

export const make = (
  transport: ClientTransport,
): Effect.Effect<DurableStreamsClient, DurableStreamsClientError, Scope.Scope> =>
  Effect.gen(function* () {
    const nextId = yield* Ref.make(0)
    const pending = new Map<string, PendingResponse>()
    const inbound = yield* transport.subscribe().pipe(Effect.mapError(transportError("subscribe")))
    yield* inbound.pipe(
      Stream.runForEach(handleInbound(pending)),
      Effect.forkScoped,
    )

    return yield* Effect.sync((): DurableStreamsClient => {
      const client: DurableStreamsClient = {
        create: (path, contentType, options) =>
          sendCommand(transport, nextId, pending, "create", {
            _tag: "CreateStream",
            path,
            contentType,
            ...(options?.closed !== undefined && { closed: options.closed }),
          }).pipe(
            Effect.flatMap((response) =>
              expectResponse("create", response, (success) => {
                if (success._tag !== "Created" && success._tag !== "AlreadyExists") {
                  return responseError("create", success._tag)
                }
                return responseValue({
                  path,
                  tailOffset: success.tailOffset,
                  closed: success.closed,
                  contentType: success.contentType,
                })
              }),
            ),
          ),
        stream: (path, contentType) => ({
          path,
          append: (bytes, options) => client.append(path, contentType, bytes, options),
          read: (offset) => client.read(path, offset),
        }),
        append: (path, contentType, bytes, options) =>
          sendCommand(transport, nextId, pending, "append", {
            _tag: "AppendToStream",
            path,
            contentType,
            bytes: bytesToWire(bytes),
            ...(options?.close !== undefined && { close: options.close }),
            ...(options?.expectedTailOffset !== undefined && {
              expectedTailOffset: options.expectedTailOffset,
            }),
          }).pipe(
            Effect.flatMap((response) =>
              expectResponse("append", response, (success) => {
                if (success._tag !== "Appended" && success._tag !== "Noop") {
                  return responseError("append", success._tag)
                }
                return responseValue({ tailOffset: success.tailOffset, closed: success.closed })
              }),
            ),
          ),
        read: (path, offset = "-1") =>
          sendCommand(transport, nextId, pending, "read", {
            _tag: "ReadStream",
            path,
            offset,
          }).pipe(
            Effect.flatMap((response) =>
              expectResponse("read", response, (success) => {
                if (success._tag !== "ReadResult") {
                  return responseError("read", success._tag)
                }
                return responseValue(
                  success.records.map((record) => ({
                    bytes: bytesFromWire(record.bytes),
                    fromOffset: record.fromOffset,
                    nextOffset: record.nextOffset,
                    contentType: record.contentType,
                    closed: record.closed,
                  })),
                )
              }),
            ),
          ),
        head: (path) =>
          sendCommand(transport, nextId, pending, "head", {
            _tag: "HeadStream",
            path,
          }).pipe(
            Effect.flatMap((response) =>
              expectResponse("head", response, (success) => {
                if (success._tag !== "HeadResult") {
                  return responseError("head", success._tag)
                }
                return responseValue({
                  tailOffset: success.tailOffset,
                  closed: success.closed,
                  contentType: success.contentType,
                })
              }),
            ),
          ),
        delete: (path) =>
          sendCommand(transport, nextId, pending, "delete", {
            _tag: "DeleteStream",
            path,
          }).pipe(
            Effect.flatMap((response) =>
              expectResponse("delete", response, (success) => {
                if (success._tag !== "Deleted" && success._tag !== "NotFound") {
                  return responseError("delete", success._tag)
                }
                return responseValue(success._tag)
              }),
            ),
          ),
      }
      return client
    })
  })
