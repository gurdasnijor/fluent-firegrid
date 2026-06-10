import { Brand, Data, Schema } from "effect"

export type MessageId = string & Brand.Brand<"MessageId">
export const MessageId = Brand.nominal<MessageId>()

export type ClientId = string & Brand.Brand<"ClientId">
export const ClientId = Brand.nominal<ClientId>()

export const TransportMetadata = Schema.Record({
  key: Schema.String,
  value: Schema.Unknown,
})
export type TransportMetadata = typeof TransportMetadata.Type

export const TransportMessage = Schema.Struct({
  id: Schema.String.pipe(Schema.brand("MessageId")),
  type: Schema.String,
  payload: Schema.String,
  metadata: Schema.optionalWith(TransportMetadata, { default: () => ({}) }),
})
export type TransportMessage = typeof TransportMessage.Type

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting" | "error"

export class TransportError extends Data.TaggedError("TransportError")<
  Readonly<{
    readonly message: string
    readonly cause?: unknown
  }>
> {}

export class ConnectionError extends Data.TaggedError("ConnectionError")<
  Readonly<{
    readonly message: string
    readonly cause?: unknown
  }>
> {}

export class ServerStartError extends Data.TaggedError("ServerStartError")<
  Readonly<{
    readonly message: string
    readonly cause?: unknown
  }>
> {}

export class MessageParseError extends Data.TaggedError("MessageParseError")<
  Readonly<{
    readonly message: string
    readonly rawData?: unknown
  }>
> {}

export const makeMessageId = (id: string): MessageId => MessageId(id)
export const makeClientId = (id: string): ClientId => ClientId(id)

export const makeTransportMessage = (
  id: string,
  type: string,
  payload: string,
  metadata: Readonly<Record<string, unknown>> = {},
): TransportMessage => ({
  id: makeMessageId(id),
  type,
  payload,
  metadata,
})

export const parseTransportMessage = Schema.decodeUnknown(TransportMessage)
export const encodeTransportMessage = Schema.encode(TransportMessage)
