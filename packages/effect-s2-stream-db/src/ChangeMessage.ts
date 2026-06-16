import { Effect, Schema } from "effect"

/**
 * The Durable Streams State Protocol message vocabulary, Schema-first.
 *
 * An `S2StreamDb` write is one of these messages encoded to a JSON record
 * body; materialization (`MaterializedState`) is the fold over the decoded
 * stream. This module is the *only* place the wire shape lives.
 *
 * @see ../../../docs/reference/durable-streams/packages/state/STATE-PROTOCOL.md
 */

/** A state mutation. */
export const Operation = Schema.Literals(["insert", "update", "delete"])
export type Operation = typeof Operation.Type

/** A stream-management signal (not a data change). */
export const Control = Schema.Literals(["snapshot-start", "snapshot-end", "reset"])
export type Control = typeof Control.Type

/** insert / update / delete on an entity identified by `(type, key)`. */
export const ChangeMessage = Schema.Struct({
  type: Schema.String,
  key: Schema.String,
  value: Schema.optional(Schema.Unknown),
  old_value: Schema.optional(Schema.Unknown),
  headers: Schema.Struct({
    operation: Operation,
    txid: Schema.optional(Schema.String),
  }),
})
export type ChangeMessage = typeof ChangeMessage.Type

/** snapshot boundary / reset marker. */
export const ControlMessage = Schema.Struct({
  headers: Schema.Struct({
    control: Control,
    offset: Schema.optional(Schema.String),
  }),
})
export type ControlMessage = typeof ControlMessage.Type

/**
 * A State-Protocol message. `ChangeMessage` is tried first; a record carrying
 * `headers.control` (no `operation`) falls through to `ControlMessage`.
 */
export const Message = Schema.Union([ChangeMessage, ControlMessage])
export type Message = typeof Message.Type

/** Narrow a decoded message to a data change. */
export const isChange = (message: Message): message is ChangeMessage =>
  "operation" in message.headers

/** Narrow a decoded message to a control signal. */
export const isControl = (message: Message): message is ControlMessage =>
  "control" in message.headers

const Json = Schema.UnknownFromJsonString

/** Encode a message to a JSON record body (the S2 `AppendRecord.string` body). */
export const encode = (message: Message): Effect.Effect<string, Schema.SchemaError> =>
  Schema.encodeEffect(Message)(message).pipe(Effect.flatMap(Schema.encodeEffect(Json)))

/** Decode a JSON record body back to a message (the fold input). */
export const decode = (body: string): Effect.Effect<Message, Schema.SchemaError> =>
  Schema.decodeEffect(Json)(body).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Message)))
