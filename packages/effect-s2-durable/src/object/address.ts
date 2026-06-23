import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

/**
 * An object call id encodes enough owner identity to derive the owner stream by a
 * pure decode. It is addressing, not state-machine transition logic.
 */
const ObjectCallId = Schema.fromJsonString(
  Schema.Struct({
    object: Schema.String,
    key: Schema.String,
    method: Schema.String,
    nonce: Schema.String
  })
)

export type ObjectCallIdParts = typeof ObjectCallId.Type

/**
 * Reserved namespace prefix for object call ids. An id is an object call only if
 * it carries this prefix, so service ids and idempotency keys do not accidentally
 * route to an owner stream.
 */
export const OBJECT_ID_PREFIX = "durable.object.v1:"

/** Encode `{ object, key, method, nonce }` into a namespaced, opaque call id string. */
export const encodeObjectCallId = (parts: ObjectCallIdParts): Effect.Effect<string, Schema.SchemaError> =>
  Schema.encodeEffect(ObjectCallId)(parts).pipe(
    Effect.map((json) => OBJECT_ID_PREFIX + json),
    Effect.withSpan("effect-s2-durable.callId.encode")
  )

/**
 * Decode a namespaced object call id back to its parts. A string without the
 * reserved prefix decodes a deliberately invalid payload and fails.
 */
export const decodeObjectCallId = (id: string): Effect.Effect<ObjectCallIdParts, Schema.SchemaError> =>
  Schema.decodeUnknownEffect(ObjectCallId)(id.startsWith(OBJECT_ID_PREFIX) ? id.slice(OBJECT_ID_PREFIX.length) : "")
    .pipe(Effect.withSpan("effect-s2-durable.callId.decode"))
