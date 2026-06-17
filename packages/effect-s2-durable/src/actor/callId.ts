import { Effect, Schema } from "effect"

/**
 * `ActorCallId` — the reversible Effect Schema codec for a callId (`ROUTING`).
 *
 * A callId is a `{ owner, method, nonce }` encoded to a single string; `owner`
 * is itself encoded through the object's own key schema, so the owner is
 * recoverable by a **pure decode** (`ROUTING.1/3`). The owner becomes an S2 path
 * segment only by encoding it through this codec — never a hand-built string.
 *
 * `decode ∘ encode` and `encode ∘ decode` round-trip (`ROUTING.3`).
 */
export interface CallIdParts<Owner> {
  readonly owner: Owner
  readonly method: string
  readonly nonce: string
}

/** Build a callId codec for a given owner key schema. */
export const makeActorCallId = <Owner extends Schema.Top>(owner: Owner) =>
  Schema.fromJsonString(
    Schema.Struct({
      owner,
      method: Schema.String,
      nonce: Schema.String,
    }),
  )

/** The default codec: an opaque string owner (`object("counter")` keyed by string). */
export const ActorCallId = makeActorCallId(Schema.String)

// ── instrumented edges (the firelab production path; the codec itself is pure) ──

/** Encode `{ owner, method, nonce }` to the callId string. */
export const encodeCallId = <A>(
  codec: Schema.Codec<A, string>,
  parts: A,
): Effect.Effect<string, Schema.SchemaError> =>
  Schema.encodeEffect(codec)(parts).pipe(Effect.withSpan("effect-s2-durable.callId.encode"))

/** Decode a callId string back to its parts — owner recovery is a pure decode. */
export const decodeCallId = <A>(
  codec: Schema.Codec<A, string>,
  encoded: string,
): Effect.Effect<A, Schema.SchemaError> =>
  Schema.decodeUnknownEffect(codec)(encoded).pipe(Effect.withSpan("effect-s2-durable.callId.decode"))
