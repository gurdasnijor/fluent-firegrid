import { Schema } from "effect"

/**
 * The canonical ordered `ActorEvent` log (`object-actor-model` Model A,
 * `LAYERING.6/7`). An object stream is one append-only log of these events, read
 * by S2 `seq_num` via `effect-s2.readDecoded(ActorEvent)` — NOT a `StreamDb`
 * table-fold. The latest-value table fold is a projection over this log
 * (`ActorSnapshot`), never the persisted format.
 *
 * Payload fields (`input`, `value`, `exit`, …) are **already-schema-encoded JSON
 * values**: the `ActorLog.append` boundary (a later slice) schema-encodes a
 * handler's input/result/state to a JSON value before wrapping it in an event, so
 * the whole event round-trips through `readDecoded`'s JSON codec. The core type
 * carries the encoded form; it does not re-encode.
 */

// ── ActorExit — the durable encoding of a settled call's outcome (COMPLETION.1) ──

/** Success | Failure | Interrupt | Defect — the four settlement shapes. */
export const ActorExit = Schema.Union([
  Schema.TaggedStruct("Success", { value: Schema.Unknown }),
  Schema.TaggedStruct("Failure", { error: Schema.Unknown }),
  Schema.TaggedStruct("Interrupt", {}),
  Schema.TaggedStruct("Defect", { defect: Schema.Unknown }),
])
export type ActorExit = typeof ActorExit.Type

// ── CheckpointSnapshot — the durable, serializable form of an ActorSnapshot ──────

/**
 * The `Checkpointed` event's payload: a flattened, JSON-serializable snapshot
 * (Maps become entry arrays). The pure transition only reads `cursor`; resuming
 * a projection from a checkpoint is a later slice (`CHECKPOINTING`).
 */
export const CheckpointSnapshot = Schema.Struct({
  cursor: Schema.Number,
  pending: Schema.Array(Schema.String),
  active: Schema.NullOr(Schema.String),
  results: Schema.Array(Schema.Struct({ callId: Schema.String, exit: ActorExit })),
  // the live set — composite identities are kept as distinct fields (never delimiter-joined).
  signals: Schema.Array(Schema.Struct({ callId: Schema.String, name: Schema.String, value: Schema.Unknown })),
  state: Schema.Array(Schema.Struct({ table: Schema.String, key: Schema.String, value: Schema.Unknown })),
})
export type CheckpointSnapshot = typeof CheckpointSnapshot.Type

// ── ActorEvent — the tagged log vocabulary ───────────────────────────────────────

/** An exclusive call admitted to the accept-log (`ADMISSION.1-1`). */
export const Accepted = Schema.TaggedStruct("Accepted", {
  callId: Schema.String,
  method: Schema.String,
  input: Schema.Unknown,
})

/** A `run()`/`state` journal fact for a call, path-keyed by `step` (`PLANNING.3`). */
export const Journaled = Schema.TaggedStruct("Journaled", {
  callId: Schema.String,
  step: Schema.String,
  value: Schema.Unknown,
})

/** An external resolution appended as an event (`INGRESS.1`). */
export const SignalResolved = Schema.TaggedStruct("SignalResolved", {
  callId: Schema.String,
  name: Schema.String,
  value: Schema.Unknown,
})

/** A call settles by appending exactly one of these (`COMPLETION.1`). */
export const Completed = Schema.TaggedStruct("Completed", {
  callId: Schema.String,
  exit: ActorExit,
})

/**
 * A user-state mutation — the projection's source for materialized state. `op`
 * carries the full `state(Table)` surface (`set` and `delete`); `value` is present
 * for `set`, absent for `delete` (a tombstone).
 */
export const StateChanged = Schema.TaggedStruct("StateChanged", {
  op: Schema.Literals(["set", "delete"]),
  table: Schema.String,
  key: Schema.String,
  value: Schema.optional(Schema.Unknown),
})

/** A durable checkpoint marker carrying the snapshot at `cursor` (`CHECKPOINTING.2`). */
export const Checkpointed = Schema.TaggedStruct("Checkpointed", {
  cursor: Schema.Number,
  snapshot: CheckpointSnapshot,
})

/** The full event vocabulary read by `seq_num` order. */
export const ActorEvent = Schema.Union([
  Accepted,
  Journaled,
  SignalResolved,
  Completed,
  StateChanged,
  Checkpointed,
])
export type ActorEvent = typeof ActorEvent.Type

/** One ordered log record: its S2 `seq_num` and the decoded `ActorEvent`. */
export interface LogEntry {
  readonly seqNum: number
  readonly event: ActorEvent
}
