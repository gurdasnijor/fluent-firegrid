import { Effect, Schema } from "effect"
import { CodecError } from "./errors.ts"

/**
 * §4.3 record taxonomy, modeled Schema-first. Each variant is a `TaggedClass`,
 * so the wire codec is `Schema.fromJsonString` (no hand-rolled JSON) and all
 * branching is exhaustive `Match.tag`.
 *
 * Entries are keyed by a stable **name**, not a positional index. Name-addressing
 * is what lets durable ops compose under Effect's concurrency (`Effect.all`,
 * `Effect.fork`, `Effect.race`) — the order fibers issue ops in no longer has to
 * be deterministic, only the names do. (This dissolves SDD Q5 and is the property
 * the user-facing combinator API is built on.) Names must be unique per
 * invocation; reusing a name for a different op kind is a divergence.
 *
 * Step results / workflow input / event payloads are genuinely opaque here (the
 * SDK is generic over them), so `Schema.Unknown` is the honest type.
 */

export class Ok extends Schema.TaggedClass<Ok>("Ok")("Ok", {
  value: Schema.Unknown,
}) {}

export class Err extends Schema.TaggedClass<Err>("Err")("Err", {
  error: Schema.Unknown,
}) {}

export const StepOutcome = Schema.Union([Ok, Err])
export type StepOutcome = typeof StepOutcome.Type

export class Seed extends Schema.TaggedClass<Seed>("Seed")("Seed", {
  /** base wall-clock for the deterministic Clock */
  epochMillis: Schema.Number,
  /** seed for the deterministic Random PRNG */
  random: Schema.Number,
  /** genesis input for the workflow handler */
  input: Schema.Unknown,
}) {}

export class Step extends Schema.TaggedClass<Step>("Step")("Step", {
  name: Schema.String,
  outcome: StepOutcome,
}) {}

export class TimerSet extends Schema.TaggedClass<TimerSet>("TimerSet")("TimerSet", {
  name: Schema.String,
  fireAt: Schema.Number,
}) {}

export class TimerFired extends Schema.TaggedClass<TimerFired>("TimerFired")("TimerFired", {
  name: Schema.String,
}) {}

export class Awakeable extends Schema.TaggedClass<Awakeable>("Awakeable")("Awakeable", {
  name: Schema.String,
}) {}

export class AwakeableDone extends Schema.TaggedClass<AwakeableDone>("AwakeableDone")("AwakeableDone", {
  name: Schema.String,
  value: Schema.Unknown,
}) {}

/** Records keyed by name in `byName` (a snapshot never nests another snapshot). */
export const OpRecord = Schema.Union([Step, TimerSet, TimerFired, Awakeable, AwakeableDone])
export type OpRecord = typeof OpRecord.Type

export class Snapshot extends Schema.TaggedClass<Snapshot>("Snapshot")("Snapshot", {
  covers: Schema.Number,
  records: Schema.Array(OpRecord),
  seed: Schema.NullOr(Seed),
  input: Schema.Unknown,
}) {}

export class Completed extends Schema.TaggedClass<Completed>("Completed")("Completed", {
  outcome: StepOutcome,
}) {}

export const JournalRecord = Schema.Union([
  Seed,
  Step,
  TimerSet,
  TimerFired,
  Awakeable,
  AwakeableDone,
  Snapshot,
  Completed,
])
export type JournalRecord = typeof JournalRecord.Type

/** The name an op-record is keyed by. */
export const recordKey = (rec: OpRecord): string => rec.name

/** The kind signature used for divergence detection (AC-2): same name, different tag. */
export const recordSignature = (rec: JournalRecord): string => rec._tag

const codec = Schema.fromJsonString(JournalRecord)
const decodeJson = Schema.decodeEffect(codec)
const encodeJson = Schema.encodeEffect(codec)
const encoder = new TextEncoder()
const decoder = new TextDecoder()

const toCodecError = (cause: unknown): CodecError =>
  new CodecError({ details: "journal record codec failure", cause })

export const encodeRecord = (rec: JournalRecord): Effect.Effect<Uint8Array, CodecError> =>
  encodeJson(rec).pipe(
    Effect.map((json) => encoder.encode(json)),
    Effect.mapError(toCodecError),
  )

export const encodeRecords = (
  recs: ReadonlyArray<JournalRecord>,
): Effect.Effect<ReadonlyArray<Uint8Array>, CodecError> => Effect.forEach(recs, encodeRecord)

export const decodeRecord = (bytes: Uint8Array): Effect.Effect<JournalRecord, CodecError> =>
  decodeJson(decoder.decode(bytes)).pipe(Effect.mapError(toCodecError))

