import { Effect } from "effect"
import { CodecError } from "./errors.ts"

/**
 * The §4.3 record taxonomy. In a production build `kind`/`op`/`name` live in S2
 * record *headers* (arbitrary bytes) and the payload in the body; for the spike
 * we keep the whole record self-describing and lean on a JSON codec. The S2
 * service stays byte-oriented (matching the repo's `DurableStreamLog`); decoding
 * happens only in `fold`.
 */

export type StepOutcome =
  | { readonly _tag: "ok"; readonly value: unknown }
  | { readonly _tag: "error"; readonly error: unknown }

export type SeedData = {
  /** base wall-clock for the deterministic Clock */
  readonly epochMillis: number
  /** seed for the deterministic Random PRNG */
  readonly random: number
}

/** A folded checkpoint: the op-bearing records (+seed) needed to resume from head. */
export type SnapshotState = {
  readonly records: ReadonlyArray<JournalRecord>
  readonly seed: SeedData | null
  readonly input: unknown
}

export type JournalRecord =
  | { readonly kind: "lease-fenced"; readonly epoch: string }
  | { readonly kind: "seed"; readonly seed: SeedData; readonly input: unknown }
  | { readonly kind: "step"; readonly op: number; readonly name: string; readonly outcome: StepOutcome }
  | { readonly kind: "timer-set"; readonly op: number; readonly name: string; readonly fireAt: number }
  | { readonly kind: "timer-fired"; readonly op: number }
  | { readonly kind: "awakeable"; readonly op: number; readonly name: string; readonly id: string }
  | { readonly kind: "awakeable-done"; readonly op: number; readonly value: unknown }
  | { readonly kind: "snapshot"; readonly covers: number; readonly state: SnapshotState }
  | { readonly kind: "completed"; readonly outcome: StepOutcome }

export type RecordKind = JournalRecord["kind"]

/** The op-index a record is keyed by in the journal, if any. */
export const recordOp = (rec: JournalRecord): number | null => {
  switch (rec.kind) {
    case "step":
    case "timer-set":
    case "timer-fired":
    case "awakeable":
    case "awakeable-done":
      return rec.op
    default:
      return null
  }
}

/** A short `(kind,name)` signature used for divergence detection (AC-2). */
export const recordSignature = (rec: JournalRecord): string => {
  switch (rec.kind) {
    case "step":
    case "timer-set":
    case "awakeable":
      return `${rec.kind}:${rec.name}`
    default:
      return rec.kind
  }
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

export const encodeRecord = (rec: JournalRecord): Uint8Array =>
  encoder.encode(JSON.stringify(rec))

export const encodeRecords = (
  recs: ReadonlyArray<JournalRecord>,
): ReadonlyArray<Uint8Array> => recs.map(encodeRecord)

export const decodeRecord = (bytes: Uint8Array): Effect.Effect<JournalRecord, CodecError> =>
  Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)) as JournalRecord,
    catch: (cause) =>
      new CodecError({ details: "failed to decode journal record", cause }),
  })
