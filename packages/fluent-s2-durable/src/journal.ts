import type { ReadRecord } from "@s2-dev/streamstore"
import { Array, Effect, HashMap, Match, Option, Stream } from "effect"
import type { CodecError } from "./errors.ts"
import {
  decodeRecord,
  recordKey,
  type JournalRecord,
  type OpRecord,
  type Seed,
  type StepOutcome,
} from "./record.ts"

/**
 * §5.2 — the folded view of a journal (Restate's StateMachine, scoped to one
 * invocation). `byName` maps an entry name → its latest record (a `TimerFired`
 * overwrites its `TimerSet`; an `AwakeableDone` overwrites its `Awakeable`).
 *
 * The physical `match_seq_num` is *not* derived here — fence/trim command records
 * consume seq numbers and never reach the fold — so it is read separately via
 * `S2.checkTail`.
 */
export interface Journal {
  readonly byName: HashMap.HashMap<string, OpRecord>
  readonly seed: Option.Option<Seed>
  readonly input: unknown
  readonly completed: Option.Option<StepOutcome>
}

export const emptyJournal: Journal = {
  byName: HashMap.empty(),
  seed: Option.none(),
  input: undefined,
  completed: Option.none(),
}

const setOp = (journal: Journal, rec: OpRecord): Journal => ({
  ...journal,
  byName: HashMap.set(journal.byName, recordKey(rec), rec),
})

const applyRecord = (journal: Journal, rec: JournalRecord): Journal =>
  Match.value(rec).pipe(
    Match.tag("Seed", (r) => ({ ...journal, seed: Option.some(r), input: r.input })),
    Match.tag("Step", "TimerSet", "TimerFired", "Awakeable", "AwakeableDone", (r) => setOp(journal, r)),
    Match.tag("Snapshot", (r) => ({
      byName: HashMap.fromIterable(Array.map(r.records, (o) => [recordKey(o), o] as const)),
      seed: Option.fromNullOr(r.seed),
      input: r.input,
      completed: journal.completed,
    })),
    Match.tag("Completed", (r) => ({ ...journal, completed: Option.some(r.outcome) })),
    Match.exhaustive,
  )

/** Build a `Journal` from decoded records (seq-ordered). */
export const foldRecords = (records: ReadonlyArray<JournalRecord>): Journal =>
  Array.reduce(records, emptyJournal, applyRecord)

/** Fold an S2 read session (from a snapshot cursor, then deltas) into a `Journal`. */
export const fold = <E>(
  records: Stream.Stream<ReadRecord<"bytes">, E>,
): Effect.Effect<Journal, E | CodecError> =>
  records.pipe(
    Stream.runCollect,
    Effect.flatMap((collected) => Effect.forEach(collected, (rec) => decodeRecord(rec.body))),
    Effect.map(foldRecords),
  )
