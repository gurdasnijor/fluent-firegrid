import { Effect, Stream } from "effect"
import type { CodecError } from "./errors.ts"
import { decodeRecord, recordOp, type JournalRecord, type SeedData, type StepOutcome } from "./record.ts"
import type { S2Record } from "./s2.ts"

/**
 * §5.2 — the folded view of a journal. `byOp` maps op-index → its latest record
 * (a `timer-fired` overwrites its `timer-set`; an `awakeable-done` overwrites its
 * `awakeable`). `tail` is the physical next seq used as `match_seq_num`.
 */
export interface Journal {
  readonly byOp: ReadonlyMap<number, JournalRecord>
  readonly tail: bigint
  readonly seed: SeedData | null
  readonly input: unknown
  readonly status: "running" | "completed"
  readonly completed: StepOutcome | null
}

export const emptyJournal: Journal = {
  byOp: new Map(),
  tail: 0n,
  seed: null,
  input: undefined,
  status: "running",
  completed: null,
}

interface FoldState {
  tail: bigint
  seed: SeedData | null
  input: unknown
  status: "running" | "completed"
  completed: StepOutcome | null
}

const applyOne = (
  byOp: Map<number, JournalRecord>,
  state: FoldState,
  seqNum: bigint,
  rec: JournalRecord,
): void => {
  state.tail = seqNum + 1n
  switch (rec.kind) {
    case "seed":
      state.seed = rec.seed
      state.input = rec.input
      return
    case "step":
    case "timer-set":
    case "timer-fired":
    case "awakeable":
    case "awakeable-done": {
      const op = recordOp(rec)
      if (op !== null) byOp.set(op, rec)
      return
    }
    case "snapshot": {
      byOp.clear()
      rec.state.records.forEach((r) => {
        const op = recordOp(r)
        if (op !== null) byOp.set(op, r)
      })
      if (rec.state.seed !== null) state.seed = rec.state.seed
      state.input = rec.state.input
      return
    }
    case "completed":
      state.status = "completed"
      state.completed = rec.outcome
      return
    case "lease-fenced":
      return
  }
}

/** Build a `Journal` from decoded records (seqNum-ordered). */
export const foldRecords = (
  records: ReadonlyArray<readonly [bigint, JournalRecord]>,
): Journal => {
  const byOp = new Map<number, JournalRecord>()
  const state: FoldState = {
    tail: 0n,
    seed: null,
    input: undefined,
    status: "running",
    completed: null,
  }
  records.forEach(([seqNum, rec]) => applyOne(byOp, state, seqNum, rec))
  return {
    byOp,
    tail: state.tail,
    seed: state.seed,
    input: state.input,
    status: state.status,
    completed: state.completed,
  }
}

/** Fold an S2 read session (from a snapshot cursor, then deltas) into a `Journal`. */
export const fold = <E>(
  records: Stream.Stream<S2Record, E>,
): Effect.Effect<Journal, E | CodecError> =>
  records.pipe(
    Stream.runCollect,
    Effect.flatMap((collected) =>
      Effect.forEach(collected, (rec) =>
        decodeRecord(rec.data).pipe(Effect.map((parsed) => [rec.seqNum, parsed] as const)),
      ),
    ),
    Effect.map(foldRecords),
  )
