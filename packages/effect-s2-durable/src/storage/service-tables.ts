import { Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"

/**
 * The current stateless-execution per-execution and roster schemas — ordinary
 * `effect-s2-stream-db` definitions. The canonical engine direction is tracked in
 * `docs/sdds/effect-durable-execution-sdd.md`; object calls are being moved away from
 * these two-stream/roster schemas toward the per-key S2 owner-stream model described in
 * `docs/sdds/effect-s2-durable-consolidation-sdd.md`.
 */

/** One db (one S2 stream) per execution, keyed by its id. */
export const ExecutionId = Schema.String.pipe(Schema.brand("ExecutionId"))
export type ExecutionId = typeof ExecutionId.Type

// ── per-execution tables (WorkflowDb, stream `wf/<execId>`) ───────────────────

/** The execution's own state row (one per db). `input` is the encoded request. */
class ExecutionRow extends Table<ExecutionRow>("executions")({
  executionId: Schema.String.pipe(primaryKey),
  handlerName: Schema.String,
  input: Schema.Unknown,
  status: Schema.Literals(["running", "suspended", "completed", "failed"]),
  suspended: Schema.Boolean,
  /** A virtual object's `"name:key"`, if this is an object-method execution. */
  objectKey: Schema.optional(Schema.String),
}) {}

/**
 * A terminal step outcome (the durable `run` fact, keyed `${execId}/${key}`).
 * `success` distinguishes a recorded value from a recorded typed failure;
 * `value`/`error` hold the *encoded* outcome. No row = no terminal fact yet, so
 * the action is eligible to run (a crash before this row is written re-runs it).
 */
class StepRow extends Table<StepRow>("steps")({
  stepKey: Schema.String.pipe(primaryKey),
  success: Schema.Boolean,
  value: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Unknown),
}) {}

/** A durable deferred / signal — `value` present once resolved (slice 3). */
class DeferredRow extends Table<DeferredRow>("deferreds")({
  name: Schema.String.pipe(primaryKey),
  value: Schema.optional(Schema.Unknown),
}) {}

/**
 * A journaled `state.get` result, keyed `${execId}/read/${ordinal}` (Option A). A
 * recorded read replays its *original* value, so a read-modify-write across
 * suspend/resume recomputes against the value seen on first execution, not the
 * already-mutated durable value. `value` holds the encoded `row | null`.
 */
class StateReadRow extends Table<StateReadRow>("stateReads")({
  readKey: Schema.String.pipe(primaryKey),
  value: Schema.Unknown,
}) {}

/** A durable timer (`sleep`): a `clockWakeups` row + an in-process arm (slice 2). */
class ClockWakeupRow extends Table<ClockWakeupRow>("clockWakeups")({
  name: Schema.String.pipe(primaryKey),
  deadlineMs: Schema.Number,
  status: Schema.Literals(["pending", "fired"]),
}) {}

/** One S2 stream (`wf/<execId>`) per execution, aggregating its tables. */
export class WorkflowDb extends StreamDb<WorkflowDb>("wf")({
  executions: ExecutionRow,
  steps: StepRow,
  stateReads: StateReadRow,
  deferreds: DeferredRow,
  clockWakeups: ClockWakeupRow,
}, ExecutionId) {}

// Virtual-object state moved to the per-owner `ActorEvent` log (see
// `src/object/`): admission, exclusive drain, journaled `state`, and completion now
// live on one ordered owner stream, replacing the old `obj/<name:key>` inbox +
// state store (consolidation SDD deletion targets).

// ── roster (shared cross-execution index, stream `roster/<key>`) ──────────────

/**
 * The roster index. Its State-Protocol `type` is `"roster"` — distinct from the
 * per-execution `executions` table — on its own shared stream. It is the
 * cold-start enumeration source and the home of a completed execution's result
 * after its stream is dropped.
 */
class RosterRow extends Table<RosterRow>("roster")({
  executionId: Schema.String.pipe(primaryKey),
  handlerName: Schema.String,
  status: Schema.Literals(["running", "suspended", "completed", "failed"]),
  /** A virtual object's `"name:key"` for a non-terminal row — recovery groups by it. */
  objectKey: Schema.optional(Schema.String),
  suspendKind: Schema.optional(Schema.Literals(["deferred-wait", "pending-clock"])),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.String),
  resultAcked: Schema.optional(Schema.Boolean),
  updatedMs: Schema.Number,
}) {}

/** The shared roster db. Opened once, under a single key (default `"global"`). */
export class RosterDb extends StreamDb<RosterDb>("roster")({ roster: RosterRow }) {}
