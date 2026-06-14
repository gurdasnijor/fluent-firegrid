import { Data } from "effect"

/**
 * Structured `412` from a conditional append. Carries enough to let the caller
 * distinguish *lost-lease* (fencing token moved on) from *already-written*
 * (the sequence position is taken) — the Q1 distinction the whole write-safety
 * model rests on.
 */
export class AppendCondFailed extends Data.TaggedError("AppendCondFailed")<
  Readonly<{
    readonly stream: string
    /** what we tried to write against */
    readonly expectedSeqNum?: bigint
    /** the actual tail S2 would assign next */
    readonly actualSeqNum: bigint
    /** the fencing token S2 currently holds for the stream */
    readonly currentFencingToken?: string
    /** the token we presented */
    readonly presentedFencingToken?: string
    readonly reason: "fence-mismatch" | "position-taken"
  }>
> {}

/** Any non-conditional S2 transport/protocol failure. */
export class S2Error extends Data.TaggedError("S2Error")<
  Readonly<{
    readonly operation: "append" | "read" | "checkTail" | "fence" | "trim"
    readonly stream: string
    readonly details: string
    readonly cause?: unknown
  }>
> {}

/** Codec failure decoding a journal record off the wire. */
export class CodecError extends Data.TaggedError("CodecError")<
  Readonly<{
    readonly details: string
    readonly cause?: unknown
  }>
> {}

/**
 * Replay divergence: the handler issued an op at index `op` whose
 * `(kind,name)` does not match what the journal recorded there. Fail loudly —
 * never silently corrupt (AC-2).
 */
export class DivergenceError extends Data.TaggedError("DivergenceError")<
  Readonly<{
    readonly execId: string
    readonly op: number
    readonly expected: string
    readonly actual: string
  }>
> {}

/** The current tick lost its lease to a newer epoch; abandon and let the owner run. */
export class LostLeaseError extends Data.TaggedError("LostLeaseError")<
  Readonly<{
    readonly execId: string
    readonly lease: string
  }>
> {}

/** A typed error surfaced from inside a durable step, replayed from the journal. */
export class StepFailure extends Data.TaggedError("StepFailure")<
  Readonly<{
    readonly name: string
    readonly error: unknown
  }>
> {}

/** Anything the handler API exposes in its error channel. */
export type WfError =
  | AppendCondFailed
  | S2Error
  | CodecError
  | DivergenceError
  | LostLeaseError
  | StepFailure
