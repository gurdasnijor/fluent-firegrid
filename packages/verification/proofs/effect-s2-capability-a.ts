import { AppendInput, AppendRecord, type S2Error, SeqNumMismatchError, type StreamApi } from "effect-s2"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Stream from "effect/Stream"

import type { Proof } from "../src/Proof.ts"
import { expectWorkloadResult, property, type WorkloadContext } from "../src/Property.ts"
import { traceOperation } from "../src/TraceProof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const basinName = "capability-a-proof"
const journalRecordTypes = ["StepCompleted", "CheckpointAdvanced"] as const
export interface CapabilityAProofResult {
  readonly appendStartedAtInitialTail: boolean
  readonly replayRecordTypes: ReadonlyArray<string>
  readonly staleReplayRejectedAtSeqNum: number
  readonly tailAdvancedBy: number
}

const expectedOutcome: CapabilityAProofResult = {
  appendStartedAtInitialTail: true,
  replayRecordTypes: journalRecordTypes,
  staleReplayRejectedAtSeqNum: 2,
  tailAdvancedBy: 2
}

type Operation = WorkloadContext["operation"]

export const effectS2CapabilityAProof: Proof<CapabilityAProofResult> = {
  name: "effect-s2.capability-a.atomic-replay",
  description:
    "Proves the Capability A effect-s2 substrate: atomic StepCompleted + checkpoint append under matchSeqNum, stale replay rejected by SeqNumMismatchError, and replay reads only the original batch.",
  makeSpec: ({ trialId }) => {
    const streamName = `invocation-${trialId}`
    return property("capability-a.effect-s2.atomic-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .workload<CapabilityAProofResult>(({ operation, s2 }) =>
        Effect.gen(function*() {
          const stream = yield* s2.stream({ basin: basinName, stream: streamName })
          const step = operationRecorder(operation, streamName)

          const initialTail = yield* step(
            "check-tail.initial",
            { stream: streamName },
            stream.checkTail()
          )

          const commitAck = yield* step(
            "append.atomic-journal-commit",
            {
              matchSeqNum: initialTail.tail.seqNum,
              records: journalRecordTypes
            },
            appendJournalBatch(stream, initialTail.tail.seqNum)
          )

          const duplicateExit = yield* Effect.exit(
            step(
              "append.stale-replay",
              {
                matchSeqNum: initialTail.tail.seqNum,
                records: journalRecordTypes
              },
              replayJournalBatch(stream, initialTail.tail.seqNum)
            )
          )
          const staleReplayError = yield* expectSeqNumMismatch(duplicateExit)

          const recordTypes = yield* step(
            "read-session.replay-fold",
            { start: commitAck.start.seqNum, count: ownJournalBatch.length },
            replayRecordTypes(stream, commitAck.start.seqNum)
          )

          const finalTail = yield* step(
            "check-tail.final",
            { stream: streamName },
            stream.checkTail()
          )

          return {
            appendStartedAtInitialTail: commitAck.start.seqNum === initialTail.tail.seqNum,
            replayRecordTypes: recordTypes,
            staleReplayRejectedAtSeqNum: staleReplayError.expectedSeqNum,
            tailAdvancedBy: finalTail.tail.seqNum - initialTail.tail.seqNum
          } satisfies CapabilityAProofResult
        })
      )
      .verify(
        expectWorkloadResult(expectedOutcome),
        traceOperation("atomic-commit-observed", {
          operation: "effect-s2.append.atomic-journal-commit",
          status: "ok"
        }),
        traceOperation("stale-replay-rejected-by-seqnum", {
          operation: "effect-s2.append.stale-replay",
          status: "error",
          attributes: {
            "s2.error.code": "APPEND_CONDITION_FAILED",
            "s2.error.expected_seq_num": 2,
            "s2.error.kind": "SeqNumMismatchError",
            "s2.error.status": 412
          }
        }),
        traceOperation("replay-fold-read-original-atomic-batch", {
          operation: "effect-s2.read-session.replay-fold",
          status: "ok",
          outputContains: journalRecordTypes
        })
      )
  }
}

const ownJournalBatch = [
  AppendRecord.string({
    body: "step-1:ok",
    headers: [["durable.record.type", "StepCompleted"]]
  }),
  AppendRecord.string({
    body: "input-cursor:1",
    headers: [["durable.record.type", "CheckpointAdvanced"]]
  })
]

const operationRecorder = (operation: Operation, streamName: string) => {
  let operationId = 0
  return <A, E, R>(
    name: string,
    input: unknown,
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R> =>
    operation(`effect-s2.${name}`, input, effect, {
      key: streamName,
      operationId: ++operationId
    })
}

const appendJournalBatch = (stream: StreamApi, matchSeqNum: number) =>
  stream.append(AppendInput.create(ownJournalBatch, { matchSeqNum }))

const annotateS2Error = (error: S2Error) =>
  Effect.annotateCurrentSpan({
    "s2.error.code": error.code ?? "",
    "s2.error.expected_seq_num": error instanceof SeqNumMismatchError
      ? String(error.expectedSeqNum)
      : "",
    "s2.error.kind": error.name,
    "s2.error.status": String(error.status)
  })

const replayJournalBatch = (stream: StreamApi, matchSeqNum: number) =>
  appendJournalBatch(stream, matchSeqNum).pipe(
    Effect.tapError(annotateS2Error)
  )

const recordType = (headers: ReadonlyArray<readonly [string, string]>): string =>
  headers.find(([key]) => key === "durable.record.type")?.[1] ?? ""

const replayRecordTypes = (stream: StreamApi, startSeqNum: number) =>
  stream.readSession({
    start: { from: { seqNum: startSeqNum } },
    stop: { limits: { count: ownJournalBatch.length } }
  }).pipe(
    Stream.runCollect,
    Effect.map((records) => Array.from(records, (record) => recordType(record.headers)))
  )

const seqNumMismatchFromExit = (exit: Exit.Exit<unknown, unknown>): SeqNumMismatchError | undefined => {
  if (!Exit.isFailure(exit)) return undefined
  const failReason = exit.cause.reasons.find(Cause.isFailReason)
  return failReason?.error instanceof SeqNumMismatchError ? failReason.error : undefined
}

const expectSeqNumMismatch = Effect.fn("expectSeqNumMismatch")(function*(exit: Exit.Exit<unknown, unknown>) {
  const error = seqNumMismatchFromExit(exit)
  if (error !== undefined) return error
  return yield* new VerificationError({
    message: "expected stale replay append to fail with SeqNumMismatchError",
    cause: exit
  })
})
