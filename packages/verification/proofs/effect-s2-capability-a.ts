import { AppendInput, AppendRecord, SeqNumMismatchError } from "effect-s2"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Stream from "effect/Stream"

import type { Proof } from "../src/Proof.ts"
import { expectWorkloadResult, property } from "../src/Property.ts"
import { traceOperation } from "../src/TraceProof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const journal = [
  AppendRecord.string({ body: "step-1:ok", headers: [["durable.record.type", "StepCompleted"]] }),
  AppendRecord.string({ body: "input-cursor:1", headers: [["durable.record.type", "CheckpointAdvanced"]] })
]

const journalTypes = ["StepCompleted", "CheckpointAdvanced"]

export const effectS2CapabilityAProof: Proof = {
  name: "effect-s2.capability-a.atomic-replay",
  description:
    "Proves the Capability A effect-s2 substrate: atomic StepCompleted + checkpoint append under matchSeqNum, stale replay rejected by SeqNumMismatchError, and replay reads only the original batch.",
  makeSpec: ({ trialId }) => {
    const streamName = `invocation-${trialId}`
    return property("capability-a.effect-s2.atomic-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ operation, s2 }) =>
        Effect.gen(function*() {
          const stream = yield* s2.stream({ basin: "capability-a-proof", stream: streamName })

          const initialTail = yield* operation(
            "effect-s2.check-tail.initial",
            { stream: streamName },
            stream.checkTail(),
            { operationId: 1, key: streamName }
          )

          const commitAck = yield* operation(
            "effect-s2.append.atomic-journal-commit",
            { matchSeqNum: initialTail.tail.seqNum, records: journalTypes },
            stream.append(AppendInput.create(journal, { matchSeqNum: initialTail.tail.seqNum })),
            { operationId: 2, key: streamName }
          )

          const staleReplayExit = yield* Effect.exit(
            operation(
              "effect-s2.append.stale-replay",
              { matchSeqNum: initialTail.tail.seqNum, records: journalTypes },
              stream.append(AppendInput.create(journal, { matchSeqNum: initialTail.tail.seqNum })).pipe(
                Effect.tapError((error) =>
                  Effect.annotateCurrentSpan({
                    "s2.error.code": error.code ?? "",
                    "s2.error.expected_seq_num": error instanceof SeqNumMismatchError
                      ? String(error.expectedSeqNum)
                      : "",
                    "s2.error.kind": error.name,
                    "s2.error.status": String(error.status)
                  })
                )
              ),
              { operationId: 3, key: streamName }
            )
          )
          const staleReplayReason = Exit.isFailure(staleReplayExit)
            ? staleReplayExit.cause.reasons.find(Cause.isFailReason)
            : undefined
          const staleReplayError = staleReplayReason?.error
          if (!(staleReplayError instanceof SeqNumMismatchError)) {
            return yield* new VerificationError({
              message: "expected stale replay append to fail with SeqNumMismatchError",
              cause: staleReplayExit
            })
          }

          const recordTypes = yield* operation(
            "effect-s2.read-session.replay-fold",
            { start: commitAck.start.seqNum, count: journal.length },
            stream.readSession({
              start: { from: { seqNum: commitAck.start.seqNum } },
              stop: { limits: { count: journal.length } }
            }).pipe(
              Stream.runCollect,
              Effect.map((records) =>
                Array.from(
                  records,
                  (record) => record.headers.find(([key]) => key === "durable.record.type")?.[1] ?? ""
                )
              )
            ),
            { operationId: 4, key: streamName }
          )

          const finalTail = yield* operation(
            "effect-s2.check-tail.final",
            { stream: streamName },
            stream.checkTail(),
            { operationId: 5, key: streamName }
          )

          return {
            appendStartedAtInitialTail: commitAck.start.seqNum === initialTail.tail.seqNum,
            replayRecordTypes: recordTypes,
            staleReplayRejectedAtSeqNum: staleReplayError.expectedSeqNum,
            tailAdvancedBy: finalTail.tail.seqNum - initialTail.tail.seqNum
          }
        })
      )
      .verify(
        expectWorkloadResult({
          appendStartedAtInitialTail: true,
          replayRecordTypes: journalTypes,
          staleReplayRejectedAtSeqNum: 2,
          tailAdvancedBy: 2
        }),
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
          outputContains: journalTypes
        })
      )
  }
}
