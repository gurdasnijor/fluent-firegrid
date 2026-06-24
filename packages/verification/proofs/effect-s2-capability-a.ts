import * as Effect from "effect/Effect"

import type { Proof } from "../src/Proof.ts"
import { expectWorkloadResult, property } from "../src/Property.ts"
import { traceOperation } from "../src/TraceProof.ts"
import { durableJournal, effectS2Journal } from "./support/effect-s2-journal.ts"

const journal = durableJournal([
  { type: "StepCompleted", body: "step-1:ok" },
  { type: "CheckpointAdvanced", body: "input-cursor:1" }
])

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
          const effectS2 = effectS2Journal(operation, streamName)

          const initialTail = yield* effectS2.checkTail(stream, "initial")

          const commitAck = yield* effectS2.append(
            stream,
            "atomic-journal-commit",
            journal,
            initialTail.tail.seqNum
          )

          const staleReplayError = yield* effectS2.expectStaleReplayRejected(
            stream,
            journal,
            initialTail.tail.seqNum
          )

          const recordTypes = yield* effectS2.readRecordTypes(
            stream,
            "replay-fold",
            journal,
            commitAck.start.seqNum
          )

          const finalTail = yield* effectS2.checkTail(stream, "final")

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
          replayRecordTypes: journal.types,
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
          outputContains: journal.types
        })
      )
  }
}
