import { AppendInput, AppendRecord, SeqNumMismatchError } from "effect-s2"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

import { proof } from "../src/Proof.ts"

const journal = [
  AppendRecord.string({ body: "step-1:ok", headers: [["durable.record.type", "StepCompleted"]] }),
  AppendRecord.string({ body: "input-cursor:1", headers: [["durable.record.type", "CheckpointAdvanced"]] })
]

const journalTypes = ["StepCompleted", "CheckpointAdvanced"]

export default proof("effect-s2.capability-a.atomic-replay")
  .describedAs(
    "Proves the Capability A effect-s2 substrate: atomic StepCompleted + checkpoint append under matchSeqNum, stale replay rejected by SeqNumMismatchError, and replay reads only the original batch."
  )
  .spec(({ property, trialId }) => {
    const streamName = `invocation-${trialId}`
    return property("capability-a.effect-s2.atomic-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2 }) =>
        Effect.gen(function*() {
          const stream = yield* s2.stream({ basin: "capability-a-proof", stream: streamName })

          const initialTail = yield* stream.checkTail()
          const matchSeqNum = initialTail.tail.seqNum

          const commitAck = yield* stream.append(
            AppendInput.create(journal, { matchSeqNum })
          )

          const staleReplayError = yield* stream.append(
            AppendInput.create(journal, { matchSeqNum })
          ).pipe(
            Effect.flip,
            Effect.filterOrFail(
              (error): error is SeqNumMismatchError => error instanceof SeqNumMismatchError,
              (error) => error
            )
          )

          const recordTypes = yield* stream.readSession({
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
          )

          const finalTail = yield* stream.checkTail()

          return {
            appendStartedAtInitialTail: commitAck.start.seqNum === initialTail.tail.seqNum,
            replayRecordTypes: recordTypes,
            staleReplayRejectedAtSeqNum: staleReplayError.expectedSeqNum,
            tailAdvancedBy: finalTail.tail.seqNum - initialTail.tail.seqNum
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          appendStartedAtInitialTail: true,
          replayRecordTypes: journalTypes,
          staleReplayRejectedAtSeqNum: 2,
          tailAdvancedBy: 2
        }),
        traceSql(
          "atomic-commit-observed",
          `
          SELECT countIf(
            SpanName = 'effect-s2.append'
            AND SpanAttributes['s2.operation.status'] = 'ok'
            AND SpanAttributes['s2.append.record_count'] = '2'
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "stale-replay-rejected-by-seqnum",
          `
          SELECT countIf(
            SpanName = 'effect-s2.append'
            AND SpanAttributes['s2.operation.status'] = 'error'
            AND SpanAttributes['s2.error.kind'] = 'SeqNumMismatchError'
            AND SpanAttributes['s2.error.code'] = 'APPEND_CONDITION_FAILED'
            AND SpanAttributes['s2.error.status'] = '412'
            AND SpanAttributes['s2.error.expected_seq_num'] = '2'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "replay-read-used-production-s2-span",
          `
          SELECT countIf(SpanName = 'effect-s2.read-session') >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
