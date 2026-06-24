import {
  AppendInput,
  AppendRecord,
  basin,
  basins,
  layer as S2Layer,
  SeqNumMismatchError,
  stream as s2Stream
} from "effect-s2"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Stream from "effect/Stream"

import type { Proof } from "../src/Proof.ts"
import { expectWorkloadResult, property } from "../src/Property.ts"
import { traceSql } from "../src/TraceProof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const basinName = "capability-a-proof"

export interface CapabilityAProofResult {
  readonly appendStartedAtInitialTail: boolean
  readonly duplicateRejectedBySeqNum: boolean
  readonly duplicateExpectedSeqNum: number
  readonly readRecordTypes: ReadonlyArray<string>
  readonly recordCount: number
  readonly tailAdvancedBy: number
}

const s2Layer = (endpoint: string) =>
  S2Layer({
    accessToken: "s2_access_token",
    endpoints: {
      account: endpoint,
      basin: endpoint
    },
    retry: { maxAttempts: 1 }
  })

const durableRecord = (type: string, body: string) =>
  AppendRecord.string({
    body,
    headers: [["durable.record.type", type]]
  })

const headerValue = (
  headers: ReadonlyArray<readonly [string, string]>,
  name: string
): string | undefined => headers.find(([key]) => key === name)?.[1]

const seqNumMismatchFromExit = (exit: Exit.Exit<unknown, unknown>): SeqNumMismatchError | undefined => {
  if (!Exit.isFailure(exit)) return undefined
  const failReason = exit.cause.reasons.find(Cause.isFailReason)
  return failReason?.error instanceof SeqNumMismatchError ? failReason.error : undefined
}

export const effectS2CapabilityAProof: Proof<CapabilityAProofResult> = {
  name: "effect-s2.capability-a.atomic-replay",
  description:
    "Proves the Capability A effect-s2 substrate: atomic StepCompleted + checkpoint append under matchSeqNum, stale replay rejected by SeqNumMismatchError, and replay reads only the original batch.",
  makeSpec: ({ trialId }) => {
    const streamName = `invocation-${trialId}`
    return property("capability-a.effect-s2.atomic-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .workload<CapabilityAProofResult>(({ operation, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "expected runProperty to provide s2Endpoint" })
          }

          return yield* Effect.gen(function*() {
            yield* basins.ensure({ basin: basinName })
            const basinApi = yield* basin(basinName)
            yield* basinApi.streams.ensure({ stream: streamName })
            const stream = yield* s2Stream(basinName, streamName)

            const initialTail = yield* operation(
              "effect-s2.check-tail.initial",
              { stream: streamName },
              stream.checkTail(),
              { operationId: 1, key: streamName }
            )

            const commitAck = yield* operation(
              "effect-s2.append.atomic-journal-commit",
              {
                matchSeqNum: initialTail.tail.seqNum,
                records: ["StepCompleted", "CheckpointAdvanced"]
              },
              stream.append(AppendInput.create([
                durableRecord("StepCompleted", "step-1:ok"),
                durableRecord("CheckpointAdvanced", "input-cursor:1")
              ], { matchSeqNum: initialTail.tail.seqNum })),
              { operationId: 2, key: streamName }
            )

            const duplicateExit = yield* Effect.exit(
              operation(
                "effect-s2.append.replay-duplicate",
                {
                  matchSeqNum: initialTail.tail.seqNum,
                  records: ["StepCompleted", "CheckpointAdvanced"]
                },
                stream.append(AppendInput.create([
                  durableRecord("StepCompleted", "step-1:ok"),
                  durableRecord("CheckpointAdvanced", "input-cursor:1")
                ], { matchSeqNum: initialTail.tail.seqNum })).pipe(
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
            const duplicateError = seqNumMismatchFromExit(duplicateExit)

            const readRecords = yield* operation(
              "effect-s2.read-session.replay-fold",
              { start: commitAck.start.seqNum, count: 2 },
              stream.readSession({
                start: { from: { seqNum: commitAck.start.seqNum } },
                stop: { limits: { count: 2 } }
              }).pipe(
                Stream.runCollect,
                Effect.map((records) =>
                  Array.from(records, (record) => ({
                    seqNum: record.seqNum,
                    body: record.body,
                    type: headerValue(record.headers, "durable.record.type") ?? ""
                  }))
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
              duplicateExpectedSeqNum: duplicateError?.expectedSeqNum ?? -1,
              duplicateRejectedBySeqNum: duplicateError !== undefined,
              readRecordTypes: readRecords.map((record) => record.type),
              recordCount: readRecords.length,
              tailAdvancedBy: finalTail.tail.seqNum - initialTail.tail.seqNum
            } satisfies CapabilityAProofResult
          }).pipe(Effect.provide(s2Layer(s2Endpoint)))
        })
      )
      .verify(
        expectWorkloadResult<CapabilityAProofResult>({
          appendStartedAtInitialTail: true,
          duplicateExpectedSeqNum: 2,
          duplicateRejectedBySeqNum: true,
          readRecordTypes: ["StepCompleted", "CheckpointAdvanced"],
          recordCount: 2,
          tailAdvancedBy: 2
        }),
        traceSql(
          "atomic-commit-observed",
          `
          SELECT countIf(
            SpanName = 'verification.operation'
            AND SpanAttributes['firegrid.operation.name'] = 'effect-s2.append.atomic-journal-commit'
            AND SpanAttributes['firegrid.operation.status'] = 'ok'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "duplicate-replay-rejected-by-seqnum",
          `
          SELECT countIf(
            SpanName = 'verification.operation'
            AND SpanAttributes['firegrid.operation.name'] = 'effect-s2.append.replay-duplicate'
            AND SpanAttributes['firegrid.operation.status'] = 'error'
            AND SpanAttributes['s2.error.kind'] = 'SeqNumMismatchError'
            AND SpanAttributes['s2.error.code'] = 'APPEND_CONDITION_FAILED'
            AND SpanAttributes['s2.error.status'] = '412'
            AND SpanAttributes['s2.error.expected_seq_num'] = '2'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "replay-fold-read-original-atomic-batch",
          `
          SELECT countIf(
            SpanName = 'verification.operation'
            AND SpanAttributes['firegrid.operation.name'] = 'effect-s2.read-session.replay-fold'
            AND SpanAttributes['firegrid.operation.status'] = 'ok'
            AND SpanAttributes['firegrid.operation.output.json'] LIKE '%StepCompleted%'
            AND SpanAttributes['firegrid.operation.output.json'] LIKE '%CheckpointAdvanced%'
          ) = 1 AS ok
          FROM trial_spans
        `
        )
      )
  }
}
