import { AppendInput, AppendRecord, basin, basins, layer as S2Layer, stream as s2Stream } from "effect-s2"
import { Clock, Effect, Exit, Stream } from "effect"
import { describe, expect, it } from "vitest"

import { expectWorkloadResult, property, runProperty, traceSql, VerificationError } from "../src/index.ts"
import { layer as TraceRuntimeLayer } from "../src/TraceRuntime.ts"

const basinName = "capability-a-proof"
const LiveTraceLayer = TraceRuntimeLayer({ serviceName: "firegrid-verification-effect-s2-proof" })

interface CapabilityAProofResult {
  readonly appendStartedAtInitialTail: boolean
  readonly duplicateRejected: boolean
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

describe("effect-s2 capability proof", () => {
  it("proves atomic own-journal commit and replay CAS rejection against real s2 lite", () =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis
      const trialId = `capability-a-effect-s2-${now}`
      const streamName = `invocation-${trialId}`
      const spec = property("capability-a.effect-s2.atomic-replay-proof")
        .s2Lite({ persistence: "local-root" })
        .workload(({ operation, s2Endpoint }) =>
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
                  ], { matchSeqNum: initialTail.tail.seqNum })),
                  { operationId: 3, key: streamName }
                )
              )

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
                duplicateRejected: Exit.isFailure(duplicateExit),
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
            duplicateRejected: true,
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
            "duplicate-replay-rejected",
            `
            SELECT countIf(
              SpanName = 'verification.operation'
              AND SpanAttributes['firegrid.operation.name'] = 'effect-s2.append.replay-duplicate'
              AND SpanAttributes['firegrid.operation.status'] = 'error'
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

      const trial = yield* runProperty(spec, { trialId })

      expect(trial.result._tag).toBe("Success")
    }).pipe(
      Effect.provide(LiveTraceLayer),
      Effect.scoped,
      Effect.runPromise
    ), 30_000)
})
