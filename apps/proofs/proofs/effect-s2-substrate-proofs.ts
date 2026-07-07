import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  SeqNumMismatchError
} from "@s2-dev/streamstore"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

import { type Proof, proof } from "../src/Proof.ts"
import type { Check, Verifiers } from "../src/Property.ts"
import type { StreamApi } from "../src/S2Runtime.ts"
import type { TraceProof } from "../src/TraceProof.ts"

type ProofCheck<A> = Check<A> | TraceProof

interface StreamProofConfig<A> {
  readonly name: string
  readonly description: string
  readonly propertyName: string
  readonly basin: string
  readonly streamPrefix: string
  readonly run: (stream: StreamApi) => Effect.Effect<A, unknown>
  readonly verify: (verifiers: Verifiers<A>) => ReadonlyArray<ProofCheck<A>>
}

const streamProof = <A>(config: StreamProofConfig<A>): Proof<A> =>
  proof(config.name)
    .describedAs(config.description)
    .spec(({ property, trialId }) => {
      const streamName = `${config.streamPrefix}-${trialId}`
      return property(config.propertyName)
        .s2Lite({ persistence: "local-root" })
        .workload(({ s2 }) =>
          Effect.gen(function*() {
            const stream = yield* s2.stream({ basin: config.basin, stream: streamName })
            return yield* config.run(stream)
          })
        )
        .verify(config.verify)
    })

const readAfterAppendRecords = [
  AppendRecord.string({ body: "step-1" }),
  AppendRecord.string({ body: "step-2" }),
  AppendRecord.string({ body: "checkpoint" })
]

const readAfterAppendProof = streamProof({
  name: "effect-s2.capability-a.read-after-append",
  description:
    "Proves the S2 substrate visibility guarantee Capability A relies on: after an acknowledged append, check-tail advances and read-session returns the full batch in order.",
  propertyName: "capability-a.effect-s2.read-after-append-proof",
  basin: "capability-a-proof",
  streamPrefix: "read-after-append",
  run: (stream) =>
    Effect.gen(function*() {
      const initialTail = yield* stream.checkTail()
      const ack = yield* stream.append(AppendInput.create(readAfterAppendRecords, {
        matchSeqNum: initialTail.tail.seqNum
      }))
      const afterAppendTail = yield* stream.checkTail()
      const readBack = yield* stream.readSession({
        start: { from: { seqNum: ack.start.seqNum } },
        stop: { limits: { count: readAfterAppendRecords.length } }
      }).pipe(
        Stream.runCollect,
        Effect.map((records) => Array.from(records, (record) => record.body))
      )

      return {
        ackStartedAtInitialTail: ack.start.seqNum === initialTail.tail.seqNum,
        batchWasContiguous: ack.end.seqNum - ack.start.seqNum === readAfterAppendRecords.length,
        readBack,
        tailAdvancedBy: afterAppendTail.tail.seqNum - initialTail.tail.seqNum
      }
    }),
  verify: ({ expect, traceSql }) => [
    expect.workloadResult({
      ackStartedAtInitialTail: true,
      batchWasContiguous: true,
      readBack: ["step-1", "step-2", "checkpoint"],
      tailAdvancedBy: 3
    }),
    traceSql(
      "append-batch-observed",
      `
      SELECT countIf(
        SpanName = 'effect-s2.append'
        AND SpanAttributes['s2.operation.status'] = 'ok'
        AND SpanAttributes['s2.append.record_count'] = '3'
      ) = 1 AS ok
      FROM trial_spans
    `
    ),
    traceSql(
      "read-after-append-used-production-spans",
      `
      SELECT countIf(SpanName = 'effect-s2.check-tail') >= 2
        AND countIf(SpanName = 'effect-s2.read-session') >= 1 AS ok
      FROM trial_spans
    `
    )
  ]
})

const cursorFoldProof = streamProof({
  name: "effect-s2.capability-a.cursor-fold",
  description:
    "Proves the restart primitive Capability A needs: folding from a persisted cursor reads exactly the records at or after that cursor, in stream order.",
  propertyName: "capability-a.effect-s2.cursor-fold-proof",
  basin: "capability-a-proof",
  streamPrefix: "cursor-fold",
  run: (stream) =>
    Effect.gen(function*() {
      const initialTail = yield* stream.checkTail()
      const prefixAck = yield* stream.append(
        AppendInput.create([
          AppendRecord.string({ body: "already-folded-0" }),
          AppendRecord.string({ body: "already-folded-1" })
        ], { matchSeqNum: initialTail.tail.seqNum })
      )
      const restartCursor = prefixAck.end.seqNum
      const suffixAck = yield* stream.append(
        AppendInput.create([
          AppendRecord.string({ body: "after-cursor-0" }),
          AppendRecord.string({ body: "after-cursor-1" })
        ], { matchSeqNum: restartCursor })
      )
      const foldedAfterCursor = yield* stream.readSession({
        start: { from: { seqNum: restartCursor } },
        stop: { limits: { count: 2 } }
      }).pipe(
        Stream.runCollect,
        Effect.map((records) =>
          Array.from(records, (record) => ({
            body: record.body,
            seqNum: record.seqNum
          }))
        )
      )

      return {
        cursorEqualsSuffixStart: restartCursor === suffixAck.start.seqNum,
        foldedAfterCursor,
        prefixAdvancedBy: prefixAck.end.seqNum - prefixAck.start.seqNum,
        suffixAdvancedBy: suffixAck.end.seqNum - suffixAck.start.seqNum
      }
    }),
  verify: ({ expect, traceSql }) => [
    expect.workloadResult({
      cursorEqualsSuffixStart: true,
      foldedAfterCursor: [
        { body: "after-cursor-0", seqNum: 2 },
        { body: "after-cursor-1", seqNum: 3 }
      ],
      prefixAdvancedBy: 2,
      suffixAdvancedBy: 2
    }),
    traceSql(
      "cursor-fold-read-observed",
      `
      SELECT countIf(SpanName = 'effect-s2.read-session') >= 1
        AND countIf(
          SpanName = 'effect-s2.append'
          AND SpanAttributes['s2.operation.status'] = 'ok'
          AND SpanAttributes['s2.append.record_count'] = '2'
        ) = 2 AS ok
      FROM trial_spans
    `
    )
  ]
})

const matchSeqNumContentionProof = streamProof({
  name: "effect-s2.capability-b.match-seq-num-contention",
  description:
    "Proves the conditional-append contention primitive Capability B uses for single-writer state: two appends at the same tail cannot both commit.",
  propertyName: "capability-b.effect-s2.match-seq-num-contention-proof",
  basin: "capability-b-proof",
  streamPrefix: "match-seq-num-contention",
  run: (stream) =>
    Effect.gen(function*() {
      const initialTail = yield* stream.checkTail()
      const matchSeqNum = initialTail.tail.seqNum
      const winner = yield* stream.append(
        AppendInput.create([AppendRecord.string({ body: "winner" })], { matchSeqNum })
      )
      const loser = yield* stream.append(
        AppendInput.create([AppendRecord.string({ body: "loser" })], { matchSeqNum })
      ).pipe(
        Effect.flip,
        Effect.filterOrFail(
          (error): error is SeqNumMismatchError => error instanceof SeqNumMismatchError,
          (error) => error
        )
      )
      const committedRecords = yield* stream.readSession({
        start: { from: { seqNum: initialTail.tail.seqNum } },
        stop: { limits: { count: 1 } }
      }).pipe(
        Stream.runCollect,
        Effect.map((records) => Array.from(records, (record) => record.body))
      )
      const finalTail = yield* stream.checkTail()

      return {
        committedRecords,
        loserExpectedSeqNum: loser.expectedSeqNum,
        tailAdvancedBy: finalTail.tail.seqNum - initialTail.tail.seqNum,
        winnerStartSeqNum: winner.start.seqNum
      }
    }),
  verify: ({ expect, traceSql }) => [
    expect.workloadResult({
      committedRecords: ["winner"],
      loserExpectedSeqNum: 1,
      tailAdvancedBy: 1,
      winnerStartSeqNum: 0
    }),
    traceSql(
      "stale-conditional-append-rejected",
      `
      SELECT countIf(
        SpanName = 'effect-s2.append'
        AND SpanAttributes['s2.operation.status'] = 'error'
        AND SpanAttributes['s2.error.kind'] = 'SeqNumMismatchError'
        AND SpanAttributes['s2.error.code'] = 'APPEND_CONDITION_FAILED'
        AND SpanAttributes['s2.error.status'] = '412'
        AND SpanAttributes['s2.error.expected_seq_num'] = '1'
      ) = 1 AS ok
      FROM trial_spans
    `
    )
  ]
})

const fenceSemanticsProof = streamProof({
  name: "effect-s2.capability-b.fence-semantics",
  description:
    "Proves the cooperative S2 fencing semantics Capability B relies on: the correct token writes, a stale token is rejected, and an unfenced write is not protected.",
  propertyName: "capability-b.effect-s2.fence-semantics-proof",
  basin: "capability-b-proof",
  streamPrefix: "fence-semantics",
  run: (stream) =>
    Effect.gen(function*() {
      const ownerToken = "owner-a"
      const fenceAck = yield* stream.append(AppendInput.create([AppendRecord.fence(ownerToken)]))
      const guardedAck = yield* stream.append(
        AppendInput.create(
          [AppendRecord.string({ body: "guarded-owner-write" })],
          { fencingToken: ownerToken }
        )
      )
      const staleTokenError = yield* stream.append(
        AppendInput.create(
          [AppendRecord.string({ body: "stale-owner-write" })],
          { fencingToken: "owner-b" }
        )
      ).pipe(
        Effect.flip,
        Effect.filterOrFail(
          (error): error is FencingTokenMismatchError => error instanceof FencingTokenMismatchError,
          (error) => error
        )
      )
      const unfencedAck = yield* stream.append(
        AppendInput.create([AppendRecord.string({ body: "unfenced-write" })])
      )
      const dataRecords = yield* stream.readSession({
        start: { from: { seqNum: 0 } },
        stop: { limits: { count: 10 } },
        ignoreCommandRecords: true
      }).pipe(
        Stream.runCollect,
        Effect.map((records) => Array.from(records, (record) => record.body))
      )

      return {
        dataRecords,
        fenceSeqNum: fenceAck.start.seqNum,
        guardedSeqNum: guardedAck.start.seqNum,
        staleTokenExpected: staleTokenError.expectedFencingToken,
        unfencedSeqNum: unfencedAck.start.seqNum
      }
    }),
  verify: ({ expect, traceSql }) => [
    expect.workloadResult({
      dataRecords: ["guarded-owner-write", "unfenced-write"],
      fenceSeqNum: 0,
      guardedSeqNum: 1,
      staleTokenExpected: "owner-a",
      unfencedSeqNum: 2
    }),
    traceSql(
      "stale-fencing-token-rejected",
      `
      SELECT countIf(
        SpanName = 'effect-s2.append'
        AND SpanAttributes['s2.operation.status'] = 'error'
        AND SpanAttributes['s2.error.kind'] = 'FencingTokenMismatchError'
        AND SpanAttributes['s2.error.code'] = 'APPEND_CONDITION_FAILED'
        AND SpanAttributes['s2.error.status'] = '412'
        AND SpanAttributes['s2.error.expected_fencing_token'] = 'owner-a'
      ) = 1 AS ok
      FROM trial_spans
    `
    ),
    traceSql(
      "cooperative-fence-allowed-unfenced-write",
      `
      SELECT countIf(
        SpanName = 'effect-s2.append'
        AND SpanAttributes['s2.operation.status'] = 'ok'
      ) = 3 AS ok
      FROM trial_spans
    `
    )
  ]
})

export const effectS2SubstrateProofs = [
  readAfterAppendProof,
  cursorFoldProof,
  matchSeqNumContentionProof,
  fenceSemanticsProof
] as const
