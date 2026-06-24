import { AppendInput, AppendRecord, type S2Error, SeqNumMismatchError, type StreamApi } from "effect-s2"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Stream from "effect/Stream"

import type { operation as verificationOperation } from "../../src/Property.ts"
import { VerificationError } from "../../src/VerificationError.ts"

type VerificationOperation = typeof verificationOperation

export interface DurableJournalEntry {
  readonly type: string
  readonly body: string
}

export interface DurableJournalBatch {
  readonly records: ReadonlyArray<AppendRecord>
  readonly types: ReadonlyArray<string>
}

export const durableJournal = (entries: ReadonlyArray<DurableJournalEntry>): DurableJournalBatch => ({
  records: entries.map((entry) =>
    AppendRecord.string({
      body: entry.body,
      headers: [["durable.record.type", entry.type]]
    })
  ),
  types: entries.map((entry) => entry.type)
})

export const effectS2Journal = (operation: VerificationOperation, streamName: string) => {
  let operationId = 0
  const record = <A, E, R>(
    name: string,
    input: unknown,
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R> =>
    operation(`effect-s2.${name}`, input, effect, {
      key: streamName,
      operationId: ++operationId
    })

  const append = (
    stream: StreamApi,
    name: string,
    batch: DurableJournalBatch,
    matchSeqNum: number
  ) =>
    record(
      `append.${name}`,
      { matchSeqNum, records: batch.types },
      stream.append(AppendInput.create(batch.records, { matchSeqNum }))
    )

  return {
    append,
    checkTail: (stream: StreamApi, name: string) =>
      record(
        `check-tail.${name}`,
        { stream: streamName },
        stream.checkTail()
      ),
    expectStaleReplayRejected: (stream: StreamApi, batch: DurableJournalBatch, matchSeqNum: number) =>
      Effect.gen(function*() {
        const exit = yield* Effect.exit(
          record(
            "append.stale-replay",
            { matchSeqNum, records: batch.types },
            stream.append(AppendInput.create(batch.records, { matchSeqNum })).pipe(
              Effect.tapError(annotateS2Error)
            )
          )
        )
        return yield* expectSeqNumMismatch(exit)
      }),
    readRecordTypes: (stream: StreamApi, name: string, batch: DurableJournalBatch, startSeqNum: number) =>
      record(
        `read-session.${name}`,
        { start: startSeqNum, count: batch.records.length },
        stream.readSession({
          start: { from: { seqNum: startSeqNum } },
          stop: { limits: { count: batch.records.length } }
        }).pipe(
          Stream.runCollect,
          Effect.map((records) => Array.from(records, (record) => recordType(record.headers)))
        )
      )
  }
}

const annotateS2Error = (error: S2Error) =>
  Effect.annotateCurrentSpan({
    "s2.error.code": error.code ?? "",
    "s2.error.expected_seq_num": error instanceof SeqNumMismatchError
      ? String(error.expectedSeqNum)
      : "",
    "s2.error.kind": error.name,
    "s2.error.status": String(error.status)
  })

const recordType = (headers: ReadonlyArray<readonly [string, string]>): string =>
  headers.find(([key]) => key === "durable.record.type")?.[1] ?? ""

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
