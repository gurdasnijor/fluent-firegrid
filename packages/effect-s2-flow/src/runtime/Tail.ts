import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import type * as S2 from "effect-s2"

import { type FlowError, flowError } from "./FlowError.ts"
import type { FlowRecord } from "./Record.ts"
import * as RuntimeRecord from "./Record.ts"

const maxReadRecords = 1000

export const catchUp = Effect.fn("Tail.catchUp")(function*(
  stream: S2.StreamApi,
  fromSeqNum: number,
  apply: (record: FlowRecord) => Effect.Effect<void>
) {
  const tail = yield* stream.checkTail().pipe(
    Effect.map((response) => response.tail.seqNum),
    Effect.mapError((cause) => flowError("check-tail", "failed to check S2 stream tail", cause))
  )
  let cursor = fromSeqNum

  while (cursor < tail) {
    const count = Math.min(tail - cursor, maxReadRecords)
    const batch = yield* stream.read({
      start: { from: { seqNum: cursor }, clamp: true },
      stop: { limits: { count } }
    }).pipe(
      Effect.mapError((cause) => flowError("read-session", "failed to catch up S2 stream tail", cause))
    )

    if (batch.records.length === 0) {
      return cursor
    }

    const records = batch.records.filter((record) => record.seqNum < tail).map(RuntimeRecord.fromReadRecord)
    yield* Effect.forEach(records, apply, { discard: true })

    const last = records.at(-1)
    if (last === undefined) {
      return cursor
    }
    cursor = last.seqNum + 1
  }

  return cursor
})

export const follow = (stream: S2.StreamApi, fromSeqNum: number): Stream.Stream<FlowRecord, FlowError> =>
  Stream.unwrap(
    Ref.make(fromSeqNum).pipe(
      Effect.map((cursorRef) => Stream.fromIterableEffectRepeat(readNext(stream, cursorRef)))
    )
  )

const readNext = (
  stream: S2.StreamApi,
  cursorRef: Ref.Ref<number>
): Effect.Effect<ReadonlyArray<FlowRecord>, FlowError> =>
  Effect.gen(function*() {
    const cursor = yield* Ref.get(cursorRef)
    const batch = yield* stream.read({
      start: { from: { seqNum: cursor }, clamp: true },
      stop: {
        limits: { count: maxReadRecords },
        waitSecs: 1
      }
    }).pipe(
      Effect.mapError((cause) => flowError("read-session", "failed to follow S2 stream tail", cause))
    )
    const records = batch.records
      .filter((record) => record.seqNum >= cursor)
      .map(RuntimeRecord.fromReadRecord)
    const last = records.at(-1)

    if (last !== undefined) {
      yield* Ref.set(cursorRef, last.seqNum + 1)
    }

    return records
  })
