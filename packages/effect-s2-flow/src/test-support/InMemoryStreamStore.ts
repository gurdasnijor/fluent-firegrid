import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"

import { type FlowError, flowError } from "../runtime/FlowError.ts"
import type { AppendAck, AppendBatch, FlowRecord, StreamStore } from "../runtime/StreamStore.ts"

export interface InMemoryStreamStore<A> extends StreamStore<A> {
  readonly externalAppend: (values: ReadonlyArray<A>) => Effect.Effect<AppendAck<A>, FlowError>
  readonly records: Effect.Effect<ReadonlyArray<FlowRecord<A>>>
}

export const make = Effect.fn("InMemoryStreamStore.make")(function*<A>() {
  const recordsRef = yield* Ref.make<ReadonlyArray<FlowRecord<A>>>([])
  const pubsub = yield* PubSub.unbounded<FlowRecord<A>>()

  const append = (batch: AppendBatch<A>): Effect.Effect<AppendAck<A>, FlowError> =>
    Effect.gen(function*() {
      if (batch.values.length === 0) {
        return yield* flowError("write", "append batch must contain at least one value")
      }

      const records = yield* Ref.modify(recordsRef, (current) => {
        const start = current.length
        const appended = batch.values.map((value, index): FlowRecord<A> => ({
          seqNum: start + index,
          value,
          ...(batch.ownerId === undefined ? {} : { ownerId: batch.ownerId }),
          ...(batch.writeId === undefined ? {} : { writeId: batch.writeId })
        }))
        return [appended, [...current, ...appended]]
      })

      yield* Effect.forEach(records, (record) => PubSub.publish(pubsub, record), { discard: true })
      return {
        startSeqNum: records[0]!.seqNum,
        endSeqNum: records[records.length - 1]!.seqNum + 1,
        records
      } satisfies AppendAck<A>
    })

  return {
    append,
    externalAppend: (values) => append({ values }),
    checkTail: Effect.map(Ref.get(recordsRef), (records) => records.length),
    readSession: (fromSeqNum: number) =>
      Stream.unwrap(
        Effect.gen(function*() {
          const snapshot = yield* Ref.get(recordsRef)
          const live = Stream.fromPubSub(pubsub).pipe(Stream.filter((record) => record.seqNum >= fromSeqNum))
          return Stream.concat(
            Stream.fromIterable(snapshot.filter((record) => record.seqNum >= fromSeqNum)),
            live
          )
        })
      ),
    records: Ref.get(recordsRef)
  } satisfies InMemoryStreamStore<A>
})
