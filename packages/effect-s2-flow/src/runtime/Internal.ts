import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import type * as S2 from "effect-s2"

import type { FlowError } from "./FlowError.ts"
import type { FlowRecord } from "./Record.ts"
import * as Tail from "./Tail.ts"

export type Project<S> = (state: S, applied: number) => unknown

export interface RecordEvent {
  readonly _tag: "Record"
  readonly record: FlowRecord
}

export interface StateCursor<S> {
  readonly appliedRef: Ref.Ref<number>
  readonly changes: PubSub.PubSub<FlowRecord>
  readonly reduce: (state: S, record: FlowRecord) => S
  readonly stateRef: Ref.Ref<S>
}

export interface ApplyResult<S> {
  readonly applied: number
  readonly appliedNow: boolean
  readonly state: S
}

export type Changes = Stream.Stream<FlowRecord>

export const changesStream = (changes: PubSub.PubSub<FlowRecord>): Changes => Stream.fromPubSub(changes)

const forkRecords = <Event, E, R>(
  records: Stream.Stream<FlowRecord, E, R>,
  events: Queue.Queue<Event>,
  toEvent: (record: FlowRecord) => Event
): Effect.Effect<void, never, Scope.Scope | R> =>
  records.pipe(
    Stream.runForEach((record) => Queue.offer(events, toEvent(record))),
    Effect.forkScoped,
    Effect.asVoid
  )

const forkRecordEvents = <Event, E, R>(
  records: Stream.Stream<FlowRecord, E, R>,
  events: Queue.Queue<Event>
): Effect.Effect<void, never, Scope.Scope | R> =>
  forkRecords(records, events, (record) => ({ _tag: "Record" as const, record }) as Event)

export const forkEvents = <Event, E, R>(
  events: Queue.Queue<Event>,
  handle: (event: Event) => Effect.Effect<void, E, R>
): Effect.Effect<void, never, Scope.Scope | R> =>
  Queue.take(events).pipe(
    Effect.flatMap(handle),
    Effect.forever,
    Effect.forkScoped,
    Effect.asVoid
  )

export const startTail = <Event>(
  stream: S2.StreamApi,
  fromSeqNum: number,
  applyCaughtUpRecord: (record: FlowRecord) => Effect.Effect<void>,
  events: Queue.Queue<Event>
): Effect.Effect<void, FlowError, Scope.Scope> =>
  Effect.gen(function*() {
    const cursor = yield* Tail.catchUp(stream, fromSeqNum, applyCaughtUpRecord)
    yield* forkRecordEvents(Tail.follow(stream, cursor), events)
  })

export const completeProjectedRead = Effect.fn("completeProjectedRead")(function*<S>(
  stateRef: Ref.Ref<S>,
  appliedRef: Ref.Ref<number>,
  reply: Deferred.Deferred<unknown>,
  project: Project<S>
) {
  const state = yield* Ref.get(stateRef)
  const applied = yield* Ref.get(appliedRef)
  yield* Deferred.succeed(reply, project(state, applied))
})

export const applyRecord = <S>(
  record: FlowRecord,
  cursor: StateCursor<S>,
  afterApply?: (record: FlowRecord) => Effect.Effect<void>
): Effect.Effect<ApplyResult<S>> =>
  Effect.gen(function*() {
    const currentApplied = yield* Ref.get(cursor.appliedRef)
    const currentState = yield* Ref.get(cursor.stateRef)
    if (record.seqNum !== currentApplied) {
      return {
        applied: currentApplied,
        appliedNow: false,
        state: currentState
      }
    }

    const state = cursor.reduce(currentState, record)
    const applied = record.seqNum + 1
    yield* Ref.set(cursor.stateRef, state)
    yield* Ref.set(cursor.appliedRef, applied)
    if (afterApply !== undefined) {
      yield* afterApply(record)
    }
    yield* PubSub.publish(cursor.changes, record)
    return {
      applied,
      appliedNow: true,
      state
    }
  })
