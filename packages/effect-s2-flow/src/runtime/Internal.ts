import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

import type { FlowRecord } from "./StreamStore.ts"

export type Project<S> = (state: S, applied: number) => unknown

export interface RecordEvent<A> {
  readonly _tag: "Record"
  readonly record: FlowRecord<A>
}

export interface StateCursor<S, A> {
  readonly appliedRef: Ref.Ref<number>
  readonly changes: PubSub.PubSub<AppliedChange<S, A>>
  readonly reduce: (state: S, record: FlowRecord<A>) => S
  readonly stateRef: Ref.Ref<S>
}

export interface AppliedChange<S, A> {
  readonly record: FlowRecord<A>
  readonly state: S
  readonly applied: number
}

export interface ApplyResult<S> {
  readonly applied: number
  readonly appliedNow: boolean
  readonly state: S
}

export type Changes<A> = Stream.Stream<FlowRecord<A>>

export const appliedChangesStream = <S, A>(
  changes: PubSub.PubSub<AppliedChange<S, A>>
): Stream.Stream<AppliedChange<S, A>> => Stream.fromPubSub(changes)

export const changesStream = <S, A>(changes: PubSub.PubSub<AppliedChange<S, A>>): Changes<A> =>
  appliedChangesStream(changes).pipe(Stream.map((change) => change.record))

const forkRecords = <A, Event, E, R>(
  records: Stream.Stream<FlowRecord<A>, E, R>,
  events: Queue.Queue<Event>,
  toEvent: (record: FlowRecord<A>) => Event
): Effect.Effect<void, never, Scope.Scope | R> =>
  records.pipe(
    Stream.runForEach((record) => Queue.offer(events, toEvent(record))),
    Effect.forkScoped,
    Effect.asVoid
  )

export const forkRecordEvents = <A, Event, E, R>(
  records: Stream.Stream<FlowRecord<A>, E, R>,
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

export const applyRecord = <S, A>(
  record: FlowRecord<A>,
  cursor: StateCursor<S, A>,
  afterApply?: (record: FlowRecord<A>) => Effect.Effect<void>
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
    yield* PubSub.publish(cursor.changes, { applied, record, state })
    return {
      applied,
      appliedNow: true,
      state
    }
  })
