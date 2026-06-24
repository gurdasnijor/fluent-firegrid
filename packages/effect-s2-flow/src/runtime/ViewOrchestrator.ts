import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as Stream from "effect/Stream"

import { type FlowError, flowError } from "./FlowError.ts"
import * as Internal from "./Internal.ts"
import type { FlowRecord, StreamStore } from "./StreamStore.ts"

export interface OrchestratorConfig {
  readonly commandCapacity: number
  readonly changesCapacity: number
  readonly readTimeout: Duration.Input
}

const defaultConfig: OrchestratorConfig = {
  commandCapacity: 64,
  changesCapacity: 256,
  readTimeout: "1 second"
}

type Event<S, A> =
  | Internal.RecordEvent<A>
  | { readonly _tag: "ReadEventual"; readonly project: Internal.Project<S>; readonly reply: Deferred.Deferred<unknown> }
  | {
    readonly _tag: "ReadStrong"
    readonly atTail: number
    readonly project: Internal.Project<S>
    readonly reply: Deferred.Deferred<unknown>
  }

interface PendingRead<S> {
  readonly atTail: number
  readonly project: Internal.Project<S>
  readonly reply: Deferred.Deferred<unknown>
}

type ViewContext<S, A> = Internal.StateCursor<S, A> & {
  readonly pendingRef: Ref.Ref<ReadonlyArray<PendingRead<S>>>
}

export interface ViewOrchestrator<S, A> {
  readonly read: <B>(project: (state: S, applied: number) => B) => Effect.Effect<B>
  readonly readStrong: <B>(project: (state: S, applied: number) => B) => Effect.Effect<B, FlowError>
  readonly applied: Effect.Effect<number>
  readonly changes: Stream.Stream<FlowRecord<A>>
  readonly appliedChanges: Stream.Stream<Internal.AppliedChange<S, A>>
}

export interface ViewOrchestratorOptions<S, A> {
  readonly store: StreamStore<A>
  readonly initial: S
  readonly reduce: (state: S, record: FlowRecord<A>) => S
  readonly fromSeqNum?: number
  readonly config?: Partial<OrchestratorConfig>
}

export const make = Effect.fn("ViewOrchestrator.make")(function*<S, A>(options: ViewOrchestratorOptions<S, A>) {
  const config = { ...defaultConfig, ...options.config }
  const events = yield* Queue.bounded<Event<S, A>>(config.commandCapacity)
  const changes = yield* PubSub.dropping<Internal.AppliedChange<S, A>>(config.changesCapacity)
  const appliedRef = yield* Ref.make(options.fromSeqNum ?? 0)
  const stateRef = yield* Ref.make(options.initial)
  const pendingRef = yield* Ref.make<ReadonlyArray<PendingRead<S>>>([])

  yield* Internal.forkRecordEvents(options.store.readSession(options.fromSeqNum ?? 0), events)

  yield* Internal.forkEvents(events, (event) =>
    handleEvent(event, {
      appliedRef,
      changes,
      pendingRef,
      reduce: options.reduce,
      stateRef
    }))

  return {
    read: <B>(project: (state: S, applied: number) => B) =>
      Effect.gen(function*() {
        const reply = yield* Deferred.make<unknown>()
        yield* Queue.offer(events, { _tag: "ReadEventual", project, reply })
        const value = yield* Deferred.await(reply)
        return value as B
      }),
    readStrong: <B>(project: (state: S, applied: number) => B) =>
      Effect.gen(function*() {
        const atTail = yield* options.store.checkTail
        const reply = yield* Deferred.make<unknown>()
        yield* Queue.offer(events, { _tag: "ReadStrong", atTail, project, reply })
        const value = yield* Deferred.await(reply).pipe(
          Effect.timeoutOption(config.readTimeout),
          Effect.flatMap((option) =>
            option._tag === "Some"
              ? Effect.succeed(option.value)
              : Effect.fail(flowError("read-timeout", `strong read did not reach tail ${atTail}`))
          )
        )
        return value as B
      }),
    applied: Ref.get(appliedRef),
    changes: Internal.changesStream(changes),
    appliedChanges: Internal.appliedChangesStream(changes)
  } satisfies ViewOrchestrator<S, A>
})

const handleEvent = <S, A>(
  event: Event<S, A>,
  ctx: ViewContext<S, A>
) =>
  Effect.gen(function*() {
    switch (event._tag) {
      case "Record": {
        const result = yield* Internal.applyRecord(event.record, ctx)
        if (result.appliedNow) {
          yield* drainPending(ctx.pendingRef, result.state, result.applied)
        }
        break
      }
      case "ReadEventual": {
        yield* Internal.completeProjectedRead(ctx.stateRef, ctx.appliedRef, event.reply, event.project)
        break
      }
      case "ReadStrong": {
        const applied = yield* Ref.get(ctx.appliedRef)
        if (event.atTail <= applied) {
          yield* Internal.completeProjectedRead(ctx.stateRef, ctx.appliedRef, event.reply, event.project)
        } else {
          yield* Ref.update(ctx.pendingRef, (pending) => [...pending, event])
        }
        break
      }
    }
  })

const drainPending = <S>(
  pendingRef: Ref.Ref<ReadonlyArray<PendingRead<S>>>,
  state: S,
  applied: number
) =>
  Effect.gen(function*() {
    const pending = yield* Ref.get(pendingRef)
    const ready = pending.filter((read) => read.atTail <= applied)
    const waiting = pending.filter((read) => read.atTail > applied)
    yield* Ref.set(pendingRef, waiting)
    yield* Effect.forEach(ready, (read) => Deferred.succeed(read.reply, read.project(state, applied)), {
      discard: true
    })
  })
