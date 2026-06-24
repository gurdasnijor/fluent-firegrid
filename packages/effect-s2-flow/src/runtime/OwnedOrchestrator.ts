import * as Deferred from "effect/Deferred"
import type { Input as DurationInput } from "effect/Duration"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"

import { type FlowError, flowError } from "./FlowError.ts"
import * as Internal from "./Internal.ts"
import type { AppendAck, FlowRecord, StreamStore } from "./StreamStore.ts"

export interface OwnedOrchestratorConfig {
  readonly commandCapacity: number
  readonly changesCapacity: number
  readonly writeCapacity: number
  readonly writeTimeout: DurationInput
}

const defaultConfig: OwnedOrchestratorConfig = {
  commandCapacity: 64,
  changesCapacity: 256,
  writeCapacity: 64,
  writeTimeout: "1 second"
}

type Event<S, A> =
  | Internal.RecordEvent<A>
  | {
    readonly _tag: "WriteAck"
    readonly ack: AppendAck<A>
    readonly reply: Deferred.Deferred<AppendAck<A>, FlowError>
  }
  | { readonly _tag: "Read"; readonly project: Internal.Project<S>; readonly reply: Deferred.Deferred<unknown> }

interface PendingOwn<A> {
  readonly ack: AppendAck<A>
  readonly reply: Deferred.Deferred<AppendAck<A>, FlowError>
}

type OwnedContext<S, A> = Internal.StateCursor<S, A> & {
  readonly appliedOwnRef: Ref.Ref<ReadonlySet<number>>
  readonly ownerId: string
  readonly pendingOwnRef: Ref.Ref<ReadonlyMap<number, PendingOwn<A>>>
}

export interface OwnedOrchestrator<S, A> {
  readonly write: (values: ReadonlyArray<A>) => Effect.Effect<AppendAck<A>, FlowError>
  readonly read: <B>(project: (state: S, applied: number) => B) => Effect.Effect<B>
  readonly applied: Effect.Effect<number>
  readonly changes: Internal.Changes<A>
}

export interface OwnedOrchestratorOptions<S, A> {
  readonly store: StreamStore<A>
  readonly ownerId: string
  readonly fencingToken: string
  readonly initial: S
  readonly reduce: (state: S, record: FlowRecord<A>) => S
  readonly fromSeqNum?: number
  readonly config?: Partial<OwnedOrchestratorConfig>
}

export const make = Effect.fn("OwnedOrchestrator.make")(function*<S, A>(options: OwnedOrchestratorOptions<S, A>) {
  const config = { ...defaultConfig, ...options.config }
  const events = yield* Queue.bounded<Event<S, A>>(config.commandCapacity)
  const writes = yield* Queue.bounded<{
    readonly values: ReadonlyArray<A>
    readonly writeId: number
    readonly reply: Deferred.Deferred<AppendAck<A>, FlowError>
  }>(config.writeCapacity)
  const changes = yield* PubSub.dropping<FlowRecord<A>>(config.changesCapacity)
  const appliedRef = yield* Ref.make(options.fromSeqNum ?? 0)
  const stateRef = yield* Ref.make(options.initial)
  const pendingOwnRef = yield* Ref.make<ReadonlyMap<number, PendingOwn<A>>>(new Map())
  const appliedOwnRef = yield* Ref.make<ReadonlySet<number>>(new Set())
  const writeIdRef = yield* Ref.make(0)

  yield* Internal.forkRecordEvents(options.store.readSession(options.fromSeqNum ?? 0), events)

  yield* Queue.take(writes).pipe(
    Effect.flatMap(({ values, writeId, reply }) =>
      options.store.append({
        values,
        ownerId: options.ownerId,
        writeId,
        fencingToken: options.fencingToken
      }).pipe(
        Effect.matchEffect({
          onFailure: (error) => Deferred.fail(reply, error),
          onSuccess: (ack) => Queue.offer(events, { _tag: "WriteAck" as const, ack, reply })
        })
      )
    ),
    Effect.forever,
    Effect.forkScoped
  )

  yield* Internal.forkEvents(events, (event) =>
    handleEvent(event, {
      appliedOwnRef,
      appliedRef,
      changes,
      ownerId: options.ownerId,
      pendingOwnRef,
      reduce: options.reduce,
      stateRef
    }))

  return {
    write: (values) =>
      Effect.gen(function*() {
        const reply = yield* Deferred.make<AppendAck<A>, FlowError>()
        const writeId = yield* Ref.updateAndGet(writeIdRef, (id) => id + 1)
        yield* Queue.offer(writes, { values, writeId, reply })
        return yield* Deferred.await(reply).pipe(
          Effect.timeoutOption(config.writeTimeout),
          Effect.flatMap((option) =>
            option._tag === "Some"
              ? Effect.succeed(option.value)
              : Effect.fail(flowError("write-timeout", "owned write was not applied before the deadline"))
          )
        )
      }),
    read: <B>(project: (state: S, applied: number) => B) =>
      Effect.gen(function*() {
        const reply = yield* Deferred.make<unknown>()
        yield* Queue.offer(events, { _tag: "Read", project, reply })
        const value = yield* Deferred.await(reply)
        return value as B
      }),
    applied: Ref.get(appliedRef),
    changes: Internal.changesStream(changes)
  } satisfies OwnedOrchestrator<S, A>
})

const handleEvent = <S, A>(
  event: Event<S, A>,
  ctx: OwnedContext<S, A>
) =>
  Effect.gen(function*() {
    switch (event._tag) {
      case "Record": {
        if (event.record.ownerId === ctx.ownerId) {
          const appliedOwn = yield* Ref.get(ctx.appliedOwnRef)
          if (appliedOwn.has(event.record.seqNum)) {
            return
          }
        }
        const result = yield* applyOwnedRecord(event.record, ctx)
        if (result.appliedNow) {
          yield* completePendingAt(event.record.seqNum, ctx)
        }
        yield* drainPendingOwn(ctx)
        break
      }
      case "WriteAck": {
        const appliedOwn = yield* Ref.get(ctx.appliedOwnRef)
        const pending = yield* Ref.get(ctx.pendingOwnRef)
        const unapplied = event.ack.records.filter((record) => !appliedOwn.has(record.seqNum))
        if (unapplied.length === 0) {
          yield* Deferred.succeed(event.reply, event.ack)
          return
        }
        const nextPending = new Map([
          ...pending,
          ...unapplied.map((record) => [record.seqNum, { ack: event.ack, reply: event.reply }] as const)
        ])
        yield* Ref.set(ctx.pendingOwnRef, nextPending)
        yield* drainPendingOwn(ctx)
        break
      }
      case "Read": {
        yield* Internal.completeProjectedRead(ctx.stateRef, ctx.appliedRef, event.reply, event.project)
        break
      }
    }
  })

const drainPendingOwn = <S, A>(ctx: OwnedContext<S, A>) =>
  Effect.gen(function*() {
    let applied = yield* Ref.get(ctx.appliedRef)
    let pending = yield* Ref.get(ctx.pendingOwnRef)
    let next = pending.get(applied)

    while (next !== undefined) {
      const record = next.ack.records.find((candidate) => candidate.seqNum === applied)
      if (record === undefined) {
        return
      }
      yield* applyOwnedRecord(record, ctx)
      const afterApply = new Map(yield* Ref.get(ctx.pendingOwnRef))
      afterApply.delete(applied)
      yield* Ref.set(ctx.pendingOwnRef, afterApply)
      yield* completeAckIfApplied(next.ack, next.reply, ctx.appliedOwnRef)
      applied = yield* Ref.get(ctx.appliedRef)
      pending = yield* Ref.get(ctx.pendingOwnRef)
      next = pending.get(applied)
    }
  })

const completePendingAt = <A>(
  seqNum: number,
  ctx: {
    readonly appliedOwnRef: Ref.Ref<ReadonlySet<number>>
    readonly pendingOwnRef: Ref.Ref<ReadonlyMap<number, PendingOwn<A>>>
  }
) =>
  Effect.gen(function*() {
    const pending = yield* Ref.get(ctx.pendingOwnRef)
    const own = pending.get(seqNum)
    if (own !== undefined) {
      const nextPending = new Map(pending)
      nextPending.delete(seqNum)
      yield* Ref.set(ctx.pendingOwnRef, nextPending)
      yield* completeAckIfApplied(own.ack, own.reply, ctx.appliedOwnRef)
    }
  })

const applyOwnedRecord = <S, A>(
  record: FlowRecord<A>,
  ctx: OwnedContext<S, A>
) =>
  Internal.applyRecord(
    record,
    ctx,
    record.ownerId === ctx.ownerId
      ? (appliedRecord) => Ref.update(ctx.appliedOwnRef, (set) => new Set(set).add(appliedRecord.seqNum))
      : undefined
  )

const completeAckIfApplied = <A>(
  ack: AppendAck<A>,
  reply: Deferred.Deferred<AppendAck<A>, FlowError>,
  appliedOwnRef: Ref.Ref<ReadonlySet<number>>
) =>
  Effect.gen(function*() {
    const appliedOwn = yield* Ref.get(appliedOwnRef)
    if (ack.records.every((record) => appliedOwn.has(record.seqNum))) {
      yield* Deferred.succeed(reply, ack)
    }
  })
