import * as Deferred from "effect/Deferred"
import type { Input as DurationInput } from "effect/Duration"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import { AppendInput } from "effect-s2"
import * as S2 from "effect-s2"

import { FlowError, flowError } from "./FlowError.ts"
import * as Internal from "./Internal.ts"
import type { FlowRecord, OwnedAppendAck, StringFlowAppendRecord } from "./Record.ts"
import * as RuntimeRecord from "./Record.ts"
import * as Tail from "./Tail.ts"

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

type Event<S> =
  | Internal.RecordEvent
  | {
    readonly _tag: "WriteAck"
    readonly ack: OwnedAppendAck
    readonly reply: Deferred.Deferred<OwnedAppendAck, FlowError>
  }
  | { readonly _tag: "Read"; readonly project: Internal.Project<S>; readonly reply: Deferred.Deferred<unknown> }

interface PendingOwn {
  readonly ack: OwnedAppendAck
  readonly reply: Deferred.Deferred<OwnedAppendAck, FlowError>
}

type OwnedContext<S> = Internal.StateCursor<S> & {
  readonly appliedOwnRef: Ref.Ref<ReadonlySet<number>>
  readonly ownerId: string
  readonly pendingOwnRef: Ref.Ref<ReadonlyMap<number, PendingOwn>>
}

export interface OwnedOrchestrator<S> {
  readonly write: (records: ReadonlyArray<StringFlowAppendRecord>) => Effect.Effect<OwnedAppendAck, FlowError>
  readonly read: <B>(project: (state: S, applied: number) => B) => Effect.Effect<B>
  readonly applied: Effect.Effect<number>
  readonly changes: Internal.Changes
}

export interface OwnedOrchestratorOptions<S> {
  readonly basin: string
  readonly stream: string
  readonly streamOptions?: S2.StreamOptions
  readonly ownerId: string
  readonly fencingToken: string
  readonly initial: S
  readonly reduce: (state: S, record: FlowRecord) => S
  readonly fromSeqNum?: number
  readonly config?: Partial<OwnedOrchestratorConfig>
}

export const make = Effect.fn("OwnedOrchestrator.make")(function*<S>(options: OwnedOrchestratorOptions<S>) {
  const config = { ...defaultConfig, ...options.config }
  const events = yield* Queue.bounded<Event<S>>(config.commandCapacity)
  const writes = yield* Queue.bounded<{
    readonly records: ReadonlyArray<StringFlowAppendRecord>
    readonly writeId: number
    readonly reply: Deferred.Deferred<OwnedAppendAck, FlowError>
  }>(config.writeCapacity)
  const changes = yield* PubSub.dropping<FlowRecord>(config.changesCapacity)
  const appliedRef = yield* Ref.make(options.fromSeqNum ?? 0)
  const stateRef = yield* Ref.make(options.initial)
  const pendingOwnRef = yield* Ref.make<ReadonlyMap<number, PendingOwn>>(new Map())
  const appliedOwnRef = yield* Ref.make<ReadonlySet<number>>(new Set())
  const writeIdRef = yield* Ref.make(0)
  const s2 = yield* S2.stream(options.basin, options.stream, options.streamOptions).pipe(
    Effect.mapError((cause) => flowError("read-session", "failed to open S2 stream", cause))
  )
  const appendSession = yield* s2.appendSession().pipe(
    Effect.mapError((cause) => flowError("write", "failed to open S2 append session", cause))
  )

  const applyCaughtUpRecord = (record: FlowRecord) =>
    applyOwnedRecord(record, {
      appliedOwnRef,
      appliedRef,
      changes,
      ownerId: options.ownerId,
      pendingOwnRef,
      reduce: options.reduce,
      stateRef
    }).pipe(Effect.asVoid)
  const cursor = yield* Tail.catchUp(s2, options.fromSeqNum ?? 0, applyCaughtUpRecord)

  yield* Internal.forkRecordEvents(Tail.follow(s2, cursor), events)

  yield* Queue.take(writes).pipe(
    Effect.flatMap(({ records, writeId, reply }) => {
      const prepare = Effect.try({
        try: () => {
          const ownedRecords = records.map((record) => RuntimeRecord.ownedRecord(record, options.ownerId, writeId))
          return {
            input: AppendInput.create(ownedRecords, { fencingToken: options.fencingToken }),
            ownedRecords
          }
        },
        catch: (cause) => flowError("write", "invalid owned append input", cause)
      })
      return prepare.pipe(
        Effect.flatMap(({ input, ownedRecords }) =>
          appendSession.submit(input).pipe(
            Effect.flatMap((ticket) => ticket.ack),
            Effect.flatMap((ack) =>
              Effect.try({
                try: () =>
                  ({
                    startSeqNum: ack.start.seqNum,
                    endSeqNum: ack.end.seqNum,
                    records: ownedRecords.map((record, index) =>
                      RuntimeRecord.appendRecordToFlowRecord(ack.start.seqNum + index, record)
                    )
                  }) satisfies OwnedAppendAck,
                catch: (cause) => flowError("write", "failed to project S2 append ack", cause)
              })
            )
          )
        ),
        Effect.matchEffect({
          onFailure: (error) =>
            Deferred.fail(reply, error instanceof FlowError ? error : flowError("write", "S2 append failed", error)),
          onSuccess: (ack) => Queue.offer(events, { _tag: "WriteAck" as const, ack, reply })
        })
      )
    }),
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
    write: (records) =>
      Effect.gen(function*() {
        const reply = yield* Deferred.make<OwnedAppendAck, FlowError>()
        const writeId = yield* Ref.updateAndGet(writeIdRef, (id) => id + 1)
        yield* Queue.offer(writes, { records, writeId, reply })
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
  } satisfies OwnedOrchestrator<S>
})

const handleEvent = <S>(
  event: Event<S>,
  ctx: OwnedContext<S>
) =>
  Effect.gen(function*() {
    switch (event._tag) {
      case "Record": {
        if (RuntimeRecord.ownerId(event.record) === ctx.ownerId) {
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

const drainPendingOwn = <S>(ctx: OwnedContext<S>) =>
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

const completePendingAt = (
  seqNum: number,
  ctx: {
    readonly appliedOwnRef: Ref.Ref<ReadonlySet<number>>
    readonly pendingOwnRef: Ref.Ref<ReadonlyMap<number, PendingOwn>>
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

const applyOwnedRecord = <S>(
  record: FlowRecord,
  ctx: OwnedContext<S>
) =>
  Internal.applyRecord(
    record,
    ctx,
    RuntimeRecord.ownerId(record) === ctx.ownerId
      ? (appliedRecord) => Ref.update(ctx.appliedOwnRef, (set) => new Set(set).add(appliedRecord.seqNum))
      : undefined
  )

const completeAckIfApplied = (
  ack: OwnedAppendAck,
  reply: Deferred.Deferred<OwnedAppendAck, FlowError>,
  appliedOwnRef: Ref.Ref<ReadonlySet<number>>
) =>
  Effect.gen(function*() {
    const appliedOwn = yield* Ref.get(appliedOwnRef)
    if (ack.records.every((record) => appliedOwn.has(record.seqNum))) {
      yield* Deferred.succeed(reply, ack)
    }
  })
