import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"

import { type FlowError, flowError } from "../runtime/FlowError.ts"
import type * as Internal from "../runtime/Internal.ts"
import type { StreamStore } from "../runtime/StreamStore.ts"
import * as ViewOrchestrator from "../runtime/ViewOrchestrator.ts"
import type { EventStreamDef } from "../stream/EventStream.ts"
import type { EventRecord } from "../stream/Record.ts"

export interface Snapshot<K, V> {
  readonly entries: ReadonlyArray<readonly [K, V]>
  readonly fromSeqNum: number
}

export interface TableViewDef<K, A, V> {
  readonly name: string
  readonly source: EventStreamDef<K, A>
  readonly key: (record: EventRecord<K, A>) => K
  readonly reduce: (
    state: ReadonlyMap<K, V>,
    record: EventRecord<K, A>
  ) => ReadonlyMap<K, V>
  readonly snapshot?: Snapshot<K, V>
}

export interface TableView<K, V> {
  readonly get: (key: K) => Effect.Effect<Option.Option<V>>
  readonly getStrong: (key: K) => Effect.Effect<Option.Option<V>, FlowError>
  readonly refresh: Effect.Effect<number, FlowError>
  readonly entries: Effect.Effect<ReadonlyArray<readonly [K, V]>>
  readonly changes: Stream.Stream<readonly [K, V], FlowError>
  readonly snapshotAndChanges: Stream.Stream<readonly [K, V], FlowError>
}

export interface TableViewOptions<K, A, V> {
  readonly store: StreamStore<EventRecord<K, A>>
  readonly definition: TableViewDef<K, A, V>
  readonly config?: Partial<ViewOrchestrator.OrchestratorConfig>
}

export const define = <K, A, V>(definition: TableViewDef<K, A, V>): TableViewDef<K, A, V> => definition

const fromSnapshot = <K, V>(snapshot?: Snapshot<K, V>): ReadonlyMap<K, V> => new Map(snapshot?.entries ?? [])

const fromSeqNum = <K, V>(snapshot?: Snapshot<K, V>): number => snapshot?.fromSeqNum ?? 0

const reduceFlowRecord = <K, A, V>(
  reduce: TableViewDef<K, A, V>["reduce"]
) =>
(state: ReadonlyMap<K, V>, record: { readonly value: EventRecord<K, A> }): ReadonlyMap<K, V> =>
  reduce(state, record.value)

const getValue = <K, V>(state: ReadonlyMap<K, V>, key: K): Option.Option<V> => Option.fromUndefinedOr(state.get(key))

export const make = Effect.fn("TableView.make")(function*<K, A, V>(
  options: TableViewOptions<K, A, V>
) {
  const orchestrator = yield* ViewOrchestrator.make({
    store: options.store,
    initial: fromSnapshot(options.definition.snapshot),
    reduce: reduceFlowRecord(options.definition.reduce),
    fromSeqNum: fromSeqNum(options.definition.snapshot),
    ...(options.config === undefined ? {} : { config: options.config })
  })

  const entries = orchestrator.read((state) => [...state.entries()] as ReadonlyArray<readonly [K, V]>)
  const readChange = (change: Internal.AppliedChange<ReadonlyMap<K, V>, EventRecord<K, A>>) => {
    const key = options.definition.key(change.record.value)
    const value = change.state.get(key)
    return value === undefined
      ? Effect.fail(flowError("read-session", `table view ${options.definition.name} missing changed key`))
      : Effect.succeed([key, value] as const)
  }

  const changes = orchestrator.appliedChanges.pipe(Stream.mapEffect(readChange))

  const snapshotAndChanges = Stream.unwrap(
    Effect.gen(function*() {
      const queue = yield* Stream.toQueue(orchestrator.appliedChanges, {
        capacity: options.config?.changesCapacity ?? 256
      })
      const snapshot = yield* orchestrator.read((state, applied) => ({
        applied,
        entries: [...state.entries()] as ReadonlyArray<readonly [K, V]>
      }))
      const liveChanges = Stream.fromQueue(queue).pipe(
        Stream.filter((change) => change.applied > snapshot.applied),
        Stream.mapEffect(readChange)
      )
      return Stream.concat(Stream.fromIterable(snapshot.entries), liveChanges)
    })
  )

  return {
    get: (key) => orchestrator.read((state) => getValue(state, key)),
    getStrong: (key) => orchestrator.readStrong((state) => getValue(state, key)),
    refresh: orchestrator.readStrong((_, applied) => applied),
    entries,
    changes,
    snapshotAndChanges
  } satisfies TableView<K, V>
})
