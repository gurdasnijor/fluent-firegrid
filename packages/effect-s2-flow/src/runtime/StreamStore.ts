import type * as Effect from "effect/Effect"
import type * as Stream from "effect/Stream"

import type { FlowError } from "./FlowError.ts"

export interface FlowRecord<A> {
  readonly seqNum: number
  readonly value: A
  readonly ownerId?: string
  readonly writeId?: number
}

export interface AppendBatch<A> {
  readonly values: ReadonlyArray<A>
  readonly ownerId?: string
  readonly writeId?: number
  readonly fencingToken?: string
}

export interface AppendAck<A> {
  readonly startSeqNum: number
  readonly endSeqNum: number
  readonly records: ReadonlyArray<FlowRecord<A>>
}

export interface StreamStore<A> {
  readonly append: (batch: AppendBatch<A>) => Effect.Effect<AppendAck<A>, FlowError>
  readonly checkTail: Effect.Effect<number, FlowError>
  readonly readSession: (fromSeqNum: number) => Stream.Stream<FlowRecord<A>, FlowError>
}
