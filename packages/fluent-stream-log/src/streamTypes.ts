import type { Effect, Stream } from "effect"
import type { Offset, ReadOffset, StreamPath } from "./domainTypes.ts"
import type { DurableStreamLogError } from "./errors.ts"

export interface CreateStream {
  readonly path: StreamPath
  readonly contentType: string
  readonly closed?: boolean
}

export type CreateStreamResult =
  | {
      readonly _tag: "Created"
      readonly metadata: StreamMetadata
    }
  | {
      readonly _tag: "AlreadyExists"
      readonly metadata: StreamMetadata
    }

export interface AppendStream {
  readonly path: StreamPath
  readonly contentType: string
  readonly seq?: string
  readonly expectedTailOffset?: Offset
  readonly close?: boolean
  readonly producer?: ProducerFence
}

export interface AppendRequest extends AppendStream {
  readonly messages: readonly Uint8Array[]
}

export interface ProducerFence {
  readonly producerId: string
  readonly epoch: number
  readonly seq: number
}

export type AppendResult =
  | {
      readonly _tag: "Appended"
      readonly metadata: StreamMetadata
      readonly records: readonly StreamRecord[]
      readonly tailAdvanced: TailAdvanced
    }
  | {
      readonly _tag: "Noop"
      readonly metadata: StreamMetadata
    }
  | {
      readonly _tag: "Duplicate"
      readonly metadata: StreamMetadata
      readonly highestSeq?: number
    }
  | {
      readonly _tag: "AlreadyClosed"
      readonly finalOffset: Offset
    }
  | {
      readonly _tag: "Fenced"
      readonly currentEpoch: number
    }
  | {
      readonly _tag: "SequenceGap"
      readonly expectedSeq: number
      readonly receivedSeq: number
    }

export interface ReadPosition {
  readonly path: StreamPath
  readonly offset: ReadOffset
  readonly subOffset?: number
  readonly limit?: number
}

export interface StreamRecord {
  readonly path: StreamPath
  readonly fromOffset: Offset
  readonly nextOffset: Offset
  readonly bytes: Uint8Array
  readonly contentType: string
  readonly closed: boolean
}

export interface StreamMetadata {
  readonly path: StreamPath
  readonly tailOffset: Offset
  readonly closed: boolean
  readonly contentType: string
}

export interface TailAdvanced {
  readonly path: StreamPath
  readonly tailOffset: Offset
  readonly closed: boolean
}

export type ChangeEvent =
  | {
      readonly _tag: "Chunk"
      readonly record: StreamRecord
    }
  | {
      readonly _tag: "CaughtUp"
      readonly path: StreamPath
      readonly offset: Offset
    }
  | {
      readonly _tag: "Closed"
      readonly path: StreamPath
      readonly finalOffset: Offset
    }

export interface ReadWindow {
  readonly records: readonly StreamRecord[]
  readonly nextOffset: Offset
  readonly upToDate: boolean
  readonly closed: boolean
}

export interface ForkStream {
  readonly path: StreamPath
  readonly source: StreamPath
  readonly atOffset?: Offset
  readonly contentType?: string
}

export interface TrimStream {
  readonly path: StreamPath
  readonly before: Offset
}

export type DeleteStreamResult =
  | {
      readonly _tag: "Deleted"
      readonly path: StreamPath
    }
  | {
      readonly _tag: "NotFound"
      readonly path: StreamPath
    }

export type StreamRecordStream = Stream.Stream<StreamRecord, DurableStreamLogError>
export type ChangeEventStream = Stream.Stream<ChangeEvent, DurableStreamLogError>
export type TailAdvancedStream = Stream.Stream<TailAdvanced, DurableStreamLogError>

export type StreamEffect<A> = Effect.Effect<A, DurableStreamLogError>
