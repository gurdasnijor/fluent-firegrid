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
  readonly expectedTailOffset?: Offset
  readonly close?: boolean
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

export interface ReadPosition {
  readonly path: StreamPath
  readonly offset: ReadOffset
  readonly subOffset?: number
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
export type TailAdvancedStream = Stream.Stream<TailAdvanced, DurableStreamLogError>

export type StreamEffect<A> = Effect.Effect<A, DurableStreamLogError>
