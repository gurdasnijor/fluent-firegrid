import type { Effect, Scope, Stream } from "effect"
import type {
  Offset,
  AppendResult,
  ReadOffset,
  StreamMetadata,
  StreamPath,
  StreamRecord,
} from "@firegrid/fluent-stream-log"
import type { JsonValue, StreamBody } from "./content.ts"

export interface ProducerFence {
  readonly producerId: string
  readonly epoch: number
  readonly seq: number
}

export interface CreateStreamCommand {
  readonly path: StreamPath
  readonly contentType: string
  readonly body?: StreamBody
  readonly closed?: boolean
}

export interface ForkStreamCommand {
  readonly path: StreamPath
  readonly source: StreamPath
  readonly atOffset?: Offset
  readonly contentType?: string
}

export interface AppendStreamCommand {
  readonly path: StreamPath
  readonly contentType: string
  readonly body?: StreamBody
  readonly seq?: string
  readonly close?: boolean
  readonly expectedTailOffset?: Offset
  readonly producer?: ProducerFence
}

export interface ReadStreamCommand {
  readonly path: StreamPath
  readonly offset?: ReadOffset
  readonly limit?: number
}

export interface FollowStreamCommand {
  readonly path: StreamPath
  readonly offset?: ReadOffset
}

export type StreamProblem =
  | {
      readonly _tag: "BadRequest"
      readonly code: "BAD_REQUEST"
      readonly message: string
    }
  | {
      readonly _tag: "Conflict"
      readonly code: "CONFLICT"
      readonly message: string
    }
  | {
      readonly _tag: "NotFound"
      readonly code: "NOT_FOUND"
      readonly message: string
    }
  | {
      readonly _tag: "Gone"
      readonly code: "GONE"
      readonly message: string
    }
  | {
      readonly _tag: "PayloadTooLarge"
      readonly code: "PAYLOAD_TOO_LARGE"
      readonly message: string
    }

export type CreateStreamOutcome =
  | {
      readonly _tag: "Created"
      readonly metadata: StreamMetadata
    }
  | {
      readonly _tag: "AlreadyExists"
      readonly metadata: StreamMetadata
    }
  | StreamProblem

export type AppendStreamOutcome =
  | AppendResult
  | {
      readonly _tag: "WriteToClosed"
      readonly finalOffset: Offset
    }
  | {
      readonly _tag: "ContentMismatch"
      readonly expected: string
      readonly actual: string
    }
  | {
      readonly _tag: "OffsetConflict"
      readonly expectedTailOffset: Offset
      readonly actualTailOffset: Offset
    }
  | StreamProblem

export type HeadStreamOutcome =
  | {
      readonly _tag: "Head"
      readonly metadata: StreamMetadata
    }
  | StreamProblem

export type ReadStreamOutcome =
  | {
      readonly _tag: "Read"
      readonly records: readonly StreamRecord[]
      readonly nextOffset: Offset
      readonly upToDate: boolean
      readonly closed: boolean
    }
  | {
      readonly _tag: "ReadJson"
      readonly items: readonly JsonValue[]
      readonly nextOffset: Offset
      readonly upToDate: boolean
      readonly closed: boolean
    }
  | StreamProblem

export type DeleteStreamOutcome =
  | {
      readonly _tag: "Deleted"
      readonly path: StreamPath
    }
  | StreamProblem

export type StreamEvent =
  | {
      readonly _tag: "Records"
      readonly records: readonly StreamRecord[]
    }
  | {
      readonly _tag: "CaughtUp"
      readonly offset: Offset
    }
  | {
      readonly _tag: "Closed"
      readonly finalOffset: Offset
    }

export interface DurableStreamsChannel {
  readonly create: (command: CreateStreamCommand) => Effect.Effect<CreateStreamOutcome>
  readonly append: (command: AppendStreamCommand) => Effect.Effect<AppendStreamOutcome>
  readonly head: (path: StreamPath) => Effect.Effect<HeadStreamOutcome>
  readonly read: (command: ReadStreamCommand) => Effect.Effect<ReadStreamOutcome>
  readonly readJson: (command: ReadStreamCommand) => Effect.Effect<ReadStreamOutcome>
  readonly follow: (
    command: FollowStreamCommand,
  ) => Effect.Effect<Stream.Stream<StreamEvent, StreamProblem>, StreamProblem, Scope.Scope>
  readonly delete: (path: StreamPath) => Effect.Effect<DeleteStreamOutcome>
}

export interface DurableStreamsServer extends DurableStreamsChannel {
  readonly fork: (command: ForkStreamCommand) => Effect.Effect<CreateStreamOutcome>
}
