import { Context, type Effect, type Scope, type Stream } from "effect"
import type { DurableStreamLogError } from "./errors.ts"
import type {
  AppendRequest,
  AppendResult,
  ChangeEvent,
  CreateStream,
  CreateStreamResult,
  DeleteStreamResult,
  ForkStream,
  ReadPosition,
  ReadWindow,
  StreamMetadata,
  TrimStream,
} from "./streamTypes.ts"
import type { StreamPath } from "./domainTypes.ts"

export interface DurableStreamLog {
  readonly create: (
    request: CreateStream,
  ) => Effect.Effect<CreateStreamResult, DurableStreamLogError>

  readonly append: (
    request: AppendRequest,
  ) => Effect.Effect<AppendResult, DurableStreamLogError>

  readonly read: (
    from: ReadPosition,
  ) => Effect.Effect<ReadWindow, DurableStreamLogError>

  readonly changes: (
    from: ReadPosition,
  ) => Effect.Effect<Stream.Stream<ChangeEvent, DurableStreamLogError>, DurableStreamLogError, Scope.Scope>

  readonly head: (path: StreamPath) => Effect.Effect<StreamMetadata, DurableStreamLogError>

  readonly fork: (request: ForkStream) => Effect.Effect<CreateStreamResult, DurableStreamLogError>

  readonly trim: (request: TrimStream) => Effect.Effect<void, DurableStreamLogError>

  readonly delete: (path: StreamPath) => Effect.Effect<DeleteStreamResult, DurableStreamLogError>
}

export class DurableStreamLogTag extends Context.Service<DurableStreamLogTag, DurableStreamLog>()(
  "@firegrid/fluent-stream-log/DurableStreamLog",
) {}
