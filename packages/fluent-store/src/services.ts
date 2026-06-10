import { Context, type Effect, type Scope, type Sink, type Stream } from "effect"
import type { DurableStreamLogError } from "./errors.ts"
import type {
  AppendResult,
  AppendStream,
  CreateStream,
  CreateStreamResult,
  DeleteStreamResult,
  ReadPosition,
  StreamMetadata,
  StreamRecord,
  TailAdvanced,
} from "./streamTypes.ts"
import type { StreamPath } from "./domainTypes.ts"

export interface DurableStreamLog {
  readonly create: (
    request: CreateStream,
  ) => Effect.Effect<CreateStreamResult, DurableStreamLogError>

  readonly append: (
    request: AppendStream,
  ) => Sink.Sink<AppendResult, Uint8Array, Uint8Array, DurableStreamLogError>

  readonly read: (
    from: ReadPosition,
  ) => Effect.Effect<Stream.Stream<StreamRecord, DurableStreamLogError>, DurableStreamLogError>

  readonly subscribe: (
    from: ReadPosition,
  ) => Effect.Effect<Stream.Stream<StreamRecord, DurableStreamLogError>, DurableStreamLogError, Scope.Scope>

  readonly subscribeAll: () => Effect.Effect<
    Stream.Stream<TailAdvanced, DurableStreamLogError>,
    DurableStreamLogError,
    Scope.Scope
  >

  readonly head: (path: StreamPath) => Effect.Effect<StreamMetadata, DurableStreamLogError>

  readonly delete: (path: StreamPath) => Effect.Effect<DeleteStreamResult, DurableStreamLogError>
}

export class DurableStreamLogTag extends Context.Service<DurableStreamLogTag, DurableStreamLog>()(
  "@firegrid/fluent-store/DurableStreamLog",
) {}
