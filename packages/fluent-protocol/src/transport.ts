import { Context, Data, Schema } from "effect"
import type { Cause, Queue } from "effect"
import type { Effect, Scope } from "effect"
import { Offset } from "@firegrid/fluent-stream-log"
import type { Append, Close, Create, Delete, Head, Read, ReadLive, Request } from "./request.ts"
import type {
  AppendResponse,
  CloseResponse,
  CreateResponse,
  DeleteResponse,
  HeadResponse,
  ReadResponse,
} from "./response.ts"
import { WireRecord } from "./response.ts"

export type ResponseOf<R extends Request> =
  R extends Append ? AppendResponse :
  R extends Close ? CloseResponse :
  R extends Create ? CreateResponse :
  R extends Read ? ReadResponse :
  R extends Head ? HeadResponse :
  R extends Delete ? DeleteResponse :
  never

export class RecordBatch extends Schema.TaggedClass<RecordBatch>("RecordBatch")("RecordBatch", {
  records: Schema.Array(WireRecord),
}) {}

export class Control extends Schema.TaggedClass<Control>("Control")("Control", {
  nextOffset: Offset,
  upToDate: Schema.Boolean,
  closed: Schema.Boolean,
  cursor: Schema.optional(Schema.String),
}) {}

export const ReadEvent = Schema.Union([RecordBatch, Control])
export type ReadEvent = typeof ReadEvent.Type

export class TransportError extends Data.TaggedError("TransportError")<
  Readonly<{
    readonly message: string
    readonly cause?: unknown
  }>
> {}

// Effect v4 beta in this repo does not expose effect/Mailbox yet. This queue is
// the local stand-in for the same scoped live-read session semantics.
export type ReadEventQueue = Queue.Dequeue<ReadEvent, TransportError | Cause.Done<void>>

export interface DurableTransportService {
  readonly call: <R extends Request>(request: R) => Effect.Effect<ResponseOf<R>, TransportError>
  readonly stream: (
    request: ReadLive,
  ) => Effect.Effect<ReadEventQueue, TransportError, Scope.Scope>
}

export class DurableTransport extends Context.Service<DurableTransport, DurableTransportService>()(
  "@firegrid/fluent-protocol/DurableTransport",
) {}
