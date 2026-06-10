import { Context, Schema } from "effect"
import type { Effect, Scope } from "effect"
import type * as Mailbox from "effect/Mailbox"
import { TransportError } from "@firegrid/fluent-transport"
import { Offset } from "@firegrid/fluent-store"
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

export const ReadEvent = Schema.Union(RecordBatch, Control)
export type ReadEvent = typeof ReadEvent.Type

export interface DurableTransportService {
  readonly call: <R extends Request>(request: R) => Effect.Effect<ResponseOf<R>, TransportError>
  readonly stream: (
    request: ReadLive,
  ) => Effect.Effect<Mailbox.ReadonlyMailbox<ReadEvent, TransportError>, TransportError, Scope.Scope>
}

export class DurableTransport extends Context.Tag("@firegrid/fluent-protocol/DurableTransport")<
  DurableTransport,
  DurableTransportService
>() {}

export { TransportError }
