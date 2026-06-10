import { Chunk, Effect, Stream } from "effect"
import {
  appendBytes,
  appendEmpty,
  ContentTypeMismatchError,
  InvalidOffsetError,
  OffsetConflictError,
  ProducerEpochRegressionError,
  ProducerSequenceGapError,
  StreamClosedError,
  StreamNotFoundError,
  type AppendResult,
  type AppendStream,
  type CreateStream,
  type DurableStreamLog,
  type DurableStreamLogError,
  type ReadPosition,
  type StreamMetadata,
  type StreamRecord,
} from "@firegrid/fluent-store"
import type * as Req from "./request.ts"
import * as Res from "./response.ts"
import type { ResponseOf } from "./transport.ts"

export const wireRecord = (record: StreamRecord): Res.WireRecord =>
  new Res.WireRecord({
    path: record.path,
    fromOffset: record.fromOffset,
    nextOffset: record.nextOffset,
    bytes: record.bytes,
    contentType: record.contentType,
    closed: record.closed,
  })

const appendRequest = (request: Req.Append): AppendStream => ({
  path: request.path,
  contentType: request.contentType,
  ...(request.close && { close: request.close }),
  ...(request.expectedTailOffset !== undefined && {
    expectedTailOffset: request.expectedTailOffset,
  }),
  ...(request.producer !== undefined && {
    producer: request.producer,
  }),
})

const createRequest = (request: Req.Create): CreateStream => ({
  path: request.path,
  contentType: request.contentType,
  closed: request.closed,
})

export const readPosition = (request: Req.Read | Req.ReadLive): ReadPosition => ({
  path: request.path,
  offset: request.offset,
})

const appended = (result: AppendResult): Res.Appended | Res.AppendDuplicate =>
  result._tag === "Duplicate"
    ? new Res.AppendDuplicate({
        nextOffset: result.metadata.tailOffset,
        closed: result.metadata.closed,
      })
    : new Res.Appended({
        nextOffset: result.metadata.tailOffset,
        closed: result.metadata.closed,
      })

const created = (result: { readonly _tag: "Created" | "AlreadyExists"; readonly metadata: StreamMetadata }) =>
  result._tag === "Created"
    ? new Res.Created({
        tailOffset: result.metadata.tailOffset,
        closed: result.metadata.closed,
        contentType: result.metadata.contentType,
      })
    : new Res.AlreadyExists({
        tailOffset: result.metadata.tailOffset,
        closed: result.metadata.closed,
        contentType: result.metadata.contentType,
      })

const appendError = (error: unknown): Effect.Effect<Res.AppendResponse> => {
  if (error instanceof StreamClosedError) {
    return Effect.succeed(new Res.WriteToClosed({ finalOffset: error.finalOffset }))
  }
  if (error instanceof ContentTypeMismatchError) {
    return Effect.succeed(
      new Res.ContentMismatch({
        code: "content-mismatch",
        expected: error.expected,
        actual: error.actual,
      }),
    )
  }
  if (error instanceof OffsetConflictError) {
    return Effect.succeed(
      new Res.OffsetConflict({
        code: "offset-conflict",
        expectedTailOffset: error.expectedTailOffset,
        actualTailOffset: error.actualTailOffset,
      }),
    )
  }
  if (error instanceof ProducerEpochRegressionError) {
    return Effect.succeed(new Res.EpochFenced({ currentEpoch: error.currentEpoch }))
  }
  if (error instanceof ProducerSequenceGapError) {
    return Effect.succeed(
      new Res.SequenceGap({
        expectedSeq: error.expectedSeq,
        receivedSeq: error.receivedSeq,
      }),
    )
  }
  if (error instanceof StreamNotFoundError) {
    return Effect.succeed(new Res.StreamNotFound())
  }
  return defect(error)
}

const readError = (error: unknown): Effect.Effect<Res.ReadResponse> => {
  if (error instanceof InvalidOffsetError) {
    return Effect.succeed(new Res.InvalidOffset({ offset: error.offset }))
  }
  if (error instanceof StreamNotFoundError) {
    return Effect.succeed(new Res.StreamNotFound())
  }
  return defect(error)
}

const headError = (error: unknown): Effect.Effect<Res.HeadResponse> =>
  error instanceof StreamNotFoundError
    ? Effect.succeed(new Res.StreamNotFound())
    : defect(error)

const closeError = (error: unknown): Effect.Effect<Res.CloseResponse> => {
  if (error instanceof StreamClosedError) {
    return Effect.succeed(new Res.Closed({ finalOffset: error.finalOffset }))
  }
  if (error instanceof ContentTypeMismatchError) {
    return Effect.succeed(
      new Res.ContentMismatch({
        code: "content-mismatch",
        expected: error.expected,
        actual: error.actual,
      }),
    )
  }
  if (error instanceof OffsetConflictError) {
    return Effect.succeed(
      new Res.OffsetConflict({
        code: "offset-conflict",
        expectedTailOffset: error.expectedTailOffset,
        actualTailOffset: error.actualTailOffset,
      }),
    )
  }
  if (error instanceof ProducerEpochRegressionError) {
    return Effect.succeed(new Res.EpochFenced({ currentEpoch: error.currentEpoch }))
  }
  if (error instanceof ProducerSequenceGapError) {
    return Effect.succeed(
      new Res.SequenceGap({
        expectedSeq: error.expectedSeq,
        receivedSeq: error.receivedSeq,
      }),
    )
  }
  if (error instanceof StreamNotFoundError) {
    return Effect.succeed(new Res.StreamNotFound())
  }
  return defect(error)
}

const unexpectedStoreError = (error: DurableStreamLogError): Effect.Effect<never> =>
  defect(error)

const defect = (error: unknown): Effect.Effect<never> =>
  Effect.sync(() => {
    throw error
  })

const create = (log: DurableStreamLog, request: Req.Create): Effect.Effect<Res.CreateResponse> =>
  log.create(createRequest(request)).pipe(
    Effect.map(created),
    Effect.catchTags({
      StreamLogError: unexpectedStoreError,
      StreamClosedError: unexpectedStoreError,
      ContentTypeMismatchError: unexpectedStoreError,
      OffsetConflictError: unexpectedStoreError,
      ProducerEpochRegressionError: unexpectedStoreError,
      ProducerSequenceGapError: unexpectedStoreError,
      StreamNotFoundError: unexpectedStoreError,
      InvalidOffsetError: unexpectedStoreError,
    }),
  )

const append = (log: DurableStreamLog, request: Req.Append): Effect.Effect<Res.AppendResponse> =>
  appendBytes(log, appendRequest(request), request.bytes).pipe(
    Effect.map(appended),
    Effect.catchTags({
      StreamClosedError: appendError,
      ContentTypeMismatchError: appendError,
      OffsetConflictError: appendError,
      ProducerEpochRegressionError: appendError,
      ProducerSequenceGapError: appendError,
      StreamNotFoundError: appendError,
      InvalidOffsetError: unexpectedStoreError,
      StreamLogError: unexpectedStoreError,
    }),
  )

const closeAppend = (
  log: DurableStreamLog,
  request: Req.Close,
  metadata: StreamMetadata,
): Effect.Effect<Res.Closed | Res.Appended | Res.AppendDuplicate, DurableStreamLogError> =>
  metadata.closed
    ? Effect.succeed(new Res.Closed({ finalOffset: metadata.tailOffset }))
    : appendEmpty(log, {
        path: request.path,
        contentType: metadata.contentType,
        close: true,
        ...(request.producer !== undefined && {
          producer: request.producer,
        }),
      }).pipe(Effect.map(appended))

const close = (log: DurableStreamLog, request: Req.Close): Effect.Effect<Res.CloseResponse> =>
  log.head(request.path).pipe(
    Effect.flatMap((metadata) => closeAppend(log, request, metadata)),
    Effect.catchTags({
      StreamClosedError: closeError,
      ContentTypeMismatchError: closeError,
      OffsetConflictError: closeError,
      ProducerEpochRegressionError: closeError,
      ProducerSequenceGapError: closeError,
      StreamNotFoundError: closeError,
      InvalidOffsetError: unexpectedStoreError,
      StreamLogError: unexpectedStoreError,
    }),
  )

const read = (log: DurableStreamLog, request: Req.Read): Effect.Effect<Res.ReadResponse> =>
  log.head(request.path).pipe(
    Effect.flatMap((metadata) =>
      log.read(readPosition(request)).pipe(
        Effect.flatMap(Stream.runCollect),
        Effect.map(Chunk.toReadonlyArray),
        Effect.map((records) => {
          const wired = records.map(wireRecord)
          const last = wired[wired.length - 1]
          return new Res.ReadResult({
            records: wired,
            nextOffset: last?.nextOffset ?? metadata.tailOffset,
            upToDate: true,
            closed: last?.closed ?? metadata.closed,
          })
        }),
      ),
    ),
    Effect.catchTags({
      InvalidOffsetError: readError,
      StreamNotFoundError: readError,
      StreamClosedError: readError,
      ContentTypeMismatchError: readError,
      OffsetConflictError: readError,
      ProducerEpochRegressionError: unexpectedStoreError,
      ProducerSequenceGapError: unexpectedStoreError,
      StreamLogError: unexpectedStoreError,
    }),
  )

const head = (log: DurableStreamLog, request: Req.Head): Effect.Effect<Res.HeadResponse> =>
  log.head(request.path).pipe(
    Effect.map(
      (metadata) =>
        new Res.HeadResult({
          tailOffset: metadata.tailOffset,
          closed: metadata.closed,
          contentType: metadata.contentType,
        }),
    ),
    Effect.catchTags({
      StreamNotFoundError: headError,
      StreamLogError: unexpectedStoreError,
      StreamClosedError: unexpectedStoreError,
      ContentTypeMismatchError: unexpectedStoreError,
      OffsetConflictError: unexpectedStoreError,
      ProducerEpochRegressionError: unexpectedStoreError,
      ProducerSequenceGapError: unexpectedStoreError,
      InvalidOffsetError: unexpectedStoreError,
    }),
  )

const deleteStream = (log: DurableStreamLog, request: Req.Delete): Effect.Effect<Res.DeleteResponse> =>
  log.delete(request.path).pipe(
    Effect.map((result) => result._tag === "Deleted" ? new Res.Deleted() : new Res.StreamNotFound()),
    Effect.catchTags({
      StreamLogError: unexpectedStoreError,
      StreamClosedError: unexpectedStoreError,
      ContentTypeMismatchError: unexpectedStoreError,
      OffsetConflictError: unexpectedStoreError,
      ProducerEpochRegressionError: unexpectedStoreError,
      ProducerSequenceGapError: unexpectedStoreError,
      StreamNotFoundError: unexpectedStoreError,
      InvalidOffsetError: unexpectedStoreError,
    }),
  )

export function handle<R extends Req.Request>(
  log: DurableStreamLog,
  request: R,
): Effect.Effect<ResponseOf<R>>
export function handle(log: DurableStreamLog, request: Req.Request): Effect.Effect<typeof Res.Response.Type> {
  switch (request._tag) {
    case "Create":
      return create(log, request)
    case "Append":
      return append(log, request)
    case "Close":
      return close(log, request)
    case "Read":
      return read(log, request)
    case "Head":
      return head(log, request)
    case "Delete":
      return deleteStream(log, request)
  }
}
