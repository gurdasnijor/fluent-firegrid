import { Effect, Stream } from "effect"
import {
  BeginningOffset,
  type ChangeEvent,
  ContentTypeMismatchError,
  InvalidOffsetError,
  OffsetConflictError,
  OffsetTrimmedError,
  ProducerEpochRegressionError,
  ProducerSequenceGapError,
  StreamClosedError,
  StreamGoneError,
  StreamNotFoundError,
  type AppendResult,
  type CreateStreamResult,
  type DurableStreamLog,
  type DurableStreamLogError,
  type ReadPosition,
} from "@firegrid/fluent-stream-log"
import { decodeJsonRecords, encodeBody, InvalidContent } from "./content.ts"
import type {
  AppendStreamCommand,
  AppendStreamOutcome,
  CreateStreamOutcome,
  DeleteStreamOutcome,
  DurableStreamsServer,
  HeadStreamOutcome,
  FollowStreamCommand,
  ReadStreamCommand,
  ReadStreamOutcome,
  StreamEvent,
  StreamProblem,
} from "./model.ts"

const badRequest = (message: string): StreamProblem => ({
  _tag: "BadRequest",
  code: "BAD_REQUEST",
  message,
})

const conflict = (message: string): StreamProblem => ({
  _tag: "Conflict",
  code: "CONFLICT",
  message,
})

const notFound = (message = "stream not found"): StreamProblem => ({
  _tag: "NotFound",
  code: "NOT_FOUND",
  message,
})

const gone = (message = "stream was deleted"): StreamProblem => ({
  _tag: "Gone",
  code: "GONE",
  message,
})

const readPosition = (command: ReadStreamCommand | FollowStreamCommand): ReadPosition => ({
  path: command.path,
  offset: command.offset ?? BeginningOffset,
  ...("limit" in command && command.limit !== undefined && { limit: command.limit }),
})

const streamProblem = (error: DurableStreamLogError | InvalidContent): StreamProblem => {
  if (error instanceof InvalidContent) {
    return badRequest(error.message)
  }
  if (error instanceof StreamNotFoundError) {
    return notFound()
  }
  if (error instanceof StreamGoneError) {
    return gone()
  }
  if (error instanceof InvalidOffsetError) {
    return badRequest(`invalid offset: ${error.offset}`)
  }
  if (error instanceof OffsetTrimmedError) {
    return badRequest(`offset was trimmed; earliest readable offset is ${error.earliest}`)
  }
  return conflict(error._tag)
}

const appendOutcome = (result: AppendResult): AppendStreamOutcome => {
  return result
}

const createOutcome = (result: CreateStreamResult): CreateStreamOutcome => ({
  _tag: result._tag,
  metadata: result.metadata,
})

const createConflict = (message: string): CreateStreamOutcome => ({
  _tag: "Conflict",
  code: "CONFLICT",
  message,
})

const appendError = (error: DurableStreamLogError | InvalidContent): AppendStreamOutcome => {
  if (error instanceof StreamClosedError) {
    return {
      _tag: "WriteToClosed",
      finalOffset: error.finalOffset,
    }
  }
  if (error instanceof ContentTypeMismatchError) {
    return {
      _tag: "ContentMismatch",
      expected: error.expected,
      actual: error.actual,
    }
  }
  if (error instanceof OffsetConflictError) {
    return {
      _tag: "OffsetConflict",
      expectedTailOffset: error.expectedTailOffset,
      actualTailOffset: error.actualTailOffset,
    }
  }
  if (error instanceof ProducerEpochRegressionError) {
    return {
      _tag: "Fenced",
      currentEpoch: error.currentEpoch,
    }
  }
  if (error instanceof ProducerSequenceGapError) {
    return {
      _tag: "SequenceGap",
      expectedSeq: error.expectedSeq,
      receivedSeq: error.receivedSeq,
    }
  }
  return streamProblem(error)
}

const changeEvent = (event: ChangeEvent): StreamEvent => {
  switch (event._tag) {
    case "Chunk":
      return {
        _tag: "Records",
        records: [event.record],
      }
    case "CaughtUp":
      return {
        _tag: "CaughtUp",
        offset: event.offset,
      }
    case "Closed":
      return {
        _tag: "Closed",
        finalOffset: event.finalOffset,
      }
  }
}

export const makeServer = (log: DurableStreamLog): DurableStreamsServer => ({
  create: (command) =>
    encodeBody(command.contentType, command.body).pipe(
      Effect.flatMap((messages) =>
        log.create({
          path: command.path,
          contentType: command.contentType,
          ...(messages.length === 0 && command.closed !== undefined && { closed: command.closed }),
        }).pipe(
          Effect.flatMap((created) => {
            switch (created._tag) {
              case "AlreadyExists":
                if (created.metadata.contentType !== command.contentType) {
                  return Effect.succeed(createConflict("stream exists with different content-type"))
                }
                if (command.closed !== undefined && created.metadata.closed !== command.closed) {
                  return Effect.succeed(createConflict("stream exists with different closed state"))
                }
                return Effect.succeed(createOutcome(created))
              case "Created":
                return messages.length === 0 && command.closed !== true
                  ? Effect.succeed(createOutcome(created))
                  : log.append({
                    path: command.path,
                    contentType: command.contentType,
                    messages,
                    ...(command.closed !== undefined && { close: command.closed }),
                  }).pipe(
                    Effect.map((appended) =>
                      createOutcome({
                        _tag: "Created",
                        metadata: appended._tag === "Appended" ? appended.metadata : created.metadata,
                      }),
                    ),
                  )
            }
          }),
        ),
      ),
      Effect.catch((error) => Effect.succeed(streamProblem(error))),
    ),

  fork: (command) =>
    log.fork({
      path: command.path,
      source: command.source,
      ...(command.atOffset !== undefined && { atOffset: command.atOffset }),
      ...(command.contentType !== undefined && { contentType: command.contentType }),
    }).pipe(
      Effect.map(createOutcome),
      Effect.catch((error) => Effect.succeed(streamProblem(error))),
    ),

  append: (command) =>
    encodeBody(command.contentType, command.body, { rejectEmptyJsonArray: true }).pipe(
      Effect.flatMap((messages): Effect.Effect<AppendResult, DurableStreamLogError | InvalidContent> => {
        if (messages.length === 0 && command.close !== true) {
          return Effect.fail(new InvalidContent({ message: "empty append requires close" }))
        }
        return log.append({
          path: command.path,
          contentType: command.contentType,
          messages,
          ...(command.seq !== undefined && { seq: command.seq }),
          ...(command.close !== undefined && { close: command.close }),
          ...(command.expectedTailOffset !== undefined && {
            expectedTailOffset: command.expectedTailOffset,
          }),
          ...(command.producer !== undefined && { producer: command.producer }),
        })
      }),
      Effect.map(appendOutcome),
      Effect.catch((error) => Effect.succeed(appendError(error))),
    ),

  head: (path) =>
    log.head(path).pipe(
      Effect.map((metadata): HeadStreamOutcome => ({ _tag: "Head", metadata })),
      Effect.catch((error) => Effect.succeed(streamProblem(error))),
    ),

  read: (command) =>
    log.read(readPosition(command)).pipe(
      Effect.map((window): ReadStreamOutcome => ({
        _tag: "Read",
        records: window.records,
        nextOffset: window.nextOffset,
        upToDate: window.upToDate,
        closed: window.closed,
      })),
      Effect.catch((error) => Effect.succeed(streamProblem(error))),
    ),

  readJson: (command) =>
    log.read(readPosition(command)).pipe(
      Effect.flatMap((window) =>
        decodeJsonRecords(window.records).pipe(
          Effect.map((items): ReadStreamOutcome => ({
            _tag: "ReadJson",
            items,
            nextOffset: window.nextOffset,
            upToDate: window.upToDate,
            closed: window.closed,
          })),
        ),
      ),
      Effect.catch((error) => Effect.succeed(streamProblem(error))),
    ),

  follow: (command) =>
    log.changes(readPosition(command)).pipe(
      Effect.map((events) => events.pipe(Stream.map(changeEvent), Stream.mapError(streamProblem))),
      Effect.mapError(streamProblem),
    ),

  delete: (path) =>
    log.delete(path).pipe(
      Effect.map((result): DeleteStreamOutcome =>
        result._tag === "Deleted"
          ? { _tag: "Deleted", path: result.path }
          : notFound(),
      ),
      Effect.catch((error) => Effect.succeed(streamProblem(error))),
    ),
})

export const closeStream = (
  server: DurableStreamsServer,
  command: Omit<AppendStreamCommand, "body" | "close">,
) => server.append({ ...command, close: true })
