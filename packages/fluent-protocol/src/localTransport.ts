import { Effect, Queue, Stream, type Cause } from "effect"
import type { ChangeEvent, DurableStreamLog } from "@firegrid/fluent-stream-log"
import { Control, RecordBatch, TransportError, type DurableTransportService, type ReadEvent } from "./transport.ts"
import { handle, readPosition, wireRecord } from "./handler.ts"
import type { Request } from "./request.ts"

const transportError = (cause: unknown) =>
  new TransportError({
    message: "Durable stream local transport failed",
    cause,
  })

const offerChange = (
  queue: Queue.Queue<ReadEvent, TransportError | Cause.Done<void>>,
  change: ChangeEvent,
) => {
  switch (change._tag) {
    case "Chunk":
      return Queue.offer(queue, new RecordBatch({ records: [wireRecord(change.record)] })).pipe(Effect.asVoid)
    case "CaughtUp":
      return Queue.offer(
        queue,
        new Control({
          nextOffset: change.offset,
          upToDate: true,
          closed: false,
        }),
      ).pipe(Effect.asVoid)
    case "Closed":
      return Queue.offer(
        queue,
        new Control({
          nextOffset: change.finalOffset,
          upToDate: true,
          closed: true,
        }),
      ).pipe(Effect.andThen(Queue.end(queue)), Effect.asVoid)
  }
}

export const makeLocalTransport = (
  log: DurableStreamLog,
): Effect.Effect<DurableTransportService> =>
  Effect.succeed({
    call: <R extends Request>(request: R) => handle(log, request),
    stream: (request) =>
      Effect.acquireRelease(
        Queue.make<ReadEvent, TransportError | Cause.Done<void>>({ capacity: 256 }),
        (queue) => Queue.shutdown(queue),
      ).pipe(
        Effect.tap((queue) =>
          log.changes(readPosition(request)).pipe(
            Effect.flatMap((events) => events.pipe(Stream.runForEach((event) => offerChange(queue, event)))),
            Effect.catch((error) => Queue.fail(queue, transportError(error)).pipe(Effect.asVoid)),
            Effect.forkScoped,
          ),
        ),
      ),
  })
