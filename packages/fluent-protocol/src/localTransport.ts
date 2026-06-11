import { Effect, Queue, Ref, Stream, type Cause } from "effect"
import {
  initialOffset,
  type DurableStreamLog,
  type Offset,
  type StreamRecord,
} from "@firegrid/fluent-stream-log"
import { TransportError } from "@firegrid/fluent-transport"
import { handle, readPosition, wireRecord } from "./handler.ts"
import type { Request } from "./request.ts"
import { Control, RecordBatch, type DurableTransportService, type ReadEvent } from "./transport.ts"

interface StreamState {
  readonly nextOffset: Offset
  readonly closed: boolean
}

const transportError = (cause: unknown) =>
  new TransportError({
    message: "Durable protocol local stream failed",
    cause,
  })

const offerRecord = (
  queue: Queue.Queue<ReadEvent, TransportError | Cause.Done<void>>,
  state: Ref.Ref<StreamState>,
  record: StreamRecord,
) =>
  Ref.set(state, {
    nextOffset: record.nextOffset,
    closed: record.closed,
  }).pipe(
    Effect.andThen(Queue.offer(queue, new RecordBatch({ records: [wireRecord(record)] }))),
    Effect.asVoid,
  )

const endIfClosed = (
  queue: Queue.Queue<ReadEvent, TransportError | Cause.Done<void>>,
  state: Ref.Ref<StreamState>,
) =>
  Ref.get(state).pipe(
    Effect.flatMap((current) =>
      current.closed
        ? Queue.offer(
            queue,
            new Control({
              nextOffset: current.nextOffset,
              upToDate: true,
              closed: true,
            }),
          ).pipe(Effect.andThen(Queue.end(queue)), Effect.asVoid)
        : Effect.void,
    ),
  )

export const makeLocalTransport = (log: DurableStreamLog): Effect.Effect<DurableTransportService> =>
  Effect.succeed({
    call: <R extends Request>(request: R) =>
      handle(log, request),
    stream: (request) =>
      Effect.acquireRelease(
        Queue.make<ReadEvent, TransportError | Cause.Done<void>>({ capacity: 256 }),
        (queue) => Queue.shutdown(queue),
      ).pipe(
        Effect.tap((queue) =>
          Ref.make<StreamState>({ nextOffset: initialOffset, closed: false }).pipe(
            Effect.flatMap((state) =>
              log.subscribe(readPosition(request)).pipe(
                Effect.flatMap((records) =>
                  records.pipe(
                    Stream.takeUntil((record) => record.closed),
                    Stream.runForEach((record) => offerRecord(queue, state, record)),
                  ),
                ),
                Effect.andThen(endIfClosed(queue, state)),
                Effect.catch((error) => Queue.fail(queue, transportError(error)).pipe(Effect.asVoid)),
                Effect.forkScoped,
              ),
            ),
          ),
        ),
        Effect.map((queue) => queue),
      ),
  })
