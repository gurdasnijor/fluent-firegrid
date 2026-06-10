import { Effect, Ref, Stream, type Scope } from "effect"
import * as Mailbox from "effect/Mailbox"
import {
  initialOffset,
  type DurableStreamLog,
  type Offset,
  type StreamRecord,
} from "@firegrid/fluent-store"
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
  mailbox: Mailbox.Mailbox<ReadEvent, TransportError>,
  state: Ref.Ref<StreamState>,
  record: StreamRecord,
) =>
  Ref.set(state, {
    nextOffset: record.nextOffset,
    closed: record.closed,
  }).pipe(
    Effect.zipRight(mailbox.offer(new RecordBatch({ records: [wireRecord(record)] }))),
    Effect.asVoid,
  )

const endIfClosed = (
  mailbox: Mailbox.Mailbox<ReadEvent, TransportError>,
  state: Ref.Ref<StreamState>,
) =>
  Ref.get(state).pipe(
    Effect.flatMap((current) =>
      current.closed
        ? mailbox.offer(
            new Control({
              nextOffset: current.nextOffset,
              upToDate: true,
              closed: true,
            }),
          ).pipe(Effect.zipRight(mailbox.end), Effect.asVoid)
        : Effect.void,
    ),
  )

export const makeLocalTransport = (log: DurableStreamLog): Effect.Effect<DurableTransportService> =>
  Effect.succeed({
    call: <R extends Request>(request: R) =>
      handle(log, request),
    stream: (request) =>
      Effect.acquireRelease(
        Mailbox.make<ReadEvent, TransportError>(),
        (mailbox) => mailbox.shutdown,
      ).pipe(
        Effect.tap((mailbox) =>
          Ref.make<StreamState>({ nextOffset: initialOffset, closed: false }).pipe(
            Effect.flatMap((state) =>
              log.subscribe(readPosition(request)).pipe(
                Effect.flatMap((records) =>
                  records.pipe(
                    Stream.takeUntil((record) => record.closed),
                    Stream.runForEach((record) => offerRecord(mailbox, state, record)),
                  ),
                ),
                Effect.zipRight(endIfClosed(mailbox, state)),
                Effect.catchAll((error) => mailbox.fail(transportError(error)).pipe(Effect.asVoid)),
                Effect.forkScoped,
              ),
            ),
          ),
        ),
        Effect.map((mailbox) => mailbox as Mailbox.ReadonlyMailbox<ReadEvent, TransportError>),
      ) as Effect.Effect<Mailbox.ReadonlyMailbox<ReadEvent, TransportError>, TransportError, Scope.Scope>,
  })
