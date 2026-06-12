import { Effect, Option, PubSub, Stream, SynchronizedRef, pipe, type Scope } from "effect"
import {
  BeginningOffset,
  ContentTypeMismatchError,
  InvalidOffsetError,
  NowOffset,
  OffsetConflictError,
  OffsetTrimmedError,
  StreamClosedError,
  StreamGoneError,
  StreamLogError,
  StreamNotFoundError,
  initialOffset,
  makeOffset,
  type AppendRequest,
  type AppendResult,
  type ChangeEvent,
  type CreateStream,
  type CreateStreamResult,
  type DeleteStreamResult,
  type DurableStreamLog,
  type DurableStreamLogError,
  type ForkStream,
  type Offset,
  type ReadPosition,
  type ReadWindow,
  type StreamId,
  type StreamMetadata,
  type StreamPath,
  type StreamRecord,
  type TrimStream,
} from "@firegrid/fluent-stream-log"

interface ProducerState {
  readonly epoch: number
  readonly lastSeq: number
  readonly lastOffset: Offset
  readonly closed: boolean
}

interface ForkLink {
  readonly sourceId: StreamId
  readonly divergenceOffset: Offset
}

interface EventStream {
  readonly id: StreamId
  readonly localRecords: readonly StreamRecord[]
  readonly pubsub: PubSub.PubSub<ChangeEvent>
  readonly metadata: StreamMetadata
  readonly earliestOffset: Offset
  readonly lastSeq?: string
  readonly producers: ReadonlyMap<string, ProducerState>
  readonly refCount: number
  readonly gone: boolean
  readonly fork?: ForkLink
}

interface Value {
  readonly nextId: number
  readonly pathToId: ReadonlyMap<StreamPath, StreamId>
  readonly streamsById: ReadonlyMap<StreamId, EventStream>
}

interface Commit<A> {
  readonly result: A
  readonly events: readonly PublishEvent[]
}

interface PublishEvent {
  readonly pubsub: PubSub.PubSub<ChangeEvent>
  readonly event: ChangeEvent
}

type ProducerDecision =
  | { readonly _tag: "Proceed" }
  | { readonly _tag: "Duplicate"; readonly metadata: StreamMetadata; readonly highestSeq: number }
  | { readonly _tag: "Fenced"; readonly currentEpoch: number }
  | { readonly _tag: "SequenceGap"; readonly expectedSeq: number; readonly receivedSeq: number }

const streamId = (nextId: number): StreamId => `stream-${nextId}` as StreamId

const offsetNumber = (offset: Offset): number => Number(offset)

const isInMemoryOffset = (offset: string): boolean => /^[0-9]+$/.test(offset)

const nextOffset = (offset: Offset, by: number): Offset => makeOffset(offsetNumber(offset) + by)

const metadataFor = (request: CreateStream): StreamMetadata => ({
  path: request.path,
  tailOffset: initialOffset,
  closed: request.closed === true,
  contentType: request.contentType,
})

const createResult = (request: CreateStream, stream: EventStream | undefined): CreateStreamResult =>
  stream === undefined
    ? { _tag: "Created", metadata: metadataFor(request) }
    : { _tag: "AlreadyExists", metadata: stream.metadata }

const rawStream = (path: StreamPath, value: Value): Option.Option<EventStream> =>
  Option.fromUndefinedOr(value.pathToId.get(path)).pipe(
    Option.flatMap((id) => Option.fromUndefinedOr(value.streamsById.get(id))),
  )

const getStream = (
  path: StreamPath,
  value: Value,
): Effect.Effect<EventStream, StreamNotFoundError | StreamGoneError> =>
  pipe(
    rawStream(path, value),
    Option.match({
      onNone: () => Effect.fail(new StreamNotFoundError({ path })),
      onSome: (stream) =>
        stream.gone
          ? Effect.fail(new StreamGoneError({ path }))
          : Effect.succeed(stream),
    }),
  )

const updateStream = (value: Value, stream: EventStream): Value => ({
  ...value,
  streamsById: new Map(value.streamsById).set(stream.id, stream),
})

const insertStream = (value: Value, path: StreamPath, stream: EventStream): Value => ({
  nextId: value.nextId + 1,
  pathToId: new Map(value.pathToId).set(path, stream.id),
  streamsById: new Map(value.streamsById).set(stream.id, stream),
})

const removeStream = (value: Value, stream: EventStream): Value => ({
  ...value,
  pathToId: new Map([...value.pathToId].filter(([path]) => path !== stream.metadata.path)),
  streamsById: new Map([...value.streamsById].filter(([id]) => id !== stream.id)),
})

const releaseSourceRef = (value: Value, source: EventStream): Value => {
  const released: EventStream = { ...source, refCount: Math.max(0, source.refCount - 1) }
  if (!released.gone || released.refCount > 0) {
    return updateStream(value, released)
  }
  const removed = removeStream(value, released)
  if (released.fork === undefined) {
    return removed
  }
  const parent = removed.streamsById.get(released.fork.sourceId)
  return parent === undefined ? removed : releaseSourceRef(removed, parent)
}

const commitEvents = (events: readonly PublishEvent[]) =>
  Effect.forEach(events, ({ event, pubsub }) => PubSub.publish(pubsub, event), { discard: true })

const commitResult = <A>(result: A): Commit<A> => ({
  result,
  events: [],
})

const metadataWithTail = (stream: EventStream, tailOffset: Offset, closed: boolean): StreamMetadata => ({
  ...stream.metadata,
  tailOffset,
  closed,
})

const duplicateResult = (metadata: StreamMetadata, highestSeq?: number): AppendResult => ({
  _tag: "Duplicate",
  metadata,
  ...(highestSeq !== undefined && { highestSeq }),
})

const producerDecision = (request: AppendRequest, stream: EventStream): ProducerDecision => {
  if (request.producer === undefined) {
    return { _tag: "Proceed" }
  }

  const producer = request.producer
  const current = stream.producers.get(producer.producerId)

  if (current === undefined || producer.epoch > current.epoch) {
    return producer.seq === 0
      ? { _tag: "Proceed" }
      : { _tag: "SequenceGap", expectedSeq: 0, receivedSeq: producer.seq }
  }

  if (producer.epoch < current.epoch) {
    return { _tag: "Fenced", currentEpoch: current.epoch }
  }

  if (producer.seq <= current.lastSeq) {
    return {
      _tag: "Duplicate",
      metadata: metadataWithTail(stream, current.lastOffset, current.closed),
      highestSeq: current.lastSeq,
    }
  }

  const expectedSeq = current.lastSeq + 1
  return producer.seq === expectedSeq
    ? { _tag: "Proceed" }
    : { _tag: "SequenceGap", expectedSeq, receivedSeq: producer.seq }
}

const validateConcreteOffset = (
  position: ReadPosition,
  stream: EventStream,
): Effect.Effect<Offset, InvalidOffsetError | OffsetTrimmedError> => {
  const offset = position.offset === BeginningOffset
    ? initialOffset
    : position.offset === NowOffset
    ? stream.metadata.tailOffset
    : position.offset

  if (!isInMemoryOffset(offset) || Number.isNaN(offsetNumber(offset))) {
    return Effect.fail(new InvalidOffsetError({ path: position.path, offset }))
  }
  if (offset > stream.metadata.tailOffset) {
    return Effect.fail(new InvalidOffsetError({ path: position.path, offset }))
  }
  if (offset < stream.earliestOffset) {
    return Effect.fail(new OffsetTrimmedError({ path: position.path, earliest: stream.earliestOffset }))
  }
  return Effect.succeed(offset)
}

const recordsForStream = (value: Value, stream: EventStream): readonly StreamRecord[] => {
  if (stream.fork === undefined) {
    return stream.localRecords
  }
  const source = value.streamsById.get(stream.fork.sourceId)
  const inherited = source === undefined
    ? []
    : recordsForStream(value, source)
        .filter((record) => record.fromOffset < stream.fork!.divergenceOffset)
        .map((record) => ({ ...record, path: stream.metadata.path }))
  return [...inherited, ...stream.localRecords]
}

const recordsFrom = (
  value: Value,
  stream: EventStream,
  offset: Offset,
  toExclusive?: Offset,
): readonly StreamRecord[] =>
  recordsForStream(value, stream).filter(
    (record) =>
      record.fromOffset >= offset &&
      (toExclusive === undefined || record.fromOffset < toExclusive),
  )

const appendCommit = (
  request: AppendRequest,
  value: Value,
  stream: EventStream,
): Effect.Effect<readonly [Commit<AppendResult>, Value], DurableStreamLogError> => {
  const producer = producerDecision(request, stream)
  switch (producer._tag) {
    case "Duplicate":
      return Effect.succeed([commitResult(duplicateResult(producer.metadata, producer.highestSeq)), value] as const)
    case "Fenced":
      return Effect.succeed([commitResult({ _tag: "Fenced", currentEpoch: producer.currentEpoch }), value] as const)
    case "SequenceGap":
      return Effect.succeed([
        commitResult({
          _tag: "SequenceGap",
          expectedSeq: producer.expectedSeq,
          receivedSeq: producer.receivedSeq,
        }),
        value,
      ] as const)
    case "Proceed":
      break
  }

  if (stream.gone) {
    return Effect.fail(new StreamGoneError({ path: request.path }))
  }

  const closeOnly = request.close === true && request.messages.length === 0
  if (stream.metadata.closed) {
    return closeOnly
      ? Effect.succeed([commitResult({ _tag: "AlreadyClosed", finalOffset: stream.metadata.tailOffset }), value] as const)
      : Effect.fail(new StreamClosedError({ path: request.path, finalOffset: stream.metadata.tailOffset }))
  }

  if (request.messages.length > 0 && request.contentType !== stream.metadata.contentType) {
    return Effect.fail(
      new ContentTypeMismatchError({
        path: request.path,
        expected: stream.metadata.contentType,
        actual: request.contentType,
      }),
    )
  }

  if (request.expectedTailOffset !== undefined && request.expectedTailOffset !== stream.metadata.tailOffset) {
    return Effect.fail(
      new OffsetConflictError({
        path: request.path,
        expectedTailOffset: request.expectedTailOffset,
        actualTailOffset: stream.metadata.tailOffset,
      }),
    )
  }

  if (request.seq !== undefined && stream.lastSeq !== undefined && request.seq <= stream.lastSeq) {
    return Effect.fail(
      new OffsetConflictError({
        path: request.path,
        expectedTailOffset: nextOffset(stream.metadata.tailOffset, 1),
        actualTailOffset: stream.metadata.tailOffset,
      }),
    )
  }

  const closed = request.close === true
  const records = request.messages.map((bytes, index): StreamRecord => {
    const fromOffset = nextOffset(stream.metadata.tailOffset, index)
    return {
      path: request.path,
      fromOffset,
      nextOffset: nextOffset(fromOffset, 1),
      bytes,
      contentType: stream.metadata.contentType,
      closed: closed && index === request.messages.length - 1,
    }
  })
  const tailOffset = records[records.length - 1]?.nextOffset ?? stream.metadata.tailOffset
  const metadata = metadataWithTail(stream, tailOffset, closed)
  const producers = new Map(stream.producers)
  if (request.producer !== undefined) {
    producers.set(request.producer.producerId, {
      epoch: request.producer.epoch,
      lastSeq: request.producer.seq,
      lastOffset: tailOffset,
      closed,
    })
  }
  const updatedStream: EventStream = {
    ...stream,
    localRecords: [...stream.localRecords, ...records],
    metadata,
    ...(request.seq !== undefined && { lastSeq: request.seq }),
    producers,
  }
  const events: PublishEvent[] = [
    ...records.map((record): PublishEvent => ({
      pubsub: stream.pubsub,
      event: { _tag: "Chunk", record },
    })),
  ]
  if (closed) {
    events.push({
      pubsub: stream.pubsub,
      event: { _tag: "Closed", path: request.path, finalOffset: tailOffset },
    })
  }
  return Effect.succeed([
    {
      result: {
        _tag: "Appended",
        metadata,
        records,
        tailAdvanced: {
          path: request.path,
          tailOffset,
          closed,
        },
      },
      events,
    },
    updateStream(value, updatedStream),
  ] as const)
}

const readWindow = (
  value: Value,
  position: ReadPosition,
  stream: EventStream,
): Effect.Effect<ReadWindow, InvalidOffsetError | OffsetTrimmedError> =>
  validateConcreteOffset(position, stream).pipe(
    Effect.map((offset) => {
      const all = recordsFrom(value, stream, offset)
      const records = position.limit === undefined ? all : all.slice(0, position.limit)
      const last = records[records.length - 1]
      const readAllAvailable = records.length === all.length
      const nextReadOffset = last?.nextOffset ?? stream.metadata.tailOffset
      return {
        records,
        nextOffset: nextReadOffset,
        upToDate: readAllAvailable && nextReadOffset >= stream.metadata.tailOffset,
        closed: stream.metadata.closed,
      }
    }),
  )

const changesFrom = (
  value: SynchronizedRef.SynchronizedRef<Value>,
  position: ReadPosition,
): Effect.Effect<Stream.Stream<ChangeEvent, DurableStreamLogError>, DurableStreamLogError, Scope.Scope> =>
  Effect.gen(function* () {
    const initial = yield* pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) => getStream(position.path, current)),
    )
    const subscription = yield* PubSub.subscribe(initial.pubsub)
    const snapshotValue = yield* SynchronizedRef.get(value)
    const snapshot = yield* getStream(position.path, snapshotValue)
    const offset = yield* validateConcreteOffset(position, snapshot)
    const backlog = recordsFrom(snapshotValue, snapshot, offset, snapshot.metadata.tailOffset).map(
      (record): ChangeEvent => ({ _tag: "Chunk", record }),
    )
    const caughtUp: ChangeEvent = {
      _tag: "CaughtUp",
      path: position.path,
      offset: snapshot.metadata.tailOffset,
    }
    if (snapshot.metadata.closed) {
      return Stream.fromIterable([
        ...backlog,
        caughtUp,
        { _tag: "Closed", path: position.path, finalOffset: snapshot.metadata.tailOffset } satisfies ChangeEvent,
      ])
    }
    const live = Stream.fromSubscription(subscription).pipe(
      Stream.filter((event) => {
        switch (event._tag) {
          case "Chunk":
            return event.record.fromOffset >= snapshot.metadata.tailOffset
          case "Closed":
            return true
          case "CaughtUp":
            return false
        }
      }),
      Stream.takeUntil((event) => event._tag === "Closed"),
    )
    return Stream.fromIterable([...backlog, caughtUp]).pipe(Stream.concat(live))
  })

const makeStore = (value: SynchronizedRef.SynchronizedRef<Value>): DurableStreamLog => ({
  create: (request) =>
    PubSub.unbounded<ChangeEvent>().pipe(
      Effect.flatMap((pubsub) =>
        SynchronizedRef.modify(value, (current) => {
          const existing = pipe(rawStream(request.path, current), Option.getOrUndefined)
          if (existing !== undefined) {
            return [createResult(request, existing), current] as const
          }
          const id = streamId(current.nextId)
          const metadata = metadataFor(request)
          const stream: EventStream = {
            id,
            localRecords: [],
            pubsub,
            metadata,
            earliestOffset: initialOffset,
            producers: new Map<string, ProducerState>(),
            refCount: 0,
            gone: false,
          }
          return [
            createResult(request, undefined),
            insertStream(current, request.path, stream),
          ] as const
        }),
      ),
      Effect.withSpan("durable_stream_log.inmemory.create", {
        attributes: {
          "stream.content_type": request.contentType,
          "stream.closed_requested": request.closed === true,
        },
      }),
    ),

  append: (request) =>
    SynchronizedRef.modifyEffect(value, (current) =>
      pipe(
        rawStream(request.path, current),
        Option.match({
          onNone: () => Effect.fail(new StreamNotFoundError({ path: request.path })),
          onSome: (stream) =>
            appendCommit(request, current, stream).pipe(
              Effect.tap(([commit]) => commitEvents(commit.events)),
            ),
        }),
      ),
    ).pipe(
      Effect.map((commit) => commit.result),
      Effect.withSpan("durable_stream_log.inmemory.append", {
        attributes: {
          "stream.content_type": request.contentType,
          "stream.message_count": request.messages.length,
          "stream.message_bytes": request.messages.reduce((sum, bytes) => sum + bytes.byteLength, 0),
          "stream.close_requested": request.close === true,
          "stream.has_producer": request.producer !== undefined,
        },
      }),
    ),

  read: (position) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) =>
        getStream(position.path, current).pipe(
          Effect.flatMap((stream) => readWindow(current, position, stream)),
        ),
      ),
      Effect.withSpan("durable_stream_log.inmemory.read", {
        attributes: {
          "stream.offset": position.offset,
          "stream.limit": position.limit ?? "none",
        },
      }),
    ),

  changes: (position) =>
    changesFrom(value, position).pipe(
      Effect.withSpan("durable_stream_log.inmemory.changes", {
        attributes: {
          "stream.offset": position.offset,
          "stream.limit": position.limit ?? "none",
        },
      }),
    ),

  head: (path) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) => getStream(path, current).pipe(Effect.map((stream) => stream.metadata))),
      Effect.withSpan("durable_stream_log.inmemory.head"),
    ),

  fork: (request: ForkStream) =>
    PubSub.unbounded<ChangeEvent>().pipe(
      Effect.flatMap((pubsub) =>
        SynchronizedRef.modifyEffect(value, (current) => {
          const existing = pipe(rawStream(request.path, current), Option.getOrUndefined)
          if (existing !== undefined) {
            return Effect.succeed([
              createResult({ path: request.path, contentType: existing.metadata.contentType }, existing),
              current,
            ] as const)
          }
          return getStream(request.source, current).pipe(
            Effect.flatMap((source) => {
              const contentType = request.contentType ?? source.metadata.contentType
              if (contentType !== source.metadata.contentType) {
                return Effect.fail(
                  new ContentTypeMismatchError({
                    path: request.path,
                    expected: source.metadata.contentType,
                    actual: contentType,
                  }),
                )
              }
              const divergenceOffset = request.atOffset ?? source.metadata.tailOffset
              const id = streamId(current.nextId)
              const metadata: StreamMetadata = {
                path: request.path,
                tailOffset: divergenceOffset,
                closed: false,
                contentType,
              }
              const fork: EventStream = {
                id,
                localRecords: [],
                pubsub,
                metadata,
                earliestOffset: initialOffset,
                producers: new Map<string, ProducerState>(),
                refCount: 0,
                gone: false,
                fork: { sourceId: source.id, divergenceOffset },
              }
              const sourceWithRef: EventStream = { ...source, refCount: source.refCount + 1 }
              const withSourceRef = updateStream(current, sourceWithRef)
              return Effect.succeed([
                { _tag: "Created", metadata } satisfies CreateStreamResult,
                insertStream(withSourceRef, request.path, fork),
              ] as const)
            }),
          )
        }),
      ),
      Effect.withSpan("durable_stream_log.inmemory.fork", {
        attributes: {
          "stream.at_offset": request.atOffset ?? "head",
        },
      }),
    ),

  trim: (request: TrimStream) =>
    SynchronizedRef.modifyEffect(value, (current) =>
      getStream(request.path, current).pipe(
        Effect.flatMap((stream) => {
          const dependent = [...current.streamsById.values()].find(
            (candidate) =>
              candidate.fork?.sourceId === stream.id &&
              request.before > candidate.fork.divergenceOffset,
          )
          if (dependent !== undefined) {
            return Effect.fail(
              new StreamLogError({
                operation: "trim",
                path: request.path,
                details: `trim would pass dependent fork ${dependent.metadata.path}`,
              }),
            )
          }
          const updated: EventStream = {
            ...stream,
            localRecords: stream.localRecords.filter((record) => record.nextOffset > request.before),
            earliestOffset: stream.earliestOffset > request.before ? stream.earliestOffset : request.before,
          }
          return Effect.succeed([undefined, updateStream(current, updated)] as const)
        }),
      ),
    ).pipe(
      Effect.withSpan("durable_stream_log.inmemory.trim", {
        attributes: {
          "stream.before": request.before,
        },
      }),
    ),

  delete: (path) =>
    SynchronizedRef.modify(value, (current): readonly [DeleteStreamResult, Value] => {
      const stream = pipe(rawStream(path, current), Option.getOrUndefined)
      if (stream === undefined) {
        return [{ _tag: "NotFound", path } satisfies DeleteStreamResult, current] as const
      }
      if (stream.refCount > 0) {
        return [
          { _tag: "Deleted", path } satisfies DeleteStreamResult,
          updateStream(current, { ...stream, gone: true }),
        ] as const
      }
      const deleted = removeStream(current, stream)
      if (stream.fork === undefined) {
        return [{ _tag: "Deleted", path } satisfies DeleteStreamResult, deleted] as const
      }
      const source = deleted.streamsById.get(stream.fork.sourceId)
      if (source === undefined) {
        return [{ _tag: "Deleted", path } satisfies DeleteStreamResult, deleted] as const
      }
      return [
        { _tag: "Deleted", path } satisfies DeleteStreamResult,
        releaseSourceRef(deleted, source),
      ] as const
    }).pipe(Effect.withSpan("durable_stream_log.inmemory.delete")),
})

export const make = (): Effect.Effect<DurableStreamLog> =>
  SynchronizedRef.make<Value>({
    nextId: 0,
    pathToId: new Map<StreamPath, StreamId>(),
    streamsById: new Map<StreamId, EventStream>(),
  }).pipe(Effect.map(makeStore))
