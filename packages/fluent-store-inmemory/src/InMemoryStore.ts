import { Chunk, Effect, HashMap, Option, PubSub, Sink, Stream, SynchronizedRef, pipe, type Scope } from "effect"
import {
  BeginningOffset,
  ContentTypeMismatchError,
  InvalidOffsetError,
  NowOffset,
  OffsetConflictError,
  StreamClosedError,
  StreamNotFoundError,
  initialOffset,
  makeOffset,
  type AppendResult,
  type AppendStream,
  type CreateStream,
  type CreateStreamResult,
  type DeleteStreamResult,
  type DurableStreamLogError,
  type DurableStreamLog,
  type ReadPosition,
  type StreamMetadata,
  type StreamPath,
  type StreamRecord,
  type TailAdvanced,
} from "@firegrid/fluent-store"

interface EventStream {
  readonly records: Chunk.Chunk<StreamRecord>
  readonly pubsub: PubSub.PubSub<StreamRecord>
  readonly metadata: StreamMetadata
}

interface Value {
  readonly streamsByPath: HashMap.HashMap<StreamPath, EventStream>
  readonly allTails: PubSub.PubSub<TailAdvanced>
}

export interface InMemoryStore {
  readonly create: (request: CreateStream) => Effect.Effect<CreateStreamResult, never>
  readonly append: (
    request: AppendStream,
  ) => Sink.Sink<AppendResult, Uint8Array, Uint8Array, DurableStreamLogError>
  readonly read: (
    from: ReadPosition,
  ) => Effect.Effect<Stream.Stream<StreamRecord, never>, DurableStreamLogError>
  readonly subscribe: (
    from: ReadPosition,
  ) => Effect.Effect<Stream.Stream<StreamRecord, never>, DurableStreamLogError, Scope.Scope>
  readonly subscribeAll: () => Effect.Effect<Stream.Stream<TailAdvanced, never>, never, Scope.Scope>
  readonly head: (path: StreamPath) => Effect.Effect<StreamMetadata, StreamNotFoundError>
  readonly delete: (path: StreamPath) => Effect.Effect<DeleteStreamResult, never>
}

const pubSubCapacity = 256

const emptyEventStream = (metadata: StreamMetadata): Effect.Effect<EventStream, never> =>
  PubSub.bounded<StreamRecord>(pubSubCapacity).pipe(
    Effect.map((pubsub) => ({
      records: Chunk.empty<StreamRecord>(),
      pubsub,
      metadata,
    })),
  )

const metadataFor = (request: CreateStream): StreamMetadata => ({
  path: request.path,
  tailOffset: initialOffset,
  closed: request.closed === true,
  contentType: request.contentType,
})

const createStream =
  (request: CreateStream) =>
  (value: Value): Effect.Effect<Value, never> =>
    pipe(
      value.streamsByPath,
      HashMap.get(request.path),
      Option.match({
        onSome: () => Effect.succeed(value),
        onNone: () =>
          emptyEventStream(metadataFor(request)).pipe(
            Effect.map((stream) => ({
              ...value,
              streamsByPath: HashMap.set(value.streamsByPath, request.path, stream),
            })),
          ),
      }),
    )

const createResult = (request: CreateStream, value: Value): CreateStreamResult =>
  pipe(
    value.streamsByPath,
    HashMap.get(request.path),
    Option.match({
      onSome: (stream) => ({ _tag: "AlreadyExists", metadata: stream.metadata }),
      onNone: () => ({ _tag: "Created", metadata: metadataFor(request) }),
    }),
  )

const validateAppend = (
  request: AppendStream,
  stream: EventStream,
): Effect.Effect<void, StreamClosedError | ContentTypeMismatchError | OffsetConflictError> => {
  if (stream.metadata.closed) {
    return Effect.fail(
      new StreamClosedError({
        path: request.path,
        finalOffset: stream.metadata.tailOffset,
      }),
    )
  }

  if (request.contentType !== stream.metadata.contentType) {
    return Effect.fail(
      new ContentTypeMismatchError({
        path: request.path,
        expected: stream.metadata.contentType,
        actual: request.contentType,
      }),
    )
  }

  if (
    request.expectedTailOffset !== undefined &&
    request.expectedTailOffset !== stream.metadata.tailOffset
  ) {
    return Effect.fail(
      new OffsetConflictError({
        path: request.path,
        expectedTailOffset: request.expectedTailOffset,
        actualTailOffset: stream.metadata.tailOffset,
      }),
    )
  }

  return Effect.void
}

const makeRecords = (
  request: AppendStream,
  stream: EventStream,
  chunks: readonly Uint8Array[],
): readonly StreamRecord[] => {
  const shouldClose = request.close === true
  const inputs = chunks.length > 0 || shouldClose ? chunks : []
  const closeOnly = inputs.length === 0 && shouldClose
  const recordsInput = closeOnly ? [new Uint8Array()] : inputs
  const start = stream.records.length

  return recordsInput.map((bytes, index) => {
    const fromOffset = makeOffset(start + index)
    const nextOffset = makeOffset(start + index + 1)
    return {
      path: request.path,
      fromOffset,
      nextOffset,
      bytes,
      contentType: stream.metadata.contentType,
      closed: shouldClose && index === recordsInput.length - 1,
    }
  })
}

const appendRecords = (
  stream: EventStream,
  records: readonly StreamRecord[],
  close: boolean,
): EventStream => {
  const tailOffset =
    records.length > 0 ? records[records.length - 1]?.nextOffset : stream.metadata.tailOffset
  return {
    ...stream,
    records: Chunk.appendAll(stream.records, Chunk.fromIterable(records)),
    metadata: {
      ...stream.metadata,
      tailOffset: tailOffset ?? stream.metadata.tailOffset,
      closed: close,
    },
  }
}

const publishRecords = (stream: EventStream, records: readonly StreamRecord[]) =>
  pipe(stream.pubsub, PubSub.publishAll(records))

const publishTail = (allTails: PubSub.PubSub<TailAdvanced>, tailAdvanced: TailAdvanced) =>
  pipe(allTails, PubSub.publish(tailAdvanced))

interface AppendCommit {
  readonly result: AppendResult
  readonly publish: Effect.Effect<void, never>
}

const commitAppend = (
  request: AppendStream,
  chunks: readonly Uint8Array[],
  value: Value,
  stream: EventStream,
): Effect.Effect<readonly [AppendCommit, Value], never> => {
  const records = makeRecords(request, stream, chunks)
  if (records.length === 0) {
    const result: AppendResult = {
      _tag: "Noop",
      metadata: stream.metadata,
    }
    return Effect.succeed([{ result, publish: Effect.void }, value] as const)
  }

  const updatedStream = appendRecords(stream, records, request.close === true)
  const tailAdvanced: TailAdvanced = {
    path: request.path,
    tailOffset: updatedStream.metadata.tailOffset,
    closed: updatedStream.metadata.closed,
  }
  const updatedValue: Value = {
    ...value,
    streamsByPath: HashMap.set(value.streamsByPath, request.path, updatedStream),
  }
  const result: AppendResult = {
    _tag: "Appended",
    metadata: updatedStream.metadata,
    records,
    tailAdvanced,
  }
  return Effect.succeed([
    {
      result,
      publish: pipe(
        publishRecords(updatedStream, records),
        Effect.zipRight(publishTail(value.allTails, tailAdvanced)),
        Effect.asVoid,
      ),
    },
    updatedValue,
  ] as const)
}

const appendCollected =
  (request: AppendStream, chunks: readonly Uint8Array[]) =>
  (value: Value): Effect.Effect<readonly [AppendCommit, Value], DurableStreamLogError> =>
    pipe(
      value.streamsByPath,
      HashMap.get(request.path),
      Option.match({
        onNone: () => Effect.fail(new StreamNotFoundError({ path: request.path })),
        onSome: (stream) =>
          pipe(
            validateAppend(request, stream),
            Effect.zipRight(commitAppend(request, chunks, value, stream)),
          ),
      }),
    )

const collectChunks = (request: AppendStream, value: SynchronizedRef.SynchronizedRef<Value>) =>
  Sink.foldChunksEffect<readonly Uint8Array[], Uint8Array, never, never>(
    [] as readonly Uint8Array[],
    () => true,
    (chunks, chunk) => Effect.succeed([...chunks, ...Chunk.toReadonlyArray(chunk)]),
  ).pipe(
    Sink.mapEffect((chunks) =>
      SynchronizedRef.modifyEffect(value, (current) => appendCollected(request, chunks)(current)).pipe(
        Effect.tap((commit) => commit.publish),
        Effect.map((commit) => commit.result),
      ),
    ),
  )

const indexFromOffset = (
  position: ReadPosition,
  stream: EventStream,
): Effect.Effect<number, InvalidOffsetError> => {
  if (position.offset === BeginningOffset) {
    return Effect.succeed(0)
  }

  if (position.offset === NowOffset) {
    return Effect.succeed(stream.records.length)
  }

  const offset = position.offset
  if (offset === stream.metadata.tailOffset) {
    return Effect.succeed(stream.records.length)
  }

  const index = Chunk.findFirstIndex(stream.records, (record) => record.fromOffset >= offset)
  return pipe(
    index,
    Option.match({
      onNone: () => Effect.fail(new InvalidOffsetError({ path: position.path, offset })),
      onSome: Effect.succeed,
    }),
  )
}

const historicalStreamFrom = (position: ReadPosition, stream: EventStream) =>
  pipe(
    indexFromOffset(position, stream),
    Effect.map((index) => pipe(stream.records, Chunk.drop(index), Stream.fromChunk)),
  )

const createHistoricalOrEmpty = (
  position: ReadPosition,
  maybeStream: Option.Option<EventStream>,
): Effect.Effect<Stream.Stream<StreamRecord, never>, InvalidOffsetError> =>
  pipe(
    maybeStream,
    Option.match({
      onNone: () => Effect.succeed(Stream.empty),
      onSome: (stream) => historicalStreamFrom(position, stream),
    }),
  )

const streamForSubscribe = (
  position: ReadPosition,
  value: Value,
): Effect.Effect<EventStream, StreamNotFoundError> =>
  pipe(
    value.streamsByPath,
    HashMap.get(position.path),
    Option.match({
      onSome: Effect.succeed,
      onNone: () => Effect.fail(new StreamNotFoundError({ path: position.path })),
    }),
  )

const subscribeFrom = (
  value: SynchronizedRef.SynchronizedRef<Value>,
  from: ReadPosition,
): Effect.Effect<Stream.Stream<StreamRecord, never>, DurableStreamLogError, Scope.Scope> =>
  Effect.gen(function* () {
    const stream = yield* pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) => streamForSubscribe(from, current)),
    )
    const liveQueue = yield* PubSub.subscribe(stream.pubsub)
    const snapshot = yield* pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) => streamForSubscribe(from, current)),
    )
    const historical = yield* historicalStreamFrom(from, snapshot)
    const live = Stream.fromQueue(liveQueue).pipe(
      Stream.filter((record) => record.fromOffset >= snapshot.metadata.tailOffset),
    )
    return historical.pipe(Stream.concat(live))
  })

const makeStore = (value: SynchronizedRef.SynchronizedRef<Value>): InMemoryStore => ({
  create: (request) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.map((current) => createResult(request, current)),
      Effect.zipLeft(SynchronizedRef.updateEffect(value, createStream(request))),
    ),
  append: (request) => collectChunks(request, value),
  read: (from) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) =>
        createHistoricalOrEmpty(from, pipe(current.streamsByPath, HashMap.get(from.path))),
      ),
    ),
  subscribe: (from) =>
    subscribeFrom(value, from),
  subscribeAll: () =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) => PubSub.subscribe(current.allTails)),
      Effect.map(Stream.fromQueue),
    ),
  head: (path) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) =>
        pipe(
          current.streamsByPath,
          HashMap.get(path),
          Option.match({
            onNone: () => Effect.fail(new StreamNotFoundError({ path })),
            onSome: (stream) => Effect.succeed(stream.metadata),
          }),
        ),
      ),
    ),
  delete: (path) =>
    SynchronizedRef.modify(value, (current) => [
      pipe(
        current.streamsByPath,
        HashMap.get(path),
        Option.match({
          onNone: () => ({ _tag: "NotFound", path }) satisfies DeleteStreamResult,
          onSome: () => ({ _tag: "Deleted", path }) satisfies DeleteStreamResult,
        }),
      ),
      {
        ...current,
        streamsByPath: HashMap.remove(current.streamsByPath, path),
      },
    ]),
})

export const make = (): Effect.Effect<InMemoryStore, never> =>
  PubSub.bounded<TailAdvanced>(pubSubCapacity).pipe(
    Effect.flatMap((allTails) =>
      SynchronizedRef.make<Value>({
        streamsByPath: HashMap.empty<StreamPath, EventStream>(),
        allTails,
      }).pipe(
        Effect.map(makeStore),
      ),
    ),
  )

export const makeDurableStreamLog = (): Effect.Effect<DurableStreamLog, never> => make()
