import { Effect, HashMap, Option, PubSub, Sink, Stream, SynchronizedRef, pipe, type Scope } from "effect"
import {
  BeginningOffset,
  ContentTypeMismatchError,
  InvalidOffsetError,
  NowOffset,
  OffsetConflictError,
  ProducerEpochRegressionError,
  ProducerSequenceGapError,
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
} from "@firegrid/fluent-stream-log"

interface EventStream {
  readonly records: readonly StreamRecord[]
  readonly pubsub: PubSub.PubSub<StreamRecord>
  readonly metadata: StreamMetadata
}

interface Value {
  readonly streamsByPath: ReadonlyMap<StreamPath, EventStream>
  readonly allTails: PubSub.PubSub<TailAdvanced>
  readonly producerStates: HashMap.HashMap<string, ProducerState>
}

interface ProducerState {
  readonly epoch: number
  readonly lastSeq: number
}

const pubSubCapacity = 256

const emptyEventStream = (metadata: StreamMetadata): Effect.Effect<EventStream, never> =>
  PubSub.bounded<StreamRecord>(pubSubCapacity).pipe(
    Effect.map((pubsub) => ({
      records: [],
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
  (value: Value): Effect.Effect<Value, never> => {
    if (value.streamsByPath.has(request.path)) {
      return Effect.succeed(value)
    }
    return emptyEventStream(metadataFor(request)).pipe(
      Effect.map((stream) => {
        const streamsByPath = new Map(value.streamsByPath)
        streamsByPath.set(request.path, stream)
        return {
          ...value,
          streamsByPath,
        }
      }),
    )
  }

const createResult = (request: CreateStream, value: Value): CreateStreamResult =>
  value.streamsByPath.has(request.path)
    ? { _tag: "AlreadyExists", metadata: value.streamsByPath.get(request.path)!.metadata }
    : { _tag: "Created", metadata: metadataFor(request) }

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
    records: [...stream.records, ...records],
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

const producerKey = (request: AppendStream): string | undefined =>
  request.producer === undefined ? undefined : `${request.path}\u0000${request.producer.producerId}`

const duplicateCommit = (metadata: StreamMetadata): AppendCommit => ({
  result: {
    _tag: "Duplicate",
    metadata,
  },
  publish: Effect.void,
})

const updateProducerState = (
  value: Value,
  request: AppendStream,
): Value => {
  const key = producerKey(request)
  if (key === undefined || request.producer === undefined) {
    return value
  }
  return {
    ...value,
    producerStates: HashMap.set(value.producerStates, key, {
      epoch: request.producer.epoch,
      lastSeq: request.producer.seq,
    }),
  }
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
    streamsByPath: new Map(value.streamsByPath).set(request.path, updatedStream),
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
        Effect.andThen(publishTail(value.allTails, tailAdvanced)),
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
      (streams) => streams.get(request.path),
      Option.fromUndefinedOr,
      Option.match({
        onNone: () => Effect.fail(new StreamNotFoundError({ path: request.path })),
        onSome: (stream) =>
          pipe(
            validateAppend(request, stream),
            Effect.andThen(validateProducer(request, value, stream)),
            Effect.flatMap((producer) =>
              producer._tag === "Duplicate"
                ? Effect.succeed([duplicateCommit(stream.metadata), value] as const)
                : commitAppend(request, chunks, value, stream).pipe(
                    Effect.map(([commit, updatedValue]) => [
                      commit,
                      updateProducerState(updatedValue, request),
                    ] as const),
                  ),
            ),
          ),
      }),
    )

type ProducerDecision =
  | { readonly _tag: "Proceed" }
  | { readonly _tag: "Duplicate" }

const validateProducer = (
  request: AppendStream,
  value: Value,
  stream: EventStream,
): Effect.Effect<ProducerDecision, ProducerEpochRegressionError | ProducerSequenceGapError> => {
  const key = producerKey(request)
  if (key === undefined || request.producer === undefined) {
    return Effect.succeed({ _tag: "Proceed" })
  }

  const current = HashMap.get(value.producerStates, key)
  if (Option.isNone(current) || request.producer.epoch > current.value.epoch) {
    return request.producer.seq === 0
      ? Effect.succeed({ _tag: "Proceed" })
      : Effect.fail(
          new ProducerSequenceGapError({
            path: request.path,
            producerId: request.producer.producerId,
            expectedSeq: 0,
            receivedSeq: request.producer.seq,
          }),
        )
  }

  if (request.producer.epoch < current.value.epoch) {
    return Effect.fail(
      new ProducerEpochRegressionError({
        path: request.path,
        producerId: request.producer.producerId,
        currentEpoch: current.value.epoch,
      }),
    )
  }

  if (request.producer.seq <= current.value.lastSeq) {
    return Effect.succeed({ _tag: "Duplicate" })
  }

  const expectedSeq = current.value.lastSeq + 1
  if (request.producer.seq !== expectedSeq) {
    return Effect.fail(
      new ProducerSequenceGapError({
        path: stream.metadata.path,
        producerId: request.producer.producerId,
        expectedSeq,
        receivedSeq: request.producer.seq,
      }),
    )
  }

  return Effect.succeed({ _tag: "Proceed" })
}

const collectChunks = (request: AppendStream, value: SynchronizedRef.SynchronizedRef<Value>) =>
  Sink.collect<Uint8Array>().pipe(
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

  const index = stream.records.findIndex((record) => record.fromOffset >= offset)
  return index === -1
    ? Effect.fail(new InvalidOffsetError({ path: position.path, offset }))
    : Effect.succeed(index)
}

const historicalStreamFrom = (position: ReadPosition, stream: EventStream) =>
  pipe(
    indexFromOffset(position, stream),
    Effect.map((index) => Stream.fromIterable(stream.records.slice(index))),
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
    (streams) => streams.get(position.path),
    Option.fromUndefinedOr,
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
    const live = Stream.fromSubscription(liveQueue).pipe(
      Stream.filter((record) => record.fromOffset >= snapshot.metadata.tailOffset),
    )
    return historical.pipe(Stream.concat(live))
  })

const makeStore = (value: SynchronizedRef.SynchronizedRef<Value>): DurableStreamLog => ({
  create: (request) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.map((current) => createResult(request, current)),
      Effect.tap(() => SynchronizedRef.updateEffect(value, createStream(request))),
    ),
  append: (request) => collectChunks(request, value),
  read: (from) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) =>
        createHistoricalOrEmpty(from, Option.fromUndefinedOr(current.streamsByPath.get(from.path))),
      ),
    ),
  subscribe: (from) =>
    subscribeFrom(value, from),
  subscribeAll: () =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) => PubSub.subscribe(current.allTails)),
      Effect.map(Stream.fromSubscription),
    ),
  head: (path) =>
    pipe(
      value,
      SynchronizedRef.get,
      Effect.flatMap((current) =>
        pipe(
          current.streamsByPath,
          (streams) => streams.get(path),
          Option.fromUndefinedOr,
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
        (streams) => streams.get(path),
        Option.fromUndefinedOr,
        Option.match({
          onNone: () => ({ _tag: "NotFound", path }) satisfies DeleteStreamResult,
          onSome: () => ({ _tag: "Deleted", path }) satisfies DeleteStreamResult,
        }),
      ),
      {
        ...current,
        streamsByPath: new Map(
          [...current.streamsByPath].filter(([streamPath]) => streamPath !== path),
        ),
      },
    ]),
})

export const make = (): Effect.Effect<DurableStreamLog, never> =>
  PubSub.bounded<TailAdvanced>(pubSubCapacity).pipe(
    Effect.flatMap((allTails) =>
      SynchronizedRef.make<Value>({
        streamsByPath: new Map<StreamPath, EventStream>(),
        allTails,
        producerStates: HashMap.empty<string, ProducerState>(),
      }).pipe(
        Effect.map(makeStore),
      ),
    ),
  )
