import {
  AppendInput,
  AppendRecord,
  RangeNotSatisfiableError,
  S2,
  S2Endpoints,
  S2Error,
  SeqNumMismatchError,
  type AppendAck,
  type ReadRecord,
} from "@s2-dev/streamstore"
import { Effect, PubSub, Stream, SynchronizedRef, type Scope } from "effect"
import {
  BeginningOffset,
  ContentTypeMismatchError,
  InvalidOffsetError,
  NowOffset,
  OffsetConflictError,
  OffsetTrimmedError,
  PayloadTooLargeError,
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

export interface S2LiteStreamLogOptions {
  readonly endpoint: string
  readonly basin?: string
  readonly token?: string
  readonly streamPrefix?: string
}

interface S2Position {
  readonly seqNum: number
  readonly timestamp: Date
}

type S2Record = ReadRecord<"bytes">

type S2AppendResponse = AppendAck

interface S2StreamRef {
  readonly streamName: string
}

interface S2RecordsResponse {
  readonly records: readonly S2Record[]
  readonly tail?: S2Position
}

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

interface LocalStream {
  readonly id: StreamId
  readonly storage: S2StreamRef
  readonly metadata: StreamMetadata
  readonly earliestOffset: Offset
  readonly baseOffset: Offset
  readonly localTailSeq: number
  readonly pubsub: PubSub.PubSub<ChangeEvent>
  readonly producers: ReadonlyMap<string, ProducerState>
  readonly refCount: number
  readonly gone: boolean
  readonly fork?: ForkLink
  readonly lastSeq?: string
}

interface Value {
  readonly nextId: number
  readonly pathToId: ReadonlyMap<StreamPath, StreamId>
  readonly streamsById: ReadonlyMap<StreamId, LocalStream>
}

type ProducerDecision =
  | { readonly _tag: "Proceed" }
  | { readonly _tag: "Duplicate"; readonly metadata: StreamMetadata; readonly highestSeq: number }
  | { readonly _tag: "Fenced"; readonly currentEpoch: number }
  | { readonly _tag: "SequenceGap"; readonly expectedSeq: number; readonly receivedSeq: number }

interface DeleteCommit {
  readonly result: DeleteStreamResult
  readonly deletedStorage: readonly S2StreamRef[]
}

interface S2Client {
  readonly ensureBasin: Effect.Effect<void, StreamLogError>
  readonly ensureStorage: (storage: S2StreamRef) => Effect.Effect<"created" | "exists", StreamLogError>
  readonly appendRecords: (
    storage: S2StreamRef,
    request: {
      readonly records: readonly Uint8Array[]
      readonly matchSeqNum: number
    },
  ) => Effect.Effect<S2AppendResponse, StreamLogError | OffsetConflictError | PayloadTooLargeError>
  readonly readRecords: (
    storage: S2StreamRef,
    request: {
      readonly seqNum: number
      readonly count?: number
    },
  ) => Effect.Effect<S2RecordsResponse, StreamLogError>
  readonly deleteStorage: (storage: S2StreamRef) => Effect.Effect<void, StreamLogError>
}

const normalizedNamePart = (input: string, fallback: string, maxLength: number): string => {
  const normalized = input
    .toLowerCase()
    .replace(/[^a-z0-9-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .replace(/-+/gu, "-")
    .slice(0, maxLength)
    .replace(/^-+|-+$/gu, "")
  return normalized.length >= 2 ? normalized : fallback
}

const stableHash = (input: string): string => {
  let hash = 2166136261
  for (let index = 0; index < input.length; index++) {
    hash ^= input.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0).toString(36)
}

const streamNameFor = (prefix: string, path: StreamPath, id: StreamId): string =>
  `${normalizedNamePart(prefix, "ds", 24)}-${stableHash(`${path}\0${id}`)}-${normalizedNamePart(id, "stream", 32)}`
    .slice(0, 64)
    .replace(/-+$/gu, "")

const pathToStorage = (prefix: string, path: StreamPath, id: StreamId): S2StreamRef => ({
  streamName: streamNameFor(prefix, path, id),
})

const streamId = (nextId: number): StreamId => `s2-stream-${nextId}` as StreamId

const offsetNumber = (offset: Offset): number => Number(offset)

const isS2Offset = (offset: string): boolean => /^[0-9]+$/.test(offset)

const nextOffset = (offset: Offset, by: number): Offset => makeOffset(offsetNumber(offset) + by)

const addOffsets = (base: Offset, by: number): Offset => makeOffset(offsetNumber(base) + by)

const metadataFor = (request: CreateStream): StreamMetadata => ({
  path: request.path,
  tailOffset: initialOffset,
  closed: request.closed === true,
  contentType: request.contentType,
})

const streamLogError = (
  operation: StreamLogError["operation"],
  details: string,
  cause?: unknown,
  path?: string,
): StreamLogError =>
  new StreamLogError({
    operation,
    details,
    ...(cause !== undefined && { cause }),
    ...(path !== undefined && { path }),
  })

const storagePath = (basinName: string, storage: S2StreamRef): string =>
  `${basinName}/${storage.streamName}`

const sdkStreamLogError = (
  operation: StreamLogError["operation"],
  basinName: string,
  storage: S2StreamRef,
  cause: unknown,
): StreamLogError => {
  if (cause instanceof S2Error) {
    return streamLogError(
      operation,
      `S2 ${operation} failed${cause.code === undefined ? "" : `: ${cause.code}`}`,
      cause,
      storagePath(basinName, storage),
    )
  }
  return streamLogError(operation, `S2 ${operation} failed`, cause, storagePath(basinName, storage))
}

const isPayloadTooLarge = (cause: unknown): boolean =>
  cause instanceof S2Error &&
  (cause.status === 413 || /too large|exceeds maximum|maximum.*bytes|metered/i.test(cause.message))

const appendInput = (
  basinName: string,
  storage: S2StreamRef,
  records: readonly Uint8Array[],
  matchSeqNum: number,
): Effect.Effect<AppendInput, PayloadTooLargeError | StreamLogError> =>
  Effect.try({
    try: () =>
      AppendInput.create(
        records.map((body) => AppendRecord.bytes({ body })),
        { matchSeqNum },
      ),
    catch: (cause) =>
      isPayloadTooLarge(cause)
        ? new PayloadTooLargeError({ path: storagePath(basinName, storage) as StreamPath })
        : sdkStreamLogError("append", basinName, storage, cause),
  })

const rangeTail = (error: RangeNotSatisfiableError): S2Position | undefined =>
  error.tail === undefined
    ? undefined
    : {
      seqNum: error.tail.seq_num,
      timestamp: new Date(error.tail.timestamp),
    }

const makeClient = (options: S2LiteStreamLogOptions): S2Client => {
  const endpoint = options.endpoint.replace(/\/+$/u, "")
  const basinName = normalizedNamePart(options.basin ?? "fluent-firegrid", "fluent-firegrid", 48)
  const s2 = new S2({
    accessToken: options.token ?? "s2-lite",
    endpoints: new S2Endpoints({
      account: endpoint,
      basin: endpoint,
    }),
    retry: {
      appendRetryPolicy: "noSideEffects",
      maxAttempts: 3,
    },
  })
  const basin = s2.basin(basinName)
  const stream = (storage: S2StreamRef) => basin.stream(storage.streamName)

  return {
    ensureBasin: Effect.tryPromise({
      try: async () => {
        await s2.basins.ensure({ basin: basinName })
      },
      catch: (cause) => streamLogError("create", "S2 basin ensure failed", cause, basinName),
    }),

    ensureStorage: (storage) =>
      Effect.tryPromise({
        try: async () => {
          const ensuredStream = await basin.streams.ensure({ stream: storage.streamName })
          return ensuredStream.result === "created" ? "created" as const : "exists" as const
        },
        catch: (cause) => sdkStreamLogError("create", basinName, storage, cause),
      }),

    appendRecords: (storage, request) =>
      appendInput(basinName, storage, request.records, request.matchSeqNum).pipe(
        Effect.flatMap((input) =>
          Effect.tryPromise({
            try: () => stream(storage).append(input),
            catch: (cause) => {
              if (cause instanceof SeqNumMismatchError) {
                return new OffsetConflictError({
                  path: storagePath(basinName, storage) as StreamPath,
                  expectedTailOffset: makeOffset(request.matchSeqNum),
                  actualTailOffset: makeOffset(cause.expectedSeqNum),
                })
              }
              return isPayloadTooLarge(cause)
                ? new PayloadTooLargeError({ path: storagePath(basinName, storage) as StreamPath })
                : sdkStreamLogError("append", basinName, storage, cause)
            },
          }),
        ),
      ),

    readRecords: (storage, request) =>
      Effect.tryPromise({
        try: () =>
          stream(storage).read(
            {
              start: { from: { seqNum: request.seqNum }, clamp: true },
              ...(request.count === undefined
                ? {}
                : { stop: { limits: { count: request.count } } }),
            },
            { as: "bytes" },
          ),
        catch: (cause) =>
          cause instanceof RangeNotSatisfiableError
            ? cause
            : sdkStreamLogError("read", basinName, storage, cause),
      }).pipe(
        Effect.catchIf(
          (cause): cause is RangeNotSatisfiableError => cause instanceof RangeNotSatisfiableError,
          (cause) => {
            const tail = rangeTail(cause)
            return Effect.succeed(tail === undefined ? { records: [] } : { records: [], tail })
          },
        ),
      ),

    deleteStorage: (storage) =>
      Effect.tryPromise({
        try: () => basin.streams.delete({ stream: storage.streamName }),
        catch: (cause) => cause,
      }).pipe(
        Effect.catchIf(
          (cause): cause is S2Error => cause instanceof S2Error && cause.status === 404,
          () => Effect.void,
        ),
        Effect.mapError((cause) => sdkStreamLogError("delete", basinName, storage, cause)),
      ),
  }
}

const producerDecision = (request: AppendRequest, stream: LocalStream): ProducerDecision => {
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
      metadata: {
        ...stream.metadata,
        tailOffset: current.lastOffset,
        closed: current.closed,
      },
      highestSeq: current.lastSeq,
    }
  }

  const expectedSeq = current.lastSeq + 1
  return producer.seq === expectedSeq
    ? { _tag: "Proceed" }
    : { _tag: "SequenceGap", expectedSeq, receivedSeq: producer.seq }
}

const validateOffset = (
  position: ReadPosition,
  metadata: StreamMetadata,
  earliestOffset: Offset,
): Effect.Effect<Offset, InvalidOffsetError | OffsetTrimmedError> => {
  const offset = position.offset === BeginningOffset
    ? initialOffset
    : position.offset === NowOffset
    ? metadata.tailOffset
    : position.offset

  if (!isS2Offset(offset) || Number.isNaN(offsetNumber(offset)) || offset > metadata.tailOffset) {
    return Effect.fail(new InvalidOffsetError({ path: position.path, offset }))
  }
  if (offset < earliestOffset) {
    return Effect.fail(new OffsetTrimmedError({ path: position.path, earliest: earliestOffset }))
  }
  return Effect.succeed(offset)
}

const streamRecords = (
  path: StreamPath,
  contentType: string,
  records: readonly S2Record[],
  closed: boolean,
  baseOffset: Offset,
  earliestOffset: Offset,
): readonly StreamRecord[] =>
  records.flatMap((record, index) => {
    const fromOffset = addOffsets(baseOffset, record.seqNum)
    const next = nextOffset(fromOffset, 1)
    if (next <= earliestOffset) {
      return []
    }
    return {
      path,
      fromOffset,
      nextOffset: next,
      bytes: record.body,
      contentType,
      closed: closed && index === records.length - 1,
    }
  })

const rawStream = (value: Value, path: StreamPath): LocalStream | undefined => {
  const id = value.pathToId.get(path)
  return id === undefined ? undefined : value.streamsById.get(id)
}

const getLocalStream = (
  value: Value,
  path: StreamPath,
): Effect.Effect<LocalStream, StreamNotFoundError | StreamGoneError> => {
  const stream = rawStream(value, path)
  return stream === undefined
    ? Effect.fail(new StreamNotFoundError({ path }))
    : stream.gone
    ? Effect.fail(new StreamGoneError({ path }))
    : Effect.succeed(stream)
}

const updateStream = (value: Value, stream: LocalStream): Value => ({
  ...value,
  streamsById: new Map(value.streamsById).set(stream.id, stream),
})

const insertStream = (value: Value, path: StreamPath, stream: LocalStream): Value => ({
  nextId: value.nextId + 1,
  pathToId: new Map(value.pathToId).set(path, stream.id),
  streamsById: new Map(value.streamsById).set(stream.id, stream),
})

const removeStream = (value: Value, stream: LocalStream): Value => ({
  ...value,
  pathToId: new Map([...value.pathToId].filter(([path]) => path !== stream.metadata.path)),
  streamsById: new Map([...value.streamsById].filter(([id]) => id !== stream.id)),
})

const releaseSourceRef = (
  value: Value,
  source: LocalStream,
): { readonly value: Value; readonly deletedStorage: readonly S2StreamRef[] } => {
  const released: LocalStream = { ...source, refCount: Math.max(0, source.refCount - 1) }
  if (!released.gone || released.refCount > 0) {
    return { value: updateStream(value, released), deletedStorage: [] }
  }
  const removed = removeStream(value, released)
  const deleted = [released.storage]
  if (released.fork === undefined) {
    return { value: removed, deletedStorage: deleted }
  }
  const parent = removed.streamsById.get(released.fork.sourceId)
  if (parent === undefined) {
    return { value: removed, deletedStorage: deleted }
  }
  const cascade = releaseSourceRef(removed, parent)
  return { value: cascade.value, deletedStorage: [...deleted, ...cascade.deletedStorage] }
}

const appendToStream = (
  client: S2Client,
  request: AppendRequest,
  stream: LocalStream,
): Effect.Effect<{
  readonly result: AppendResult
  readonly stream: LocalStream
  readonly events: readonly ChangeEvent[]
}, DurableStreamLogError> => {
  const producer = producerDecision(request, stream)
  switch (producer._tag) {
    case "Duplicate":
      return Effect.succeed({
        result: {
          _tag: "Duplicate",
          metadata: producer.metadata,
          highestSeq: producer.highestSeq,
        },
        stream,
        events: [],
      })
    case "Fenced":
      return Effect.succeed({
        result: { _tag: "Fenced", currentEpoch: producer.currentEpoch },
        stream,
        events: [],
      })
    case "SequenceGap":
      return Effect.succeed({
        result: {
          _tag: "SequenceGap",
          expectedSeq: producer.expectedSeq,
          receivedSeq: producer.receivedSeq,
        },
        stream,
        events: [],
      })
    case "Proceed":
      break
  }

  const closeOnly = request.close === true && request.messages.length === 0
  if (stream.gone) {
    return Effect.fail(new StreamGoneError({ path: request.path }))
  }

  if (stream.metadata.closed) {
    return closeOnly
      ? Effect.succeed({
        result: { _tag: "AlreadyClosed", finalOffset: stream.metadata.tailOffset },
        stream,
        events: [],
      })
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

  if (closeOnly) {
    const metadata = { ...stream.metadata, closed: true }
    const producers = new Map(stream.producers)
    if (request.producer !== undefined) {
      producers.set(request.producer.producerId, {
        epoch: request.producer.epoch,
        lastSeq: request.producer.seq,
        lastOffset: metadata.tailOffset,
        closed: true,
      })
    }
    const updated = { ...stream, metadata, producers }
    return Effect.succeed({
      result: {
        _tag: "Noop",
        metadata,
      },
      stream: updated,
      events: [{ _tag: "Closed", path: request.path, finalOffset: metadata.tailOffset }],
    })
  }

  const matchSeqNum = stream.localTailSeq
  return client.appendRecords(stream.storage, {
    records: request.messages,
    matchSeqNum,
  }).pipe(
    Effect.map((response) => {
      const closed = request.close === true
      const records = request.messages.map((bytes, index): StreamRecord => {
        const fromOffset = addOffsets(stream.baseOffset, response.start.seqNum + index)
        return {
          path: request.path,
          fromOffset,
          nextOffset: nextOffset(fromOffset, 1),
          bytes,
          contentType: stream.metadata.contentType,
          closed: closed && index === request.messages.length - 1,
        }
      })
      const tailOffset = addOffsets(stream.baseOffset, response.end.seqNum)
      const metadata = {
        ...stream.metadata,
        tailOffset,
        closed,
      }
      const producers = new Map(stream.producers)
      if (request.producer !== undefined) {
        producers.set(request.producer.producerId, {
          epoch: request.producer.epoch,
          lastSeq: request.producer.seq,
          lastOffset: tailOffset,
          closed,
        })
      }
      const events: ChangeEvent[] = [
        ...records.map((record): ChangeEvent => ({ _tag: "Chunk", record })),
      ]
      if (closed) {
        events.push({ _tag: "Closed", path: request.path, finalOffset: tailOffset })
      }
      return {
        result: {
          _tag: "Appended",
          metadata,
          records,
          tailAdvanced: {
            path: request.path,
            tailOffset,
            closed,
          },
        } satisfies AppendResult,
        stream: {
          ...stream,
          metadata,
          localTailSeq: response.end.seqNum,
          ...(request.seq !== undefined && { lastSeq: request.seq }),
          producers,
        },
        events,
      }
    }),
  )
}

const publishEvents = (
  pubsub: PubSub.PubSub<ChangeEvent>,
  events: readonly ChangeEvent[],
): Effect.Effect<void> =>
  Effect.forEach(events, (event) => PubSub.publish(pubsub, event), { discard: true })

const localRecordsForStream = (
  client: S2Client,
  stream: LocalStream,
): Effect.Effect<readonly StreamRecord[], StreamLogError> =>
  client.readRecords(stream.storage, { seqNum: 0 }).pipe(
    Effect.map((response) =>
      streamRecords(
        stream.metadata.path,
        stream.metadata.contentType,
        response.records,
        stream.metadata.closed,
        stream.baseOffset,
        stream.earliestOffset,
      ),
    ),
  )

const recordsForStream = (
  client: S2Client,
  value: Value,
  stream: LocalStream,
): Effect.Effect<readonly StreamRecord[], StreamLogError> => {
  const local = localRecordsForStream(client, stream)
  if (stream.fork === undefined) {
    return local
  }
  const source = value.streamsById.get(stream.fork.sourceId)
  if (source === undefined) {
    return local
  }
  return recordsForStream(client, value, source).pipe(
    Effect.flatMap((sourceRecords) =>
      local.pipe(
        Effect.map((localRecords) => [
          ...sourceRecords
            .filter((record) => record.fromOffset < stream.fork!.divergenceOffset)
            .map((record) => ({ ...record, path: stream.metadata.path })),
          ...localRecords,
        ]),
      ),
    ),
  )
}

const recordsFrom = (
  client: S2Client,
  value: Value,
  stream: LocalStream,
  offset: Offset,
  toExclusive?: Offset,
): Effect.Effect<readonly StreamRecord[], StreamLogError> =>
  recordsForStream(client, value, stream).pipe(
    Effect.map((records) =>
      records.filter(
        (record) =>
          record.fromOffset >= offset &&
          (toExclusive === undefined || record.fromOffset < toExclusive),
      ),
    ),
  )

const makeStore = (
  value: SynchronizedRef.SynchronizedRef<Value>,
  client: S2Client,
  options: S2LiteStreamLogOptions,
): DurableStreamLog => {
  const prefix = options.streamPrefix ?? options.basin ?? "fluent-firegrid"
  return {
    create: (request: CreateStream) =>
      PubSub.unbounded<ChangeEvent>().pipe(
        Effect.flatMap((pubsub) =>
          SynchronizedRef.modifyEffect(value, (current): Effect.Effect<
            readonly [CreateStreamResult, Value],
            StreamLogError
          > => {
            const existing = rawStream(current, request.path)
            if (existing !== undefined) {
              return Effect.succeed([
                { _tag: "AlreadyExists", metadata: existing.metadata } satisfies CreateStreamResult,
                current,
              ] as const)
            }
            const id = streamId(current.nextId)
            const storage = pathToStorage(prefix, request.path, id)
            return client.ensureStorage(storage).pipe(
              Effect.map(() => {
                const metadata = metadataFor(request)
                const stream: LocalStream = {
                  id,
                  storage,
                  metadata,
                  earliestOffset: initialOffset,
                  baseOffset: initialOffset,
                  localTailSeq: 0,
                  pubsub,
                  producers: new Map<string, ProducerState>(),
                  refCount: 0,
                  gone: false,
                }
                return [
                  { _tag: "Created", metadata } satisfies CreateStreamResult,
                  insertStream(current, request.path, stream),
                ] as const
              }),
            )
          }),
        ),
      ),

    append: (request) =>
      SynchronizedRef.modifyEffect(value, (current) =>
        getLocalStream(current, request.path).pipe(
          Effect.flatMap((stream) =>
            appendToStream(client, request, stream).pipe(
              Effect.tap(({ events }) => publishEvents(stream.pubsub, events)),
              Effect.map(({ result, stream: updated }) => [
                result,
                updateStream(current, updated),
              ] as const),
            ),
          ),
        ),
      ),

    read: (position) =>
      SynchronizedRef.get(value).pipe(
        Effect.flatMap((current) =>
          getLocalStream(current, position.path).pipe(
            Effect.flatMap((stream) =>
              validateOffset(position, stream.metadata, stream.earliestOffset).pipe(
                Effect.flatMap((offset) => recordsFrom(client, current, stream, offset)),
                Effect.map((available): ReadWindow => {
                  const records = position.limit === undefined ? available : available.slice(0, position.limit)
                  const last = records[records.length - 1]
                  const readAllAvailable = records.length === available.length
                  const nextReadOffset = last?.nextOffset ?? stream.metadata.tailOffset
                  return {
                    records,
                    nextOffset: nextReadOffset,
                    upToDate: readAllAvailable && nextReadOffset >= stream.metadata.tailOffset,
                    closed: stream.metadata.closed,
                  }
                }),
              ),
            ),
          ),
        ),
      ),

    changes: (
      position,
    ): Effect.Effect<Stream.Stream<ChangeEvent, DurableStreamLogError>, DurableStreamLogError, Scope.Scope> =>
      Effect.gen(function* () {
        const current = yield* SynchronizedRef.get(value)
        const initial = yield* getLocalStream(current, position.path)
        const subscription = yield* PubSub.subscribe(initial.pubsub)
        const snapshotValue = yield* SynchronizedRef.get(value)
        const snapshot = yield* getLocalStream(snapshotValue, position.path)
        const offset = yield* validateOffset(position, snapshot.metadata, snapshot.earliestOffset)
        const backlogRecords = yield* recordsFrom(client, snapshotValue, snapshot, offset, snapshot.metadata.tailOffset)
        const backlogEvents = backlogRecords.map((record): ChangeEvent => ({ _tag: "Chunk", record }))
        const caughtUp: ChangeEvent = {
          _tag: "CaughtUp",
          path: position.path,
          offset: snapshot.metadata.tailOffset,
        }
        if (snapshot.metadata.closed) {
          return Stream.fromIterable([
            ...backlogEvents,
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
        return Stream.fromIterable([...backlogEvents, caughtUp]).pipe(Stream.concat(live))
      }),

    head: (path) =>
      SynchronizedRef.get(value).pipe(
        Effect.flatMap((current) => getLocalStream(current, path)),
        Effect.map((stream) => stream.metadata),
      ),

    fork: (request: ForkStream) =>
      PubSub.unbounded<ChangeEvent>().pipe(
        Effect.flatMap((pubsub) =>
          SynchronizedRef.modifyEffect(value, (current): Effect.Effect<
            readonly [CreateStreamResult, Value],
            DurableStreamLogError
          > => {
            const existing = rawStream(current, request.path)
            if (existing !== undefined) {
              return Effect.succeed([
                { _tag: "AlreadyExists", metadata: existing.metadata } satisfies CreateStreamResult,
                current,
              ] as const)
            }
            return Effect.gen(function* () {
              const source = yield* getLocalStream(current, request.source)
              const contentType = request.contentType ?? source.metadata.contentType
              if (contentType !== source.metadata.contentType) {
                return yield* Effect.fail(
                  new ContentTypeMismatchError({
                    path: request.path,
                    expected: source.metadata.contentType,
                    actual: contentType,
                  }),
                )
              }
              const divergenceOffset = request.atOffset ?? source.metadata.tailOffset
              if (divergenceOffset > source.metadata.tailOffset) {
                return yield* Effect.fail(new InvalidOffsetError({ path: request.path, offset: divergenceOffset }))
              }
              const id = streamId(current.nextId)
              const storage = pathToStorage(prefix, request.path, id)
              yield* client.ensureStorage(storage)
              const metadata: StreamMetadata = {
                path: request.path,
                tailOffset: divergenceOffset,
                closed: false,
                contentType,
              }
              const fork: LocalStream = {
                id,
                storage,
                metadata,
                earliestOffset: initialOffset,
                baseOffset: divergenceOffset,
                localTailSeq: 0,
                pubsub,
                producers: new Map<string, ProducerState>(),
                refCount: 0,
                gone: false,
                fork: { sourceId: source.id, divergenceOffset },
              }
              const withSourceRef = updateStream(current, { ...source, refCount: source.refCount + 1 })
              return [
                { _tag: "Created", metadata } satisfies CreateStreamResult,
                insertStream(withSourceRef, request.path, fork),
              ] as const
            })
          }),
        ),
      ),

    trim: (request: TrimStream) =>
      SynchronizedRef.modifyEffect(value, (current) =>
        getLocalStream(current, request.path).pipe(
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
            const updated: LocalStream = {
              ...stream,
              earliestOffset: stream.earliestOffset > request.before ? stream.earliestOffset : request.before,
            }
            return Effect.succeed([undefined, updateStream(current, updated)] as const)
          }),
        ),
      ),

    delete: (path) =>
      SynchronizedRef.modify(value, (current): readonly [DeleteCommit, Value] => {
        const stream = rawStream(current, path)
        if (stream === undefined) {
          return [{ result: { _tag: "NotFound", path }, deletedStorage: [] }, current] as const
        }
        if (stream.refCount > 0) {
          return [
            { result: { _tag: "Deleted", path }, deletedStorage: [] },
            updateStream(current, { ...stream, gone: true }),
          ] as const
        }
        const deleted = removeStream(current, stream)
        if (stream.fork === undefined) {
          return [
            { result: { _tag: "Deleted", path }, deletedStorage: [stream.storage] },
            deleted,
          ] as const
        }
        const source = deleted.streamsById.get(stream.fork.sourceId)
        if (source === undefined) {
          return [
            { result: { _tag: "Deleted", path }, deletedStorage: [stream.storage] },
            deleted,
          ] as const
        }
        const cascade = releaseSourceRef(deleted, source)
        return [
          {
            result: { _tag: "Deleted", path },
            deletedStorage: [stream.storage, ...cascade.deletedStorage],
          },
          cascade.value,
        ] as const
      }).pipe(
        Effect.tap((commit) =>
          Effect.forEach(commit.deletedStorage, (storage) => client.deleteStorage(storage), { discard: true }),
        ),
        Effect.map((commit) => commit.result),
      ),
  }
}

export const make = (options: S2LiteStreamLogOptions): Effect.Effect<DurableStreamLog, StreamLogError> =>
  Effect.gen(function* () {
    const client = makeClient(options)
    yield* client.ensureBasin
    const value = yield* SynchronizedRef.make<Value>({
      nextId: 0,
      pathToId: new Map<StreamPath, StreamId>(),
      streamsById: new Map<StreamId, LocalStream>(),
    })
    return makeStore(value, client, options)
  })
