import { Effect, Layer, Option, PubSub, Stream, SynchronizedRef, pipe, type Scope } from "effect"
import { AppendCondFailed, type S2Error } from "./errors.ts"
import { S2, type AppendOptions, type S2Record, type S2Service } from "./s2.ts"

/**
 * `s2-lite`: an in-memory emulation of the only two S2 primitives the runtime
 * depends on — a fencing token (set-once-per-lease, highest wins) and a
 * conditional append guarded by `match_seq_num`. Faithful enough to exercise
 * AC-1…AC-6 with no cloud. `seqNum` is a global, monotonic, never-reset physical
 * position; `trim` drops bytes below a cursor but never rewinds the counter.
 */

interface StreamState {
  readonly records: ReadonlyArray<S2Record>
  readonly nextSeq: bigint
  readonly fence: string | null
  readonly pubsub: PubSub.PubSub<S2Record>
}

interface Value {
  readonly streams: ReadonlyMap<string, StreamState>
}

const pubSubCapacity = 1024

const emptyStream = (): Effect.Effect<StreamState> =>
  PubSub.bounded<S2Record>(pubSubCapacity).pipe(
    Effect.map((pubsub) => ({ records: [], nextSeq: 0n, fence: null, pubsub })),
  )

const getOrCreate = (
  value: Value,
  stream: string,
): Effect.Effect<readonly [StreamState, Value]> => {
  const existing = value.streams.get(stream)
  return pipe(
    existing,
    Option.fromUndefinedOr,
    Option.match({
      onSome: (st) => Effect.succeed([st, value] as const),
      onNone: () =>
        emptyStream().pipe(
          Effect.map((st) => {
            const streams = new Map(value.streams)
            streams.set(stream, st)
            return [st, { streams }] as const
          }),
        ),
    }),
  )
}

const withStream = (value: Value, stream: string, st: StreamState): Value => ({
  streams: new Map(value.streams).set(stream, st),
})

/** Highest token wins; tokens are fixed-width numeric strings so lexical == numeric. */
const fenceWins = (incoming: string, current: string | null): boolean =>
  current === null || incoming >= current

interface AppendCommit {
  readonly result: { readonly tail: bigint }
  readonly publish: Effect.Effect<void>
}

const commitAppend = (
  value: Value,
  stream: string,
  st: StreamState,
  records: ReadonlyArray<Uint8Array>,
): readonly [AppendCommit, Value] => {
  const assigned: ReadonlyArray<S2Record> = records.map((data, index) => ({
    seqNum: st.nextSeq + BigInt(index),
    data,
  }))
  const seq = st.nextSeq + BigInt(records.length)
  const nextState: StreamState = {
    ...st,
    records: [...st.records, ...assigned],
    nextSeq: seq,
  }
  return [
    {
      result: { tail: seq },
      publish: pipe(st.pubsub, PubSub.publishAll(assigned), Effect.asVoid),
    },
    withStream(value, stream, nextState),
  ] as const
}

const appendInto =
  (stream: string, records: ReadonlyArray<Uint8Array>, opts: AppendOptions) =>
  (value: Value): Effect.Effect<readonly [AppendCommit, Value], AppendCondFailed> =>
    getOrCreate(value, stream).pipe(
      Effect.flatMap(([st, created]) => {
        if (
          opts.fencingToken !== undefined &&
          st.fence !== null &&
          opts.fencingToken !== st.fence
        ) {
          return Effect.fail(
            new AppendCondFailed({
              stream,
              actualSeqNum: st.nextSeq,
              currentFencingToken: st.fence,
              presentedFencingToken: opts.fencingToken,
              reason: "fence-mismatch",
            }),
          )
        }
        if (opts.matchSeqNum !== undefined && opts.matchSeqNum !== st.nextSeq) {
          return Effect.fail(
            new AppendCondFailed({
              stream,
              expectedSeqNum: opts.matchSeqNum,
              actualSeqNum: st.nextSeq,
              reason: "position-taken",
            }),
          )
        }
        return Effect.succeed(commitAppend(created, stream, st, records))
      }),
    )

const historical = (st: StreamState, from: bigint): Stream.Stream<S2Record> =>
  Stream.fromIterable(st.records.filter((r) => r.seqNum >= from))

const followFrom = (
  ref: SynchronizedRef.SynchronizedRef<Value>,
  stream: string,
  from: bigint,
): Effect.Effect<Stream.Stream<S2Record, S2Error>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const st = yield* SynchronizedRef.modifyEffect(ref, (v) =>
      getOrCreate(v, stream).pipe(Effect.map(([s, nv]) => [s, nv] as const)),
    )
    const subscription = yield* PubSub.subscribe(st.pubsub)
    const snapshot = yield* SynchronizedRef.get(ref).pipe(
      Effect.map((v) => v.streams.get(stream) ?? st),
    )
    const live = Stream.fromSubscription(subscription).pipe(
      Stream.filter((r) => r.seqNum >= from && r.seqNum >= snapshot.nextSeq),
    )
    return historical(snapshot, from).pipe(Stream.concat(live))
  })

const makeService = (ref: SynchronizedRef.SynchronizedRef<Value>): S2Service => ({
  append: (stream, records, opts) =>
    SynchronizedRef.modifyEffect(ref, (value) =>
      appendInto(stream, records, opts ?? {})(value),
    ).pipe(
      Effect.tap((commit) => commit.publish),
      Effect.map((commit) => commit.result),
    ),

  read: (stream, from, opts) =>
    opts?.follow === true
      ? Stream.unwrap(followFrom(ref, stream, from))
      : Stream.unwrap(
          SynchronizedRef.get(ref).pipe(
            Effect.map((value) => {
              const existing = value.streams.get(stream)
              return pipe(
                existing,
                Option.fromUndefinedOr,
                Option.match({
                  onNone: () => Stream.empty as Stream.Stream<S2Record, S2Error>,
                  onSome: (st) => historical(st, from),
                }),
              )
            }),
          ),
        ),

  checkTail: (stream) =>
    SynchronizedRef.get(ref).pipe(
      Effect.map((value) => value.streams.get(stream)?.nextSeq ?? 0n),
    ),

  checkFence: (stream) =>
    SynchronizedRef.get(ref).pipe(
      Effect.map((value) => value.streams.get(stream)?.fence ?? null),
    ),

  fence: (stream, token) =>
    SynchronizedRef.updateEffect(ref, (value) =>
      getOrCreate(value, stream).pipe(
        Effect.map(([st, created]) =>
          fenceWins(token, st.fence)
            ? withStream(created, stream, { ...st, fence: token })
            : created,
        ),
      ),
    ),

  trim: (stream, upTo) =>
    SynchronizedRef.update(ref, (value) => {
      const st = value.streams.get(stream)
      if (st === undefined) return value
      return withStream(value, stream, {
        ...st,
        records: st.records.filter((r) => r.seqNum >= upTo),
      })
    }),
})

/** Build a fresh in-memory S2 store. Reuse the same instance across worker restarts. */
export const make: Effect.Effect<S2Service> = SynchronizedRef.make<Value>({
  streams: new Map(),
}).pipe(Effect.map(makeService))

/** Layer wrapping an already-built store (share it across simulated restarts). */
export const layerWith = (service: S2Service): Layer.Layer<S2> => Layer.succeed(S2, service)

/** Layer with a fresh store — for single-run, non-crash scenarios. */
export const layer: Layer.Layer<S2> = Layer.effect(S2, make)
