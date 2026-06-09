/**
 * STM-backed in-memory `Store` (STORE.3). One `TMap` of stream records; the
 * append decision + mutation is one STM transaction (the §5.2 precedence and
 * producer rules are decided and applied atomically). Wake evaluation is out of
 * scope for this slice.
 *
 * Offsets are stream-local byte counts (STORE.6): the offset of a record is the
 * cumulative byte length before it, and `Stream-Next-Offset` is the tail (total
 * byte length) AFTER the append. This makes catch-up reads a byte slice from
 * the requested offset and keeps offsets per-stream monotonic with no global
 * counter.
 */
import { Effect, Layer, Option, STM, Schema, TMap } from "effect"
import * as ProtocolError from "./ProtocolError.ts"
import { UintFromString } from "./Protocol.ts"
import * as Store from "./Store.ts"
import type * as Protocol from "./Protocol.ts"

interface ProducerState {
  readonly epoch: number
  readonly highestAcceptedSeq: number
}

interface StreamRecord {
  readonly path: Protocol.StreamPath
  readonly contentType: string
  readonly bytes: Uint8Array
  readonly closed: boolean
  /**
   * Identity of the writer that closed the stream, when known. A retried close
   * by the same producer tuple is idempotent (PRODUCERS.7); a different writer
   * appending to a closed stream is a `ClosedConflict`.
   */
  readonly closedBy: Option.Option<string>
  /** Last accepted `Stream-Seq` for lexicographic regression checks. */
  readonly lastStreamSeq: Option.Option<string>
  /** Per-producer-id epoch/seq state for idempotent-producer decisions. */
  readonly producers: ReadonlyMap<string, ProducerState>
}

interface MemoryState {
  readonly streams: TMap.TMap<Protocol.StreamPath, StreamRecord>
}

const offsetOf = (n: number): Protocol.Offset => String(n)

const concatBytes = (a: Uint8Array, b: Uint8Array): Uint8Array => {
  const out = new Uint8Array(a.length + b.length)
  out.set(a, 0)
  out.set(b, a.length)
  return out
}

/** Marker identity for a close performed by a plain (non-producer) writer. */
const producerKey = (input: Protocol.AppendRequest): Option.Option<string> =>
  Option.map(input.idempotentProducer, (p) => `producer:${p.id}`)

/**
 * The pure append decision over a stream record. Returns the decision plus, for
 * accepted/duplicate-with-close cases, the post-append record. This is the
 * single §5.2 precedence path; HTTP decoding never duplicates it.
 *
 * Precedence (after the close-retry idempotency check):
 *   closed-stream conflict > content-type mismatch > stream-seq regression >
 *   producer rules.
 */
const decide = (
  record: StreamRecord,
  input: Protocol.AppendRequest,
): {
  decision: Protocol.AppendDecision
  next: Option.Option<StreamRecord>
} => {
  const writer = producerKey(input)

  // --- Idempotent close retry (PRODUCERS.7) -------------------------------
  // A retried close by the SAME producer tuple is a duplicate success, even on
  // an already-closed stream. Checked BEFORE the generic closed-stream
  // conflict.
  if (
    record.closed &&
    input.close &&
    Option.isSome(input.idempotentProducer) &&
    Option.isSome(record.closedBy) &&
    Option.isSome(writer) &&
    writer.value === record.closedBy.value
  ) {
    const p = input.idempotentProducer.value
    const ps = record.producers.get(p.id)
    const highest = ps ? ps.highestAcceptedSeq : p.seq
    return {
      decision: {
        _tag: "ProducerDuplicate",
        nextOffset: offsetOf(record.bytes.length),
        closed: true,
        producerEpoch: ps ? ps.epoch : p.epoch,
        highestAcceptedSeq: highest,
      },
      next: Option.none(),
    }
  }

  // --- Closed-stream conflict ---------------------------------------------
  // Any other append to a closed stream conflicts (different writer, or a plain
  // append). 409 + Stream-Closed: true.
  if (record.closed) {
    return {
      decision: {
        _tag: "ClosedConflict",
        finalOffset: offsetOf(record.bytes.length),
      },
      next: Option.none(),
    }
  }

  // --- Content-type mismatch ----------------------------------------------
  // Close-only (empty body) appends ignore content type (PROTOCOL close-only
  // semantics); a content-bearing append must match the stream content type.
  const hasBody = input.entityBody.length > 0
  if (
    hasBody &&
    input.contentType !== "" &&
    input.contentType !== record.contentType
  ) {
    return { decision: { _tag: "ContentTypeMismatch" }, next: Option.none() }
  }

  // --- Stream-seq regression ----------------------------------------------
  if (Option.isSome(input.streamSeq)) {
    const seq = input.streamSeq.value
    if (
      Option.isSome(record.lastStreamSeq) &&
      seq <= record.lastStreamSeq.value
    ) {
      return { decision: { _tag: "StreamSeqRegression" }, next: Option.none() }
    }
  }

  // --- Producer rules ------------------------------------------------------
  if (Option.isSome(input.idempotentProducer)) {
    const p = input.idempotentProducer.value
    const ps = record.producers.get(p.id)

    if (ps !== undefined) {
      // Stale epoch -> fenced (PRODUCERS.2).
      if (p.epoch < ps.epoch) {
        return {
          decision: { _tag: "ProducerFenced", currentEpoch: ps.epoch },
          next: Option.none(),
        }
      }

      if (p.epoch === ps.epoch) {
        const expected = ps.highestAcceptedSeq + 1
        // Duplicate (PRODUCERS.1 / .6): seq <= highest accepted.
        if (p.seq <= ps.highestAcceptedSeq) {
          return {
            decision: {
              _tag: "ProducerDuplicate",
              nextOffset: offsetOf(record.bytes.length),
              closed: record.closed,
              producerEpoch: ps.epoch,
              highestAcceptedSeq: ps.highestAcceptedSeq,
            },
            next: Option.none(),
          }
        }
        // Gap: seq > expected.
        if (p.seq > expected) {
          return {
            decision: {
              _tag: "ProducerGap",
              expectedSeq: expected,
              receivedSeq: p.seq,
            },
            next: Option.none(),
          }
        }
        // p.seq === expected -> accept below.
      } else {
        // Epoch advance (p.epoch > ps.epoch). F1 (PRODUCERS.8): a non-zero seq
        // on an epoch advance is a ProducerGap(expected: 0). seq === 0
        // establishes the new epoch and accepts.
        if (p.seq !== 0) {
          return {
            decision: {
              _tag: "ProducerGap",
              expectedSeq: 0,
              receivedSeq: p.seq,
            },
            next: Option.none(),
          }
        }
      }
    } else {
      // First append for this producer id. F1 also applies to a brand-new
      // producer whose epoch advance presents a non-zero seq.
      if (p.seq !== 0) {
        return {
          decision: { _tag: "ProducerGap", expectedSeq: 0, receivedSeq: p.seq },
          next: Option.none(),
        }
      }
    }

    // Accept the producer append.
    const newBytes = concatBytes(record.bytes, input.entityBody)
    const producers = new Map(record.producers)
    producers.set(p.id, { epoch: p.epoch, highestAcceptedSeq: p.seq })
    const next: StreamRecord = {
      ...record,
      bytes: newBytes,
      // `record.closed` is provably false here (the closed-stream conflict
      // returned early above), so the post-append closed flag is just `input.close`.
      closed: input.close,
      closedBy: input.close ? writer : record.closedBy,
      lastStreamSeq: Option.isSome(input.streamSeq)
        ? input.streamSeq
        : record.lastStreamSeq,
      producers,
    }
    return {
      decision: {
        _tag: "ProducerAccepted",
        nextOffset: offsetOf(newBytes.length),
        closed: next.closed,
        producerEpoch: p.epoch,
        highestAcceptedSeq: p.seq,
      },
      next: Option.some(next),
    }
  }

  // --- Plain (non-producer) accepted --------------------------------------
  const newBytes = concatBytes(record.bytes, input.entityBody)
  const next: StreamRecord = {
    ...record,
    bytes: newBytes,
    // `record.closed` is provably false here (closed-stream conflict returned
    // early above), so the post-append closed flag is just `input.close`.
    closed: input.close,
    closedBy: input.close ? writer : record.closedBy,
    lastStreamSeq: Option.isSome(input.streamSeq)
      ? input.streamSeq
      : record.lastStreamSeq,
  }
  return {
    decision: {
      _tag: "PlainAccepted",
      nextOffset: offsetOf(newBytes.length),
      closed: next.closed,
    },
    next: Option.some(next),
  }
}

const makeStore = (state: MemoryState): Store.StoreShape => {
  const getRecord = (
    path: Protocol.StreamPath,
  ): STM.STM<StreamRecord, ProtocolError.NotFound> =>
    STM.gen(function* () {
      const existing = yield* TMap.get(state.streams, path)
      if (Option.isNone(existing)) {
        return yield* STM.fail(new ProtocolError.NotFound({ path }))
      }
      return existing.value
    })

  const createStream = (
    input: Protocol.CreateRequest,
  ): Effect.Effect<Protocol.CreateDecision, ProtocolError.ProtocolError> =>
    STM.commit(
      STM.gen(function* () {
        const existing = yield* TMap.get(state.streams, input.path)
        if (Option.isSome(existing)) {
          const rec = existing.value
          // Idempotent create requires matching config (content type).
          if (rec.contentType !== input.contentType) {
            return yield* STM.fail(
              new ProtocolError.CreateConflict({
                path: input.path,
                reason: "content-type mismatch with existing stream",
              }),
            )
          }
          return {
            _tag: "AlreadyExists" as const,
            tailOffset: offsetOf(rec.bytes.length),
            closed: rec.closed,
          }
        }
        const record: StreamRecord = {
          path: input.path,
          contentType: input.contentType,
          bytes: input.entityBody,
          closed: input.close,
          closedBy: Option.none(),
          lastStreamSeq: Option.none(),
          producers: new Map(),
        }
        yield* TMap.set(state.streams, input.path, record)
        return {
          _tag: "Created" as const,
          tailOffset: offsetOf(record.bytes.length),
          closed: record.closed,
        }
      }),
    )

  const append = (
    input: Protocol.AppendRequest,
  ): Effect.Effect<Protocol.AppendResult, ProtocolError.ProtocolError> =>
    STM.commit(
      STM.gen(function* () {
        const existing = yield* TMap.get(state.streams, input.path)
        if (Option.isNone(existing)) {
          return yield* STM.fail(
            new ProtocolError.NotFound({ path: input.path }),
          )
        }
        const { decision, next } = decide(existing.value, input)
        if (Option.isSome(next)) {
          yield* TMap.set(state.streams, input.path, next.value)
          return {
            append: decision,
            tailAdvanced: Option.some({
              path: input.path,
              tailOffset: offsetOf(next.value.bytes.length),
              closed: next.value.closed,
            }),
          }
        }
        return { append: decision, tailAdvanced: Option.none() }
      }),
    )

  const read = (
    path: Protocol.StreamPath,
    offset: Protocol.Offset,
  ): Effect.Effect<Protocol.ReadChunk, ProtocolError.ProtocolError> =>
    STM.commit(getRecord(path)).pipe(
      Effect.flatMap((rec) => {
        const tail = rec.bytes.length
        // `-1` is the begin sentinel; any other value is a byte offset decoded
        // through the shared `UintFromString` Schema (no hand-written numeric
        // parsing — effect-server.TOOLING.1).
        const decoded =
          offset === "-1"
            ? Option.some(0)
            : Schema.decodeOption(UintFromString)(offset)
        if (Option.isNone(decoded) || decoded.value > tail) {
          return Effect.fail(
            new ProtocolError.BadRequest({
              reason: `invalid offset: ${offset}`,
            }),
          )
        }
        const entityBody = rec.bytes.slice(decoded.value)
        return Effect.succeed({
          path,
          contentType: rec.contentType,
          entityBody,
          nextOffset: offsetOf(tail),
          upToDate: true,
          closed: rec.closed,
        })
      }),
    )

  const head = (
    path: Protocol.StreamPath,
  ): Effect.Effect<Protocol.StreamTail, ProtocolError.ProtocolError> =>
    STM.commit(getRecord(path)).pipe(
      Effect.map((rec) => ({
        path,
        tailOffset: offsetOf(rec.bytes.length),
        closed: rec.closed,
        contentType: rec.contentType,
      })),
    )

  const deleteStream = (
    path: Protocol.StreamPath,
  ): Effect.Effect<void, ProtocolError.ProtocolError> =>
    STM.commit(
      STM.gen(function* () {
        yield* getRecord(path)
        yield* TMap.remove(state.streams, path)
      }),
    )

  return {
    createStream,
    append,
    read,
    head,
    deleteStream,
  }
}

/** A `Layer` providing the STM-backed in-memory `Store`. */
export const layer: Layer.Layer<Store.Store> = Layer.effect(
  Store.Store,
  Effect.gen(function* () {
    const streams = yield* STM.commit(
      TMap.empty<Protocol.StreamPath, StreamRecord>(),
    )
    return makeStore({ streams })
  }),
)
