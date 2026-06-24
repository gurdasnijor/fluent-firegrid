import {
  type AppendAck,
  AppendInput,
  type AppendRecord,
  S2Client,
  S2Conflict,
  S2NotFound,
  type S2Record
} from "effect-s2"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { FlowError } from "./errors.ts"

// these S2 error subclasses share a `_tag` of "S2Error" (they extend it), so
// discriminate by schema membership, not by `_tag` — matching `StreamDb`.
const isS2Conflict = Schema.is(S2Conflict)
const isS2NotFound = Schema.is(S2NotFound)

// ── public surface ───────────────────────────────────────────────────────────

/** Tuning knobs for the orchestrator (all optional; sensible defaults). */
export interface OrchestratorConfig {
  /** Command-mailbox capacity (bounded ⇒ backpressure on intake). Default 256. */
  readonly commandCapacity?: number
  /** Write-mailbox capacity (bounded ⇒ backpressure on the write side). Default 256. */
  readonly writeCapacity?: number
  /** `changes` PubSub capacity; slowest-consumer drops, never OOMs a hot stream. Default 256. */
  readonly changesCapacity?: number
  /** A stalled tail reader must not pin a client read/write forever. Default 30s. */
  readonly readDeadline?: Duration.Input
}

export interface OrchestratorOptions<S> {
  /** The physical S2 stream this orchestrator owns (one instance per key). */
  readonly stream: string
  /** State at `fromCursor` (e.g. folded from a snapshot; empty for a fresh stream). */
  readonly initial: S
  /** Fold a record into the state. Called once per record, in stream order. */
  readonly reduce: (state: S, record: S2Record) => S
  /** Seq-num to begin folding from. Default 0 (full replay). */
  readonly fromCursor?: number
  readonly config?: OrchestratorConfig
}

/** Fenced single-writer extras: the token carried on every owner write. */
export interface OwnedOrchestratorOptions<S> extends OrchestratorOptions<S> {
  /**
   * Fencing token carried on every owner write (cooperative fencing — a stale
   * owner's tokened write 412s). Absent is allowed (no claim yet); ownership
   * claim/lease is Layer 6.
   */
  readonly fencingToken?: string
}

export interface Orchestrator<S> {
  /**
   * Append records to the owned stream. On the fenced owner the returned ack
   * resolves **after** the records are folded locally in stream order
   * (read-your-writes); on a view it resolves on durable ack (eventual apply).
   */
  readonly write: (records: ReadonlyArray<AppendRecord>) => Effect.Effect<AppendAck, FlowError>
  /** Project the current applied state (eventual: the local apply-prefix). */
  readonly readEventual: <A>(project: (state: S, applied: number) => A) => Effect.Effect<A, FlowError>
  /** Linearizable read: `checkTail` then defer until `applied ≥ tail`, then project. */
  readonly readStrong: <A>(project: (state: S, applied: number) => A) => Effect.Effect<A, FlowError>
  /** The current applied-prefix cursor (next seq-num to apply). */
  readonly applied: Effect.Effect<number>
  /** Post-apply notifications, one per folded record (drives Layer 7 resume). */
  readonly changes: Stream.Stream<S2Record>
}

// ── internals ────────────────────────────────────────────────────────────────

interface StrongCmd<S> {
  readonly _tag: "ReadStrong"
  readonly atTail: number
  readonly project: (state: S, applied: number) => unknown
  readonly reply: Deferred.Deferred<unknown, FlowError>
}
interface EventualCmd<S> {
  readonly _tag: "ReadEventual"
  readonly project: (state: S, applied: number) => unknown
  readonly reply: Deferred.Deferred<unknown, FlowError>
}
type Cmd<S> = StrongCmd<S> | EventualCmd<S>

interface WriteCmd {
  readonly records: ReadonlyArray<AppendRecord>
  readonly reply: Deferred.Deferred<AppendAck, FlowError>
}

interface OwnAck {
  readonly k: "ownAck"
  readonly records: ReadonlyArray<S2Record>
  readonly end: number
  readonly reply: Deferred.Deferred<AppendAck, FlowError>
  readonly ack: AppendAck
}

type Ev<S> =
  | { readonly k: "rec"; readonly r: S2Record }
  | { readonly k: "cmd"; readonly c: Cmd<S> }
  | OwnAck

const resolveConfig = (config: OrchestratorConfig | undefined) => ({
  commandCapacity: config?.commandCapacity ?? 256,
  writeCapacity: config?.writeCapacity ?? 256,
  changesCapacity: config?.changesCapacity ?? 256,
  readDeadline: config?.readDeadline ?? Duration.seconds(30)
})

/** Reconstruct the records of an own batch from the ack-assigned seq-nums. */
const ownRecordsOf = (records: ReadonlyArray<AppendRecord>, ack: AppendAck): ReadonlyArray<S2Record> => {
  const base = ack.start.seqNum
  const ts = ack.start.timestamp instanceof Date ? ack.start.timestamp.getTime() : 0
  return records.map((record, index) => ({
    seqNum: base + index,
    timestamp: ts,
    headers: (record.headers ?? []) as ReadonlyArray<readonly [string, string]>,
    body: typeof record.body === "string" ? record.body : ""
  }))
}

/**
 * The shared engine. Both surfaces are demand-built from this single,
 * dedup-by-seq-num apply path — which is precisely what makes own writes apply
 * **exactly once** regardless of whether the ack or the tail reader reaches a
 * record first (the SDD's "tail reader never double-applies own records"):
 *
 * - `fenced` (OwnedOrchestrator): every write carries the token; the writer
 *   hands the ack back into the loop as an `ownAck`, whose records are folded
 *   in stream order and whose reply resolves only *after* that fold (RYW).
 * - unfenced (ViewOrchestrator): writes are plain appends, the reply resolves on
 *   durable ack, and every record (including own) is folded purely on tail.
 *
 * A record is applied iff it lands at the contiguous `applied` cursor; any copy
 * with `seqNum < applied` is already folded and skipped, so the two ingress
 * paths can never double-apply.
 */
const make = <S>(
  options: OwnedOrchestratorOptions<S>,
  fenced: boolean
): Effect.Effect<Orchestrator<S>, FlowError, Scope.Scope | S2Client> =>
  Effect.gen(function*() {
    const client = yield* S2Client
    const cfg = resolveConfig(options.config)
    const stream = options.stream
    const fromCursor = options.fromCursor ?? 0

    // Ensure the stream exists before tailing or writing — otherwise the tail
    // reader and the first append race to auto-create it, which surfaces a
    // spurious 404 (the same reason `StreamDb.open` creates up front). An
    // already-existing stream (409) is the normal case for a re-instantiated
    // owner; any other failure is a real defect.
    yield* client.createStream({ stream }).pipe(
      Effect.asVoid,
      Effect.catch((cause) =>
        isS2Conflict(cause) ? Effect.void : Effect.fail(new FlowError({ reason: "open", stream, cause }))
      )
    )

    const writes = yield* Queue.bounded<WriteCmd>(cfg.writeCapacity)
    const command = yield* Queue.bounded<Cmd<S>>(cfg.commandCapacity)
    const ownAcks = yield* Queue.unbounded<OwnAck>()
    const changes = yield* PubSub.dropping<S2Record>(cfg.changesCapacity)
    const appliedRef = yield* Ref.make(fromCursor)

    // owned by the single consumer fiber below — never touched off it.
    let state = options.initial
    let applied = fromCursor
    const buffer = new Map<number, S2Record>()
    const ownReplies: Array<{
      readonly end: number
      readonly reply: Deferred.Deferred<AppendAck, FlowError>
      readonly ack: AppendAck
    }> = []
    const strongPending: Array<StrongCmd<S>> = []

    const applyRecord = (record: S2Record) =>
      Effect.gen(function*() {
        state = options.reduce(state, record)
        applied = record.seqNum + 1
        yield* Ref.set(appliedRef, applied)
        yield* PubSub.publish(changes, record)
      })

    const drain = Effect.gen(function*() {
      // fold contiguous records at the apply cursor.
      let next = buffer.get(applied)
      while (next !== undefined) {
        buffer.delete(applied)
        yield* applyRecord(next)
        next = buffer.get(applied)
      }
      // resolve own-write replies whose batch is fully folded (read-your-writes).
      for (let i = ownReplies.length - 1; i >= 0; i--) {
        const waiter = ownReplies[i]!
        if (waiter.end <= applied) {
          ownReplies.splice(i, 1)
          yield* Deferred.succeed(waiter.reply, waiter.ack)
        }
      }
      // resolve strong reads whose target tail is now applied.
      for (let i = strongPending.length - 1; i >= 0; i--) {
        const waiter = strongPending[i]!
        if (waiter.atTail <= applied) {
          strongPending.splice(i, 1)
          yield* Deferred.succeed(waiter.reply, waiter.project(state, applied))
        }
      }
    })

    const bufferRecord = (record: S2Record) => {
      if (record.seqNum >= applied && !buffer.has(record.seqNum)) buffer.set(record.seqNum, record)
    }

    const handle = (ev: Ev<S>) =>
      Effect.gen(function*() {
        switch (ev.k) {
          case "rec": {
            bufferRecord(ev.r)
            yield* drain
            break
          }
          case "ownAck": {
            ev.records.forEach(bufferRecord)
            ownReplies.push({ end: ev.end, reply: ev.reply, ack: ev.ack })
            yield* drain
            break
          }
          case "cmd": {
            const c = ev.c
            if (c._tag === "ReadEventual") {
              yield* Deferred.succeed(c.reply, c.project(state, applied))
            } else if (c.atTail <= applied) {
              yield* Deferred.succeed(c.reply, c.project(state, applied))
            } else {
              strongPending.push(c)
            }
            break
          }
        }
      })

    // every owner write carries the fencing token (cooperative fencing); a view
    // writes unfenced.
    const appendOptions = fenced && options.fencingToken !== undefined
      ? { fencingToken: options.fencingToken }
      : undefined

    // the writer fiber: drains the write mailbox in submission order (one ack
    // awaited before the next ⇒ durable order = submit order).
    yield* Stream.fromQueue(writes).pipe(
      Stream.runForEach((w) =>
        client.append(stream, AppendInput.create(w.records, appendOptions)).pipe(
          Effect.matchCauseEffect({
            onFailure: (cause) => Deferred.fail(w.reply, new FlowError({ reason: "write", stream, cause })),
            onSuccess: (ack) =>
              fenced
                ? Queue.offer(ownAcks, {
                  k: "ownAck",
                  records: ownRecordsOf(w.records, ack),
                  end: ack.tail.seqNum,
                  reply: w.reply,
                  ack
                })
                : Deferred.succeed(w.reply, ack)
          })
        )),
      Effect.forkScoped
    )

    // the tailing reader: a long-poll loop of bounded reads from the apply
    // cursor, feeding a queue. Unlike a long-lived read session, each bounded
    // read returns on its own — there is no session to `cancel()` on scope
    // teardown, so the orchestrator tears down promptly (the same reason the
    // owner-stream tail in the durable engine reads in bounded batches).
    const tailRecords = yield* Queue.unbounded<S2Record>()
    const pollTail = (cursor: number): Effect.Effect<number, never> =>
      client.readBatch(stream, { start: { from: { seqNum: cursor }, clamp: true }, stop: { waitSecs: 0 } }).pipe(
        Effect.flatMap((batch) =>
          Effect.forEach(batch.records, (r) =>
            Queue.offer(tailRecords, {
              seqNum: r.seqNum,
              timestamp: r.timestamp.getTime(),
              headers: r.headers,
              body: r.body
            }), { discard: true }).pipe(
              Effect.flatMap(() => {
                const last = batch.records[batch.records.length - 1]
                // an empty read (idle, or a backend ignoring `waitSecs`): a small
                // pause prevents a hot loop; otherwise advance past the batch.
                return last === undefined
                  ? Effect.as(Effect.sleep("20 millis"), cursor)
                  : Effect.succeed(last.seqNum + 1)
              })
            )
        ),
        // a not-yet-created stream (404) or a read at the tail (416) just means
        // "nothing yet" — pause and retry from the same cursor.
        Effect.catch(() => Effect.as(Effect.sleep("50 millis"), cursor)),
        Effect.flatMap(pollTail)
      )
    yield* pollTail(fromCursor).pipe(Effect.forkScoped)

    // the select!: commands ⊕ own-acks ⊕ tail records, one consumer fiber over
    // queues only (no long-lived session in the merged stream).
    const recordStream = Stream.fromQueue(tailRecords).pipe(Stream.map((r): Ev<S> => ({ k: "rec", r })))
    const commandStream = Stream.fromQueue(command).pipe(Stream.map((c): Ev<S> => ({ k: "cmd", c })))
    const ackStream: Stream.Stream<Ev<S>> = Stream.fromQueue(ownAcks)

    yield* recordStream.pipe(
      Stream.merge(commandStream),
      Stream.merge(ackStream),
      Stream.runForEach(handle),
      Effect.forkScoped
    )

    const deadlined = <A>(effect: Effect.Effect<A, FlowError>): Effect.Effect<A, FlowError> =>
      effect.pipe(
        Effect.timeout(cfg.readDeadline),
        Effect.catchTag("TimeoutError", () => Effect.fail(new FlowError({ reason: "readTimeout", stream })))
      )

    const write = (records: ReadonlyArray<AppendRecord>): Effect.Effect<AppendAck, FlowError> =>
      Effect.gen(function*() {
        const reply = yield* Deferred.make<AppendAck, FlowError>()
        yield* Queue.offer(writes, { records, reply })
        return yield* Deferred.await(reply).pipe(deadlined)
      })

    const readEventual = <A>(project: (state: S, applied: number) => A): Effect.Effect<A, FlowError> =>
      Effect.gen(function*() {
        const reply = yield* Deferred.make<unknown, FlowError>()
        yield* Queue.offer(command, { _tag: "ReadEventual", project, reply })
        return (yield* Deferred.await(reply).pipe(deadlined)) as A
      })

    const readStrong = <A>(project: (state: S, applied: number) => A): Effect.Effect<A, FlowError> =>
      Effect.gen(function*() {
        // a not-yet-created stream has tail 0 — fold the initial state, don't fail.
        const atTail = yield* client.checkTail(stream).pipe(
          Effect.map((tail) => tail.tail.seqNum),
          Effect.catch((cause) =>
            isS2NotFound(cause)
              ? Effect.succeed(0)
              : Effect.fail(new FlowError({ reason: "read", stream, cause }))
          )
        )
        const reply = yield* Deferred.make<unknown, FlowError>()
        yield* Queue.offer(command, { _tag: "ReadStrong", atTail, project, reply })
        return (yield* Deferred.await(reply).pipe(deadlined)) as A
      })

    return {
      write,
      readEventual,
      readStrong,
      applied: Ref.get(appliedRef),
      changes: Stream.fromPubSub(changes)
    }
  })

// ── constructors ─────────────────────────────────────────────────────────────

/**
 * Fenced single-writer (apply-on-ack, in stream order ⇒ read-your-writes).
 * One per key; the single active writer for its stream. Backs Processors and
 * exclusive object handlers (Layer 4/10).
 */
export const makeOwned = <S>(
  options: OwnedOrchestratorOptions<S>
): Effect.Effect<Orchestrator<S>, FlowError, Scope.Scope | S2Client> => make(options, true)

/**
 * Multi-primary (apply-on-tail, eventual reads; `readStrong` is the linearizable
 * escape hatch). N replicas run it concurrently over one stream. Backs
 * TableViews and shared object reads (Layer 3).
 */
export const makeView = <S>(
  options: OrchestratorOptions<S>
): Effect.Effect<Orchestrator<S>, FlowError, Scope.Scope | S2Client> => make(options, false)
