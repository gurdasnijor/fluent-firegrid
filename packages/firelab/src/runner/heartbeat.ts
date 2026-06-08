// tf-ewo: runner heartbeat.
//
// Architecture: span-queue at the OTel/Effect boundary, all state in
// Refs, ticker via Effect.repeat(... Schedule.forever). The OTel
// SpanProcessor is a thin adapter that does exactly one thing — hand
// spans to an Effect-owned Queue. Everything else (state, formatting,
// I/O, scheduling) is Effect.
//
// Why this shape:
//   - SINGLE owner of mutable state (Refs, all Effect-managed). No
//     class-private fields drifting outside Effect's awareness.
//   - Span consumption is a Stream — composable. Adding metrics /
//     filters / per-event side channels is `Stream.tap` on the chain,
//     not new methods on a class.
//   - Adaptive interval is a Ref<number> mutated inside the tick effect
//     based on activity-this-tick. State machine in one place; schedule
//     stays a pure `Schedule.forever`.
//   - Backpressure for free via the Queue: if the heartbeat's ticker
//     can't keep up, the queue buffers; the OTel exporter pipeline is
//     never blocked by anything beyond a `Queue.unsafeOffer`.
//   - Per-event line under `--watch` is a conditional Stream.tap arm —
//     composes naturally with the per-event state update.
//   - Final digest on scope close lives in an `Effect.addFinalizer` —
//     canonical "do X when scope ends" pattern; no shutdown plumbing on
//     the processor.
//
// Output volume bounded: digest line every 2-10s by default (adaptive
// doubles on idle, resets on activity). ~6 lines/min peak under LLM
// load. `--watch` adds one compact line per span (interactive only).

import {
  Clock,
  Console,
  Duration,
  Effect,
  Queue,
  Ref,
  Schedule,
  Stream,
} from "effect"
import type * as Scope from "effect/Scope"
import type {
  ReadableSpan,
  SpanProcessor,
} from "../observability/node.ts"
import { nsFromHrTime, nsToMs } from "./trace.ts"

interface HeartbeatOptions {
  /**
   * Minimum interval between digest lines. Heartbeat starts at this
   * interval; doubles on consecutive idle ticks up to `maxInterval`;
   * resets to this floor on activity.
   */
  readonly minInterval: Duration.DurationInput
  /**
   * Maximum interval the adaptive backoff will climb to.
   */
  readonly maxInterval: Duration.DurationInput
  /**
   * If true, emit a compact one-line summary per span (in addition to
   * the periodic digest). Useful for `--watch`-style interactive
   * debugging.
   */
  readonly perEvent: boolean
}

interface HeartbeatState {
  readonly totalSpans: number
  readonly sides: ReadonlyMap<string, number>
  readonly lastSpanName: string | undefined
  readonly lastSpanEndMs: number
}

const emptyState = (nowMs: number): HeartbeatState => ({
  totalSpans: 0,
  sides: new Map<string, number>(),
  lastSpanName: undefined,
  lastSpanEndMs: nowMs,
})

const pad2 = (n: number): string => n.toString().padStart(2, "0")
const formatElapsed = (ms: number): string => {
  const totalSec = Math.floor(ms / 1000)
  const min = Math.floor(totalSec / 60)
  const sec = totalSec % 60
  return `${pad2(min)}:${pad2(sec)}`
}

const formatSides = (sides: ReadonlyMap<string, number>): string => {
  if (sides.size === 0) return "{}"
  return "{" + [...sides.entries()]
    .sort((left, right) => right[1] - left[1])
    .map(([key, value]) => `${key}=${value}`)
    .join(",") + "}"
}

const incrementSide = (
  sides: ReadonlyMap<string, number>,
  side: string,
): ReadonlyMap<string, number> => {
  const next = new Map(sides)
  next.set(side, (next.get(side) ?? 0) + 1)
  return next
}

const updateStateForSpan = (
  state: HeartbeatState,
  span: ReadableSpan,
  nowMs: number,
): HeartbeatState => {
  const side = span.attributes["firegrid.side"]
  return {
    totalSpans: state.totalSpans + 1,
    sides: typeof side === "string"
      ? incrementSide(state.sides, side)
      : state.sides,
    lastSpanName: span.name,
    lastSpanEndMs: nowMs,
  }
}

// `--watch` per-event line. Compact format mirrors the digest's
// vocabulary (elapsed-since-start prefix + side tag) so operators don't
// have to learn two formats.
const formatEventLine = (
  span: ReadableSpan,
  startMs: number,
  nowMs: number,
): string => {
  const elapsedMs = nowMs - startMs
  const side = (span.attributes["firegrid.side"] as string | undefined) ?? "-"
  const durationMs = nsToMs(nsFromHrTime(span.duration))
  return `[${formatElapsed(elapsedMs)}] [${side}] ${span.name} (${durationMs.toFixed(1)}ms)`
}

const formatDigestLine = (input: {
  readonly snapshot: HeartbeatState
  readonly delta: number
  readonly nowMs: number
  readonly startMs: number
  readonly currentIntervalMs: number
}): string => {
  const { snapshot, delta, nowMs, startMs, currentIntervalMs } = input
  const elapsedMs = nowMs - startMs
  const idleMs = nowMs - snapshot.lastSpanEndMs
  const idleSec = Math.floor(idleMs / 1000)
  // Threshold = 2× current interval so the ⚠ marker lags activity
  // changes appropriately — a single missed tick doesn't trigger;
  // sustained silence does.
  const idleMarker = (delta === 0 && idleMs >= currentIntervalMs * 2)
    ? `  ⚠ idle ${idleSec}s`
    : ""
  const lastInfo = snapshot.lastSpanName !== undefined
    ? `last=${snapshot.lastSpanName} +${(idleMs / 1000).toFixed(1)}s`
    : "last=<none>"
  return `[${formatElapsed(elapsedMs)}] spans=${snapshot.totalSpans} (+${delta})  sides=${formatSides(snapshot.sides)}  ${lastInfo}${idleMarker}`
}

// The OTel seam: a thin SpanProcessor that does exactly one thing —
// hand spans to an Effect-owned Queue. No state, no formatting, no I/O.
// Lifecycle methods are no-ops; final emit is handled by an Effect
// finalizer in makeHeartbeat's scope.
const makeQueueProcessor = (
  queue: Queue.Queue<ReadableSpan>,
): SpanProcessor => ({
  onStart: () => {},
  onEnd: (span: ReadableSpan) => {
    Queue.unsafeOffer(queue, span)
  },
  forceFlush: () => Promise.resolve(),
  shutdown: () => Promise.resolve(),
})

// Returns `Effect<..., never, Scope>` deliberately — the heartbeat's
// forked ticker/stream fibers and final-digest finalizer attach to the
// CALLER's scope (the runner's outer `Effect.scoped` envelope). Wrapping
// in `Effect.scoped` here would close the scope on `return`, cancelling
// the very fibers we just forked.
export const makeHeartbeat = (
  options: HeartbeatOptions,
): Effect.Effect<{ readonly processor: SpanProcessor }, never, Scope.Scope> =>
  Effect.gen(function*() {
    const startMs = yield* Clock.currentTimeMillis
    const minIntervalMs = Duration.toMillis(Duration.decode(options.minInterval))
    const maxIntervalMs = Duration.toMillis(Duration.decode(options.maxInterval))

    const queue = yield* Queue.unbounded<ReadableSpan>()
    const stateRef = yield* Ref.make(emptyState(startMs))
    const sinceLastDigest = yield* Ref.make(0)
    const intervalRef = yield* Ref.make(minIntervalMs)

    // Consume spans: update state + delta counter; optionally emit
    // per-event line. Forked into scope — dies cleanly on shutdown.
    yield* Stream.fromQueue(queue).pipe(
      Stream.tap(span =>
        Effect.gen(function*() {
          const nowMs = yield* Clock.currentTimeMillis
          yield* Ref.update(stateRef, state =>
            updateStateForSpan(state, span, nowMs))
          yield* Ref.update(sinceLastDigest, n => n + 1)
          if (options.perEvent) {
            yield* Console.error(formatEventLine(span, startMs, nowMs))
          }
        })),
      Stream.runDrain,
      Effect.forkScoped,
    )

    // One tick: sleep current interval, then read+reset delta, emit
    // digest, adapt interval based on activity-this-tick. Sleeping
    // FIRST means no zero-state digest at t=0.
    const tick = Effect.gen(function*() {
      const currentIntervalMs = yield* Ref.get(intervalRef)
      yield* Effect.sleep(Duration.millis(currentIntervalMs))
      const delta = yield* Ref.getAndSet(sinceLastDigest, 0)
      const snapshot = yield* Ref.get(stateRef)
      const nowMs = yield* Clock.currentTimeMillis
      yield* Console.error(
        formatDigestLine({ snapshot, delta, nowMs, startMs, currentIntervalMs }),
      )
      // Adaptive backoff: reset to floor on activity, double-cap on
      // idle. Exponential-backoff applied to observation interval, not
      // retry interval.
      yield* Ref.set(
        intervalRef,
        delta > 0 ? minIntervalMs : Math.min(currentIntervalMs * 2, maxIntervalMs),
      )
    })

    yield* Effect.repeat(tick, Schedule.forever).pipe(Effect.forkScoped)

    // Final digest on scope close so the operator sees the closing
    // state. Canonical Effect "do X when scope ends" pattern.
    yield* Effect.addFinalizer(() =>
      Effect.gen(function*() {
        const delta = yield* Ref.getAndSet(sinceLastDigest, 0)
        const snapshot = yield* Ref.get(stateRef)
        const nowMs = yield* Clock.currentTimeMillis
        const currentIntervalMs = yield* Ref.get(intervalRef)
        yield* Console.error(
          formatDigestLine({ snapshot, delta, nowMs, startMs, currentIntervalMs }),
        )
      }))

    return { processor: makeQueueProcessor(queue) }
  })
