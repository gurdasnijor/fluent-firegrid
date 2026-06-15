import { Array, Effect, Fiber, Latch, Layer, Match, Option, Ref } from "effect"
import {
  DispatchLayer,
  S2,
  TimerHeapLayer,
  decodeRecord,
  makeWorker,
  type JournalRecord,
  type S2Service,
  type Worker,
  type WorkerConfig,
} from "../src/index.ts"

/**
 * §9 fault-injection harness — Effect-native, wrapping the *real* S2 service. A
 * named kill-point wraps `append`: when armed and a matching record is about to
 * be written it optionally lets the write *land* (modelling "appended-but-not-
 * acked") or not ("executed-but-not-appended"), opens a latch the test awaits,
 * then hangs (`Effect.never`) — modelling abrupt process death mid-operation.
 * A crash is then just `Fiber.interrupt` of the worker's run-loop.
 */
export interface FaultPredicate {
  readonly onStream: (stream: string) => boolean
  readonly onRecord: (rec: Option.Option<JournalRecord>) => boolean
  /** true = commit the append before crashing (appended-but-not-acked). */
  readonly landBeforeCrash: boolean
}

export interface FaultyS2 {
  readonly service: S2Service
  readonly arm: (predicate: FaultPredicate) => Effect.Effect<void>
  /** resolves when the armed kill-point fires (the worker is then hung). */
  readonly crashed: Effect.Effect<void>
}

/** Kill when the named `Step` is about to be appended; `land` = let it commit first. */
export const killOnStep = (name: string, land: boolean): FaultPredicate => ({
  onStream: (stream) => stream.startsWith("wf/") && !stream.endsWith("/inbox"),
  onRecord: (rec) =>
    Option.match(rec, {
      onNone: () => false,
      onSome: (r) => Match.value(r).pipe(
        Match.tag("Step", (s) => s.name === name),
        Match.orElse(() => false),
      ),
    }),
  landBeforeCrash: land,
})

export const makeFaultyS2: Effect.Effect<FaultyS2, never, S2> = Effect.gen(function* () {
  const base = yield* Effect.service(S2)
  const armed = yield* Ref.make<Option.Option<FaultPredicate>>(Option.none())
  const latch = yield* Latch.make(false)

  const firstRecord = (
    records: ReadonlyArray<Uint8Array>,
  ): Effect.Effect<Option.Option<JournalRecord>> =>
    Option.match(Array.head(records), {
      onNone: () => Effect.succeed(Option.none<JournalRecord>()),
      onSome: (bytes) =>
        decodeRecord(bytes).pipe(
          Effect.map(Option.some),
          Effect.orElseSucceed(() => Option.none<JournalRecord>()),
        ),
    })

  const append: S2Service["append"] = (stream, records, opts) =>
    Effect.gen(function* () {
      const armedNow = yield* Ref.get(armed)
      return yield* Option.match(armedNow, {
        onNone: () => base.append(stream, records, opts),
        onSome: (predicate) =>
          Effect.gen(function* () {
            const rec = yield* firstRecord(records)
            const fire = predicate.onStream(stream) && predicate.onRecord(rec)
            if (!fire) return yield* base.append(stream, records, opts)
            yield* Ref.set(armed, Option.none()) // one-shot
            if (predicate.landBeforeCrash) yield* base.append(stream, records, opts)
            yield* latch.open
            return yield* Effect.never
          }),
      })
    })

  return {
    service: { ...base, append },
    arm: (predicate) => Ref.set(armed, Option.some(predicate)),
    crashed: latch.await,
  }
})

const infra = Layer.provideMerge(TimerHeapLayer, DispatchLayer)

export interface RunningWorker<I, O> {
  readonly worker: Worker<I, O>
  readonly fiber: Fiber.Fiber<never, unknown>
}

/**
 * Spin up a worker (own Dispatch + TimerHeap) over a given S2 service and fork
 * its run-loop as a structured child — i.e. a fresh process pointed at the same
 * durable journal. `Fiber.interrupt(fiber)` is the crash.
 */
export const spawnWorker = <I, O, R>(
  service: S2Service,
  config: WorkerConfig<I, O, R>,
): Effect.Effect<RunningWorker<I, O>> =>
  Effect.gen(function* () {
    const worker = yield* makeWorker(config).pipe(
      Effect.provideService(S2, service),
      Effect.provide(infra),
    )
    const fiber = yield* Effect.forkChild(worker.runLoop)
    return { worker, fiber }
  })

export const crash = <I, O>(running: RunningWorker<I, O>): Effect.Effect<void> =>
  Fiber.interrupt(running.fiber)
