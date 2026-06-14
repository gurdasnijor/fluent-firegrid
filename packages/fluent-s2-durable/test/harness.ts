import { Effect, Layer, ManagedRuntime, Ref } from "effect"
import {
  DispatchLayer,
  S2InMemory,
  TimerHeapLayer,
  decodeRecord,
  makeWorker,
  type Dispatch,
  type JournalRecord,
  type S2,
  type S2Service,
  type TimerHeap,
  type Worker,
  type WorkerConfig,
} from "../src/index.ts"

/**
 * §9 fault-injection harness. A named kill-point wraps the shared S2 `append`:
 * when armed and a matching record is about to be written it optionally lets the
 * write *land* (modelling "appended-but-not-acked") or not ("executed-but-not-
 * appended"), signals the test, then hangs (`Effect.never`) until the worker's
 * runtime is disposed — modelling abrupt process death mid-operation.
 */
export interface FaultPredicate {
  readonly onStream: (stream: string) => boolean
  readonly onRecord: (rec: JournalRecord | null) => boolean
  /** true = commit the append before crashing (appended-but-not-acked). */
  readonly landBeforeCrash: boolean
}

export interface FaultyS2 {
  readonly service: S2Service
  readonly arm: (predicate: FaultPredicate) => Effect.Effect<void>
  /** Resolves when the armed kill-point fires (the worker is then hung). */
  readonly crashed: Promise<void>
}

export const makeFaultyS2: Effect.Effect<FaultyS2> = Effect.gen(function* () {
  const base = yield* S2InMemory.make
  const armed = yield* Ref.make<FaultPredicate | null>(null)
  let signalCrash: () => void = () => {}
  const crashed = new Promise<void>((resolve) => {
    signalCrash = resolve
  })

  const append: S2Service["append"] = (stream, records, opts) =>
    Effect.gen(function* () {
      const predicate = yield* Ref.get(armed)
      if (predicate !== null && predicate.onStream(stream)) {
        const first =
          records.length > 0
            ? yield* decodeRecord(records[0]!).pipe(Effect.orElseSucceed(() => null))
            : null
        if (predicate.onRecord(first)) {
          yield* Ref.set(armed, null) // one-shot
          if (predicate.landBeforeCrash) {
            yield* base.append(stream, records, opts)
          }
          yield* Effect.sync(() => signalCrash())
          return yield* Effect.never
        }
      }
      return yield* base.append(stream, records, opts)
    })

  return {
    service: { ...base, append },
    arm: (predicate) => Ref.set(armed, predicate),
    crashed,
  }
})

const workerLayer = (
  s2Service: S2Service,
): Layer.Layer<S2 | Dispatch | TimerHeap> =>
  Layer.mergeAll(S2InMemory.layerWith(s2Service), Layer.provideMerge(TimerHeapLayer, DispatchLayer))

export interface RunningWorker<I, O> {
  readonly worker: Worker<I, O>
  readonly runtime: ManagedRuntime.ManagedRuntime<S2 | Dispatch | TimerHeap, never>
  /** Start the host pump in the background. */
  readonly run: () => void
  /** Simulate process death: dispose the runtime, interrupting every fiber. */
  readonly crash: () => Promise<void>
}

/**
 * Spin up a fresh worker (own Dispatch + TimerHeap) over a *shared* S2 store —
 * i.e. a new process pointed at the same durable journal.
 */
export const spawnWorker = async <I, O, R>(
  s2Service: S2Service,
  config: WorkerConfig<I, O, R>,
): Promise<RunningWorker<I, O>> => {
  const runtime = ManagedRuntime.make(workerLayer(s2Service))
  const worker = await runtime.runPromise(makeWorker(config))
  return {
    worker,
    runtime,
    run: () => {
      runtime.runFork(worker.runLoop)
    },
    crash: () => runtime.dispose(),
  }
}

/** Await the harness crash signal. */
export const awaitCrash = (faulty: FaultyS2): Promise<void> => faulty.crashed

/** Small real-time pause for letting durable timers fire in tests. */
export const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))
