import { FluentRuntimeLive, FluentSources, FluentStore } from "@firegrid/fluent-runtime"
import { Effect, Layer } from "effect"
import type { FirelabHost, FirelabHostEnv } from "../../types.ts"

// Sleep key (timerId) and target time "T". Explicit epoch values — never a
// wall/worker clock — so the durable timer mechanism, not a process-local sleep,
// is what carries the timer.
const SESSION_ID = "fluent-durable-sleep-session"
const TURN_ID = "fluent-durable-sleep-turn"
const SLEEP_KEY = "s1"
const FIRE_AT_EPOCH_MS = 1_000

/**
 * Durable-sleep vertical slice over the REAL fluent-runtime timer machinery
 * (Store.scheduleTurnTimer + Sources.fireDueTurnTimers → Store.fireTurnTimer)
 * against firelab's DurableStreamTestServer. No `Clock.sleep` / process-local
 * timer is the durable mechanism — time enters only as a durable append.
 *
 * Sequence (matches fluent-durable-sleep.feature):
 *  1. record TimerScheduled BEFORE the handler parks;
 *  2. [park] — nothing process-local remembers the timer;
 *  3. time "T" arrives → the timer SOURCE materializes a durable TimerFired,
 *     driven with an explicit `nowEpochMs` (never a local clock);
 *  4. post-wake re-drive resolves from the journal idempotently — firing again
 *     must NOT append a duplicate TimerFired and must NOT reschedule.
 */
const runDurableSleep = Effect.gen(function*() {
  const store = yield* FluentStore
  const sources = yield* FluentSources

  yield* store.createSession({
    sessionId: SESSION_ID,
    agent: "firelab-fluent-durable-sleep",
  })
  yield* store.startTurn({
    sessionId: SESSION_ID,
    turnId: TURN_ID,
    prompt: "sleep until T",
  })

  // (1) Timer intent recorded BEFORE the park.
  yield* store.scheduleTurnTimer({
    sessionId: SESSION_ID,
    turnId: TURN_ID,
    timerId: SLEEP_KEY,
    fireAtEpochMs: FIRE_AT_EPOCH_MS,
  })

  // (2) [park] then time "T" arrives — the timer source appends a durable
  // TimerFired. The source reads the journal, so nothing process-local bridged
  // the gap between schedule and fire.
  yield* sources.fireDueTurnTimers({
    sessionId: SESSION_ID,
    turnId: TURN_ID,
    nowEpochMs: FIRE_AT_EPOCH_MS,
  })

  // (3) Post-wake re-drive (simulated restart/replay): resolves from the journal.
  // The already-fired timer must NOT fire again and must NOT reschedule.
  yield* sources.fireDueTurnTimers({
    sessionId: SESSION_ID,
    turnId: TURN_ID,
    nowEpochMs: FIRE_AT_EPOCH_MS + 5,
  })
}).pipe(
  Effect.withSpan("firegrid.sim.fluent_durable_sleep.host.run"),
)

export const host = (
  env: FirelabHostEnv,
): Layer.Layer<FirelabHost, unknown> =>
  Layer.scopedDiscard(
    runDurableSleep.pipe(
      Effect.provide(FluentRuntimeLive({
        durableStreamsBaseUrl: env.durableStreamsBaseUrl,
        namespace: env.namespace,
      })),
    ),
  )
