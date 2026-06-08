import { FiregridConfig } from "../../config.ts"
import { Effect, Schedule } from "effect"

// Airgapped from host code (firelab driver rule: import only @firegrid/client-sdk
// + effect). Mirrors the host scenario by the product contract — the driver
// observes the durable TURN stream independently, knowing only the sleep key,
// target time, and the recorded event shapes.
const SESSION_ID = "fluent-durable-sleep-session"
const TURN_ID = "fluent-durable-sleep-turn"
const SLEEP_KEY = "s1"
const FIRE_AT_EPOCH_MS = 1_000

const turnStreamPath = (namespace: string): string =>
  [namespace, "sessions", SESSION_ID, "turns", TURN_ID].map(encodeURIComponent).join("/")

interface ReadStreamResult {
  readonly events: ReadonlyArray<Record<string, unknown>>
}

interface DurableSleepResult {
  readonly scheduledCount: number
  readonly firedCount: number
  readonly scheduledTargetMatches: boolean
  readonly firedHasDurableFiredAt: boolean
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const readStream = (
  baseUrl: string,
  path: string,
): Effect.Effect<ReadStreamResult, Error> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${baseUrl}/v1/stream/${path}?offset=-1`, {
        method: "GET",
      })
      if (!response.ok) {
        throw new Error(`read ${path} failed with ${response.status}`)
      }
      const parsed: unknown = await response.json()
      if (!Array.isArray(parsed)) {
        throw new Error(`read ${path} returned a non-array payload`)
      }
      return { events: parsed.filter(isRecord) }
    },
    catch: (cause) => cause instanceof Error ? cause : new Error(String(cause)),
  })

const scheduledFor = (read: ReadStreamResult): ReadonlyArray<Record<string, unknown>> =>
  read.events.filter((e) =>
    e["type"] === "turn.timer_scheduled" && e["timerId"] === SLEEP_KEY)

const firedFor = (read: ReadStreamResult): ReadonlyArray<Record<string, unknown>> =>
  read.events.filter((e) =>
    e["type"] === "turn.timer_fired" && e["timerId"] === SLEEP_KEY)

const retryUntil = <A>(
  effect: Effect.Effect<A, Error>,
  predicate: (value: A) => boolean,
  message: string,
): Effect.Effect<A, Error> =>
  effect.pipe(
    Effect.flatMap((value) =>
      predicate(value) ? Effect.succeed(value) : Effect.fail(new Error(message)),
    ),
    Effect.retry({
      // Firelab driver-only observation wait; the durable timer write path is
      // exercised host-side through the timer source.
      // eslint-disable-next-line local/no-fixed-polling
      schedule: Schedule.spaced("100 millis").pipe(
        // eslint-disable-next-line local/no-fixed-polling
        Schedule.intersect(Schedule.recurs(50)),
      ),
    }),
  )

export const driver: Effect.Effect<DurableSleepResult, Error, FiregridConfig> = Effect.gen(
  function*() {
    const config = yield* FiregridConfig
    if (config.durableStreamsBaseUrl === undefined || config.namespace === undefined) {
      return yield* Effect.fail(
        new Error("fluent-durable-sleep requires durableStreamsBaseUrl and namespace"),
      )
    }
    const path = turnStreamPath(config.namespace)

    // Wait until the durable TimerFired has landed on the turn stream.
    const read = yield* retryUntil(
      readStream(config.durableStreamsBaseUrl, path),
      (r) => scheduledFor(r).length >= 1 && firedFor(r).length >= 1,
      "durable sleep did not append turn.timer_scheduled + turn.timer_fired to the turn stream",
    )

    const scheduled = scheduledFor(read)
    const fired = firedFor(read)
    const firstScheduled = scheduled[0]
    const firstFired = fired[0]

    const result: DurableSleepResult = {
      scheduledCount: scheduled.length,
      firedCount: fired.length,
      scheduledTargetMatches: firstScheduled?.["fireAtEpochMs"] === FIRE_AT_EPOCH_MS,
      // firedAt comes from the durable append, not a fabricated local clock — assert
      // it is present and is at/after the scheduled target.
      firedHasDurableFiredAt: typeof firstFired?.["firedAtEpochMs"] === "number" &&
        (firstFired["firedAtEpochMs"]) >= FIRE_AT_EPOCH_MS,
    }

    // Idempotent replay: the re-drive must NOT have produced duplicate facts.
    if (result.scheduledCount !== 1) {
      return yield* Effect.fail(
        new Error(`expected exactly 1 TimerScheduled, found ${result.scheduledCount}`),
      )
    }
    if (result.firedCount !== 1) {
      return yield* Effect.fail(
        new Error(`expected exactly 1 TimerFired after re-drive, found ${result.firedCount}`),
      )
    }
    if (!result.scheduledTargetMatches) {
      return yield* Effect.fail(new Error("TimerScheduled did not carry target time T"))
    }
    if (!result.firedHasDurableFiredAt) {
      return yield* Effect.fail(new Error("TimerFired did not carry a durable firedAt >= T"))
    }

    yield* Effect.annotateCurrentSpan({
      "fluent_durable_sleep.scheduled_count": result.scheduledCount,
      "fluent_durable_sleep.fired_count": result.firedCount,
      "fluent_durable_sleep.scheduled_target_matches": result.scheduledTargetMatches,
      "fluent_durable_sleep.fired_has_durable_fired_at": result.firedHasDurableFiredAt,
    })
    return result
  },
).pipe(
  Effect.withSpan("firelab.fluent_durable_sleep.driver", {
    attributes: {
      "firegrid.bead": "tf-eioo",
      "firegrid.simulation.intent": "fluent-durable-sleep-timer-mechanism",
    },
  }),
)
