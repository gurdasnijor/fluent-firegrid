import { FiregridConfig } from "../../config.ts"
import { Effect, Schedule } from "effect"

const sessionId = "fluent-durable-wait-session"
const reviewTurnId = "fluent-durable-wait-review-turn"
const mainTurnId = "fluent-durable-wait-main-turn"
const consumerId = "fluent-durable-wait-consumer"
const completionFact = "fluent.durable_wait.witness.complete"

interface DurableWaitObservation {
  readonly sessionRows: number
  readonly reviewTurnRows: number
  readonly mainTurnRows: number
  readonly wakeRows: number
  readonly consumerOffset: string
  readonly completionObserved: boolean
}

const pathFrom = (
  namespace: string,
  parts: ReadonlyArray<string>,
): string =>
  [
    namespace,
    ...parts,
  ].map(encodeURIComponent).join("/")

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const field = (
  row: Record<string, unknown>,
  key: string,
): unknown => row[key]

const readStream = (
  baseUrl: string,
  path: string,
): Effect.Effect<ReadonlyArray<Record<string, unknown>>, Error> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${baseUrl}/v1/stream/${path}?offset=-1`)
      if (!response.ok) {
        throw new Error(`read ${path} failed with ${response.status}: ${await response.text()}`)
      }
      const body: unknown = await response.json()
      if (!Array.isArray(body)) {
        throw new Error(`read ${path} returned a non-array payload`)
      }
      return body.filter(isRecord)
    },
    catch: cause => cause instanceof Error ? cause : new Error(String(cause)),
  })

const readConsumer = (
  baseUrl: string,
): Effect.Effect<Record<string, unknown>, Error> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${baseUrl}/consumers/${consumerId}`)
      if (!response.ok) {
        throw new Error(`consumer read failed with ${response.status}: ${await response.text()}`)
      }
      const body: unknown = await response.json()
      if (!isRecord(body)) {
        throw new Error("consumer endpoint returned a non-object payload")
      }
      return body
    },
    catch: cause => cause instanceof Error ? cause : new Error(String(cause)),
  })

const offsetFor = (
  consumer: Record<string, unknown>,
  path: string,
): string => {
  const streams = field(consumer, "streams")
  if (!Array.isArray(streams)) return ""
  const stream = streams.filter(isRecord).find(row => field(row, "path") === path)
  return stream === undefined ? "" : String(field(stream, "offset"))
}

const hasSessionEvent = (
  rows: ReadonlyArray<Record<string, unknown>>,
  name: string,
): boolean =>
  rows.some(row =>
    field(row, "type") === "session.event_appended" &&
    field(row, "name") === name,
  )

const observe = (
  baseUrl: string,
  namespace: string,
): Effect.Effect<DurableWaitObservation, Error> =>
  Effect.gen(function*() {
    const sessionRows = yield* readStream(baseUrl, pathFrom(namespace, ["sessions", sessionId]))
    const completionObserved = hasSessionEvent(sessionRows, completionFact)
    if (!completionObserved) {
      return yield* Effect.fail(new Error("completion fact not observed yet"))
    }

    const reviewTurnRows = yield* readStream(baseUrl, pathFrom(namespace, [
      "sessions",
      sessionId,
      "turns",
      reviewTurnId,
    ]))
    const mainTurnRows = yield* readStream(baseUrl, pathFrom(namespace, [
      "sessions",
      sessionId,
      "turns",
      mainTurnId,
    ]))
    const wakeRows = yield* readStream(baseUrl, pathFrom(namespace, ["fluent-durable-wait", "wake"]))
    const consumer = yield* readConsumer(baseUrl)
    return {
      sessionRows: sessionRows.length,
      reviewTurnRows: reviewTurnRows.length,
      mainTurnRows: mainTurnRows.length,
      wakeRows: wakeRows.length,
      consumerOffset: offsetFor(
        consumer,
        `/v1/stream/${pathFrom(namespace, ["fluent-durable-wait", "work"])}`,
      ),
      completionObserved,
    }
  }).pipe(
    Effect.retry({
      // Driver observation wait only; the coverage oracle judges host-substrate spans.
      // eslint-disable-next-line local/no-fixed-polling
      schedule: Schedule.spaced("100 millis").pipe(
        // eslint-disable-next-line local/no-fixed-polling
        Schedule.intersect(Schedule.recurs(50)),
      ),
    }),
  )

export const driver: Effect.Effect<DurableWaitObservation, Error, FiregridConfig> =
  Effect.gen(function*() {
    const config = yield* FiregridConfig
    if (config.durableStreamsBaseUrl === undefined || config.namespace === undefined) {
      return yield* Effect.fail(
        new Error("fluent-durable-wait requires durableStreamsBaseUrl and namespace"),
      )
    }
    const observation = yield* observe(config.durableStreamsBaseUrl, config.namespace)
    yield* Effect.annotateCurrentSpan({
      "firegrid.fluent_durable_wait.session_rows": observation.sessionRows,
      "firegrid.fluent_durable_wait.review_turn_rows": observation.reviewTurnRows,
      "firegrid.fluent_durable_wait.main_turn_rows": observation.mainTurnRows,
      "firegrid.fluent_durable_wait.wake_rows": observation.wakeRows,
      "firegrid.fluent_durable_wait.consumer_offset": observation.consumerOffset,
      "firegrid.fluent_durable_wait.completion_observed": observation.completionObserved,
    })
    return observation
  }).pipe(
    Effect.withSpan("firelab.fluent_durable_wait.driver", {
      attributes: {
        "firegrid.bead": "tf-g5wz",
        "firegrid.simulation.intent": "fluent-durable-wait",
      },
    }),
  )
