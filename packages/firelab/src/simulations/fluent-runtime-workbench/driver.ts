import { FiregridConfig } from "../../config.ts"
import { Effect, Schedule } from "effect"

const sessionId = "fluent-workbench-session"
const turnId = "fluent-workbench-turn"
const waitId = "review-wait"
const reviewKey = "reviews/review-delivery-1"

const streamPath = (
  namespace: string,
  segments: ReadonlyArray<string>,
): string =>
  [
    namespace,
    ...segments,
  ].map(encodeURIComponent).join("/")

const sessionStreamPath = (namespace: string): string =>
  streamPath(namespace, ["sessions", sessionId])

const turnStreamPath = (namespace: string): string =>
  streamPath(namespace, ["sessions", sessionId, "turns", turnId])

interface ReadStreamResult {
  readonly events: ReadonlyArray<Record<string, unknown>>
  readonly streamClosed: boolean
}

interface FluentRuntimeWorkbenchResult {
  readonly sessionEvents: number
  readonly turnEvents: number
  readonly matched: boolean
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
      return {
        events: parsed.filter(isRecord),
        streamClosed: response.headers.get("stream-closed") === "true",
      }
    },
    catch: (cause) => cause instanceof Error ? cause : new Error(String(cause)),
  })

const retryUntil = <A>(
  effect: Effect.Effect<A, Error>,
  predicate: (value: A) => boolean,
  message: string,
): Effect.Effect<A, Error> =>
  effect.pipe(
    Effect.flatMap(value =>
      predicate(value)
        ? Effect.succeed(value)
        : Effect.fail(new Error(message))),
    Effect.retry({
      // Firelab driver-only observation wait; product wakeup is exercised host-side.
      // eslint-disable-next-line local/no-fixed-polling
      schedule: Schedule.spaced("100 millis").pipe(
        // eslint-disable-next-line local/no-fixed-polling
        Schedule.intersect(Schedule.recurs(50)),
      ),
    }),
  )

const hasReviewEvent = (
  read: ReadStreamResult,
): boolean =>
  read.events.some(event =>
    event["type"] === "review.posted" && event["key"] === reviewKey)

const hasWaitMatch = (
  read: ReadStreamResult,
): boolean =>
  read.events.some(event =>
    event["type"] === "turn.wait_matched" &&
    event["sessionId"] === sessionId &&
    event["turnId"] === turnId &&
    event["waitId"] === waitId)

export const driver: Effect.Effect<FluentRuntimeWorkbenchResult, Error, FiregridConfig> =
  Effect.gen(function*() {
    const config = yield* FiregridConfig
    if (config.durableStreamsBaseUrl === undefined || config.namespace === undefined) {
      return yield* Effect.fail(
        new Error("fluent-runtime-workbench requires durableStreamsBaseUrl and namespace"),
      )
    }
    const session = yield* retryUntil(
      readStream(config.durableStreamsBaseUrl, sessionStreamPath(config.namespace)),
      hasReviewEvent,
      "host did not append the review event to the session stream",
    )
    const turn = yield* retryUntil(
      readStream(config.durableStreamsBaseUrl, turnStreamPath(config.namespace)),
      hasWaitMatch,
      "host did not resolve the registered wait on the turn stream",
    )
    const matched = hasWaitMatch(turn)
    yield* Effect.annotateCurrentSpan({
      "fluent_runtime_workbench.session_events": session.events.length,
      "fluent_runtime_workbench.turn_events": turn.events.length,
      "fluent_runtime_workbench.session_has_review": hasReviewEvent(session),
      "fluent_runtime_workbench.turn_has_wait_match": matched,
      "fluent_runtime_workbench.turn_closed": turn.streamClosed,
    })
    return {
      sessionEvents: session.events.length,
      turnEvents: turn.events.length,
      matched,
    }
  }).pipe(
    Effect.withSpan("firelab.fluent_runtime_workbench.driver", {
      attributes: {
        "firegrid.bead": "tf-0ura",
        "firegrid.simulation.intent": "fluent-runtime-host-workbench",
      },
    }),
  )
