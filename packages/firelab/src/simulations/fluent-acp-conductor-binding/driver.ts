import { FiregridConfig } from "../../config.ts"
import { Effect, Schedule } from "effect"

// Airgapped from host/scenario code (firelab driver rule: import only
// @firegrid/client-sdk + effect). These mirror scenario.ts by convention — the
// driver observes the durable stream independently, knowing only the product
// contract (session path + recorded event names), not host internals.
const SESSION_ID = "fluent-acp-conductor-session"
const PROMPT_EVENT = "acp/prompt.accepted"
const CANCEL_EVENT = "acp/session.cancelled"

const sessionStreamPath = (namespace: string): string =>
  [namespace, "sessions", SESSION_ID].map(encodeURIComponent).join("/")

interface ReadStreamResult {
  readonly events: ReadonlyArray<Record<string, unknown>>
}

interface ConductorBindingResult {
  readonly sessionEvents: number
  readonly sessionCreated: boolean
  readonly promptAccepted: boolean
  readonly cancellationRecorded: boolean
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

const hasSessionCreated = (read: ReadStreamResult): boolean =>
  read.events.some((event) => event["type"] === "session.created")

const hasAppendedEvent = (read: ReadStreamResult, name: string): boolean =>
  read.events.some((event) =>
    event["type"] === "session.event_appended" && event["name"] === name)

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
      // Firelab driver-only observation wait; the product write path is
      // exercised host-side through the conductor binding.
      // eslint-disable-next-line local/no-fixed-polling
      schedule: Schedule.spaced("100 millis").pipe(
        // eslint-disable-next-line local/no-fixed-polling
        Schedule.intersect(Schedule.recurs(50)),
      ),
    }),
  )

export const driver: Effect.Effect<ConductorBindingResult, Error, FiregridConfig> = Effect.gen(
  function*() {
    const config = yield* FiregridConfig
    if (config.durableStreamsBaseUrl === undefined || config.namespace === undefined) {
      return yield* Effect.fail(
        new Error("fluent-acp-conductor-binding requires durableStreamsBaseUrl and namespace"),
      )
    }
    const path = sessionStreamPath(config.namespace)

    // Wait until all three editor ACP calls have become durable session facts.
    const session = yield* retryUntil(
      readStream(config.durableStreamsBaseUrl, path),
      (read) =>
        hasSessionCreated(read) &&
        hasAppendedEvent(read, PROMPT_EVENT) &&
        hasAppendedEvent(read, CANCEL_EVENT),
      "conductor did not append session.created + prompt.accepted + session.cancelled to the session stream",
    )

    const result: ConductorBindingResult = {
      sessionEvents: session.events.length,
      sessionCreated: hasSessionCreated(session),
      promptAccepted: hasAppendedEvent(session, PROMPT_EVENT),
      cancellationRecorded: hasAppendedEvent(session, CANCEL_EVENT),
    }
    yield* Effect.annotateCurrentSpan({
      "fluent_acp_conductor_binding.session_events": result.sessionEvents,
      "fluent_acp_conductor_binding.session_created": result.sessionCreated,
      "fluent_acp_conductor_binding.prompt_accepted": result.promptAccepted,
      "fluent_acp_conductor_binding.cancellation_recorded": result.cancellationRecorded,
    })
    return result
  },
).pipe(
  Effect.withSpan("firelab.fluent_acp_conductor_binding.driver", {
    attributes: {
      "firegrid.bead": "tf-v2nv",
      "firegrid.simulation.intent": "fluent-acp-conductor-editor-binding",
    },
  }),
)
