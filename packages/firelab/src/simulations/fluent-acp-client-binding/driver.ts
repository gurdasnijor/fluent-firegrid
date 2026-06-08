import { FiregridConfig } from "../../config.ts"
import { Effect, Schedule } from "effect"

const sessionId = "fluent-acp-client-binding-session"
const expectedFactNames = [
  "acp.session_update",
  "acp.request_permission",
  "acp.permission_result",
  "acp.ext_method",
  "acp.ext_method.result",
] as const

interface FluentAcpClientBindingResult {
  readonly sessionId: string
  readonly eventNames: ReadonlyArray<string>
}

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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const field = (
  value: Record<string, unknown>,
  key: string,
): unknown => value[key]

const appendedEvents = (
  events: ReadonlyArray<Record<string, unknown>>,
): ReadonlyArray<Record<string, unknown>> =>
  events.filter(event => field(event, "type") === "session.event_appended")

const expectNames = (
  names: ReadonlyArray<string>,
): Effect.Effect<void, Error> =>
  names.join("\n") === expectedFactNames.join("\n")
    ? Effect.void
    : Effect.fail(
      new Error(
        `unexpected fluent ACP persisted event order\nexpected: ${
          expectedFactNames.join(", ")
        }\nactual: ${names.join(", ")}`,
      ),
    )

const expectPayload = (
  events: ReadonlyArray<Record<string, unknown>>,
  name: string,
  predicate: (payload: unknown) => boolean,
): Effect.Effect<void, Error> => {
  const event = events.find(candidate => field(candidate, "name") === name)
  if (event === undefined) {
    return Effect.fail(new Error(`missing persisted event ${name}`))
  }
  return predicate(field(event, "payload"))
    ? Effect.void
    : Effect.fail(new Error(`unexpected payload for persisted event ${name}`))
}

const assertPersistedFacts = (
  events: ReadonlyArray<Record<string, unknown>>,
): Effect.Effect<ReadonlyArray<string>, Error> => {
  const appended = appendedEvents(events)
  const names = appended.map(event => String(field(event, "name")))
  return Effect.gen(function*() {
    yield* expectNames(names)
    yield* expectPayload(
      appended,
      "acp.session_update",
      (payload) => {
        if (!isRecord(payload)) return false
        const update = field(payload, "update")
        if (!isRecord(update)) return false
        return field(payload, "sessionId") === sessionId &&
          field(update, "sessionUpdate") === "agent_message_chunk"
      },
    )
    yield* expectPayload(
      appended,
      "acp.request_permission",
      payload =>
        isRecord(payload) &&
        field(payload, "sessionId") === sessionId &&
        Array.isArray(field(payload, "options")),
    )
    yield* expectPayload(
      appended,
      "acp.permission_result",
      (payload) => {
        if (!isRecord(payload)) return false
        const response = field(payload, "response")
        if (!isRecord(response)) return false
        const outcome = field(response, "outcome")
        if (!isRecord(outcome)) return false
        return field(outcome, "outcome") === "selected"
      },
    )
    yield* expectPayload(
      appended,
      "acp.ext_method",
      payload =>
        isRecord(payload) &&
        field(payload, "method") === "firegrid/tool/execute",
    )
    yield* expectPayload(
      appended,
      "acp.ext_method.result",
      (payload) => {
        if (!isRecord(payload)) return false
        const result = field(payload, "result")
        return isRecord(result) && field(result, "committed") === true
      },
    )
    return names
  })
}

const readSessionEvents = (
  baseUrl: string,
  namespace: string,
): Effect.Effect<ReadonlyArray<Record<string, unknown>>, Error> =>
  Effect.tryPromise({
    try: async () => {
      const path = sessionStreamPath(namespace)
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
      return parsed.filter(isRecord)
    },
    catch: (cause) => cause instanceof Error ? cause : new Error(String(cause)),
  })

const readAssertedFacts = (
  baseUrl: string,
  namespace: string,
): Effect.Effect<ReadonlyArray<string>, Error> =>
  readSessionEvents(baseUrl, namespace).pipe(
    Effect.flatMap(assertPersistedFacts),
    Effect.retry({
      // Firelab driver-only observation wait; host writes product facts once.
      // eslint-disable-next-line local/no-fixed-polling
      schedule: Schedule.spaced("100 millis").pipe(
        // eslint-disable-next-line local/no-fixed-polling
        Schedule.intersect(Schedule.recurs(50)),
      ),
    }),
  )

export const driver: Effect.Effect<FluentAcpClientBindingResult, Error, FiregridConfig> =
  Effect.gen(function*() {
    const config = yield* FiregridConfig
    if (config.durableStreamsBaseUrl === undefined || config.namespace === undefined) {
      return yield* Effect.fail(
        new Error("fluent-acp-client-binding requires durableStreamsBaseUrl and namespace"),
      )
    }

    const names = yield* readAssertedFacts(
      config.durableStreamsBaseUrl,
      config.namespace,
    )
    yield* Effect.annotateCurrentSpan({
      "firegrid.fluent_acp_client_binding.session_id": sessionId,
      "firegrid.fluent_acp_client_binding.event_names": names.join(","),
      "firegrid.fluent_acp_client_binding.event_count": names.length,
    })
    return { sessionId, eventNames: names }
  }).pipe(
    Effect.withSpan("firelab.fluent_acp_client_binding.driver", {
      attributes: {
        "firegrid.bead": "tf-w9uc",
        "firegrid.simulation.intent": "fluent-acp-client-binding",
      },
    }),
  )
