import { FiregridConfig } from "../../config.ts"
import { Effect, Schedule } from "effect"

const sessionId = "fluent-worker-redrive-session"
const consumerId = "fluent-worker-redrive-consumer"

const expectedNames = [
  "fluent.worker_redrive.append_failed_no_ack",
  "fluent.worker_redrive.contention.rejected",
  "fluent.worker_redrive.materialized",
  "fluent.worker_redrive.side_effect.executed",
  "fluent.worker_redrive.side_effect.result",
  "fluent.worker_redrive.l2_outcome",
  "fluent.worker_redrive.ds_ack.succeeded",
  "fluent.worker_redrive.ds_release.succeeded",
  "fluent.worker_redrive.substrate_rewake",
  "fluent.worker_redrive.materialized",
  "fluent.worker_redrive.l2_outcome",
  "fluent.worker_redrive.ds_ack.succeeded",
  "fluent.worker_redrive.ds_release.succeeded",
] as const

interface WorkerRedriveResult {
  readonly eventNames: ReadonlyArray<string>
  readonly sideEffectExecutions: number
  readonly l2Outcomes: number
  readonly wakeEvents: number
  readonly consumerOffset: string
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

const workStreamRoute = (namespace: string): string =>
  `/v1/stream/${streamPath(namespace, ["fluent-worker-redrive", "work"])}`

const wakeStreamPath = (namespace: string): string =>
  streamPath(namespace, ["fluent-worker-redrive", "wake"])

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const field = (
  value: Record<string, unknown>,
  key: string,
): unknown => value[key]

const readJsonArray = (
  baseUrl: string,
  path: string,
): Effect.Effect<ReadonlyArray<Record<string, unknown>>, Error> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${baseUrl}/v1/stream/${path}?offset=-1`, {
        method: "GET",
      })
      if (!response.ok) {
        throw new Error(`read ${path} failed with ${response.status}: ${await response.text()}`)
      }
      const parsed: unknown = await response.json()
      if (!Array.isArray(parsed)) {
        throw new Error(`read ${path} returned a non-array payload`)
      }
      return parsed.filter(isRecord)
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
        throw new Error(`read consumer failed with ${response.status}: ${await response.text()}`)
      }
      const parsed: unknown = await response.json()
      if (!isRecord(parsed)) {
        throw new Error("consumer endpoint returned a non-object payload")
      }
      return parsed
    },
    catch: cause => cause instanceof Error ? cause : new Error(String(cause)),
  })

const appendedEvents = (
  events: ReadonlyArray<Record<string, unknown>>,
): ReadonlyArray<Record<string, unknown>> =>
  events.filter(event => field(event, "type") === "session.event_appended")

const namedPayloads = (
  events: ReadonlyArray<Record<string, unknown>>,
  name: string,
): ReadonlyArray<unknown> =>
  appendedEvents(events)
    .filter(event => field(event, "name") === name)
    .map(event => field(event, "payload"))

const expectNames = (
  names: ReadonlyArray<string>,
): Effect.Effect<void, Error> =>
  names.join("\n") === expectedNames.join("\n")
    ? Effect.void
    : Effect.fail(
      new Error(
        `unexpected fluent worker redrive event order\nexpected: ${
          expectedNames.join(", ")
        }\nactual: ${names.join(", ")}`,
      ),
    )

const expectRecordPayload = (
  payload: unknown,
  description: string,
  predicate: (payload: Record<string, unknown>) => boolean,
): Effect.Effect<void, Error> =>
  isRecord(payload) && predicate(payload)
    ? Effect.void
    : Effect.fail(new Error(`unexpected payload for ${description}`))

const consumerOffsetFor = (
  consumer: Record<string, unknown>,
  path: string,
): string => {
  const streams = field(consumer, "streams")
  if (!Array.isArray(streams)) return ""
  const matched = streams
    .filter(isRecord)
    .find(stream => field(stream, "path") === path)
  return matched === undefined ? "" : String(field(matched, "offset"))
}

const assertSessionFacts = (
  events: ReadonlyArray<Record<string, unknown>>,
): Effect.Effect<{
  readonly eventNames: ReadonlyArray<string>
  readonly sideEffectExecutions: number
  readonly l2Outcomes: number
}, Error> =>
  Effect.gen(function*() {
    const appended = appendedEvents(events)
    const names = appended.map(event => String(field(event, "name")))
    yield* expectNames(names)

    const failure = namedPayloads(events, "fluent.worker_redrive.append_failed_no_ack")[0]
    yield* expectRecordPayload(
      failure,
      "append_failed_no_ack",
      payload =>
        field(payload, "failedBeforeAck") === true &&
        field(payload, "unchanged") === true &&
        field(payload, "afterOffset") === field(payload, "beforeOffset"),
    )

    const contention = namedPayloads(events, "fluent.worker_redrive.contention.rejected")[0]
    yield* expectRecordPayload(
      contention,
      "contention.rejected",
      payload => field(payload, "status") === 409 && field(payload, "code") === "EPOCH_HELD",
    )

    const materialized = namedPayloads(events, "fluent.worker_redrive.materialized")
    yield* expectRecordPayload(
      materialized[0],
      "first materialized",
      payload =>
        field(payload, "subscriptionOffset") === "-1" &&
        field(payload, "replaySource") === "session_journal_empty",
    )
    yield* expectRecordPayload(
      materialized[1],
      "second materialized",
      payload =>
        field(payload, "replaySource") === "session_journal" &&
        field(payload, "sessionEventCount") !== 0,
    )

    const l2 = namedPayloads(events, "fluent.worker_redrive.l2_outcome")
    yield* expectRecordPayload(
      l2[0],
      "first L2 outcome",
      payload => field(payload, "workId") === "first" && field(payload, "sideEffectExecuted") === true,
    )
    yield* expectRecordPayload(
      l2[1],
      "second L2 outcome",
      payload => field(payload, "workId") === "second" && field(payload, "sideEffectExecuted") === false,
    )

    const reWake = namedPayloads(events, "fluent.worker_redrive.substrate_rewake")[0]
    yield* expectRecordPayload(
      reWake,
      "substrate_rewake",
      payload =>
        typeof field(payload, "firstEpoch") === "number" &&
        typeof field(payload, "secondEpoch") === "number" &&
        Number(field(payload, "secondEpoch")) > Number(field(payload, "firstEpoch")),
    )

    const sideEffectExecutions = namedPayloads(
      events,
      "fluent.worker_redrive.side_effect.executed",
    ).length
    const l2Outcomes = l2.length
    return { eventNames: names, sideEffectExecutions, l2Outcomes }
  })

const wakeEventMatches = (
  event: Record<string, unknown>,
  type: string,
): boolean =>
  field(event, "type") === type &&
  field(event, "consumer") === consumerId

const assertWakeFacts = (
  events: ReadonlyArray<Record<string, unknown>>,
): Effect.Effect<number, Error> => {
  const wakeCount = events.filter(event => wakeEventMatches(event, "wake")).length
  const claimedCount = events.filter(event => wakeEventMatches(event, "claimed")).length
  return wakeCount >= 2 && claimedCount >= 2
    ? Effect.succeed(events.length)
    : Effect.fail(
      new Error(
        `expected at least two DS wake and claimed events, got wake=${wakeCount} claimed=${claimedCount}`,
      ),
    )
}

const readAssertedFacts = (
  baseUrl: string,
  namespace: string,
): Effect.Effect<WorkerRedriveResult, Error> =>
  Effect.gen(function*() {
    const sessionEvents = yield* readJsonArray(baseUrl, sessionStreamPath(namespace))
    const session = yield* assertSessionFacts(sessionEvents)
    const wakeEvents = yield* readJsonArray(baseUrl, wakeStreamPath(namespace))
    const wakeEventCount = yield* assertWakeFacts(wakeEvents)
    const consumer = yield* readConsumer(baseUrl)
    const offset = consumerOffsetFor(consumer, workStreamRoute(namespace))
    if (offset === "") {
      return yield* Effect.fail(new Error("consumer did not expose the work stream offset"))
    }
    return {
      eventNames: session.eventNames,
      sideEffectExecutions: session.sideEffectExecutions,
      l2Outcomes: session.l2Outcomes,
      wakeEvents: wakeEventCount,
      consumerOffset: offset,
    }
  }).pipe(
    Effect.retry({
      // Driver-only observation wait; host-side DS wake and redrive are not polled.
      // eslint-disable-next-line local/no-fixed-polling
      schedule: Schedule.spaced("100 millis").pipe(
        // eslint-disable-next-line local/no-fixed-polling
        Schedule.intersect(Schedule.recurs(50)),
      ),
    }),
  )

export const driver: Effect.Effect<WorkerRedriveResult, Error, FiregridConfig> =
  Effect.gen(function*() {
    const config = yield* FiregridConfig
    if (config.durableStreamsBaseUrl === undefined || config.namespace === undefined) {
      return yield* Effect.fail(
        new Error("fluent-worker-redrive requires durableStreamsBaseUrl and namespace"),
      )
    }
    const result = yield* readAssertedFacts(
      config.durableStreamsBaseUrl,
      config.namespace,
    )
    yield* Effect.annotateCurrentSpan({
      "firegrid.fluent_worker_redrive.event_count": result.eventNames.length,
      "firegrid.fluent_worker_redrive.side_effect_executions": result.sideEffectExecutions,
      "firegrid.fluent_worker_redrive.l2_outcomes": result.l2Outcomes,
      "firegrid.fluent_worker_redrive.wake_events": result.wakeEvents,
      "firegrid.fluent_worker_redrive.consumer_offset": result.consumerOffset,
    })
    return result
  }).pipe(
    Effect.withSpan("firelab.fluent_worker_redrive.driver", {
      attributes: {
        "firegrid.bead": "tf-hupp",
        "firegrid.simulation.intent": "fluent-worker-redrive",
      },
    }),
  )

