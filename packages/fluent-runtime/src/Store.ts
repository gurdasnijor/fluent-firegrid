import { HttpClient } from "@effect/platform"
import { Environment } from "@marcbachmann/cel-js"
import { Context, Data, Effect, Layer } from "effect"
import {
  DurableStream,
  type Endpoint,
  type HeadResult,
  type ProducerAppendResult,
} from "effect-durable-streams"
import {
  SessionEventSchema,
  TurnEventSchema,
  type SessionEvent,
  type SessionHandle,
  type SessionId,
  type StateChangeMessage,
  type TimerId,
  type TurnEvent,
  type TurnHandle,
  type TurnId,
  type TurnWaitRegisteredEvent,
  type WaitId,
} from "./Domain.ts"

export class FluentRuntimeError extends Data.TaggedError("FluentRuntimeError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export interface StoreConfig {
  readonly durableStreamsBaseUrl: string
  readonly namespace: string
}

export interface CreateSessionInput {
  readonly sessionId: SessionId
  readonly agent: string
}

export interface AppendSessionEventInput {
  readonly sessionId: SessionId
  readonly name: string
  readonly payload: unknown
}

export interface ProducerFence {
  readonly producerId: string
  readonly epoch: number
  readonly seq: number
}

export interface AppendSessionEventFencedInput extends AppendSessionEventInput {
  readonly fence: ProducerFence
}

export interface AppendSessionEventFencedResult {
  readonly handle: SessionHandle
  readonly write: ProducerAppendResult
}

export interface AppendStateChangeFencedInput {
  readonly sessionId: SessionId
  readonly change: StateChangeMessage
  readonly fence: ProducerFence
}

export interface AppendStateChangeFencedResult {
  readonly handle: SessionHandle
  readonly write: ProducerAppendResult
  readonly change: StateChangeMessage
}

export interface StartTurnInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly prompt: string
}

export interface CompleteTurnInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly result: unknown
}

export interface FailTurnInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly message: string
}

export interface ScheduleTurnTimerInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly timerId: TimerId
  readonly fireAtEpochMs: number
}

export interface ScheduleTurnTimerResult {
  readonly turn: TurnHandle
  readonly write: ProducerAppendResult
}

export interface FireTurnTimerInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly timerId: TimerId
  readonly firedAtEpochMs: number
}

export interface FireTurnTimerResult {
  readonly turn: TurnHandle
  readonly write: ProducerAppendResult
}

export interface RegisterTurnWaitInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly waitId: WaitId
  readonly predicate: string
  readonly afterOffset: string
  readonly self?: unknown
}

export interface RegisterTurnWaitResult {
  readonly turn: TurnHandle
  readonly write: ProducerAppendResult
}

export interface MatchTurnWaitInput {
  readonly sessionId: SessionId
  readonly turnId: TurnId
  readonly waitId: WaitId
  readonly matchedOffset: string
  readonly event: unknown
}

export type MatchTurnWaitResult =
  | {
    readonly _tag: "Matched"
    readonly turn: TurnHandle
    readonly write: ProducerAppendResult
  }
  | {
    readonly _tag: "NotMatched"
    readonly turn: TurnHandle
    readonly registered: TurnWaitRegisteredEvent
  }

export interface ReadTurnResult {
  readonly turn: TurnHandle
  readonly events: ReadonlyArray<TurnEvent>
  readonly head: HeadResult
  readonly streamClosed: boolean
}

export type StoreRequirements = never

export class FluentStore extends Context.Tag("@firegrid/fluent-runtime/Store/FluentStore")<
  FluentStore,
  {
    readonly sessionUrl: (sessionId: SessionId) => string
    readonly turnUrl: (sessionId: SessionId, turnId: TurnId) => string
    readonly createSession: (
      input: CreateSessionInput,
    ) => Effect.Effect<SessionHandle, FluentRuntimeError, StoreRequirements>
    readonly appendSessionEvent: (
      input: AppendSessionEventInput,
    ) => Effect.Effect<SessionHandle, FluentRuntimeError, StoreRequirements>
    readonly appendSessionEventFenced: (
      input: AppendSessionEventFencedInput,
    ) => Effect.Effect<AppendSessionEventFencedResult, FluentRuntimeError, StoreRequirements>
    readonly appendStateChangeFenced: (
      input: AppendStateChangeFencedInput,
    ) => Effect.Effect<AppendStateChangeFencedResult, FluentRuntimeError, StoreRequirements>
    readonly collectSession: (
      sessionId: SessionId,
    ) => Effect.Effect<ReadonlyArray<SessionEvent>, FluentRuntimeError, StoreRequirements>
    readonly headSession: (
      sessionId: SessionId,
    ) => Effect.Effect<HeadResult, FluentRuntimeError, StoreRequirements>
    readonly startTurn: (
      input: StartTurnInput,
    ) => Effect.Effect<TurnHandle, FluentRuntimeError, StoreRequirements>
    readonly completeTurn: (
      input: CompleteTurnInput,
    ) => Effect.Effect<TurnHandle, FluentRuntimeError, StoreRequirements>
    readonly failTurn: (
      input: FailTurnInput,
    ) => Effect.Effect<TurnHandle, FluentRuntimeError, StoreRequirements>
    readonly scheduleTurnTimer: (
      input: ScheduleTurnTimerInput,
    ) => Effect.Effect<ScheduleTurnTimerResult, FluentRuntimeError, StoreRequirements>
    readonly fireTurnTimer: (
      input: FireTurnTimerInput,
    ) => Effect.Effect<FireTurnTimerResult, FluentRuntimeError, StoreRequirements>
    readonly registerTurnWait: (
      input: RegisterTurnWaitInput,
    ) => Effect.Effect<RegisterTurnWaitResult, FluentRuntimeError, StoreRequirements>
    readonly matchTurnWait: (
      input: MatchTurnWaitInput,
    ) => Effect.Effect<MatchTurnWaitResult, FluentRuntimeError, StoreRequirements>
    readonly readTurn: (
      sessionId: SessionId,
      turnId: TurnId,
    ) => Effect.Effect<ReadTurnResult, FluentRuntimeError, StoreRequirements>
  }
>() {}

const encodeSegment = (segment: string): string => encodeURIComponent(segment)

const normalizeBaseUrl = (baseUrl: string): string =>
  baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl

const streamUrl = (
  config: StoreConfig,
  segments: ReadonlyArray<string>,
): string =>
  `${normalizeBaseUrl(config.durableStreamsBaseUrl)}/v1/stream/${
    encodeSegment(config.namespace)
  }/${segments.map(encodeSegment).join("/")}`

const endpoint = (url: string): Endpoint => ({ url })

const sessionStream = (url: string) =>
  DurableStream.define({
    endpoint: endpoint(url),
    schema: SessionEventSchema,
  })

const turnStream = (url: string) =>
  DurableStream.define({
    endpoint: endpoint(url),
    schema: TurnEventSchema,
  })

const toRuntimeError = (
  message: string,
) =>
  (cause: unknown): FluentRuntimeError =>
    new FluentRuntimeError({ message, cause })

const jsonBatch = <A>(event: A): string => JSON.stringify([event])

const timerProducerId = (
  kind: "schedule" | "fire",
  input: {
    readonly sessionId: SessionId
    readonly turnId: TurnId
    readonly timerId: TimerId
  },
): string =>
  [
    "fluent-runtime",
    "timer",
    kind,
    encodeSegment(input.sessionId),
    encodeSegment(input.turnId),
    encodeSegment(input.timerId),
  ].join("/")

const waitProducerId = (
  kind: "register" | "match",
  input: {
    readonly sessionId: SessionId
    readonly turnId: TurnId
    readonly waitId: WaitId
  },
): string =>
  [
    "fluent-runtime",
    "wait",
    kind,
    encodeSegment(input.sessionId),
    encodeSegment(input.turnId),
    encodeSegment(input.waitId),
  ].join("/")

const celEnvironment = new Environment({ unlistedVariablesAreDyn: true })

const evaluateWaitPredicate = (
  predicate: string,
  event: unknown,
  self: unknown,
): Effect.Effect<boolean, FluentRuntimeError> =>
  Effect.try({
    try: () => celEnvironment.evaluate(predicate, { event, self }) === true,
    catch: (cause) =>
      new FluentRuntimeError({
        message: "Failed to evaluate wait predicate",
        cause,
      }),
  })

const offsetParts = (offset: string): ReadonlyArray<bigint> =>
  offset.split("_").map(part => BigInt(part))

const compareOffsets = (
  left: string,
  right: string,
): number => {
  const leftParts = offsetParts(left)
  const rightParts = offsetParts(right)
  const length = Math.max(leftParts.length, rightParts.length)
  for (let index = 0; index < length; index += 1) {
    const leftPart = leftParts[index] ?? 0n
    const rightPart = rightParts[index] ?? 0n
    if (leftPart > rightPart) return 1
    if (leftPart < rightPart) return -1
  }
  return 0
}

const isAfterOffset = (
  matchedOffset: string,
  afterOffset: string,
): Effect.Effect<boolean, FluentRuntimeError> =>
  Effect.try({
    try: () => compareOffsets(matchedOffset, afterOffset) > 0,
    catch: (cause) =>
      new FluentRuntimeError({
        message: "Failed to compare wait offsets",
        cause,
      }),
  })

const findLastEvent = <A extends TurnEvent>(
  events: ReadonlyArray<TurnEvent>,
  predicate: (event: TurnEvent) => event is A,
): A | undefined => {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index]
    if (event !== undefined && predicate(event)) return event
  }
  return undefined
}

const makeSessionHandle = (
  config: StoreConfig,
  sessionId: SessionId,
): SessionHandle => ({
  sessionId,
  eventsUrl: streamUrl(config, ["sessions", sessionId]),
})

const makeTurnHandle = (
  config: StoreConfig,
  sessionId: SessionId,
  turnId: TurnId,
): TurnHandle => ({
  sessionId,
  turnId,
  eventsUrl: streamUrl(config, ["sessions", sessionId, "turns", turnId]),
})

export const makeFluentStore = (
  config: StoreConfig,
  httpClient: HttpClient.HttpClient,
): Context.Tag.Service<typeof FluentStore> => {
  const provideHttp = <A, E>(
    effect: Effect.Effect<A, E, HttpClient.HttpClient>,
  ): Effect.Effect<A, E> =>
    Effect.provideService(effect, HttpClient.HttpClient, httpClient)

  const createSession = (
    input: CreateSessionInput,
  ) =>
    Effect.gen(function* () {
      const handle = makeSessionHandle(config, input.sessionId)
      const stream = sessionStream(handle.eventsUrl)
      yield* stream.create({ contentType: "application/json" }).pipe(
        Effect.mapError(toRuntimeError("Failed to create session stream")),
      )
      yield* stream.append({
        type: "session.created",
        sessionId: input.sessionId,
        agent: input.agent,
      }).pipe(
        Effect.mapError(toRuntimeError("Failed to append session.created event")),
      )
      return handle
    }).pipe(
      Effect.withSpan("fluent_runtime.store.session.create", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.agent": input.agent,
        },
      }),
    )

  const appendSessionEvent = (
    input: AppendSessionEventInput,
  ) =>
    Effect.gen(function* () {
      const handle = makeSessionHandle(config, input.sessionId)
      yield* sessionStream(handle.eventsUrl).append({
        type: "session.event_appended",
        sessionId: input.sessionId,
        name: input.name,
        payload: input.payload,
      }).pipe(
        Effect.mapError(toRuntimeError("Failed to append session event")),
      )
      return handle
    }).pipe(
      Effect.withSpan("fluent_runtime.store.session.append_event", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.session.event.name": input.name,
        },
      }),
    )

  const appendSessionEventFenced = (
    input: AppendSessionEventFencedInput,
  ) =>
    Effect.gen(function* () {
      const handle = makeSessionHandle(config, input.sessionId)
      const event: SessionEvent = {
        type: "session.event_appended",
        sessionId: input.sessionId,
        name: input.name,
        payload: input.payload,
      }
      const write = yield* DurableStream.appendWithProducer({
        endpoint: endpoint(handle.eventsUrl),
        schema: SessionEventSchema,
        event,
        producerId: input.fence.producerId,
        producerEpoch: input.fence.epoch,
        producerSeq: input.fence.seq,
      }).pipe(
        Effect.mapError(toRuntimeError("Failed to append fenced session event")),
      )
      return { handle, write }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.session.append_event_fenced", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.session.event.name": input.name,
          "fluent_runtime.producer.id": input.fence.producerId,
          "fluent_runtime.producer.epoch": input.fence.epoch,
          "fluent_runtime.producer.seq": input.fence.seq,
        },
      }),
    )

  const appendStateChangeFenced = (
    input: AppendStateChangeFencedInput,
  ) =>
    Effect.gen(function* () {
      const handle = makeSessionHandle(config, input.sessionId)
      const write = yield* DurableStream.appendWithProducer({
        endpoint: endpoint(handle.eventsUrl),
        schema: SessionEventSchema,
        event: input.change,
        producerId: input.fence.producerId,
        producerEpoch: input.fence.epoch,
        producerSeq: input.fence.seq,
      }).pipe(
        Effect.mapError(toRuntimeError("Failed to append fenced state change")),
      )
      return { handle, write, change: input.change }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.state_change.append_fenced", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "fluent_runtime.state_change.type": input.change.type,
          "fluent_runtime.state_change.key": input.change.key,
          "fluent_runtime.producer.id": input.fence.producerId,
          "fluent_runtime.producer.epoch": input.fence.epoch,
          "fluent_runtime.producer.seq": input.fence.seq,
        },
      }),
    )

  const collectSession = (sessionId: SessionId) =>
    sessionStream(makeSessionHandle(config, sessionId).eventsUrl).collect.pipe(
      Effect.mapError(toRuntimeError("Failed to collect session events")),
      Effect.withSpan("fluent_runtime.store.session.collect", {
        attributes: { "firegrid.session.id": sessionId },
      }),
    )

  const headSession = (sessionId: SessionId) =>
    sessionStream(makeSessionHandle(config, sessionId).eventsUrl).head.pipe(
      Effect.mapError(toRuntimeError("Failed to read session head")),
      Effect.withSpan("fluent_runtime.store.session.head", {
        attributes: { "firegrid.session.id": sessionId },
      }),
    )

  const startTurn = (
    input: StartTurnInput,
  ) =>
    Effect.gen(function* () {
      const handle = makeTurnHandle(config, input.sessionId, input.turnId)
      const stream = turnStream(handle.eventsUrl)
      yield* stream.create({ contentType: "application/json" }).pipe(
        Effect.mapError(toRuntimeError("Failed to create turn stream")),
      )
      yield* stream.append({
        type: "turn.started",
        sessionId: input.sessionId,
        turnId: input.turnId,
        prompt: input.prompt,
      }).pipe(
        Effect.mapError(toRuntimeError("Failed to append turn.started event")),
      )
      return handle
    }).pipe(
      Effect.withSpan("fluent_runtime.store.turn.start", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
        },
      }),
    )

  const closeTurnWith = (
    handle: TurnHandle,
    event: TurnEvent,
    message: string,
  ) =>
    turnStream(handle.eventsUrl).close({
      body: jsonBatch(event),
      contentType: "application/json",
    }).pipe(
      Effect.as(handle),
      Effect.mapError(toRuntimeError(message)),
    )

  const appendTurnEventWithProducer = (
    turn: TurnHandle,
    event: TurnEvent,
    producerId: string,
    message: string,
  ) =>
    DurableStream.appendWithProducer({
      endpoint: endpoint(turn.eventsUrl),
      schema: TurnEventSchema,
      event,
      producerId,
      producerEpoch: 0,
      producerSeq: 0,
    }).pipe(
      Effect.mapError(toRuntimeError(message)),
    )

  const completeTurn = (
    input: CompleteTurnInput,
  ) => {
    const handle = makeTurnHandle(config, input.sessionId, input.turnId)
    return closeTurnWith(handle, {
      type: "turn.completed",
      sessionId: input.sessionId,
      turnId: input.turnId,
      result: input.result,
    }, "Failed to append-and-close turn.completed").pipe(
      Effect.withSpan("fluent_runtime.store.turn.complete", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
          "fluent_runtime.close.atomic": true,
        },
      }),
    )
  }

  const failTurn = (
    input: FailTurnInput,
  ) => {
    const handle = makeTurnHandle(config, input.sessionId, input.turnId)
    return closeTurnWith(handle, {
      type: "turn.failed",
      sessionId: input.sessionId,
      turnId: input.turnId,
      message: input.message,
    }, "Failed to append-and-close turn.failed").pipe(
      Effect.withSpan("fluent_runtime.store.turn.fail", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
          "fluent_runtime.close.atomic": true,
        },
      }),
    )
  }

  const scheduleTurnTimer = (
    input: ScheduleTurnTimerInput,
  ) =>
    Effect.gen(function* () {
      const turn = makeTurnHandle(config, input.sessionId, input.turnId)
      const write = yield* appendTurnEventWithProducer(
        turn,
        {
          type: "turn.timer_scheduled",
          sessionId: input.sessionId,
          turnId: input.turnId,
          timerId: input.timerId,
          fireAtEpochMs: input.fireAtEpochMs,
        },
        timerProducerId("schedule", input),
        "Failed to append fenced turn.timer_scheduled event",
      )
      return { turn, write }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.turn.timer.schedule", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
          "fluent_runtime.timer.id": input.timerId,
          "fluent_runtime.timer.fire_at_epoch_ms": input.fireAtEpochMs,
        },
      }),
    )

  const fireTurnTimer = (
    input: FireTurnTimerInput,
  ) =>
    Effect.gen(function* () {
      const turn = makeTurnHandle(config, input.sessionId, input.turnId)
      const write = yield* appendTurnEventWithProducer(
        turn,
        {
          type: "turn.timer_fired",
          sessionId: input.sessionId,
          turnId: input.turnId,
          timerId: input.timerId,
          firedAtEpochMs: input.firedAtEpochMs,
        },
        timerProducerId("fire", input),
        "Failed to append fenced turn.timer_fired event",
      )
      return { turn, write }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.turn.timer.fire", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
          "fluent_runtime.timer.id": input.timerId,
          "fluent_runtime.timer.fired_at_epoch_ms": input.firedAtEpochMs,
        },
      }),
    )

  const registerTurnWait = (
    input: RegisterTurnWaitInput,
  ) =>
    Effect.gen(function* () {
      const turn = makeTurnHandle(config, input.sessionId, input.turnId)
      const write = yield* appendTurnEventWithProducer(
        turn,
        {
          type: "turn.wait_registered",
          sessionId: input.sessionId,
          turnId: input.turnId,
          waitId: input.waitId,
          predicate: input.predicate,
          afterOffset: input.afterOffset,
          ...(input.self === undefined ? {} : { self: input.self }),
        },
        waitProducerId("register", input),
        "Failed to append fenced turn.wait_registered event",
      )
      return { turn, write }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.turn.wait.register", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
          "fluent_runtime.wait.id": input.waitId,
          "fluent_runtime.wait.after_offset": input.afterOffset,
        },
      }),
    )

  const matchTurnWait = (
    input: MatchTurnWaitInput,
  ) =>
    Effect.gen(function* () {
      const turn = makeTurnHandle(config, input.sessionId, input.turnId)
      const read = yield* readTurn(input.sessionId, input.turnId)
      const registered = findLastEvent(
        read.events,
        (event): event is TurnWaitRegisteredEvent =>
          event.type === "turn.wait_registered" && event.waitId === input.waitId,
      )
      if (registered === undefined) {
        return yield* new FluentRuntimeError({
          message: `Missing durable wait registration for ${input.waitId}`,
        })
      }
      const candidateIsAfterRegistration = yield* isAfterOffset(
        input.matchedOffset,
        registered.afterOffset,
      )
      if (!candidateIsAfterRegistration) {
        return { _tag: "NotMatched" as const, turn, registered }
      }
      const matched = yield* evaluateWaitPredicate(
        registered.predicate,
        input.event,
        registered.self ?? {},
      )
      if (!matched) return { _tag: "NotMatched" as const, turn, registered }
      const write = yield* appendTurnEventWithProducer(
        turn,
        {
          type: "turn.wait_matched",
          sessionId: input.sessionId,
          turnId: input.turnId,
          waitId: input.waitId,
          matchedOffset: input.matchedOffset,
          event: input.event,
        },
        waitProducerId("match", input),
        "Failed to append fenced turn.wait_matched event",
      )
      return { _tag: "Matched" as const, turn, write }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.turn.wait.match", {
        attributes: {
          "firegrid.session.id": input.sessionId,
          "firegrid.turn.id": input.turnId,
          "fluent_runtime.wait.id": input.waitId,
          "fluent_runtime.wait.matched_offset": input.matchedOffset,
        },
      }),
    )

  const readTurn = (sessionId: SessionId, turnId: TurnId) =>
    Effect.gen(function* () {
      const handle = makeTurnHandle(config, sessionId, turnId)
      const stream = turnStream(handle.eventsUrl)
      const events = yield* stream.collect.pipe(
        Effect.mapError(toRuntimeError("Failed to collect turn events")),
      )
      const head = yield* stream.head.pipe(
        Effect.mapError(toRuntimeError("Failed to read turn head")),
      )
      return {
        turn: handle,
        events,
        head,
        streamClosed: head.streamClosed,
      }
    }).pipe(
      Effect.withSpan("fluent_runtime.store.turn.read", {
        attributes: {
          "firegrid.session.id": sessionId,
          "firegrid.turn.id": turnId,
        },
      }),
    )

  return {
    sessionUrl: (sessionId) => makeSessionHandle(config, sessionId).eventsUrl,
    turnUrl: (sessionId, turnId) => makeTurnHandle(config, sessionId, turnId).eventsUrl,
    createSession: (input) => provideHttp(createSession(input)),
    appendSessionEvent: (input) => provideHttp(appendSessionEvent(input)),
    appendSessionEventFenced: (input) => provideHttp(appendSessionEventFenced(input)),
    appendStateChangeFenced: (input) => provideHttp(appendStateChangeFenced(input)),
    collectSession: (sessionId) => provideHttp(collectSession(sessionId)),
    headSession: (sessionId) => provideHttp(headSession(sessionId)),
    startTurn: (input) => provideHttp(startTurn(input)),
    completeTurn: (input) => provideHttp(completeTurn(input)),
    failTurn: (input) => provideHttp(failTurn(input)),
    scheduleTurnTimer: (input) => provideHttp(scheduleTurnTimer(input)),
    fireTurnTimer: (input) => provideHttp(fireTurnTimer(input)),
    registerTurnWait: (input) => provideHttp(registerTurnWait(input)),
    matchTurnWait: (input) => provideHttp(matchTurnWait(input)),
    readTurn: (sessionId, turnId) => provideHttp(readTurn(sessionId, turnId)),
  }
}

export const FluentStoreLive = (
  config: StoreConfig,
): Layer.Layer<FluentStore, never, HttpClient.HttpClient> =>
  Layer.effect(
    FluentStore,
    Effect.map(
      HttpClient.HttpClient,
      (httpClient) => makeFluentStore(config, httpClient),
    ),
  )
