import {
  ackAfterDurableProductOutcome,
  DurableConsumerClient,
  DurableConsumerClientLive,
  FluentRuntimeLive,
  FluentStore,
  type AcquiredConsumer,
  type ConsumerInfo,
  type DurableConsumerClientService,
  type SessionEvent,
} from "@firegrid/fluent-runtime"
import { Effect, Either, Layer } from "effect"
import type { Context } from "effect"
import type {
  FirelabHost,
  FirelabHostEnv,
} from "../../types.ts"
import {
  agentName,
  consumerId,
  factNames,
  failureConsumerId,
  failureWakeStreamRoute,
  failureWorkStreamRoute,
  sessionId,
  wakeStreamRoute,
  workStreamRoute,
  workerA,
  workerB,
  type WorkerRedriveFactName,
} from "./scenario.ts"

type FluentStoreService = Context.Tag.Service<typeof FluentStore>

const appendFact = (
  store: FluentStoreService,
  name: WorkerRedriveFactName,
  payload: unknown,
) =>
  store.appendSessionEvent({ sessionId, name, payload }).pipe(Effect.asVoid)

const field = (
  value: Record<string, unknown>,
  key: string,
): unknown => value[key]

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

interface MatchedPayload {
  readonly payload: unknown
}

const appendedFactPayload = (
  event: SessionEvent,
  name: WorkerRedriveFactName,
): MatchedPayload | undefined => {
  if (
    event.type !== "session.event_appended" ||
    !("name" in event) ||
    event.name !== name
  ) {
    return undefined
  }
  return { payload: event.payload }
}

const journaledResult = (
  events: ReadonlyArray<SessionEvent>,
): MatchedPayload | undefined =>
  events
    .map(event => appendedFactPayload(event, factNames.sideEffectResult))
    .find(payload => payload !== undefined)

const streamOffset = (
  claim: AcquiredConsumer,
  path: string,
): string => claim.streams.find(stream => stream.path === path)?.offset ?? "-1"

const firstOffset = (
  consumer: ConsumerInfo,
): string => consumer.streams[0]?.offset ?? "-1"

const driveClaimedWake = (
  input: {
    readonly store: FluentStoreService
    readonly client: DurableConsumerClientService
    readonly claim: AcquiredConsumer
    readonly workStream: string
    readonly ackOffset: string
    readonly workId: string
  },
) =>
  Effect.gen(function*() {
    const materialized = yield* input.store.collectSession(sessionId)
    const replayed = journaledResult(materialized)
    const result = replayed?.payload ?? {
      ok: true,
      source: "executed-side-effect",
      value: "side-effect-result",
    }
    yield* appendFact(input.store, factNames.materialized, {
      workId: input.workId,
      claimEpoch: input.claim.epoch,
      subscriptionOffset: streamOffset(input.claim, input.workStream),
      sessionEventCount: materialized.length,
      replaySource: replayed === undefined ? "session_journal_empty" : "session_journal",
    })
    if (replayed === undefined) {
      yield* appendFact(input.store, factNames.sideEffectExecuted, {
        workId: input.workId,
        executionCount: 1,
      })
      yield* appendFact(input.store, factNames.sideEffectResult, {
        workId: input.workId,
        result,
      })
    }

    yield* ackAfterDurableProductOutcome(
      input.client,
      {
        consumerId,
        token: input.claim.token,
        offsets: [{ path: input.workStream, offset: input.ackOffset }],
      },
      appendFact(input.store, factNames.l2Outcome, {
        workId: input.workId,
        result,
        sideEffectExecuted: replayed === undefined,
        ackOffset: input.ackOffset,
      }),
    )
    yield* appendFact(input.store, factNames.ackSucceeded, {
      workId: input.workId,
      epoch: input.claim.epoch,
      offset: input.ackOffset,
    })
    yield* input.client.releaseConsumer({
      consumerId,
      token: input.claim.token,
    })
    yield* appendFact(input.store, factNames.releaseSucceeded, {
      workId: input.workId,
      epoch: input.claim.epoch,
    })
  })

const setupConsumer = (
  input: {
    readonly client: DurableConsumerClientService
    readonly namespace: string
    readonly consumer: string
    readonly workStream: string
    readonly wakeStream: string
  },
) =>
  Effect.gen(function*() {
    yield* input.client.createStream(input.workStream)
    yield* input.client.createStream(input.wakeStream)
    yield* input.client.registerConsumer({
      consumerId: input.consumer,
      namespace: input.namespace,
      streams: [input.workStream],
    })
    yield* input.client.configurePullWake({
      consumerId: input.consumer,
      wakeStream: input.wakeStream,
    })
  })

const proveNoAckOnFailedAppend = (
  input: {
    readonly store: FluentStoreService
    readonly client: DurableConsumerClientService
    readonly namespace: string
  },
) =>
  Effect.gen(function*() {
    const workStream = failureWorkStreamRoute(input.namespace)
    const wakeStream = failureWakeStreamRoute(input.namespace)
    yield* setupConsumer({
      client: input.client,
      namespace: input.namespace,
      consumer: failureConsumerId,
      workStream,
      wakeStream,
    })
    const work = yield* input.client.appendStream({
      routePath: workStream,
      event: { kind: "failure-work", workId: "failure-before-ack" },
    })
    const claim = yield* input.client.acquireConsumer({
      consumerId: failureConsumerId,
      worker: workerA,
    })
    const before = yield* input.client.getConsumer(failureConsumerId)
    const failed = yield* ackAfterDurableProductOutcome(
      input.client,
      {
        consumerId: failureConsumerId,
        token: claim.token,
        offsets: [{ path: workStream, offset: work.offset }],
      },
      Effect.fail(new Error("injected durable product append failure")),
    ).pipe(Effect.either)
    const after = yield* input.client.getConsumer(failureConsumerId)
    yield* input.client.releaseConsumer({
      consumerId: failureConsumerId,
      token: claim.token,
    })
    yield* appendFact(input.store, factNames.appendFailedNoAck, {
      failedBeforeAck: Either.isLeft(failed),
      beforeOffset: firstOffset(before),
      afterOffset: firstOffset(after),
      workOffset: work.offset,
      unchanged: firstOffset(before) === firstOffset(after),
    })
  })

const contentionStatus = (body: unknown): string =>
  isRecord(body) && isRecord(field(body, "error"))
    ? String(field(field(body, "error") as Record<string, unknown>, "code"))
    : "unknown"

const rejectCompetingWorker = (
  input: {
    readonly baseUrl: string
    readonly store: FluentStoreService
  },
) =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${input.baseUrl}/consumers/${consumerId}/acquire`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ worker: workerB }),
      })
      const parsed: unknown = await response.json()
      return {
        status: response.status,
        code: contentionStatus(parsed),
      }
    },
    catch: cause => cause instanceof Error ? cause : new Error(String(cause)),
  }).pipe(
    Effect.flatMap(result =>
      result.status === 409 && result.code === "EPOCH_HELD"
        ? appendFact(input.store, factNames.contentionRejected, result)
        : Effect.fail(
          new Error(
            `expected competing acquire to fail with EPOCH_HELD, got ${
              result.status
            }/${result.code}`,
          ),
        ),
    ),
    Effect.withSpan("firegrid.sim.fluent_worker_redrive.contention"),
  )

const seedWorkerRedrive = (
  env: FirelabHostEnv,
) =>
  Effect.gen(function*() {
    const store = yield* FluentStore
    const client = yield* DurableConsumerClient
    if (env.namespace === undefined) {
      return yield* Effect.fail(new Error("fluent-worker-redrive requires namespace"))
    }

    const workStream = workStreamRoute(env.namespace)
    const wakeStream = wakeStreamRoute(env.namespace)

    yield* store.createSession({
      sessionId,
      agent: agentName,
    })
    yield* proveNoAckOnFailedAppend({
      store,
      client,
      namespace: env.namespace,
    })
    yield* setupConsumer({
      client,
      namespace: env.namespace,
      consumer: consumerId,
      workStream,
      wakeStream,
    })

    const firstWork = yield* client.appendStream({
      routePath: workStream,
      event: { kind: "work", workId: "first" },
    })
    const firstClaim = yield* client.acquireConsumer({
      consumerId,
      worker: workerA,
    })
    yield* rejectCompetingWorker({
      baseUrl: env.durableStreamsBaseUrl,
      store,
    })
    const secondWork = yield* client.appendStream({
      routePath: workStream,
      event: { kind: "work", workId: "second-arrived-during-claim" },
    })
    yield* driveClaimedWake({
      store,
      client,
      claim: firstClaim,
      workStream,
      ackOffset: firstWork.offset,
      workId: "first",
    })

    const secondClaim = yield* client.acquireConsumer({
      consumerId,
      worker: workerA,
    })
    yield* appendFact(store, factNames.substrateRewake, {
      firstEpoch: firstClaim.epoch,
      secondEpoch: secondClaim.epoch,
      secondWorkOffset: secondWork.offset,
      secondClaimOffset: streamOffset(secondClaim, workStream),
    })
    yield* driveClaimedWake({
      store,
      client,
      claim: secondClaim,
      workStream,
      ackOffset: secondWork.offset,
      workId: "second",
    })
  }).pipe(
    Effect.withSpan("firegrid.sim.fluent_worker_redrive.host"),
  )

export const host = (
  env: FirelabHostEnv,
): Layer.Layer<FirelabHost, unknown> =>
  Layer.scopedDiscard(
    seedWorkerRedrive(env).pipe(
      Effect.provide(Layer.mergeAll(
        FluentRuntimeLive({
          durableStreamsBaseUrl: env.durableStreamsBaseUrl,
          namespace: env.namespace,
        }),
        DurableConsumerClientLive({
          durableStreamsBaseUrl: env.durableStreamsBaseUrl,
        }),
      )),
    ),
  )
