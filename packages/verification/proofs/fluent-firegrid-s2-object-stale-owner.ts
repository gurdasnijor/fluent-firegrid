import { basin, basins, layer as S2Layer, stream as s2Stream } from "effect-s2"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"

import { objectInvocationStreamName } from "@firegrid/fluent-firegrid-s2"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = new URL("../fixtures/fluent-firegrid-s2-object-worker.ts", import.meta.url).pathname

const portFromTrialId = (trialId: string, salt: string): number => {
  const hash = Array.from(`fluent-object-stale-owner-${trialId}-${salt}`).reduce(
    (current, char) => (current * 31 + char.charCodeAt(0)) % 10_000,
    0
  )
  return 45_000 + hash
}

const requestJson = <A>(url: string, init?: RequestInit): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(url, init)
      if (!response.ok) {
        throw new Error(`request ${url} failed with ${response.status}: ${await response.text()}`)
      }
      return await response.json() as A
    },
    catch: (cause) => new VerificationError({ cause, message: `fluent object stale-owner request failed: ${url}` })
  })

type InvocationEvent = {
  readonly _tag: string
  readonly handler?: string
}

const waitForStarted = (
  s2Endpoint: string,
  streamName: string
): Effect.Effect<number, VerificationError> => {
  const layer = S2Layer({
    accessToken: "s2_access_token",
    endpoints: {
      account: s2Endpoint,
      basin: s2Endpoint
    }
  })
  const readStartedCount = Effect.provide(
    Effect.gen(function*() {
      yield* basins.ensure({ basin: "fluent-firegrid" })
      const basinApi = yield* basin("fluent-firegrid")
      yield* basinApi.streams.ensure({ stream: streamName })
      const stream = yield* s2Stream("fluent-firegrid", streamName)
      const tail = yield* stream.checkTail()
      if (tail.tail.seqNum <= 0) return 0
      const records = yield* stream.readSession({
        start: { from: { seqNum: 0 } },
        stop: { limits: { count: tail.tail.seqNum } }
      }).pipe(Stream.runCollect)
      return Array.from(records, (record) => JSON.parse(record.body) as InvocationEvent).filter((event) =>
        event._tag === "Started" || (event._tag === "Accepted" && event.handler === "slowAdd")
      ).length
    }),
    layer
  )
  const loop = (remaining: number): Effect.Effect<number, VerificationError> =>
    readStartedCount.pipe(
      Effect.mapError((cause) =>
        new VerificationError({ cause, message: "failed to inspect S2 object invocation stream" })
      ),
      Effect.flatMap((count) => {
        if (count >= 2) return Effect.succeed(count)
        if (remaining <= 0) {
          return Effect.fail(
            new VerificationError({ message: `timed out waiting for slowAdd Started in ${streamName}` })
          )
        }
        return Effect.sleep("25 millis").pipe(Effect.andThen(loop(remaining - 1)))
      })
    )
  return loop(160)
}

export default proof("fluent-firegrid-s2.object-stale-owner")
  .describedAs(
    "Proves S2-backed object ownership recovery: host A dies after writing Started for a same-key object call, host B takes over after the lease expires, and the subsequent call observes serialized state."
  )
  .spec(({ property, trialId }) => {
    const portA = portFromTrialId(trialId, "a")
    const portB = portFromTrialId(trialId, "b")
    const hostA = `http://127.0.0.1:${portA}`
    const hostB = `http://127.0.0.1:${portB}`

    return property("fluent-firegrid-s2.object-stale-owner-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({
        a: processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: { HOST_PORT: String(portA) },
          readiness: {
            attempts: 400,
            interval: "50 millis",
            url: `${hostA}/ready`
          }
        }),
        b: processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: { HOST_PORT: String(portB) },
          readiness: {
            attempts: 400,
            interval: "50 millis",
            url: `${hostB}/ready`
          }
        })
      })
      .workload(({ faults, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 object stale-owner proof requires s2Lite" })
          }
          const namespace = `fluent-object-cross-host-${trialId}`
          const streamName = objectInvocationStreamName(
            { namespace },
            { key: "counter-1", objectName: "cross-host-counter" }
          )
          const slowRequest = yield* Effect.sync(() => {
            const controller = new AbortController()
            void fetch(`${hostA}/slow-add?by=5`, {
              method: "POST",
              signal: controller.signal
            }).catch(() => undefined)
            return controller
          })
          const startedCount = yield* waitForStarted(s2Endpoint, streamName)

          yield* faults.killHost("a")
          yield* requestJson<{ readonly now: number }>(`${hostB}/now?value=3000`, { method: "POST" })

          const recovered = yield* requestJson<{ readonly hostId: string; readonly value: number }>(
            `${hostB}/add?by=7`,
            { method: "POST" }
          )
          const loaded = yield* requestJson<{ readonly hostId: string; readonly value: number }>(`${hostB}/value`)
          yield* Effect.sync(() => slowRequest.abort())

          return {
            recovered,
            slowStartedEvents: startedCount,
            value: loaded.value
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          recovered: {
            hostId: "b",
            value: 12
          },
          slowStartedEvents: 2,
          value: 12
        }),
        traceSql(
          "fluent-object-stale-owner-killed-owner",
          `
          SELECT countIf(
            SpanName = 'verification.host.kill'
            AND SpanAttributes['firegrid.host.id'] = 'a'
            AND SpanAttributes['verification.signal'] = 'SIGKILL'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-stale-owner-started-two-workers",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-stale-owner-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
