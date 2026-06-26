import { getS2ObjectStateValue, objectStateStreamName } from "@firegrid/store"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"

import { requestJson } from "../src/HttpProofClient.ts"
import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = new URL("../fixtures/store-object-worker.ts", import.meta.url).pathname

const portFromTrialId = (trialId: string, salt: string): number => {
  const hash = Array.from(`fluent-object-replay-state-${trialId}-${salt}`).reduce(
    (current, char) => (current * 31 + char.charCodeAt(0)) % 10_000,
    0
  )
  return 45_000 + hash
}

const waitForStateValue = (
  s2Endpoint: string,
  namespace: string,
  expected: number
): Effect.Effect<number, VerificationError> => {
  const config = { namespace, s2Endpoint }
  const address = { key: "counter-1", objectName: "cross-host-counter" }
  const streamName = objectStateStreamName(config, address)
  const loop = (remaining: number): Effect.Effect<number, VerificationError> =>
    getS2ObjectStateValue(config, address, "counterState", "v").pipe(
      Effect.mapError((cause) =>
        new VerificationError({ cause, message: `failed to inspect S2 object state stream ${streamName}` })
      ),
      Effect.flatMap((value) => {
        const current = Option.isSome(value)
            && typeof value.value === "object"
            && value.value !== null
            && "value" in value.value
            && typeof value.value.value === "number"
          ? value.value.value
          : undefined
        if (current === expected) return Effect.succeed(current)
        if (remaining <= 0) {
          return Effect.fail(
            new VerificationError({
              message: `timed out waiting for object state ${streamName} to reach ${expected}`
            })
          )
        }
        return Effect.sleep("25 millis").pipe(Effect.andThen(loop(remaining - 1)))
      })
    )
  return loop(160)
}

export default proof("store.object-replay-state")
  .describedAs(
    "Proves S2-backed object state replay safety: a run that dies after state.set but before completion resumes with its journaled read and does not double-apply the mutation."
  )
  .spec(({ property, trialId }) => {
    const portA = portFromTrialId(trialId, "a")
    const portB = portFromTrialId(trialId, "b")
    const hostA = `http://127.0.0.1:${portA}`
    const hostB = `http://127.0.0.1:${portB}`

    return property("store.object-replay-state-proof")
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
            return yield* new VerificationError({ message: "fluent S2 object replay-state proof requires s2Lite" })
          }
          const namespace = `fluent-object-cross-host-${trialId}`
          const crashRequest = yield* requestJson(`${hostA}/crash-after-set?by=5`, { method: "POST" }).pipe(
            Effect.forkDetach
          )
          const preCrashValue = yield* waitForStateValue(s2Endpoint, namespace, 5)

          yield* faults.killHost("a")
          yield* requestJson<{ readonly now: number }>(`${hostB}/now?value=3000`, { method: "POST" })

          const afterTakeover = yield* requestJson<{ readonly hostId: string; readonly value: number }>(
            `${hostB}/add?by=7`,
            { method: "POST" }
          )
          const loaded = yield* requestJson<{ readonly hostId: string; readonly value: number }>(`${hostB}/value`)
          yield* Fiber.interrupt(crashRequest)

          return {
            afterTakeover,
            preCrashValue,
            value: loaded.value
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          afterTakeover: {
            hostId: "b",
            value: 12
          },
          preCrashValue: 5,
          value: 12
        }),
        traceSql(
          "fluent-object-replay-state-killed-owner",
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
          "fluent-object-replay-state-started-two-workers",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-replay-state-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
