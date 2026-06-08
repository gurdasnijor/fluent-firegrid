export const sessionId = "fluent-worker-redrive-session"
export const agentName = "firelab-fluent-worker-redrive"
export const consumerId = "fluent-worker-redrive-consumer"
export const failureConsumerId = "fluent-worker-redrive-failure-consumer"
export const workerA = "fluent-worker-redrive-worker-a"
export const workerB = "fluent-worker-redrive-worker-b"

export const factNames = {
  materialized: "fluent.worker_redrive.materialized",
  sideEffectExecuted: "fluent.worker_redrive.side_effect.executed",
  sideEffectResult: "fluent.worker_redrive.side_effect.result",
  l2Outcome: "fluent.worker_redrive.l2_outcome",
  ackSucceeded: "fluent.worker_redrive.ds_ack.succeeded",
  releaseSucceeded: "fluent.worker_redrive.ds_release.succeeded",
  contentionRejected: "fluent.worker_redrive.contention.rejected",
  appendFailedNoAck: "fluent.worker_redrive.append_failed_no_ack",
  substrateRewake: "fluent.worker_redrive.substrate_rewake",
} as const

export type WorkerRedriveFactName =
  typeof factNames[keyof typeof factNames]

const streamRoute = (
  namespace: string,
  segments: ReadonlyArray<string>,
): string =>
  `/v1/stream/${
    [
      namespace,
      ...segments,
    ].map(encodeURIComponent).join("/")
  }`

export const workStreamRoute = (namespace: string): string =>
  streamRoute(namespace, ["fluent-worker-redrive", "work"])

export const wakeStreamRoute = (namespace: string): string =>
  streamRoute(namespace, ["fluent-worker-redrive", "wake"])

export const failureWorkStreamRoute = (namespace: string): string =>
  streamRoute(namespace, ["fluent-worker-redrive", "failure-work"])

export const failureWakeStreamRoute = (namespace: string): string =>
  streamRoute(namespace, ["fluent-worker-redrive", "failure-wake"])
