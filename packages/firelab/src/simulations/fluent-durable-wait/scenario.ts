export const sessionId = "fluent-durable-wait-session"
export const reviewTurnId = "fluent-durable-wait-review-turn"
export const mainTurnId = "fluent-durable-wait-main-turn"
export const reviewWaitId = "review-posted"
export const mainWaitId = "pr-merged"
export const agentName = "firelab-fluent-durable-wait"
export const consumerId = "fluent-durable-wait-consumer"
export const workerId = "fluent-durable-wait-worker"

export const factNames = {
  correlationSnapshot: "fluent.durable_wait.correlation.snapshot",
  turnParked: "fluent.durable_wait.turn.parked",
  liveProjectionChanged: "fluent.durable_wait.live_projection.changed",
  wakeOutcome: "fluent.durable_wait.wake.outcome",
  witnessComplete: "fluent.durable_wait.witness.complete",
} as const

export type DurableWaitFactName =
  typeof factNames[keyof typeof factNames]

const route = (
  namespace: string,
  parts: ReadonlyArray<string>,
): string =>
  `/v1/stream/${
    [
      namespace,
      ...parts,
    ].map(encodeURIComponent).join("/")
  }`

export const workRoute = (namespace: string): string =>
  route(namespace, ["fluent-durable-wait", "work"])

export const wakeRoute = (namespace: string): string =>
  route(namespace, ["fluent-durable-wait", "wake"])
