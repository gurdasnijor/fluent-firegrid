import { Effect } from "effect"

export interface IncidentInput {
  readonly id: string
  readonly title: string
  readonly signal: "ci" | "runtime" | "security"
}

interface IncidentTriage {
  readonly severity: "low" | "medium" | "high"
  readonly route: "watch" | "worker" | "coordinator"
}

export const classifyIncident = (
  input: IncidentInput,
): IncidentTriage => {
  if (input.signal === "security") {
    return { severity: "high", route: "coordinator" }
  }
  if (input.signal === "runtime") {
    return { severity: "medium", route: "worker" }
  }
  return { severity: "low", route: "watch" }
}

export const draftPatchPlan = (
  input: IncidentInput,
  triage: IncidentTriage,
): string =>
  `${input.id}:${triage.route}:${triage.severity}:${input.title}`

export const publishTrace = (plan: string): string =>
  `trace:${plan}`

export const collectIncidentContext = (input: IncidentInput): string =>
  `context:${input.signal}:${input.title}`

export const openRemediation = (
  input: IncidentInput,
  plan: string,
): string =>
  `remediation:${input.id}:${plan}`

export const notifyCoordinator = (remediationId: string): string =>
  `notified:${remediationId}`

export const delayedValue = <T>(
  durationMs: number,
  value: T,
): Effect.Effect<T> =>
  Effect.as(Effect.sleep(durationMs), value)
