export interface SpanCount {
  readonly name: string
  readonly count: number
}

export interface ScenarioReport {
  readonly scenarioId: string
  readonly proofs: number
  readonly spans: ReadonlyArray<SpanCount>
}

const reports = new Map<string, ScenarioReport>()

export const recordScenarioReport = (report: ScenarioReport): void => {
  reports.set(report.scenarioId, report)
}

export const takeScenarioReport = (scenarioId: string): ScenarioReport | undefined => {
  const report = reports.get(scenarioId)
  reports.delete(scenarioId)
  return report
}
