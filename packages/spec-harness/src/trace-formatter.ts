import { Formatter, type IFormatterOptions } from "@cucumber/cucumber"
import { Query } from "@cucumber/query"
import type { Envelope, TestCaseStarted, TestStepResultStatus } from "@cucumber/messages"
import type { SpanCount } from "./report-state.ts"
import { takeScenarioReport } from "./report-state.ts"

const statusLabel = (status: TestStepResultStatus | undefined): string =>
  status ?? "UNKNOWN"

const locationLabel = (uri: string | undefined, line: number | undefined): string =>
  line === undefined ? (uri ?? "(unknown)") : `${uri}:${line}`

const formatMs = (value: number): string =>
  `${value.toFixed(value >= 10 ? 1 : 3)}ms`

export default class TraceFormatter extends Formatter {
  public static readonly documentation =
    "Prints Cucumber scenario status with chDB trace proof and span summaries."

  private readonly query = new Query()
  private readonly started = new Map<string, TestCaseStarted>()
  private scenarios = 0
  private tracedScenarios = 0
  private scenariosWithProofs = 0
  private proofs = 0
  private spans = 0
  private traces = 0
  private durationMs = 0
  private readonly statuses = new Map<string, number>()
  private readonly spanNames = new Map<string, number>()

  constructor(options: IFormatterOptions) {
    super(options)
    options.eventBroadcaster.on("envelope", (envelope: Envelope) => {
      this.query.update(envelope)
      if (envelope.testCaseStarted !== undefined) {
        this.started.set(envelope.testCaseStarted.id, envelope.testCaseStarted)
      }
      if (envelope.testCaseFinished !== undefined) {
        this.logFinished(envelope.testCaseFinished.testCaseStartedId)
      }
      if (envelope.testRunFinished !== undefined) {
        this.logSummary()
      }
    })
  }

  private logFinished(testCaseStartedId: string): void {
    const testCaseStarted = this.started.get(testCaseStartedId)
    if (testCaseStarted === undefined) return
    const pickle = this.query.findPickleBy(testCaseStarted)
    if (pickle === undefined) return

    const result = this.query.findMostSevereTestStepResultBy(testCaseStarted)
    const location = this.query.findLocationOf(pickle)
    const report = takeScenarioReport(pickle.id)
    const status = statusLabel(result?.status)

    this.scenarios += 1
    this.statuses.set(status, (this.statuses.get(status) ?? 0) + 1)

    if (report === undefined) {
      if (status !== "UNDEFINED" && status !== "SKIPPED") {
        this.log(`\n[trace] ${status} ${pickle.name} (${locationLabel(pickle.uri, location?.line)})\n`)
        this.log("  trace: unavailable\n")
      }
      return
    }

    const hasTraceEvidence = report.proofs > 0 || report.coverage.spans > 0 || report.spans.length > 0
    if (!hasTraceEvidence) return

    this.tracedScenarios += 1
    this.proofs += report.proofs
    this.spans += report.coverage.spans
    this.traces += report.coverage.traces
    this.durationMs += report.coverage.totalDurationMs
    if (report.proofs > 0) {
      this.scenariosWithProofs += 1
    }
    report.spans.forEach((span) => {
      this.spanNames.set(span.name, (this.spanNames.get(span.name) ?? 0) + span.count)
    })

    this.log(`\n[trace] ${status} ${pickle.name} (${locationLabel(pickle.uri, location?.line)})\n`)
    this.log(`  proofs: ${report.proofs}\n`)
    this.log(
      `  coverage: ${report.coverage.spans} spans, ${report.coverage.traces} traces, `
        + `${report.coverage.evidenceSpans} scenario markers\n`,
    )
    this.log(
      `  duration: total ${formatMs(report.coverage.totalDurationMs)}, `
        + `max span ${formatMs(report.coverage.maxDurationMs)}\n`,
    )
    if (report.spans.length === 0) {
      this.log("  spans: none\n")
      return
    }
    this.log("  spans:\n")
    report.spans.forEach((span) => {
      this.log(`    ${span.count} ${span.name}\n`)
    })
  }

  private logSummary(): void {
    if (this.scenarios === 0) return
    this.log("\n[trace-summary]\n")
    this.log(`  scenarios: ${this.scenarios}\n`)
    this.log(`  traced: ${this.tracedScenarios}\n`)
    this.log(`  untraced: ${this.scenarios - this.tracedScenarios}\n`)
    this.statuses.forEach((count, status) => {
      this.log(`  ${status.toLowerCase()}: ${count}\n`)
    })
    this.log(`  with proofs: ${this.scenariosWithProofs}\n`)
    this.log(`  proofs: ${this.proofs}\n`)
    this.log(`  spans: ${this.spans}\n`)
    this.log(`  traces: ${this.traces}\n`)
    this.log(`  duration: ${formatMs(this.durationMs)} total span time\n`)
    const topSpans = Array.from(this.spanNames.entries())
      .map(([name, count]): SpanCount => ({ name, count }))
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name))
      .slice(0, 10)
    if (topSpans.length === 0) return
    this.log("  top spans:\n")
    topSpans.forEach((span) => {
      this.log(`    ${span.count} ${span.name}\n`)
    })
  }
}
