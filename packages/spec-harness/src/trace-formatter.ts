import { Formatter, type IFormatterOptions } from "@cucumber/cucumber"
import { Query } from "@cucumber/query"
import type { Envelope, TestCaseStarted, TestStepResultStatus } from "@cucumber/messages"
import { takeScenarioReport } from "./report-state.ts"

const statusLabel = (status: TestStepResultStatus | undefined): string =>
  status ?? "UNKNOWN"

const locationLabel = (uri: string | undefined, line: number | undefined): string =>
  line === undefined ? (uri ?? "(unknown)") : `${uri}:${line}`

export default class TraceFormatter extends Formatter {
  public static readonly documentation =
    "Prints Cucumber scenario status with chDB trace proof and span summaries."

  private readonly query = new Query()
  private readonly started = new Map<string, TestCaseStarted>()

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

    this.log(`\n[trace] ${statusLabel(result?.status)} ${pickle.name} (${locationLabel(pickle.uri, location?.line)})\n`)
    this.query.findTestStepFinishedAndTestStepBy(testCaseStarted).forEach(([stepFinished, testStep]) => {
      const pickleStep = this.query.findPickleStepBy(testStep)
      const stepName = pickleStep?.text ?? "(hook)"
      this.log(`  ${statusLabel(stepFinished.testStepResult.status)} ${stepName}\n`)
    })

    if (report === undefined) {
      this.log("  proofs: 0\n")
      this.log("  spans: unavailable\n")
      return
    }

    this.log(`  proofs: ${report.proofs}\n`)
    if (report.spans.length === 0) {
      this.log("  spans: none\n")
      return
    }
    this.log("  spans:\n")
    report.spans.forEach((span) => {
      this.log(`    ${span.count} ${span.name}\n`)
    })
  }
}
