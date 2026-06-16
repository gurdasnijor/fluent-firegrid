import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as NodeServices from "@effect/platform-node/NodeServices"
import { Console, Effect } from "effect"
import { gapsReport, seamsCoverage } from "./runner/coverage-cli.ts"
import { listValidations, selectedValidation } from "./runner/list.ts"
import { showPerf } from "./runner/perf.ts"
import { runValidation } from "./runner/runtime.ts"
import { listRuns, showRun } from "./runner/show.ts"

const usage = `
firelab <command>

Commands:
  list
  run <validation-id> [--timeout-ms N] [--console]
  runs
  show [run-id]
  perf <run-id>
  gaps [run-id]
  seams <validation-id> [run-id]
`.trim()

const readOption = (
  args: ReadonlyArray<string>,
  name: string,
): string | undefined => {
  const index = args.indexOf(name)
  return index >= 0 ? args[index + 1] : undefined
}

const hasFlag = (args: ReadonlyArray<string>, name: string): boolean =>
  args.includes(name)

const withoutOptions = (args: ReadonlyArray<string>): ReadonlyArray<string> => {
  const out: Array<string> = []
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg === undefined) continue
    if (arg === "--console") continue
    if (arg === "--timeout-ms") {
      i += 1
      continue
    }
    out.push(arg)
  }
  return out
}

const command = (argv: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const [cmd = "help", ...rest] = argv
    switch (cmd) {
      case "list": {
        const validations = yield* listValidations
        yield* Console.log(
          validations
            .map(validation => `${validation.id}\t${validation.description}`)
            .join("\n"),
        )
        return
      }
      case "run": {
        const positional = withoutOptions(rest)
        const validationId = positional[0]
        if (validationId === undefined) return yield* Console.error(usage)
        const timeoutMs = Number(readOption(rest, "--timeout-ms") ?? "300000")
        const validation = yield* selectedValidation(validationId)
        const report = yield* runValidation(validation, {
          timeoutMs,
          console: hasFlag(rest, "--console"),
          watch: false,
        })
        if (report !== undefined && report.gatingFailing > 0) {
          yield* Effect.sync(() => {
            process.exitCode = 1
          })
        }
        return
      }
      case "runs":
        return yield* listRuns
      case "show":
        return yield* showRun(rest[0])
      case "perf": {
        const runId = rest[0]
        if (runId === undefined) return yield* Console.error(usage)
        return yield* showPerf(runId, {
          top: 15,
          idleThresholdMs: 5_000,
          findingDraft: false,
          findingThresholdMs: 30_000,
        })
      }
      case "gaps":
        return yield* gapsReport(rest[0])
      case "seams": {
        const validationId = rest[0]
        if (validationId === undefined) return yield* Console.error(usage)
        return yield* seamsCoverage(validationId, rest[1])
      }
      default:
        return yield* Console.log(usage)
    }
  })

command(process.argv.slice(2)).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain({ disableErrorReporting: false }),
)
