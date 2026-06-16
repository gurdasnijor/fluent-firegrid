import { FileSystem, Path } from "effect"
import {
  Cause,
  Console,
  Data,
  Deferred,
  Duration,
  Effect,
  Ref,
} from "effect"
import {
  compileCoverage,
  requirementId,
  type FirelabClaimResult,
  type FirelabComponentContext,
  type FirelabValidation,
} from "../types.ts"
import {
  analyzeCoverage,
  type CoverageObservations,
  printSummary as printCoverage,
} from "./coverage.ts"
import { annotateSide } from "./side.ts"
import { readTraceSpans } from "./trace.ts"
import { TelemetryLive, type TelemetryDestination } from "./telemetry.ts"

const defaultNamespace = "firelab"

type ValidationOutcome =
  | { readonly _tag: "DriverCompleted"; readonly observations: CoverageObservations }
  | { readonly _tag: "StopSignaled" }
  | { readonly _tag: "TimedOut" }

const ValidationOutcome = Data.taggedEnum<ValidationOutcome>()

const sanitizeSegment = (value: string): string =>
  value.replace(/[^A-Za-z0-9_.-]/g, "-").replace(/-+/g, "-")

// Chronological-first format so `ls .simulate/runs/` reads newest-last by
// default and tab-completion of "today's runs" actually narrows. Legacy
// runner used the same shape; we keep it for consistency.
// CLI artifact-directory filename stamp, not durable workflow state.
// effect-quality-allow-wall-clock
const newRunId = (validationId: string): string =>
  `${new Date().toISOString().replace(/[:.]/g, "-")}__${sanitizeSegment(validationId)}`

// Package-relative .simulate/ root. Resolved off this module's URL (via the
// Path service inside the run Effect) so it stays correct regardless of cwd —
// the script may be invoked from anywhere in the monorepo via `pnpm --filter`.
const artifactRootUrl = new URL("../../.simulate/", import.meta.url)


interface RunOptions {
  readonly timeoutMs: number
  // When true, also emit each completed span to stdout via the OTel
  // ConsoleSpanExporter. Off by default — the file destination is the
  // primary artifact; console is an opt-in debugging aid that's noisy
  // enough to drown the actual signal during a real run.
  readonly console: boolean
  readonly watch: boolean
}

// Only update the latest-pointer if the run produced at least one span.
// Writing eagerly at run start meant an interrupted / fast-failing run
// would clobber `latest.json` with a pointer to an empty runDir, and the
// next `show` (no arg) would TraceFileMissing on the stale
// pointer. Now this runs as a finalizer after the validation block — if
// the trace file is empty or missing, the prior valid pointer is
// preserved.
const maybeWriteLatest = (
  runId: string,
  validationId: string,
  runDir: string,
  tracePath: string,
  latestPath: string,
) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    if (!(yield* fs.exists(tracePath))) return
    const info = yield* fs.stat(tracePath)
    if (Number(info.size) === 0) return
    yield* fs.writeFileString(
      latestPath,
      JSON.stringify({ runId, validationId, runDir }, null, 2) + "\n",
    )
    yield* Effect.logDebug("latest pointer updated")
  })

const isEffect = (value: unknown): value is Effect.Effect<FirelabClaimResult, unknown, unknown> =>
  Effect.isEffect(value)

const claimResult = (value: FirelabClaimResult | Effect.Effect<FirelabClaimResult, unknown, unknown>) =>
  isEffect(value) ? value : Effect.succeed(value)

const observationFor = (actual: FirelabClaimResult) => ({
  passed: actual === undefined || actual === true,
  ...(actual === undefined ? {} : { actual }),
})

const componentContext = (
  validation: FirelabValidation<unknown>,
  runId: string,
  requirement: {
    readonly id: string
    readonly description: string
  },
): FirelabComponentContext => {
  const id = requirementId(validation, requirement.id)
  const baseKey = `${sanitizeSegment(runId)}.${sanitizeSegment(id)}`
  return {
    validationId: validation.id,
    runId,
    feature: validation.feature,
    requirementId: id,
    requirementLocalId: requirement.id,
    requirementDescription: requirement.description,
    key: baseKey,
    keyFor: (suffix) => `${baseKey}.${sanitizeSegment(suffix)}`,
  }
}

const runRequirements = (
  validation: FirelabValidation<unknown>,
  runId: string,
): Effect.Effect<CoverageObservations, never, unknown> =>
  Effect.gen(function*() {
    const entries = yield* Effect.forEach(validation.requirements, (requirement) => {
      const id = requirementId(validation, requirement.id)
      const context = componentContext(validation, runId, requirement)
      const scopedClaim = Effect.gen(function*() {
        const component = yield* validation.component(context)
        const claimed = yield* Effect.try({
          try: () => requirement.claim(component),
          catch: (error) => error,
        })
        return yield* claimResult(claimed)
      })
      return scopedClaim.pipe(
        Effect.matchCause({
          onFailure: (cause) =>
            [id, { passed: false, error: Cause.pretty(cause) }] as const,
          onSuccess: (actual) => [id, observationFor(actual)] as const,
        }),
        Effect.withSpan("firelab.requirement", {
          attributes: {
            "firelab.feature.product": validation.feature.product,
            "firelab.feature.name": validation.feature.name,
            "firelab.requirement.id": id,
          },
        }),
        Effect.annotateSpans({
          "firelab.feature.product": validation.feature.product,
          "firelab.feature.name": validation.feature.name,
          "firelab.requirement.id": id,
        }),
      )
    })
    const observations: Record<string, unknown> = {}
    entries.forEach((entry) => {
      if (entry !== undefined) {
        observations[entry[0]] = entry[1]
      }
    })
    return observations
  })

const runComponent = (
  validation: FirelabValidation<unknown>,
  runId: string,
): Effect.Effect<CoverageObservations, unknown, never> => {
  const program = runRequirements(validation, runId)
  return (validation.backend === undefined
    ? program
    : program.pipe(Effect.provide(validation.backend))) as Effect.Effect<CoverageObservations, unknown, never>
}

export const runValidation = (
  validation: FirelabValidation<unknown>,
  options: RunOptions,
) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const artifactRoot = yield* path.fromFileUrl(artifactRootUrl)
    const runsRoot = path.join(artifactRoot, "runs")
    const latestPath = path.join(artifactRoot, "latest.json")
    const namespace = defaultNamespace
    const stopSignal = yield* Deferred.make<void>()
    const sigintCount = yield* Ref.make(0)
    const runId = newRunId(validation.id)
    const runDir = path.join(runsRoot, runId)
    yield* fs.makeDirectory(runDir, { recursive: true })

    const tracePath = path.join(runDir, "trace.jsonl")
    const destination: TelemetryDestination = options.console
      ? { _tag: "console" }
      : { _tag: "file", filePath: tracePath }

    const telemetry = TelemetryLive(validation, runId, {
      namespace,
      destination,
      spanProcessors: [],
    })
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const onSigint = Ref.updateAndGet(sigintCount, n => n + 1).pipe(
          Effect.flatMap(count =>
            count === 1
              ? Deferred.complete(stopSignal, Effect.void).pipe(
                Effect.asVoid,
              )
              : Effect.sync(() => globalThis.process.exit(130)),
          ),
        )
        const handler = () => {
          Effect.runFork(onSigint)
        }
        globalThis.process.on("SIGINT", handler)
        return handler
      }),
      handler => Effect.sync(() => globalThis.process.off("SIGINT", handler)),
    )

    yield* Console.log(`run: ${runId}`)
    yield* Console.log(`dir: ${runDir}`)
    if (destination._tag === "file") {
      yield* Console.log(`trace: ${destination.filePath}`)
    }

    const outcome = yield* Effect.gen(function*() {
      yield* Effect.logInfo("validation starting").pipe(
        Effect.annotateLogs({
          runId,
          validationId: validation.id,
          namespace,
        }),
      )
      const outcome = yield* Effect.race(
        runComponent(validation, runId).pipe(
          annotateSide("driver"),
          Effect.map((observations) => ValidationOutcome.DriverCompleted({ observations })),
        ),
        Deferred.await(stopSignal).pipe(Effect.as(ValidationOutcome.StopSignaled())),
      ).pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(options.timeoutMs),
          orElse: () => Effect.succeed(ValidationOutcome.TimedOut()),
        }),
      )

      yield* Effect.annotateCurrentSpan(
        "firegrid.validation.outcome",
        outcome._tag,
      )
      yield* Effect.logInfo("validation stopped").pipe(
        Effect.annotateLogs({
          runId,
          validationId: validation.id,
          namespace,
          outcome: outcome._tag,
        }),
      )
      return outcome
    }).pipe(
      Effect.withSpan("firegrid.validation.run", {
        attributes: {
          "firegrid.validation.id": validation.id,
          "firegrid.run.id": runId,
          "firegrid.namespace": namespace,
        },
      }),
      Effect.provide(telemetry),
      // Conditional finalizer — runs on success, fail, and interrupt.
      // Only writes latest.json if at least one span actually flushed to
      // the trace file.
      Effect.ensuring(
        maybeWriteLatest(runId, validation.id, runDir, tracePath, latestPath).pipe(
          Effect.ignore,
        ),
      ),
    )

    const observations = outcome._tag === "DriverCompleted" ? outcome.observations : {}
    if (Object.keys(observations).length > 0) {
      yield* fs.writeFileString(
        path.join(runDir, "observations.json"),
        JSON.stringify(observations, null, 2) + "\n",
      )
    }

    const coverage = compileCoverage(validation)
    if (coverage === undefined) {
      yield* Console.log(
        "coverage: (none) — validation has no coverage spec; no computed verdict",
      )
      yield* Console.log(`       firelab show ${runId}   ·   firelab perf ${runId}`)
      return undefined
    }
    const spans = yield* readTraceSpans(runDir).pipe(
      Effect.orElseSucceed(() => []),
    )
    const report = analyzeCoverage(coverage, spans, observations)
    yield* printCoverage(report)
    yield* Console.log(
      `       firelab show ${runId}   ·   firelab perf ${runId}   ·   firelab gaps ${runId}   ·   firelab seams ${validation.id} ${runId}`,
    )
    return report
  }).pipe(Effect.scoped)
