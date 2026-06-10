import { FiregridConfig } from "../config.ts"
import { FileSystem, Path } from "@effect/platform"
import {
  Console,
  Config,
  Data,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Ref,
} from "effect"
// Local in-process durable-streams server for simulation runs (CLI-only).
import { DurableStreamTestServer } from "@durable-streams/server"
import {
  type FirelabHostEnv,
  type FirelabSimulation,
} from "../types.ts"
import {
  analyzeCoverage,
  printSummary as printCoverage,
} from "./coverage.ts"
import { makeHeartbeat } from "./heartbeat.ts"
import { annotateSide } from "./side.ts"
import { readTraceSpans } from "./trace.ts"
import { TelemetryLive, type TelemetryDestination } from "./telemetry.ts"

const defaultNamespace = "firelab"

const NamespaceConfig = Config.string("FIREGRID_RUNTIME_NAMESPACE").pipe(
  Config.withDefault(defaultNamespace),
)
const DurableStreamsBaseUrlConfig = Config.string("DURABLE_STREAMS_BASE_URL").pipe(
  Config.option,
)

type SimulationOutcome =
  | { readonly _tag: "DriverCompleted" }
  | { readonly _tag: "StopSignaled" }
  | { readonly _tag: "TimedOut" }

const SimulationOutcome = Data.taggedEnum<SimulationOutcome>()

const durableStreamsBaseUrl = Effect.gen(function*() {
  const configured = yield* DurableStreamsBaseUrlConfig
  if (Option.isSome(configured)) {
    return configured.value
  }
  const server = yield* Effect.acquireRelease(
    Effect.promise(async () => {
      const server = new DurableStreamTestServer({ port: 0, host: "127.0.0.1" })
      const baseUrl = await server.start()
      return { server, baseUrl }
    }),
    ({ server }) => Effect.promise(() => server.stop()),
  )
  return server.baseUrl
})

const sanitizeSegment = (value: string): string =>
  value.replace(/[^A-Za-z0-9_.-]/g, "-").replace(/-+/g, "-")

// Chronological-first format so `ls .simulate/runs/` reads newest-last by
// default and tab-completion of "today's runs" actually narrows. Legacy
// runner used the same shape; we keep it for consistency.
// CLI artifact-directory filename stamp, not durable workflow state.
// effect-quality-allow-wall-clock
const newRunId = (simulationId: string): string =>
  `${new Date().toISOString().replace(/[:.]/g, "-")}__${sanitizeSegment(simulationId)}`

// Package-relative .simulate/ root. Resolved off this module's URL (via the
// Path service inside the run Effect) so it stays correct regardless of cwd —
// the script may be invoked from anywhere in the monorepo via `pnpm --filter`.
const simulateRootUrl = new URL("../../.simulate/", import.meta.url)


interface RunOptions {
  readonly timeoutMs: number
  // When true, also emit each completed span to stdout via the OTel
  // ConsoleSpanExporter. Off by default — the file destination is the
  // primary artifact; console is an opt-in debugging aid that's noisy
  // enough to drown the actual signal during a real run.
  readonly console: boolean
  // tf-ewo: --watch flag opts the heartbeat processor into per-event
  // emission (compact one-line-per-span to stderr) in addition to the
  // periodic digest. Default false — heartbeat-only is the right shape
  // for automated lanes / CI; per-event is for interactive debugging.
  readonly watch: boolean
}

// Only update the latest-pointer if the run produced at least one span.
// Writing eagerly at run start meant an interrupted / fast-failing run
// would clobber `latest.json` with a pointer to an empty runDir, and the
// next `simulate show` (no arg) would TraceFileMissing on the stale
// pointer. Now this runs as a finalizer after the simulation block — if
// the trace file is empty or missing, the prior valid pointer is
// preserved.
const maybeWriteLatest = (
  runId: string,
  simulationId: string,
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
      JSON.stringify({ runId, simulationId, runDir }, null, 2) + "\n",
    )
    yield* Effect.logDebug("latest pointer updated")
  })

export const runSimulation = (
  simulation: FirelabSimulation<unknown>,
  options: RunOptions,
) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const simulateRoot = yield* path.fromFileUrl(simulateRootUrl)
    const runsRoot = path.join(simulateRoot, "runs")
    const latestPath = path.join(simulateRoot, "latest.json")
    const baseUrl = yield* durableStreamsBaseUrl
    const namespace = yield* NamespaceConfig
    const stopSignal = yield* Deferred.make<void>()
    const sigintCount = yield* Ref.make(0)
    const runId = newRunId(simulation.id)
    const runDir = path.join(runsRoot, runId)
    yield* fs.makeDirectory(runDir, { recursive: true })

    const tracePath = path.join(runDir, "trace.jsonl")
    const destination: TelemetryDestination = options.console
      ? { _tag: "console" }
      : { _tag: "file", filePath: tracePath }

    // Heartbeat only fires when destination is file. OTLP + console
    // already have their own activity signal (remote backend / stdout
    // spam); heartbeat exists specifically to make the invisible-file
    // path observable. `makeHeartbeat` owns the Queue + Refs + ticker
    // fiber + finalizer in its own scope; the runner just takes the
    // processor handle and forwards it to TelemetryLive.
    const heartbeat = destination._tag === "file"
      ? yield* makeHeartbeat({
        minInterval: Duration.seconds(2),
        maxInterval: Duration.seconds(10),
        perEvent: options.watch,
      })
      : undefined
    const telemetry = TelemetryLive(simulation, runId, {
      namespace,
      durableStreamsBaseUrl: baseUrl,
      destination,
      heartbeatProcessor: heartbeat?.processor,
    })
    const hostEnv: FirelabHostEnv = {
      simulationId: simulation.id,
      runId,
      namespace,
      durableStreamsBaseUrl: baseUrl,
      processEnv: globalThis.process.env,
      stopSignal: {
        complete: Deferred.complete(stopSignal, Effect.void).pipe(
          Effect.asVoid,
        ),
      },
    }
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

    yield* Effect.gen(function*() {
      yield* Effect.logInfo("simulation starting").pipe(
        Effect.annotateLogs({
          runId,
          simulationId: simulation.id,
          namespace,
          baseUrl,
        }),
      )
      if (simulation.launchHost !== false) {
        const hostLayer = simulation.host?.(hostEnv)
        if (hostLayer === undefined) {
          return yield* Effect.fail(
            new Error(`simulation ${simulation.id} must define host(env) unless launchHost is false`),
          )
        }
        // The host owns its RuntimeContext(s) + the MCP server transport; launch
        // it as a background daemon for the driver's scope. The driver talks to it
        // over `@firegrid/client-sdk/mcp` (durable-streams), never via in-process
        // host services — so it needs only `FiregridConfig`.
        yield* Layer.launch(hostLayer).pipe(Effect.forkScoped)
      }

      const clientConfig = {
        durableStreamsBaseUrl: baseUrl,
        namespace,
      }
      const outcome = yield* Effect.raceWith(
        simulation.driver.pipe(
          Effect.provideService(FiregridConfig, clientConfig),
          annotateSide("driver"),
        ),
        Deferred.await(stopSignal),
        {
          onSelfDone: (_exit, stopFiber) =>
            Fiber.interrupt(stopFiber).pipe(
              Effect.as(SimulationOutcome.DriverCompleted()),
            ),
          onOtherDone: (_exit, driverFiber) =>
            Fiber.interrupt(driverFiber).pipe(
              Effect.as(SimulationOutcome.StopSignaled()),
            ),
        },
      ).pipe(
        Effect.timeoutTo({
          duration: Duration.millis(options.timeoutMs),
          onTimeout: () => SimulationOutcome.TimedOut(),
          onSuccess: outcome => outcome,
        }),
      )

      yield* Effect.annotateCurrentSpan(
        "firegrid.simulation.outcome",
        outcome._tag,
      )
      yield* Effect.logInfo("simulation stopped").pipe(
        Effect.annotateLogs({
          runId,
          simulationId: simulation.id,
          namespace,
          baseUrl,
          outcome: outcome._tag,
        }),
      )
    }).pipe(
      Effect.withSpan("firegrid.simulation.run", {
        attributes: {
          "firegrid.simulation.id": simulation.id,
          "firegrid.run.id": runId,
          "firegrid.namespace": namespace,
          "firegrid.durable_streams.base_url": baseUrl,
        },
      }),
      Effect.provide(telemetry),
      // Conditional finalizer — runs on success, fail, and interrupt.
      // Only writes latest.json if at least one span actually flushed to
      // the trace file.
      Effect.ensuring(
        maybeWriteLatest(runId, simulation.id, runDir, tracePath, latestPath).pipe(
          Effect.ignore,
        ),
      ),
    )

    // The trace oracle. Telemetry was released with the block above, so the
    // exporter has flushed trace.jsonl — read it back and compute the verdict
    // from host-substrate spans the driver cannot forge. A sim without a
    // coverage spec runs but produces no computed verdict (migration window).
    if (simulation.coverage === undefined) {
      yield* Console.log(
        "coverage: (none) — simulation has no coverage spec; no computed verdict",
      )
      return undefined
    }
    const spans = yield* readTraceSpans(runDir).pipe(
      Effect.orElseSucceed(() => []),
    )
    const report = analyzeCoverage(simulation.coverage, spans)
    yield* printCoverage(report)
    yield* Console.log(
      `       simulate gaps ${runId}   ·   simulate seams ${simulation.id} ${runId}`,
    )
    return report
  }).pipe(Effect.scoped)
