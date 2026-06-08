import {
  FiregridOtelLive,
} from "../observability/node.ts"
import type {
  FiregridOtelDestination,
  FiregridOtelResource,
  SpanProcessor,
} from "../observability/node.ts"
import { Command, FileSystem, Path } from "@effect/platform"
import { Effect, Layer } from "effect"
import type { FirelabSimulation } from "../types.ts"

// Run-provenance attributes (Item E of the §6 observability batch). They
// identify the binary that produced the trace, independent of the run. Each
// source degrades to absent on failure — a non-git checkout or a sandboxed
// environment is a normal condition, not an error, and the trace stays useful
// without the attributes. Resolved through @effect/platform Command (git) +
// FileSystem (package version) so there is no synchronous module-load I/O.
const gitOutput = (...args: ReadonlyArray<string>) =>
  Command.string(Command.make("git", ...args)).pipe(
    Effect.map(out => out.trim()),
    Effect.orElseSucceed(() => ""),
  )

const provenanceAttributes = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const commit = yield* gitOutput("rev-parse", "HEAD")
  const branch = yield* gitOutput("rev-parse", "--abbrev-ref", "HEAD")
  const version = yield* fs
    .readFileString(yield* path.fromFileUrl(new URL("../../package.json", import.meta.url)))
    .pipe(
      Effect.map(text => (JSON.parse(text) as { version?: string }).version ?? ""),
      Effect.orElseSucceed(() => ""),
    )
  const entries: Array<readonly [string, string]> = []
  if (commit.length > 0) entries.push(["firegrid.git.commit", commit])
  if (branch.length > 0 && branch !== "HEAD") entries.push(["firegrid.git.branch", branch])
  if (version.length > 0) entries.push(["firegrid.firelab.version", version])
  return Object.fromEntries(entries)
})

const resource = (
  simulation: FirelabSimulation<unknown>,
  runId: string,
  options: {
    readonly namespace: string
    readonly durableStreamsBaseUrl: string
  },
  provenance: Record<string, string>,
): FiregridOtelResource => ({
  serviceName: "firelab",
  attributes: {
    "firegrid.simulation.id": simulation.id,
    "firegrid.run.id": runId,
    "firegrid.namespace": options.namespace,
    "firegrid.durable_streams.base_url": options.durableStreamsBaseUrl,
    "firegrid.process.role": "firelab",
    ...provenance,
  },
})

// `firegrid.side` carries the value we want to filter on more than the
// hyphen-named OTel `service.namespace` does, so we leave it as a span
// attribute (propagated via `Effect.annotateSpans` in runner/side.ts).
export type TelemetryDestination = FiregridOtelDestination

// Routing precedence:
//   1. OTEL_EXPORTER_OTLP_ENDPOINT set → OTLP HTTP (production backend).
//   2. destination._tag === "console" → ConsoleSpanExporter (opt-in --console).
//   3. default → file destination — one JSON line per span at filePath.
// The resource (incl. run-provenance) is resolved in an Effect, then the layer
// is built — `Layer.unwrapEffect` over a config Effect is the idiomatic
// @effect/opentelemetry shape (`NodeSdk.layer` likewise accepts an Effect config).
export const TelemetryLive = (
  simulation: FirelabSimulation<unknown>,
  runId: string,
  options: {
    readonly namespace: string
    readonly durableStreamsBaseUrl: string
    readonly destination: TelemetryDestination
    readonly heartbeatProcessor: SpanProcessor | undefined
  },
) =>
  Layer.unwrapEffect(
    Effect.map(provenanceAttributes, provenance =>
      FiregridOtelLive({
        resource: resource(simulation, runId, options, provenance),
        destination: options.destination,
        spanProcessors: options.heartbeatProcessor === undefined
          ? []
          : [options.heartbeatProcessor],
      })),
  )
