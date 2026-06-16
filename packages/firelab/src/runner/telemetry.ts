import {
  FiregridOtelLive,
} from "../observability/node.ts"
import type {
  FiregridOtelDestination,
  FiregridOtelResource,
  SpanProcessor,
} from "../observability/node.ts"
import type { FirelabValidation } from "../types.ts"

const resource = (
  validation: FirelabValidation<unknown>,
  runId: string,
  options: {
    readonly namespace: string
  },
): FiregridOtelResource => ({
  serviceName: "firelab",
  attributes: {
    "firegrid.validation.id": validation.id,
    "firegrid.run.id": runId,
    "firegrid.namespace": options.namespace,
    "firelab.process.role": "validation-runner",
  },
})

// `firegrid.side` carries the value we want to filter on more than the
// hyphen-named OTel `service.namespace` does, so we leave it as a span
// attribute (propagated via `Effect.annotateSpans` in runner/side.ts).
export type TelemetryDestination = FiregridOtelDestination

// Routing precedence:
//   1. destination._tag === "console" → ConsoleSpanExporter (opt-in --console).
//   2. default → file destination — one JSON line per span at filePath.
export const TelemetryLive = (
  validation: FirelabValidation<unknown>,
  runId: string,
  options: {
    readonly namespace: string
    readonly destination: TelemetryDestination
    readonly spanProcessors: ReadonlyArray<SpanProcessor>
  },
) =>
  FiregridOtelLive({
    resource: resource(validation, runId, options),
    destination: options.destination,
    spanProcessors: options.spanProcessors,
  })
