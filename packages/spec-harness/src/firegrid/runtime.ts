import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import { type ChdbClient, ChdbSession, ChdbSpanExporter, layer as ChdbLayer } from "@firegrid/observability"
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { Context, Effect, type FileSystem, Layer, type Path } from "effect"
import type { S2Client } from "effect-s2"
import { S2LiteLive } from "../s2lite.ts"

/**
 * The firegrid spec World services — the production stack the `@sql:` trace
 * proofs observe: a real (s2 lite) S2 backend, chDB (the span store + proof query
 * engine), and an OpenTelemetry pipeline that exports every span into the
 * `otel_traces` table. Step effects run with this layer ambient, so the product
 * durable engine's spans are captured under the per-scenario trace.
 *
 * Ported from the legacy cucumber-js harness runtime, as an Effect layer rather
 * than a module-global ManagedRuntime.
 */

export type WorldServices = S2Client | ChdbSession | ChdbClient | FileSystem.FileSystem | Path.Path | SpecTracing

/** A handle to force-flush the batch span processor before querying proofs. */
export class SpecTracing extends Context.Service<SpecTracing, { readonly flush: Effect.Effect<void> }>()(
  "@firegrid/spec-harness/firegrid/runtime/SpecTracing",
) {}

const ChdbLive = ChdbLayer({})

// Build the OTel pipeline + a flush handle over the same BatchSpanProcessor.
const tracingLayer = Layer.unwrap(
  Effect.gen(function*() {
    const session = yield* ChdbSession
    const processor = new BatchSpanProcessor(new ChdbSpanExporter({ session, table: "otel_traces" }))
    const sdk = NodeSdk.layer(() => ({
      resource: { serviceName: "firegrid-cucumber" },
      spanProcessor: [processor],
    }))
    return Layer.merge(sdk, Layer.succeed(SpecTracing, { flush: Effect.promise(() => processor.forceFlush()) }))
  }),
)

/**
 * The full World-services layer for a firegrid run (OTel → chDB, S2, chDB, FS,
 * Path). chDB session setup failures (`SqlError`) are a harness crash boundary,
 * so the layer dies on them rather than surfacing a typed error to every step.
 */
export const WorldServicesLive =
  // eslint-disable-next-line no-restricted-syntax -- spec harness World-services setup is a documented crash boundary
  Layer.orDie(
    Layer.mergeAll(tracingLayer, S2LiteLive, NodeFileSystem.layer, NodePath.layer).pipe(Layer.provideMerge(ChdbLive)),
  )
