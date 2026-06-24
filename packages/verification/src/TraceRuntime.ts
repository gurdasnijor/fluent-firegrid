import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import { ChdbClient, ChdbSession, ChdbSpanExporter, layer as ChdbLayer } from "@firegrid/observability"
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

import { VerificationRuntime } from "./Property.ts"
import { bindTrialSql } from "./TraceViews.ts"
import { VerificationError } from "./VerificationError.ts"

export interface TraceRuntimeConfig {
  readonly serviceName?: string
}

const escapeString = (value: string): string => value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

const spanMatchSql = (
  span: string,
  attributes: Record<string, string> | undefined
): string => {
  const attrPredicates = Object.entries(attributes ?? {})
    .map(([key, value]) => `SpanAttributes['${escapeString(key)}'] = '${escapeString(value)}'`)
    .join(" AND ")
  return `
SELECT count() > 0 AS ok
FROM otel_traces
WHERE SpanName = '${escapeString(span)}'
${attrPredicates === "" ? "" : `AND ${attrPredicates}`}
`
}

const makeWaitForSpan = (
  processor: BatchSpanProcessor,
  chdb: ChdbClient["Service"]
) =>
  Effect.fn("VerificationRuntime.waitForSpan")(function*(
    span: string,
    options: {
      readonly attributes?: Record<string, string>
      readonly attempts?: number
      readonly interval?: Duration.Input
    } = {}
  ) {
    const attempts = options.attempts ?? 200
    const interval = options.interval ?? "25 millis"
    const sql = spanMatchSql(span, options.attributes)

    const loop = (remaining: number): Effect.Effect<void, VerificationError> =>
      Effect.gen(function*() {
        yield* Effect.tryPromise({
          try: () => processor.forceFlush(),
          catch: (cause) => new VerificationError({ message: "failed to flush trace processor", cause })
        })
        const rows = yield* chdb.unsafe<{ readonly ok: boolean | number | string }>(bindTrialSql(sql, "")).pipe(
          Effect.mapError((cause) => new VerificationError({ message: `failed to query for span ${span}`, cause }))
        )
        if (rows.some((row) => row.ok === true || row.ok === 1 || row.ok === "1")) {
          return
        }
        if (remaining <= 0) {
          return yield* new VerificationError({ message: `timed out waiting for span ${span}` })
        }
        yield* Effect.sleep(interval)
        return yield* loop(remaining - 1)
      })

    return yield* loop(attempts)
  })

export const layer = (config: TraceRuntimeConfig = {}) => {
  const RuntimeAndOtel = Layer.unwrap(
    Effect.gen(function*() {
      const session = yield* ChdbSession
      const chdb = yield* ChdbClient
      const processor = new BatchSpanProcessor(
        new ChdbSpanExporter({ session, table: "otel_traces" })
      )
      const RuntimeLive = Layer.succeed(VerificationRuntime, {
        flush: Effect.tryPromise({
          try: () => processor.forceFlush(),
          catch: (cause) => new VerificationError({ message: "failed to flush trace processor", cause })
        }),
        waitForSpan: makeWaitForSpan(processor, chdb)
      })
      const OtelLive = NodeSdk.layer(() => ({
        resource: { serviceName: config.serviceName ?? "firegrid-verification" },
        spanProcessor: [processor]
      }))
      return Layer.mergeAll(OtelLive, RuntimeLive)
    })
  )

  return RuntimeAndOtel.pipe(Layer.provideMerge(ChdbLayer({})))
}
