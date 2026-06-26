import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import { ChdbClient, ChdbSession, ChdbSpanExporter, insertChdbSpanRows, layer as ChdbLayer } from "@firegrid/trace"
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base"
import * as Effect from "effect/Effect"
import { FileSystem } from "effect/FileSystem"
import * as Layer from "effect/Layer"
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as Http from "node:http"

import { TrialId, VerificationRuntime, type WaitForSpanOptions } from "./Property.ts"
import { bindTrialSql } from "./TraceViews.ts"
import { VerificationError } from "./VerificationError.ts"

export interface TraceRuntimeConfig {
  readonly serviceName?: string
}

const escapeString = (value: string): string => value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")

const spanMatchSql = (
  trialId: string,
  span: string,
  attributes: Record<string, string> | undefined
): string => {
  const attrPredicates = Object.entries(attributes ?? {})
    .map(([key, value]) => `SpanAttributes['${escapeString(key)}'] = '${escapeString(value)}'`)
    .join(" AND ")
  return `
SELECT count() > 0 AS ok
FROM otel_traces
WHERE (
  SpanAttributes['firegrid.trial.id'] = {trial_id:String}
  OR ResourceAttributes['firegrid.trial.id'] = {trial_id:String}
)
AND SpanName = '${escapeString(span)}'
${attrPredicates === "" ? "" : `AND ${attrPredicates}`}
`
}

const makeWaitForSpan = (
  processor: BatchSpanProcessor,
  chdb: ChdbClient["Service"]
) =>
  Effect.fn("VerificationRuntime.waitForSpan")(function*(
    span: string,
    options: WaitForSpanOptions = {}
  ) {
    const trialId = yield* TrialId
    if (trialId === undefined) {
      return yield* new VerificationError({
        message: `cannot wait for span ${span} outside an active verification trial`
      })
    }
    const attempts = options.attempts ?? 200
    const interval = options.interval ?? "25 millis"
    const sql = spanMatchSql(trialId, span, options.attributes)

    const loop = (remaining: number): Effect.Effect<void, VerificationError> =>
      Effect.gen(function*() {
        yield* Effect.tryPromise({
          try: () => processor.forceFlush(),
          catch: (cause) => new VerificationError({ message: "failed to flush trace processor", cause })
        })
        const rows = yield* chdb.unsafe<{ readonly ok: boolean | number | string }>(bindTrialSql(sql, trialId)).pipe(
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

interface SpanReceiver {
  readonly endpoint: string
  readonly server: Http.Server
}

const startSpanReceiver = Effect.fn("TraceRuntime.startSpanReceiver")(function*(session: ChdbSession["Service"]) {
  return yield* Effect.acquireRelease(
    Effect.callback<SpanReceiver, VerificationError>((resume) => {
      const server = Http.createServer((request, response) => {
        if (request.method !== "POST") {
          response.writeHead(405).end()
          return
        }

        const chunks: Array<Buffer> = []
        request.on("data", (chunk: Buffer) => {
          chunks.push(chunk)
        })
        request.on("end", () => {
          try {
            const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown
            if (!Array.isArray(parsed)) {
              response.writeHead(400).end("span export payload must be an array")
              return
            }
            insertChdbSpanRows(session, parsed)
            response.writeHead(204).end()
          } catch (cause) {
            response.writeHead(500).end(cause instanceof Error ? cause.message : String(cause))
          }
        })
        request.on("error", (cause) => {
          response.writeHead(500).end(cause instanceof Error ? cause.message : String(cause))
        })
      })
      const onError = (cause: Error) => {
        resume(Effect.fail(new VerificationError({ message: "failed to start span receiver", cause })))
      }
      server.once("error", onError)
      server.listen(0, "127.0.0.1", () => {
        server.off("error", onError)
        const address = server.address()
        if (address === null || typeof address === "string") {
          resume(Effect.fail(new VerificationError({ message: "span receiver did not bind to a TCP address" })))
          return
        }
        resume(Effect.succeed({
          endpoint: `http://127.0.0.1:${address.port}/v1/traces`,
          server
        }))
      })
    }),
    (receiver) =>
      Effect.callback<void>((resume) => {
        receiver.server.close(() => resume(Effect.void))
      }).pipe(Effect.ignore)
  )
})

export const layer = (config: TraceRuntimeConfig = {}) => {
  const RuntimeAndOtel = Layer.unwrap(Effect.gen(function*() {
    const fs = yield* FileSystem
    const chdbPath = yield* fs.makeTempDirectoryScoped({ prefix: "firegrid-verification-chdb-" }).pipe(
      Effect.mapError((cause) => new VerificationError({ message: "failed to create verification chDB path", cause }))
    )

    const RuntimeAndOtelForPath = Layer.unwrap(
      Effect.gen(function*() {
        const session = yield* ChdbSession
        const chdb = yield* ChdbClient
        const processor = new BatchSpanProcessor(
          new ChdbSpanExporter({ session, table: "otel_traces" })
        )
        const receiver = yield* startSpanReceiver(session)
        const RuntimeLive = Layer.succeed(VerificationRuntime, {
          flush: Effect.tryPromise({
            try: () => processor.forceFlush(),
            catch: (cause) => new VerificationError({ message: "failed to flush trace processor", cause })
          }),
          hostEnv: {
            FIREGRID_OTEL_SPAN_ENDPOINT: receiver.endpoint
          },
          waitForSpan: makeWaitForSpan(processor, chdb)
        })
        const OtelLive = NodeSdk.layer(() => ({
          resource: { serviceName: config.serviceName ?? "firegrid-verification" },
          spanProcessor: [processor]
        }))
        return Layer.mergeAll(OtelLive, RuntimeLive)
      })
    )

    return RuntimeAndOtelForPath.pipe(Layer.provideMerge(ChdbLayer({ path: chdbPath })))
  }))

  return RuntimeAndOtel.pipe(Layer.provide(NodeFileSystem.layer))
}
