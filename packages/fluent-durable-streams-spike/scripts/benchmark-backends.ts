/* eslint-disable no-restricted-syntax */
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, isAbsolute, join, resolve } from "node:path"
import { Effect } from "effect"
import { DurableStream, stream } from "@durable-streams/client"
import { DurableStreamTestServer } from "@durable-streams/server"
import * as InMemoryStreamLog from "@firegrid/fluent-stream-log-inmemory"
import * as S2LiteStreamLog from "@firegrid/fluent-stream-log-s2-lite"
import { startS2Lite, type S2LiteProcess } from "../../fluent-stream-log-s2-lite/scripts/s2-lite-process.ts"
import { startHttpServer } from "../src/httpServer.ts"
import { makeServer } from "../src/server.ts"
import { benchmarkTraceLayer, printTraceSummary } from "./benchmark-tracing.ts"

interface Stats {
  readonly min: number
  readonly mean: number
  readonly p50: number
  readonly p75: number
  readonly p95: number
  readonly p99: number
  readonly max: number
}

interface ScenarioResult {
  readonly id: string
  readonly name: string
  readonly unit: "ms" | "ops/sec" | "MB/sec"
  readonly stats?: Stats
  readonly value?: number
}

type BackendName = "baseline-memory" | "baseline-filesystem" | "in-memory" | "s2-lite"
type TracedBackendName = Extract<BackendName, "in-memory" | "s2-lite">

interface BackendTarget {
  readonly backend: BackendName
  readonly serverUrl: string
  readonly streamUrl: (path: string) => string
}

interface BackendResult {
  readonly backend: BackendName
  readonly serverUrl: string
  readonly durationMs: number
  readonly scenarios: readonly ScenarioResult[]
}

interface BenchmarkOptions {
  readonly traceOut?: string
}

const textEncoder = new TextEncoder()

const percentile = (values: readonly number[], p: number): number => {
  if (values.length === 0) {
    return 0
  }
  const sorted = [...values].sort((left, right) => left - right)
  const index = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, Math.min(index, sorted.length - 1))]!
}

const stats = (values: readonly number[]): Stats => ({
  min: Math.min(...values),
  mean: values.reduce((sum, value) => sum + value, 0) / values.length,
  p50: percentile(values, 50),
  p75: percentile(values, 75),
  p95: percentile(values, 95),
  p99: percentile(values, 99),
  max: Math.max(...values),
})

const time = async <A>(effect: () => Promise<A>): Promise<{ readonly durationMs: number; readonly value: A }> => {
  const started = process.hrtime.bigint()
  const value = await effect()
  const ended = process.hrtime.bigint()
  return {
    durationMs: Number(ended - started) / 1_000_000,
    value,
  }
}

const pathFor = (backend: string, scenario: string, index: number): string =>
  `bench-${backend}-${scenario}-${Date.now()}-${index}`

const fluentStreamUrl = (baseUrl: string, path: string): string =>
  `${baseUrl}/v1/stream/${path}`

const baselineStreamUrl = (baseUrl: string, path: string): string =>
  `${baseUrl}/${path}`

const bytes = (size: number): Uint8Array => new Uint8Array(size).fill(42)

const readBody = async (url: string): Promise<Uint8Array> => {
  const response = await stream({ url, live: false })
  return response.body()
}

const waitForFirstBytes = async (
  ds: DurableStream,
  live: "long-poll" | "sse",
): Promise<Uint8Array> => {
  const response = await ds.stream({ live })
  return new Promise<Uint8Array>((resolve, reject) => {
    const timeout = setTimeout(() => {
      response.cancel()
      reject(new Error(`${live} benchmark timed out waiting for data`))
    }, 10_000)
    const unsubscribe = response.subscribeBytes(async (chunk) => {
      if (chunk.data.length > 0) {
        clearTimeout(timeout)
        unsubscribe()
        response.cancel()
        resolve(chunk.data)
      }
      return Promise.resolve()
    })
  })
}

const measureLatency = async (
  warmup: number,
  samples: number,
  run: (index: number) => Promise<void>,
): Promise<Stats> => {
  for (let index = 0; index < warmup; index++) {
    await run(index)
  }
  const durations: number[] = []
  for (let index = 0; index < samples; index++) {
    const measured = await time(() => run(index + warmup))
    durations.push(measured.durationMs)
  }
  return stats(durations)
}

const createLatency = async (target: BackendTarget): Promise<ScenarioResult> => ({
  id: "create-p50",
  name: "Create latency",
  unit: "ms",
  stats: await measureLatency(5, 50, async (index) => {
    await DurableStream.create({
      url: target.streamUrl(pathFor(target.backend, "create", index)),
      contentType: "application/octet-stream",
    })
  }),
})

const appendLatency = async (target: BackendTarget): Promise<ScenarioResult> => {
  const url = target.streamUrl(pathFor(target.backend, "append", 0))
  await DurableStream.create({ url, contentType: "application/octet-stream" })
  const ds = new DurableStream({ url, contentType: "application/octet-stream" })
  const payload = bytes(100)
  return {
    id: "append-p50",
    name: "Append 100B latency",
    unit: "ms",
    stats: await measureLatency(10, 100, async () => {
      await ds.append(payload)
    }),
  }
}

const readLatency = async (target: BackendTarget): Promise<ScenarioResult> => {
  const url = target.streamUrl(pathFor(target.backend, "read", 0))
  await DurableStream.create({ url, contentType: "application/octet-stream" })
  const ds = new DurableStream({ url, contentType: "application/octet-stream" })
  const payload = bytes(1024)
  for (let index = 0; index < 10; index++) {
    await ds.append(payload)
  }
  return {
    id: "read-p50",
    name: "Read 10KB latency",
    unit: "ms",
    stats: await measureLatency(10, 100, async () => {
      await readBody(url)
    }),
  }
}

const roundtripLatency = async (target: BackendTarget): Promise<ScenarioResult> => ({
  id: "roundtrip-p50",
  name: "Append + long-poll latency",
  unit: "ms",
  stats: await measureLatency(5, 50, async (index) => {
    const url = target.streamUrl(pathFor(target.backend, "roundtrip", index))
    const ds = await DurableStream.create({ url, contentType: "application/octet-stream" })
    const read = waitForFirstBytes(ds, "long-poll")
    await ds.append(bytes(100))
    await read
  }),
})

const sseLatency = async (target: BackendTarget): Promise<ScenarioResult> => ({
  id: "sse-p95",
  name: "SSE first-event latency",
  unit: "ms",
  stats: await measureLatency(3, 20, async (index) => {
    const url = target.streamUrl(pathFor(target.backend, "sse", index))
    const ds = await DurableStream.create({ url, contentType: "application/json" })
    const read = waitForFirstBytes(ds, "sse")
    await ds.append(textEncoder.encode(JSON.stringify({ index })))
    await read
  }),
})

const smallThroughput = async (target: BackendTarget): Promise<ScenarioResult> => {
  const url = target.streamUrl(pathFor(target.backend, "small-throughput", 0))
  await DurableStream.create({ url, contentType: "application/octet-stream" })
  const ds = new DurableStream({ url, contentType: "application/octet-stream" })
  const count = 1_000
  const payload = bytes(100)
  const measured = await time(async () => {
    for (let index = 0; index < count; index++) {
      await ds.append(payload)
    }
  })
  return {
    id: "small-throughput",
    name: "Small-message append throughput",
    unit: "ops/sec",
    value: count / (measured.durationMs / 1000),
  }
}

const largeThroughput = async (target: BackendTarget): Promise<ScenarioResult> => {
  const url = target.streamUrl(pathFor(target.backend, "large-throughput", 0))
  await DurableStream.create({ url, contentType: "application/octet-stream" })
  const ds = new DurableStream({ url, contentType: "application/octet-stream" })
  const count = 10
  const payload = bytes(256 * 1024)
  const measured = await time(async () => {
    for (let index = 0; index < count; index++) {
      await ds.append(payload)
    }
  })
  return {
    id: "large-throughput",
    name: "Large-message 256KiB append throughput",
    unit: "MB/sec",
    value: (count * payload.byteLength) / 1024 / 1024 / (measured.durationMs / 1000),
  }
}

const runBackend = async (target: BackendTarget): Promise<BackendResult> => {
  const started = Date.now()
  const scenarios: ScenarioResult[] = []
  console.error(`Running benchmarks for ${target.backend} at ${target.serverUrl}`)
  scenarios.push(await createLatency(target))
  scenarios.push(await appendLatency(target))
  scenarios.push(await readLatency(target))
  scenarios.push(await roundtripLatency(target))
  scenarios.push(await smallThroughput(target))
  scenarios.push(await largeThroughput(target))
  scenarios.push(await sseLatency(target))
  return {
    backend: target.backend,
    serverUrl: target.serverUrl,
    durationMs: Date.now() - started,
    scenarios,
  }
}

const withBaselineMemory = async (): Promise<BackendResult> => {
  const server = new DurableStreamTestServer({ port: 0 })
  await server.start()
  try {
    return await runBackend({
      backend: "baseline-memory",
      serverUrl: server.url,
      streamUrl: (path) => baselineStreamUrl(server.url, path),
    })
  } finally {
    await server.stop()
  }
}

const withBaselineFilesystem = async (): Promise<BackendResult> => {
  const dataDir = await mkdtemp(join(tmpdir(), "durable-streams-baseline-benchmark-"))
  const server = new DurableStreamTestServer({ port: 0, dataDir })
  await server.start()
  try {
    return await runBackend({
      backend: "baseline-filesystem",
      serverUrl: server.url,
      streamUrl: (path) => baselineStreamUrl(server.url, path),
    })
  } finally {
    await server.stop()
    await rm(dataDir, { recursive: true, force: true })
  }
}

const traceLayer = (backend: TracedBackendName, options: BenchmarkOptions) =>
  options.traceOut === undefined ? undefined : benchmarkTraceLayer(options.traceOut, backend)

const withInMemory = async (options: BenchmarkOptions): Promise<BackendResult> => {
  const log = await Effect.runPromise(InMemoryStreamLog.make())
  const server = await startHttpServer(makeServer(log), { telemetry: traceLayer("in-memory", options) })
  try {
    return await runBackend({
      backend: "in-memory",
      serverUrl: server.url,
      streamUrl: (path) => fluentStreamUrl(server.url, path),
    })
  } finally {
    await server.close()
  }
}

const withS2Lite = async (options: BenchmarkOptions): Promise<BackendResult> => {
  const temp = await mkdtemp(join(tmpdir(), "fluent-ds-s2-benchmark-"))
  let s2: S2LiteProcess | undefined
  try {
    s2 = await startS2Lite(temp)
    const log = await Effect.runPromise(S2LiteStreamLog.make({
      endpoint: s2.endpoint,
      ...(process.env["S2_LITE_TOKEN"] !== undefined && { token: process.env["S2_LITE_TOKEN"] }),
      streamPrefix: process.env["S2_LITE_STREAM_PREFIX"] ?? `bench-${Date.now()}-`,
    }))
    const server = await startHttpServer(makeServer(log), { telemetry: traceLayer("s2-lite", options) })
    try {
      return await runBackend({
        backend: "s2-lite",
        serverUrl: server.url,
        streamUrl: (path) => fluentStreamUrl(server.url, path),
      })
    } finally {
      await server.close()
    }
  } finally {
    await s2?.close()
    await rm(temp, { recursive: true, force: true })
  }
}

const formatNumber = (value: number, fractionDigits = 2): string =>
  value.toLocaleString("en-US", {
    maximumFractionDigits: fractionDigits,
    minimumFractionDigits: fractionDigits,
  })

const metricValue = (scenario: ScenarioResult): number =>
  scenario.value ?? (scenario.id === "sse-p95" ? scenario.stats?.p95 : scenario.stats?.p50) ?? 0

const printSummary = (results: readonly BackendResult[]): void => {
  console.log("\nBackend Benchmark Summary")
  console.log("=========================\n")
  console.log("| Metric | Baseline memory | Baseline filesystem | Fluent in-memory | Fluent S2 Lite | In-memory / memory baseline | S2 Lite / filesystem baseline |")
  console.log("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
  const baselineMemory = results.find((result) => result.backend === "baseline-memory")
  const baselineFilesystem = results.find((result) => result.backend === "baseline-filesystem")
  const inMemory = results.find((result) => result.backend === "in-memory")
  const s2Lite = results.find((result) => result.backend === "s2-lite")
  if (baselineMemory === undefined || baselineFilesystem === undefined || inMemory === undefined || s2Lite === undefined) {
    return
  }
  const ids = baselineMemory.scenarios.map((scenario) => scenario.id)
  for (const id of ids) {
    const memoryBaseScenario = baselineMemory.scenarios.find((scenario) => scenario.id === id)
    const filesystemBaseScenario = baselineFilesystem.scenarios.find((scenario) => scenario.id === id)
    const memoryScenario = inMemory.scenarios.find((scenario) => scenario.id === id)
    const s2Scenario = s2Lite.scenarios.find((scenario) => scenario.id === id)
    if (
      memoryBaseScenario === undefined ||
      filesystemBaseScenario === undefined ||
      memoryScenario === undefined ||
      s2Scenario === undefined
    ) {
      continue
    }
    const memoryBaseValue = metricValue(memoryBaseScenario)
    const filesystemBaseValue = metricValue(filesystemBaseScenario)
    const memoryValue = metricValue(memoryScenario)
    const s2Value = metricValue(s2Scenario)
    const memoryRatio = memoryBaseValue === 0 ? 0 : memoryValue / memoryBaseValue
    const s2Ratio = filesystemBaseValue === 0 ? 0 : s2Value / filesystemBaseValue
    const unit = memoryBaseScenario.unit
    console.log(
      `| ${memoryBaseScenario.name} | ${formatNumber(memoryBaseValue)} ${unit} | ${formatNumber(filesystemBaseValue)} ${unit} | ${formatNumber(memoryValue)} ${unit} | ${formatNumber(s2Value)} ${unit} | ${formatNumber(memoryRatio, 2)}x | ${formatNumber(s2Ratio, 2)}x |`,
    )
  }
}

const main = async () => {
  const outArgIndex = process.argv.indexOf("--out")
  const rawOutPath = outArgIndex >= 0 ? process.argv[outArgIndex + 1] : undefined
  const traceArgIndex = process.argv.indexOf("--trace-out")
  const rawTracePath = traceArgIndex >= 0 ? process.argv[traceArgIndex + 1] : undefined
  const repoRoot = join(import.meta.dirname, "../../..")
  const outPath = rawOutPath === undefined
    ? undefined
    : isAbsolute(rawOutPath)
    ? rawOutPath
    : resolve(repoRoot, rawOutPath)
  const traceOut = rawTracePath === undefined
    ? undefined
    : isAbsolute(rawTracePath)
    ? rawTracePath
    : resolve(repoRoot, rawTracePath)
  if (traceOut !== undefined) {
    await rm(traceOut, { force: true })
  }
  const options: BenchmarkOptions = {
    ...(traceOut !== undefined && { traceOut }),
  }
  const results = [
    await withBaselineMemory(),
    await withBaselineFilesystem(),
    await withInMemory(options),
    await withS2Lite(options),
  ]
  const report = {
    timestamp: new Date().toISOString(),
    node: process.version,
    results,
  }
  printSummary(results)
  if (outPath !== undefined) {
    await mkdir(dirname(outPath), { recursive: true })
    await writeFile(outPath, JSON.stringify(report, null, 2))
    console.log(`\nWrote ${outPath}`)
  } else {
    console.log(JSON.stringify(report, null, 2))
  }
  if (traceOut !== undefined) {
    console.log(`\nWrote trace ${traceOut}`)
    await printTraceSummary(traceOut)
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
