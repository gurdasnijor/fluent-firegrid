import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process"
import { constants } from "node:fs"
import { access, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { delimiter, join } from "node:path"
import { createServer } from "node:net"
import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  S2,
} from "@s2-dev/streamstore"
import { Duration, Effect, Layer, Option, Schema } from "effect"
import { Activity, DurableClock, Workflow } from "effect/unstable/workflow"
import type * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import {
  layerConfig,
  layerFromConfig,
  type S2WorkflowEngineConfig,
} from "../src/index.ts"

const freePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      if (address === null || typeof address === "string") {
        server.close()
        reject(new Error("could not allocate port"))
        return
      }
      const port = address.port
      server.close(() => resolve(port))
    })
  })

const resolveS2Binary = async (): Promise<string> => {
  if (process.env.S2_BIN !== undefined) {
    return process.env.S2_BIN
  }
  const executable = process.platform === "win32" ? "s2.exe" : "s2"
  for (const directory of (process.env.PATH ?? "").split(delimiter)) {
    const candidate = join(directory, executable)
    try {
      await access(candidate, constants.X_OK)
      return candidate
    } catch {
      // Keep searching PATH.
    }
  }
  throw new Error("S2 CLI not found on PATH; install s2 or set S2_BIN")
}

interface LiteServer {
  readonly config: S2WorkflowEngineConfig
  readonly basin: string
  readonly stop: () => Promise<void>
}

const waitForLite = async (
  url: string,
  basin: string,
  signal: AbortSignal,
): Promise<void> => {
  const s2 = new S2({
    accessToken: "test-token",
    endpoints: { account: url, basin: url },
    requestTimeoutMillis: 500,
    retry: { maxAttempts: 1 },
  })
  while (!signal.aborted) {
    try {
      await s2.basins.ensure({ basin })
      return
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  throw new Error("s2 lite did not start")
}

const startLite = async (): Promise<LiteServer> => {
  const port = await freePort()
  const localRoot = await mkdtemp(join(tmpdir(), "fluent-s2-lite-"))
  const child = spawn(await resolveS2Binary(), [
    "lite",
    "--port",
    String(port),
    "--local-root",
    localRoot,
  ])
  const abort = new AbortController()
  const output: Array<string> = []
  child.stdout.on("data", (chunk) => output.push(String(chunk)))
  child.stderr.on("data", (chunk) => output.push(String(chunk)))
  child.once("exit", (code) => {
    if (!abort.signal.aborted && code !== 0) {
      output.push(`s2 lite exited with ${String(code)}`)
    }
  })

  const basin = `fgtest-${crypto.randomUUID().slice(0, 8)}`
  const url = `http://127.0.0.1:${port}/v1`
  try {
    await waitForLite(url, basin, abort.signal)
  } catch (error) {
    abort.abort()
    child.kill("SIGTERM")
    await rm(localRoot, { recursive: true, force: true })
    throw new Error(`${String(error)}\n${output.join("\n")}`, { cause: error })
  }

  return {
    basin,
    config: {
      basin,
      accessToken: "test-token",
      endpoints: { account: url, basin: url },
      streamPrefix: `test-${crypto.randomUUID()}`,
      requestTimeoutMillis: 2_000,
    },
    stop: async () => {
      abort.abort()
      await stopChild(child)
      await rm(localRoot, { recursive: true, force: true })
    },
  }
}

const stopChild = (child: ChildProcessWithoutNullStreams): Promise<void> =>
  new Promise((resolve) => {
    if (child.exitCode !== null || child.killed) {
      resolve()
      return
    }
    child.once("exit", () => resolve())
    child.kill("SIGTERM")
    setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL")
    }, 2_000).unref()
  })

const engineLayer = (config: S2WorkflowEngineConfig) =>
  layerFromConfig.pipe(Layer.provide(layerConfig(config)))

const provideWorkflow = (
  workflowLayer: Layer.Layer<never, never, WorkflowEngine.WorkflowEngine>,
  config: S2WorkflowEngineConfig,
) => workflowLayer.pipe(Layer.provideMerge(engineLayer(config)))

const listStreams = async (
  config: S2WorkflowEngineConfig,
  prefix: string,
): Promise<ReadonlyArray<string>> => {
  const options: ConstructorParameters<typeof S2>[0] = {
    accessToken: config.accessToken,
  }
  if (config.endpoints !== undefined) {
    options.endpoints = config.endpoints
  }
  const s2 = new S2(options)
  const names: Array<string> = []
  for await (const stream of s2.basin(config.basin).streams.listAll({ prefix })) {
    names.push(stream.name)
  }
  return names
}

describe("S2NativeWorkflowEngine", () => {
  let lite: LiteServer

  beforeAll(async () => {
    lite = await startLite()
  }, 20_000)

  afterAll(async () => {
    await lite.stop()
  })

  it("persists executions and activity results to S2", async () => {
    let calls = 0
    const wf = Workflow.make("s2/basic", {
      payload: { id: Schema.String },
      success: Schema.Number,
      idempotencyKey: (payload) => payload.id,
    })
    const workflowLayer = wf.toLayer(() =>
      Activity.make({
        name: "side-effect",
        success: Schema.Number,
        execute: Effect.sync(() => {
          calls += 1
          return calls
        }),
      }),
    )
    const layer = provideWorkflow(workflowLayer, lite.config)

    const result = await Effect.runPromise(
      Effect.scoped(wf.execute({ id: "same" }).pipe(Effect.provide(layer))),
    )
    const replayed = await Effect.runPromise(
      Effect.scoped(wf.execute({ id: "same" }).pipe(Effect.provide(layer))),
    )

    expect(result).toBe(1)
    expect(replayed).toBe(1)
    expect(calls).toBe(1)
  }, 20_000)

  it("polls completed workflow results from S2", async () => {
    const wf = Workflow.make("s2/poll", {
      payload: { id: Schema.String },
      success: Schema.String,
      idempotencyKey: (payload) => payload.id,
    })
    const layer = provideWorkflow(
      wf.toLayer((payload) => Effect.succeed(`done:${payload.id}`)),
      lite.config,
    )

    const executionId = await Effect.runPromise(wf.executionId({ id: "p1" }))
    await Effect.runPromise(
      Effect.scoped(wf.execute({ id: "p1" }).pipe(Effect.provide(layer))),
    )
    const polled = await Effect.runPromise(
      Effect.scoped(wf.poll(executionId).pipe(Effect.provide(layer))),
    )

    expect(Option.isSome(polled)).toBe(true)
    if (Option.isSome(polled)) {
      expect(polled.value._tag).toBe("Complete")
    }
  }, 20_000)

  it("polling a missing execution does not create an S2 stream", async () => {
    const config = {
      ...lite.config,
      streamPrefix: `missing-${crypto.randomUUID()}`,
    }
    const wf = Workflow.make("s2/missing", {
      payload: { id: Schema.String },
      success: Schema.String,
      idempotencyKey: (payload) => payload.id,
    })
    const layer = provideWorkflow(
      wf.toLayer((payload) => Effect.succeed(`done:${payload.id}`)),
      config,
    )

    const polled = await Effect.runPromise(
      Effect.scoped(wf.poll("does-not-exist").pipe(Effect.provide(layer))),
    )
    const streams = await listStreams(config, `${config.streamPrefix}/executions/`)

    expect(Option.isNone(polled)).toBe(true)
    expect(streams).toEqual([])
  }, 20_000)

  it("persists durable clock wakeups through S2", async () => {
    const wf = Workflow.make("s2/clock", {
      payload: { id: Schema.String },
      success: Schema.String,
      idempotencyKey: (payload) => payload.id,
    })
    const layer = provideWorkflow(
      wf.toLayer(() =>
        Effect.gen(function*() {
          yield* DurableClock.sleep({
            name: "wake",
            duration: Duration.millis(25),
            inMemoryThreshold: Duration.zero,
          })
          return "awake"
        }),
      ),
      lite.config,
    )

    const result = await Effect.runPromise(
      Effect.scoped(wf.execute({ id: "clock" }).pipe(Effect.provide(layer))),
    )

    expect(result).toBe("awake")
  }, 20_000)

  it("uses real S2 fencing tokens", async () => {
    const options: ConstructorParameters<typeof S2>[0] = {
      accessToken: lite.config.accessToken,
    }
    if (lite.config.endpoints !== undefined) {
      options.endpoints = lite.config.endpoints
    }
    const s2 = new S2(options)
    const streamName = `${lite.config.streamPrefix}/fencing/${crypto.randomUUID()}`
    const basin = s2.basin(lite.basin)
    await basin.streams.ensure({ stream: streamName })
    const stream = basin.stream(streamName, { forceTransport: "fetch" })

    await stream.append(AppendInput.create([AppendRecord.fence("owner-1")]))
    await stream.append(
      AppendInput.create([
        AppendRecord.string({ body: "owner-1-write" }),
      ], { fencingToken: "owner-1" }),
    )
    await stream.append(AppendInput.create([AppendRecord.fence("owner-2")]))

    await expect(
      stream.append(
        AppendInput.create([
          AppendRecord.string({ body: "stale-write" }),
        ], { fencingToken: "owner-1" }),
      ),
    ).rejects.toBeInstanceOf(FencingTokenMismatchError)
  }, 20_000)
})
