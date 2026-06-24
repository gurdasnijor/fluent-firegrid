import { createServer } from "node:net"
import { promisify } from "node:util"

import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import { AppendInput, AppendRecord, FencingTokenMismatchError } from "effect-s2"
import * as S2 from "effect-s2"
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { flowError, OwnedOrchestrator, ViewOrchestrator } from "../src/index.ts"
import type { FlowRecord } from "../src/runtime/Record.ts"
import * as Tail from "../src/runtime/Tail.ts"

const defaultS2LiteBin = "/Users/gnijor/.s2/bin/s2"
const s2LiteBin = process.env.S2_LITE_BIN ?? defaultS2LiteBin
const requiredVersion = "s2 0.37.1"
const liveStreamOptions = { forceTransport: "fetch" as const }

interface S2LiteProcess {
  readonly stdout: { readonly on: (event: "data", listener: (chunk: Buffer) => void) => void } | null
  readonly stderr: { readonly on: (event: "data", listener: (chunk: Buffer) => void) => void } | null
  readonly exitCode: number | null
  readonly kill: (signal: "SIGTERM" | "SIGKILL") => boolean
  readonly once: (event: "exit", listener: () => void) => void
}

let server: S2LiteProcess | undefined
let removeExitCleanup: (() => void) | undefined
let endpoint = ""
let basinCounter = 0

const runScoped = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | S2.S2Client>): Promise<A> =>
  effect.pipe(Effect.scoped, Effect.provide(S2.layer({
    accessToken: "unused",
    endpoints: {
      account: endpoint,
      basin: endpoint
    },
    requestTimeoutMillis: 5_000
  })), Effect.runPromise)

const reduceBodies = (state: ReadonlyArray<string>, record: FlowRecord): ReadonlyArray<string> => [
  ...state,
  record.body
]

const record = (body: string): ReturnType<typeof AppendRecord.string> => AppendRecord.string({ body })

const nextNames = (label: string) => {
  const counter = basinCounter++
  const suffix = `${Date.now().toString(36)}-${counter}`
  return {
    basin: `flow-${suffix}`,
    stream: `${label}-${suffix}`
  }
}

const withStream = <A, E>(label: string, use: (names: {
  readonly basin: string
  readonly stream: string
}) => Effect.Effect<A, E, Scope.Scope | S2.S2Client>) =>
  Effect.gen(function*() {
    const names = nextNames(label)
    yield* S2.basins.ensure({ basin: names.basin })
    const basin = yield* S2.basin(names.basin)
    yield* basin.streams.ensure({ stream: names.stream })
    return yield* use(names)
  })

const append = (names: { readonly basin: string; readonly stream: string }, records: ReadonlyArray<string>) =>
  Effect.gen(function*() {
    const stream = yield* S2.stream(names.basin, names.stream)
    return yield* stream.append(AppendInput.create(records.map(record)))
  })

beforeAll(async () => {
  const { execFile, spawn } = await import("node:child_process")
  const execFileAsync = promisify(execFile)
  const version = (await execFileAsync(s2LiteBin, ["--version"])).stdout.trim()
  console.log(`effect-s2-flow live tests using ${s2LiteBin}: ${version}`)
  expect(version).toBe(requiredVersion)

  const port = await availablePort()
  endpoint = `http://127.0.0.1:${port}`
  server = spawn(s2LiteBin, ["lite", "--port", String(port)], {
    stdio: ["ignore", "pipe", "pipe"]
  })
  const currentServer = server
  const killOnExit = () => {
    if (currentServer.exitCode === null) {
      currentServer.kill("SIGKILL")
    }
  }
  process.once("exit", killOnExit)
  removeExitCleanup = () => process.off("exit", killOnExit)

  let output = ""
  currentServer.stdout?.on("data", (chunk: Buffer) => {
    output += chunk.toString()
  })
  currentServer.stderr?.on("data", (chunk: Buffer) => {
    output += chunk.toString()
  })

  try {
    await waitForHealth(`${endpoint}/health`)
  } catch (error) {
    await stopS2Lite()
    throw new Error(`s2-lite did not become healthy at ${endpoint}: ${String(error)}\n${output}`, { cause: error })
  }
}, 30_000)

afterAll(async () => {
  await stopS2Lite()
}, 5_000)

describe("ViewOrchestrator over official s2-lite", () => {
  it("serves eventual reads from the applied real S2 tail", () =>
    runScoped(withStream("view-eventual", (names) =>
      Effect.gen(function*() {
        yield* append(names, ["1", "2"])
        const view = yield* ViewOrchestrator.make({
          ...names,
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies
        })

        yield* view.readStrong((state) => state)
        const state = yield* view.read((current) => current)

        expect(state).toEqual(["1", "2"])
        expect(yield* view.applied).toBe(2)
      })
    )))

  it("uses checkTail as the strong-read barrier for concurrent appends", () =>
    runScoped(withStream("view-strong", (names) =>
      Effect.gen(function*() {
        const view = yield* ViewOrchestrator.make({
          ...names,
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies
        })

        yield* append(names, ["after-start"])
        const state = yield* view.readStrong((current) => current)

        expect(state).toEqual(["after-start"])
        expect(yield* view.applied).toBe(1)
      })
    )))

  it("recovers from a cursor by folding real S2 records at or after it", () =>
    runScoped(withStream("view-cursor", (names) =>
      Effect.gen(function*() {
        yield* append(names, ["skip-1", "skip-2", "keep"])
        const view = yield* ViewOrchestrator.make({
          ...names,
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 2
        })

        const state = yield* view.readStrong((current) => current)

        expect(state).toEqual(["keep"])
        expect(yield* view.applied).toBe(3)
      })
    )))

})

describe("Tail cursor protocol over official s2-lite", () => {
  it("does not skip records appended between catch-up and live follow setup", () =>
    runScoped(withStream("tail-handoff", (names) =>
      Effect.gen(function*() {
        yield* append(names, ["caught-up"])
        const stream = yield* S2.stream(names.basin, names.stream, liveStreamOptions)
        const folded: Array<string> = []
        const cursor = yield* Tail.catchUp(stream, 0, (record) =>
          Effect.sync(() => {
            folded.push(record.body)
          })
        )
        yield* append(names, ["handoff"])
        const followed = yield* Tail.follow(stream, cursor).pipe(Stream.take(1), Stream.runCollect)

        expect(folded).toEqual(["caught-up"])
        expect(followed.map((record) => record.body)).toEqual(["handoff"])
      })
    )))
})

describe("OwnedOrchestrator over official s2-lite", () => {
  it("completes writes only after ordered local apply, giving read-your-writes", () =>
    runScoped(withStream("owned-ryw", (names) =>
      Effect.gen(function*() {
        yield* installFence(names, "token-a")
        const owned = yield* OwnedOrchestrator.make({
          ...names,
          streamOptions: liveStreamOptions,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 1
        })

        const ack = yield* owned.write([record("owned-1")])
        const state = yield* owned.read((current) => current)

        expect(ack.startSeqNum).toBe(1)
        expect(ack.endSeqNum).toBe(2)
        expect(state).toEqual(["owned-1"])
        expect(yield* owned.applied).toBe(2)
      })
    )))

  it("applies its own S2 records once when the tail reader observes them", () =>
    runScoped(withStream("owned-once", (names) =>
      Effect.gen(function*() {
        yield* installFence(names, "token-a")
        const owned = yield* OwnedOrchestrator.make({
          ...names,
          streamOptions: liveStreamOptions,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 1
        })

        yield* owned.write([record("1")])
        yield* owned.write([record("2")])
        yield* owned.read((current) => current)
        const state = yield* owned.read((current) => current)

        expect(state).toEqual(["1", "2"])
        expect(yield* owned.applied).toBe(3)
      })
    )))

  it("does not reorder an owned append after an earlier foreign S2 record", () =>
    runScoped(withStream("owned-order", (names) =>
      Effect.gen(function*() {
        yield* installFence(names, "token-a")
        const owned = yield* OwnedOrchestrator.make({
          ...names,
          streamOptions: liveStreamOptions,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 1
        })

        yield* append(names, ["foreign"])
        yield* owned.write([record("owned")])
        const state = yield* owned.read((current) => current)

        expect(state).toEqual(["foreign", "owned"])
        expect(yield* owned.applied).toBe(3)
      })
    )))

  it("accepts matching fencing tokens and rejects stale owned S2 appends", () =>
    runScoped(withStream("owned-fence", (names) =>
      Effect.gen(function*() {
        yield* installFence(names, "token-a")
        const matching = yield* OwnedOrchestrator.make({
          ...names,
          streamOptions: liveStreamOptions,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 1
        })
        yield* matching.write([record("accepted")])
        const accepted = yield* matching.read((current) => current)
        expect(accepted).toEqual(["accepted"])

        const stale = yield* OwnedOrchestrator.make({
          ...names,
          streamOptions: liveStreamOptions,
          ownerId: "owner-b",
          fencingToken: "token-b",
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 2,
          config: { writeTimeout: "2 seconds" }
        })

        const result = yield* stale.write([record("rejected")]).pipe(
          Effect.match({
            onFailure: (error) => ({ _tag: "Left" as const, error }),
            onSuccess: (ack) => ({ _tag: "Right" as const, ack })
          })
        )

        expect(result._tag).toBe("Left")
        if (result._tag === "Left") {
          expect(result.error.reason).toBe("write")
          expect(result.error.cause).toBeInstanceOf(FencingTokenMismatchError)
        }
      })
    )))

  it("fails invalid owned append input with typed FlowError instead of timing out", () =>
    runScoped(withStream("owned-invalid", (names) =>
      Effect.gen(function*() {
        yield* installFence(names, "token-a")
        const owned = yield* OwnedOrchestrator.make({
          ...names,
          streamOptions: liveStreamOptions,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<string>,
          reduce: reduceBodies,
          fromSeqNum: 1,
          config: { writeTimeout: "2 seconds" }
        })

        const result = yield* owned.write([]).pipe(
          Effect.match({
            onFailure: (error) => ({ _tag: "Left" as const, error }),
            onSuccess: (ack) => ({ _tag: "Right" as const, ack })
          })
        )

        expect(result._tag).toBe("Left")
        if (result._tag === "Left") {
          expect(result.error.reason).toBe("write")
          expect(result.error.message).toBe("invalid owned append input")
        }
      })
    )))
})

const installFence = (names: { readonly basin: string; readonly stream: string }, token: string) =>
  Effect.gen(function*() {
    const stream = yield* S2.stream(names.basin, names.stream)
    yield* stream.append(AppendInput.create([AppendRecord.fence(token)]))
  })

const availablePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const probe = createServer()
    probe.once("error", reject)
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address()
      if (address === null || typeof address === "string") {
        probe.close(() => reject(new Error("failed to allocate TCP port")))
        return
      }
      const port = address.port
      probe.close(() => resolve(port))
    })
  })

const waitForHealth = async (url: string): Promise<void> => {
  const deadline = Date.now() + 30_000
  let lastError: unknown

  while (Date.now() < deadline) {
    try {
      const response = await fetch(url)
      if (response.ok) {
        return
      }
      lastError = new Error(`HTTP ${response.status}`)
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }

  throw lastError
}

const stopS2Lite = async (): Promise<void> => {
  const child = server
  server = undefined
  removeExitCleanup?.()
  removeExitCleanup = undefined

  if (child === undefined || child.exitCode !== null) {
    return
  }

  child.kill("SIGTERM")
  await new Promise<void>((resolve) => {
    let resolved = false
    const done = () => {
      if (!resolved) {
        resolved = true
        resolve()
      }
    }
    const timeout = setTimeout(() => {
      if (child.exitCode === null) {
        child.kill("SIGKILL")
      }
      done()
    }, 1_000)
    timeout.unref()
    child.once("exit", () => {
      clearTimeout(timeout)
      done()
    })
  })
}
