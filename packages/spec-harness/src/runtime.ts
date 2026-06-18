import { After, AfterAll, Before, BeforeAll, type ITestCaseHookParameter, type IWorld } from "@cucumber/cucumber"
import * as NodeSdk from "@effect/opentelemetry/NodeSdk"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodePath from "@effect/platform-node/NodePath"
import { ChdbSession, ChdbSpanExporter, layer as ChdbLayer, type ChdbClient } from "@firegrid/observability"
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { Effect, FileSystem, Layer, Path } from "effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import type { S2Client } from "effect-s2"
import { recordScenarioReport } from "./report-state.ts"
import { S2LiteLive } from "./s2lite.ts"
import { normalizeProofSql, parseNamedProofs, type ProofBlock, SqlProofError } from "./sql-proofs.ts"
import { runTraceProofSql, spanCounts, spansToSummary, traceCoverage } from "./trace-queries.ts"

interface ScenarioState {
  readonly scenarioId: string
  readonly proofs: Array<ProofBlock>
}

export type HarnessServices = S2Client | ChdbSession | ChdbClient | FileSystem.FileSystem | Path.Path

// eslint-disable-next-line no-restricted-syntax -- Cucumber run-scoped harness runtime; product durability still lives in S2.
let runtime: ManagedRuntime.ManagedRuntime<HarnessServices, unknown> | undefined
// eslint-disable-next-line no-restricted-syntax -- Cucumber run-scoped trace exporter handle for per-scenario forceFlush.
let processor: BatchSpanProcessor | undefined
const states = new WeakMap<IWorld, ScenarioState>()
const parsedProofFiles = new Map<string, Map<string, string>>()

const stateFor = (world: IWorld): ScenarioState => {
  const state = states.get(world)
  if (state === undefined) {
    throw new Error("scenario state is not initialized")
  }
  return state
}

export const addTraceProof = (world: IWorld, sql: string): void => {
  stateFor(world).proofs.push({ sql: normalizeProofSql(sql) })
}

export const scenarioKey = (world: IWorld, key: string): string =>
  `${stateFor(world).scenarioId.replace(/[^A-Za-z0-9_.-]/g, "-")}-${key}`

const scenarioIdFor = (scenario: ITestCaseHookParameter): string =>
  scenario.pickle.id

const proofFileFor = (featureUri: string): Effect.Effect<string, never, Path.Path> =>
  Effect.gen(function*() {
    const path = yield* Path.Path
    return path.resolve(process.cwd(), featureUri.replace(/\.feature$/u, ".sql"))
  })

const sqlProofTagsFor = (scenario: ITestCaseHookParameter): ReadonlyArray<string> =>
  scenario.pickle.tags
    .map((tag) => tag.name)
    .filter((tag) => tag.startsWith("@sql:"))
    .map((tag) => tag.slice("@sql:".length))

const namedProofsForFile = (
  featureUri: string,
  file: string,
): Effect.Effect<Map<string, string>, SqlProofError, FileSystem.FileSystem> => {
  const parsed = parsedProofFiles.get(file)
  if (parsed !== undefined) return Effect.succeed(parsed)
  return Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const content = yield* fs.readFileString(file).pipe(
      Effect.mapError((cause) => new SqlProofError({ message: `Unable to read SQL proof file for ${featureUri}: ${file}`, cause })),
    )
    const proofs = yield* Effect.try({
      try: () => parseNamedProofs(file, content),
      catch: (cause) =>
        cause instanceof SqlProofError
          ? cause
          : new SqlProofError({ message: `Unable to parse SQL proof file for ${featureUri}: ${file}`, cause }),
    })
    parsedProofFiles.set(file, proofs)
    return proofs
  })
}

const proofsFor = (
  scenario: ITestCaseHookParameter,
): Effect.Effect<Array<ProofBlock>, SqlProofError, FileSystem.FileSystem | Path.Path> => {
  const names = sqlProofTagsFor(scenario)
  if (names.length === 0) return Effect.succeed([])
  return Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const featureUri = scenario.pickle.uri
    const file = yield* proofFileFor(featureUri)
    const exists = yield* fs.exists(file).pipe(
      Effect.mapError((cause) => new SqlProofError({ message: `Unable to check SQL proof file for ${featureUri}: ${file}`, cause })),
    )
    if (exists === false) {
      return yield* new SqlProofError({ message: `SQL proof file not found for ${featureUri}: ${file}` })
    }
    const proofs = yield* namedProofsForFile(featureUri, file)
    return yield* Effect.forEach(names, (name) => {
      const sql = proofs.get(name)
      if (sql === undefined) {
        return Effect.fail(new SqlProofError({ message: `SQL proof ${name} not found in ${file}` }))
      }
      return Effect.succeed({
        name,
        source: `${file}#${name}`,
        sql,
      })
    })
  })
}

const proofFailureMessage = (proof: ProofBlock, reason: string, observed: string): string => {
  const label = proof.name === undefined ? "inline trace proof" : `trace proof ${proof.name}`
  const source = proof.source === undefined ? "" : `\nSource: ${proof.source}`
  return `${label} failed: ${reason}${source}\n\n${proof.sql}\n\nObserved spans:\n${observed}`
}

Before(async function(this: IWorld, scenario) {
  const activeRuntime = runtime
  if (activeRuntime === undefined) {
    throw new Error("spec runtime is not initialized")
  }
  states.set(this, {
    scenarioId: scenarioIdFor(scenario),
    proofs: await activeRuntime.runPromise(proofsFor(scenario)),
  })
})

BeforeAll(async function() {
  let nextProcessor: BatchSpanProcessor | undefined
  const ChdbLive = ChdbLayer({})
  const OtelLive = Layer.unwrap(
    Effect.gen(function*() {
      const session = yield* ChdbSession
      const createdProcessor = new BatchSpanProcessor(new ChdbSpanExporter({ session, table: "otel_traces" }))
      nextProcessor = createdProcessor
      return NodeSdk.layer(() => ({
        resource: { serviceName: "firegrid-cucumber" },
        spanProcessor: [createdProcessor],
      }))
    }),
  )
  runtime = ManagedRuntime.make(
    Layer.mergeAll(OtelLive, S2LiteLive, NodeFileSystem.layer, NodePath.layer).pipe(Layer.provideMerge(ChdbLive)),
  )
  await runtime.runPromise(Effect.void)
  if (nextProcessor === undefined) {
    throw new Error("chDB span processor was not initialized")
  }
  processor = nextProcessor
})

After(async function(this: IWorld) {
  const activeRuntime = runtime
  const state = stateFor(this)
  try {
    if (activeRuntime === undefined) return
    if (processor !== undefined) {
      await processor.forceFlush()
    }
    const [spans, coverage] = await Promise.all([
      activeRuntime.runPromise(spanCounts(state.scenarioId)),
      activeRuntime.runPromise(traceCoverage(state.scenarioId)),
    ])
    recordScenarioReport({
      scenarioId: state.scenarioId,
      proofs: state.proofs.length,
      spans,
      coverage,
    })
    const observed = spansToSummary(spans)
    const failures = (await Promise.all(state.proofs.map(async(proof) => {
      try {
        const result = await activeRuntime.runPromise(runTraceProofSql(proof.sql, state.scenarioId))
        return result.ok === true ? undefined : proofFailureMessage(proof, result.reason, observed)
      } catch (cause) {
        const reason = cause instanceof Error ? cause.message : String(cause)
        return proofFailureMessage(proof, reason, observed)
      }
    }))).filter((failure): failure is string => failure !== undefined)
    if (failures.length > 0) {
      throw new Error(`Trace proofs failed (${failures.length}/${state.proofs.length}):\n\n${failures.join("\n\n")}`)
    }
  } finally {
    states.delete(this)
  }
})

AfterAll(async function() {
  try {
    await runtime?.dispose()
  } finally {
    runtime = undefined
    await processor?.shutdown()
    processor = undefined
  }
})

export const runSpecEffect = <A, E, R extends HarnessServices>(
  world: IWorld,
  effect: Effect.Effect<A, E, R>,
): Promise<A> => {
  if (runtime === undefined) {
    throw new Error("spec runtime is not initialized")
  }
  const state = stateFor(world)
  return runtime.runPromise(
    effect.pipe(
      Effect.withSpan("firegrid.scenario", {
        attributes: {
          "firegrid.scenario.id": state.scenarioId,
        },
      }),
    ),
  )
}
