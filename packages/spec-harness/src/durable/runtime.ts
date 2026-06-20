import type { Envelope } from "@cucumber/messages"
import { SourceMediaType } from "@cucumber/messages"
import { Data, Effect, FileSystem, Stream } from "effect"
import { client, type DurableExecutionError, serviceLayer } from "effect-s2-durable"
import type { S2Client } from "effect-s2"
import type { S2StreamDbError } from "effect-s2-stream-db"
import { makeRunner } from "./runner.ts"
import { makeScenario } from "./scenario.ts"
import { asEnvelope, RunEnvelopes } from "./streams.ts"
import type { SupportBundle } from "./support.ts"
import type { RunOptions, SourceInput } from "./types.ts"

/**
 * Public entry. Builds the durable Cucumber definitions (`runner` + `world`)
 * from the provided support bundle — the bundle is captured in the handler
 * closures, not a global — runs the runner to completion (idempotent by runId),
 * then streams the collected Cucumber Messages envelopes back from the run's
 * durable stream. The caller supplies an `S2Client` and a platform `FileSystem`.
 */

/** A durable service/object/workflow definition that can be registered alongside the runner. */
export type DurableDefinition = Parameters<typeof serviceLayer>[number]

export class RunnerError extends Data.TaggedError("RunnerError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export interface RunFeaturesOptions extends RunOptions {
  /** Stable id for this run; keys the durable envelope stream and dedups re-runs. */
  readonly runId: string
  /** The step definitions + hooks for this run, from `defineSteps`. */
  readonly support: SupportBundle
  /**
   * Product durable definitions the support code drives (e.g. the services /
   * objects / workflows a firegrid spec exercises). Registered in the run's
   * handler layer so step bodies can `client(...)` / `objectClient(...)` them and
   * so boot recovery can re-drive them.
   */
  readonly durableDefs?: ReadonlyArray<DurableDefinition>
}

/** Build the runner + scenario object for a support bundle and the engine layer that registers them. */
const makeCucumberRun = (support: SupportBundle, defs: ReadonlyArray<DurableDefinition>) => {
  const scenario = makeScenario(support)
  const runner = makeRunner(support, scenario)
  return { runner, layer: serviceLayer(runner, scenario, ...defs) }
}

/** The handler layer for a support bundle (+ any product defs); seeds boot recovery. */
export const durableCucumberLayer = (support: SupportBundle, defs: ReadonlyArray<DurableDefinition> = []) =>
  makeCucumberRun(support, defs).layer

const mediaTypeFor = (file: string): SourceMediaType =>
  file.endsWith(".md") ? SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_MARKDOWN : SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_PLAIN

const readSources = (
  paths: ReadonlyArray<string>,
): Effect.Effect<ReadonlyArray<SourceInput>, RunnerError, FileSystem.FileSystem> =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    return yield* Effect.forEach(paths, (file) =>
      fs.readFileString(file).pipe(
        Effect.map((data): SourceInput => ({ uri: file, data, mediaType: mediaTypeFor(file) })),
        Effect.mapError((cause) => new RunnerError({ message: `failed to read feature file ${file}`, cause })),
      ))
  })

const toRunnerError = (error: RunnerError | DurableExecutionError | S2StreamDbError): RunnerError =>
  error._tag === "RunnerError" ? error : new RunnerError({ message: error.message, cause: error })

export const runFeaturesDurable = (
  paths: ReadonlyArray<string>,
  options: RunFeaturesOptions,
): Stream.Stream<Envelope, RunnerError, S2Client | FileSystem.FileSystem> =>
  Stream.unwrap(
    readSources(paths).pipe(
      Effect.flatMap((sources) => {
        const { layer, runner } = makeCucumberRun(options.support, options.durableDefs ?? [])
        // Run the durable runner to completion (idempotent by runId), then read the
        // envelopes back from the run's durable stream — the stream is the source of
        // truth, not a returned array.
        return client(runner).run({
          runId: options.runId,
          sources,
          options: { scenarioConcurrency: options.scenarioConcurrency ?? 1 },
        }, { idempotencyKey: options.runId }).pipe(
          Effect.flatMap(() => RunEnvelopes.open(options.runId)),
          Effect.map((stream) =>
            stream.read().pipe(Stream.map((record) => asEnvelope(record.value)), Stream.mapError(toRunnerError)),
          ),
          Effect.provide(layer),
        )
      }),
      Effect.mapError(toRunnerError),
    ),
  )
