import type { Envelope } from "@cucumber/messages"
import { SourceMediaType } from "@cucumber/messages"
import { Data, Effect, FileSystem, Stream } from "effect"
import { client, type DurableExecutionError, serviceLayer } from "effect-s2-durable"
import type { S2Client } from "effect-s2"

/** A durable service/object/workflow definition that can be registered in the run's layer. */
export type DurableDefinition = Parameters<typeof serviceLayer>[number]
import type { S2StreamDbError } from "effect-s2-stream-db"
import { runner } from "./runner.ts"
import { asEnvelope, RunEnvelopes } from "./streams.ts"
import type { RunOptions, SourceInput } from "./types.ts"
import { world } from "./world.ts"

/**
 * Public entry. Drives the durable runner (`client(runner).run`) and streams the
 * collected Cucumber Messages envelopes. The runner/world handler layer is
 * provided here; the caller supplies an `S2Client` (durable backend) and a
 * platform `FileSystem`.
 */

export class RunnerError extends Data.TaggedError("RunnerError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export interface RunFeaturesOptions extends RunOptions {
  /** Stable id for this run; keys the durable envelope stream and dedups re-runs. */
  readonly runId: string
  /** Name of the support bundle registered via `defineSupport`. */
  readonly supportName: string
  /**
   * Product durable definitions the support code drives (e.g. the services /
   * objects / workflows a firegrid spec exercises). They are registered in the
   * run's handler layer so step bodies can `client(...)` / `objectClient(...)`
   * them and so boot recovery can re-drive them.
   */
  readonly durableDefs?: ReadonlyArray<DurableDefinition>
}

/** The handler layer for the durable Cucumber runner (+ any product defs); seeds boot recovery. */
export const durableCucumberLayer = (defs: ReadonlyArray<DurableDefinition> = []) =>
  serviceLayer(runner, world, ...defs)

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
      // Run the durable runner to completion (idempotent by runId), then read the
      // envelopes back from the run's durable stream — the stream is the source of
      // truth, not a returned array.
      Effect.flatMap((sources) =>
        client(runner).run({
          runId: options.runId,
          supportName: options.supportName,
          sources,
          options: { scenarioConcurrency: options.scenarioConcurrency ?? 1 },
        }, { idempotencyKey: options.runId }),
      ),
      Effect.flatMap(() => RunEnvelopes.open(options.runId)),
      Effect.map((stream) =>
        stream.read().pipe(Stream.map((record) => asEnvelope(record.value)), Stream.mapError(toRunnerError)),
      ),
      Effect.provide(durableCucumberLayer(options.durableDefs ?? [])),
      Effect.mapError(toRunnerError),
    ),
  )
