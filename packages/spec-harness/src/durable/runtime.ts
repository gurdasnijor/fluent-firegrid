import type { Envelope } from "@cucumber/messages"
import { SourceMediaType } from "@cucumber/messages"
import { Data, Effect, FileSystem, Stream } from "effect"
import { client, type DurableExecutionError, serviceLayer } from "effect-s2-durable"
import type { S2Client } from "effect-s2"
import { makeCoordinator } from "./coordinator.ts"
import type { SupportModule } from "./support.ts"
import type { RunOptions, SourceInput } from "./types.ts"
import { makeWorker } from "./worker.ts"

/**
 * Public entry for the durable Cucumber runner. The first implementation is
 * intentionally batch-returning: `client(coordinator).run` collects the full
 * ordered `Envelope[]` and we hand it back as a `Stream`. Live projection /
 * owner-stream tailing is a later optimization layered over the same handlers.
 */

export class RunnerError extends Data.TaggedError("RunnerError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export interface RunFeaturesOptions extends RunOptions {
  /** The support code (step bodies + hooks) for this run. */
  readonly support: SupportModule
}

const mediaTypeFor = (file: string): SourceMediaType =>
  file.endsWith(".md") ? SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_MARKDOWN : SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_PLAIN

// Paths are explicit feature files. Glob expansion, if needed, belongs to the
// caller — the durable entry stays Effect-native over the platform FileSystem.
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

const toRunnerError = (error: RunnerError | DurableExecutionError): RunnerError =>
  error._tag === "RunnerError" ? error : new RunnerError({ message: error.message, cause: error })

/**
 * Run the given feature files through the durable runner and stream the
 * resulting Cucumber Messages envelopes. Requires an `S2Client` (the durable
 * backend) and a platform `FileSystem`; the coordinator/worker handler layer
 * is provided here.
 */
export const runFeaturesDurable = (
  paths: ReadonlyArray<string>,
  options: RunFeaturesOptions,
): Stream.Stream<Envelope, RunnerError, S2Client | FileSystem.FileSystem> => {
  const worker = makeWorker(options.support)
  const coordinator = makeCoordinator(options.support, worker)
  const layer = serviceLayer(coordinator, worker)

  return Stream.unwrap(
    readSources(paths).pipe(
      Effect.flatMap((sources) =>
        client(coordinator).run({
          sources,
          options: { scenarioConcurrency: options.scenarioConcurrency ?? 1 },
        }),
      ),
      Effect.map((result) => Stream.fromIterable(result.envelopes)),
      Effect.provide(layer),
      Effect.mapError(toRunnerError),
    ),
  )
}
