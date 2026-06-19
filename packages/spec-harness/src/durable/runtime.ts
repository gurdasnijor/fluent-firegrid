import type { Envelope } from "@cucumber/messages"
import { SourceMediaType } from "@cucumber/messages"
import { Data, Effect, FileSystem, Stream } from "effect"
import { client, type DurableExecutionError, serviceLayer } from "effect-s2-durable"
import type { S2Client } from "effect-s2"
import { runner } from "./runner.ts"
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
  /** Name of the support bundle registered via `defineSupport`. */
  readonly supportName: string
}

/** The handler layer for the durable Cucumber runner; seeds boot recovery. */
export const DurableCucumberLive = serviceLayer(runner, world)

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

const toRunnerError = (error: RunnerError | DurableExecutionError): RunnerError =>
  error._tag === "RunnerError" ? error : new RunnerError({ message: error.message, cause: error })

export const runFeaturesDurable = (
  paths: ReadonlyArray<string>,
  options: RunFeaturesOptions,
): Stream.Stream<Envelope, RunnerError, S2Client | FileSystem.FileSystem> =>
  Stream.unwrap(
    readSources(paths).pipe(
      Effect.flatMap((sources) =>
        client(runner).run({
          supportName: options.supportName,
          sources,
          options: { scenarioConcurrency: options.scenarioConcurrency ?? 1 },
        }),
      ),
      Effect.map((result) => Stream.fromIterable(result.envelopes)),
      Effect.provide(DurableCucumberLive),
      Effect.mapError(toRunnerError),
    ),
  )
