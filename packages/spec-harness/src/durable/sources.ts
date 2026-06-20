import { SourceMediaType } from "@cucumber/messages"
import { Effect, FileSystem } from "effect"
import type { SourceInput } from "./types.ts"

/** Read feature files into `SourceInput`s for `generateMessages` (markdown vs plain by extension). */

export const mediaTypeFor = (file: string): SourceMediaType =>
  file.endsWith(".md") ? SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_MARKDOWN : SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_PLAIN

export const readSources = Effect.fn("readSources")(function*(paths: ReadonlyArray<string>) {
  const fs = yield* FileSystem.FileSystem
  return yield* Effect.forEach(paths, (file) =>
    fs.readFileString(file).pipe(
      Effect.map((data): SourceInput => ({ uri: file, data, mediaType: mediaTypeFor(file) })),
    ))
})
