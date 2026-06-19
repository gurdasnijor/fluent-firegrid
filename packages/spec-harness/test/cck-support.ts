import { globSync, readFileSync } from "node:fs"
import { createRequire } from "node:module"
import { dirname, join } from "node:path"
import type { Envelope } from "@cucumber/messages"
import { SourceMediaType } from "@cucumber/messages"
import type { SourceInput } from "../src/durable/types.ts"

// Fixture loading for the CCK gate. Test-only; reaches for the filesystem and
// the CCK package's feature/ndjson fixtures directly.

const require_ = createRequire(import.meta.url)

const featuresRoot = (): string => join(dirname(require_.resolve("@cucumber/compatibility-kit/package.json")), "features")

const featurePatterns = (sample: string): ReadonlyArray<string> => {
  const root = join(featuresRoot(), sample)
  return [join(root, "*.feature"), join(root, "*.feature.md")]
}

/** Absolute path to a file inside a CCK sample directory. */
export const cckSamplePath = (sample: string, file: string): string => join(featuresRoot(), sample, file)

/** Read a CCK sample's feature files as `SourceInput`s. */
export const loadCckSources = (sample: string): ReadonlyArray<SourceInput> =>
  featurePatterns(sample)
    .flatMap((pattern) => globSync(pattern))
    .map((file): SourceInput => ({
      uri: file,
      data: readFileSync(file, "utf8"),
      mediaType: file.endsWith(".md")
        ? SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_MARKDOWN
        : SourceMediaType.TEXT_X_CUCUMBER_GHERKIN_PLAIN,
    }))

/** Parse a CCK sample's expected `.ndjson` into envelopes. */
export const cckExpectedEnvelopes = (sample: string): ReadonlyArray<Envelope> =>
  readFileSync(join(featuresRoot(), sample, `${sample}.ndjson`), "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as Envelope)
