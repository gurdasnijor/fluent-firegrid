import { readdirSync, readFileSync, statSync } from "node:fs"
import { join } from "node:path"

const featureFiles = (dir) =>
  readdirSync(dir).flatMap((entry) => {
    const file = join(dir, entry)
    const stat = statSync(file)
    if (stat.isDirectory()) return featureFiles(file)
    return file.endsWith(".feature") ? [file] : []
  })

const sqlProofTags = () =>
  Array.from(
    new Set(
      featureFiles("features").flatMap((file) =>
        Array.from(readFileSync(file, "utf8").matchAll(/@sql:[^\s]+/g), ([tag]) => tag),
      ),
    ),
  ).sort()

const sqlProofTagExpression = () => {
  const tags = sqlProofTags()
  return tags.length === 0 ? "@__missing_sql_proofs__" : tags.join(" or ")
}

const common = {
  paths: ["features/**/*.feature"],
  import: [
    "./cucumber-tsx-register.mjs",
    "features/**/*.ts",
  ],
  format: [
    "summary",
    "./packages/spec-harness/src/trace-formatter.ts",
  ],
}

export default common

export const proofs = {
  ...common,
  tags: sqlProofTagExpression(),
}

// Alignment lint: emit step-definition usage as pure JSON (overriding the
// default formatters) so a dry run can be diffed for orphaned step definitions.
export const align = {
  ...common,
  format: ["usage-json"],
}
