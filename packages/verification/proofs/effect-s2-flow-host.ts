import { createRequire } from "node:module"
import { fileURLToPath, pathToFileURL } from "node:url"

import { processHost } from "../src/ProcessHost.ts"

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url))
const require = createRequire(import.meta.url)
const tsxRoot = require.resolve("tsx/package.json").replace(/\/package\.json$/, "")

export const effectS2FlowHost = () =>
  processHost({
    command: process.execPath,
    args: [
      "--require",
      `${tsxRoot}/dist/preflight.cjs`,
      "--import",
      pathToFileURL(`${tsxRoot}/dist/loader.mjs`).href,
      `${repoRoot}/packages/effect-s2-flow/src/HostMain.ts`
    ],
    cwd: repoRoot,
    stderr: "inherit"
  })
