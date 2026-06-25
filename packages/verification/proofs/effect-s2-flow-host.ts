import { createRequire } from "node:module"
import { fileURLToPath, pathToFileURL } from "node:url"

import { processHost } from "../src/ProcessHost.ts"

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url))
const require = createRequire(import.meta.url)
const tsxRoot = require.resolve("tsx/package.json").replace(/\/package\.json$/, "")

export const effectS2FlowHost = (env: Record<string, string> = {}) =>
  processHost({
    command: process.execPath,
    args: [
      "--require",
      `${tsxRoot}/dist/preflight.cjs`,
      "--import",
      pathToFileURL(`${tsxRoot}/dist/loader.mjs`).href,
      `${repoRoot}/packages/verification/proofs/effect-s2-flow-proof-host.ts`
    ],
    cwd: repoRoot,
    env: {
      EFFECT_S2_FLOW_FENCE_BUSY_BACKOFF: "100 millis",
      EFFECT_S2_FLOW_FENCE_LEASE: "1 second",
      EFFECT_S2_FLOW_FENCE_REFRESH_INTERVAL: "250 millis",
      ...env
    },
    stderr: "inherit"
  })
