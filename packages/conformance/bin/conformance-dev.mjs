#!/usr/bin/env node

/**
 * Development wrapper that uses tsx to run the unified dispatcher CLI from
 * TypeScript source directly. This allows you to use `pnpm link --global` and
 * see changes immediately without rebuilding.
 *
 *   conformance-dev <client|server> [options]
 */

import { spawn } from "node:child_process"
import { existsSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const srcPath = join(__dirname, "..", "src", "cli.ts")

// Resolve the tsx binary by walking up to a node_modules/.bin/tsx, so this
// works whether invoked via PATH or directly with node. Fall back to bare
// "tsx" (resolved through the shell) when no local install is found.
function resolveTsx() {
  let dir = __dirname
  for (;;) {
    const candidate = join(dir, "node_modules", ".bin", "tsx")
    if (existsSync(candidate)) return candidate
    const parent = dirname(dir)
    if (parent === dir) return "tsx"
    dir = parent
  }
}

const tsx = resolveTsx()

const child = spawn(tsx, [srcPath, ...process.argv.slice(2)], {
  stdio: "inherit",
  shell: tsx === "tsx",
})

child.on("error", (err) => {
  console.error(`Failed to launch tsx: ${err.message}`)
  process.exit(1)
})

child.on("exit", (code) => {
  process.exit(code ?? 0)
})
