#!/usr/bin/env node
/**
 * Unified Durable Streams conformance CLI.
 *
 * Dispatches to the client or server conformance engine based on the first
 * positional argument, then hands the remaining arguments to that engine's
 * own CLI unchanged.
 *
 * Usage:
 *   conformance client --run ts
 *   conformance client --run ./adapters/python_adapter.py --suite producer
 *   conformance client --bench ts
 *   conformance server --run http://localhost:4473
 *   conformance server --watch src http://localhost:4473
 */

const HELP = `
Durable Streams Conformance Test Suite

Usage:
  conformance <client|server> [options]

Subcommands:
  client    Run client conformance tests / benchmarks against a client adapter
  server    Run server conformance tests against a running server

Run a subcommand with --help for its full option list:
  conformance client --help
  conformance server --help

Examples:
  conformance client --run ts
  conformance client --bench ts --category latency
  conformance server --run http://localhost:4473
  conformance server --watch src http://localhost:4473
`

async function main(): Promise<void> {
  const [subcommand, ...rest] = process.argv.slice(2)

  if (!subcommand || subcommand === "--help" || subcommand === "-h") {
    console.log(HELP)
    process.exit(subcommand ? 0 : 1)
  }

  if (subcommand !== "client" && subcommand !== "server") {
    console.error(`Error: Unknown subcommand "${subcommand}".`)
    console.log(HELP)
    process.exit(1)
  }

  // Re-shape argv so the delegated CLI sees only its own arguments at
  // process.argv[2..], exactly as if it had been invoked directly.
  process.argv = [process.argv[0]!, process.argv[1]!, ...rest]

  // Load the selected engine's CLI as a runtime module (it runs on import).
  // The specifier is computed (not a literal) so it tracks our own runtime
  // extension: `.ts` under tsx in dev, `.js` from the built dist. The
  // subcommand is validated above, so the path is constrained to the two
  // known engine CLIs.
  const ext = import.meta.url.endsWith(".ts") ? "ts" : "js"
  await import(`./${subcommand}/cli.${ext}`)
}

main().catch((err: unknown) => {
  console.error(`Fatal error: ${err instanceof Error ? err.message : String(err)}`)
  process.exit(1)
})
