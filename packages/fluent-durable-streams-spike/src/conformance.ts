/* eslint-disable no-console */
import { spawn } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect } from "effect"
import * as InMemoryStreamLog from "@firegrid/fluent-stream-log-inmemory"
import { makeServer } from "./server.ts"
import { startHttpServer } from "./httpServer.ts"

const run = (
  command: string,
  args: ReadonlyArray<string>,
  env: NodeJS.ProcessEnv,
): Promise<number> =>
  new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: join(import.meta.dirname, "../../.."),
      env,
      stdio: "inherit",
    })
    child.on("error", reject)
    child.on("close", (code) => resolve(code ?? 1))
  })

const main = async () => {
  const passthroughArgs = process.argv.slice(2)
  const vitestArgs = passthroughArgs[0] === "--" ? passthroughArgs.slice(1) : passthroughArgs
  const temp = await mkdtemp(join(tmpdir(), "fluent-ds-conformance-"))
  const configPath = join(temp, "vitest.config.mjs")
  await writeFile(
    configPath,
    `export default { test: { include: ["src/server/test-runner.ts"], environment: "node", testTimeout: 30000, hookTimeout: 30000 } }\n`,
  )

  const log = await Effect.runPromise(InMemoryStreamLog.make())
  const server = await startHttpServer(makeServer(log))
  console.log(`Running conformance against ${server.url}`)

  try {
    const code = await run(
      "pnpm",
      [
        "--dir",
        "packages/conformance",
        "exec",
        "vitest",
        "run",
        "--config",
        configPath,
        "--root",
        ".",
        "--no-coverage",
        "--reporter=default",
        "--passWithNoTests=false",
        ...vitestArgs,
      ],
      {
        ...process.env,
        CONFORMANCE_TEST_URL: server.url,
      },
    )
    process.exitCode = code
  } finally {
    await server.close()
    await rm(temp, { recursive: true, force: true })
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
