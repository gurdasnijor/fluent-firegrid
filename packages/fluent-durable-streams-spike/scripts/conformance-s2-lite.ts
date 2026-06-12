import { spawn } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect } from "effect"
import * as S2LiteStreamLog from "@firegrid/fluent-stream-log-s2-lite"
import { startS2Lite } from "../../fluent-stream-log-s2-lite/scripts/s2-lite-process.ts"
import { startHttpServer } from "../src/httpServer.ts"
import { makeServer } from "../src/server.ts"

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
  const temp = await mkdtemp(join(tmpdir(), "fluent-ds-s2-conformance-"))
  const configPath = join(temp, "vitest.config.mjs")
  await writeFile(
    configPath,
    "export default { test: { include: [\"src/server/test-runner.ts\"], environment: \"node\", testTimeout: 30000, hookTimeout: 30000 } }\n",
  )
  const s2 = await startS2Lite(temp)

  try {
    // eslint-disable-next-line no-restricted-syntax
    const log = await Effect.runPromise(S2LiteStreamLog.make({
      endpoint: s2.endpoint,
      ...(process.env["S2_LITE_TOKEN"] !== undefined && { token: process.env["S2_LITE_TOKEN"] }),
      streamPrefix: process.env["S2_LITE_STREAM_PREFIX"] ?? `ds-spike-${Date.now()}-`,
    }))
    const server = await startHttpServer(makeServer(log))
    console.log(`Running conformance against ${server.url} backed by S2 Lite at ${s2.endpoint}`)

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
    }
  } finally {
    await s2.close()
    await rm(temp, { recursive: true, force: true })
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
