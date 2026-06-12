import { spawn, type ChildProcess } from "node:child_process"
import { existsSync } from "node:fs"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { setTimeout as delay } from "node:timers/promises"

const s2Repo = "https://github.com/s2-streamstore/s2.git"
const defaultS2Ref = "1898b55f3238ccd86a3612e413e3e583341a51e5"

export interface S2LiteProcess {
  readonly endpoint: string
  readonly close: () => Promise<void>
}

const freePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const server = createServer()
    server.on("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      if (typeof address === "object" && address !== null) {
        const port = address.port
        server.close(() => resolve(port))
      } else {
        server.close(() => reject(new Error("failed to allocate port")))
      }
    })
  })

const commandExists = async (command: string): Promise<boolean> =>
  new Promise((resolve) => {
    const child = spawn("sh", ["-lc", `command -v ${command}`], { stdio: "ignore" })
    child.on("close", (code) => resolve(code === 0))
    child.on("error", () => resolve(false))
  })

const run = (
  command: string,
  args: ReadonlyArray<string>,
  options: { readonly cwd?: string; readonly env?: NodeJS.ProcessEnv } = {},
): Promise<void> =>
  new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env,
      stdio: "inherit",
    })
    child.on("error", reject)
    child.on("close", (code) => {
      if (code === 0) {
        resolve()
      } else {
        reject(new Error(`${command} ${args.join(" ")} exited with ${code ?? "unknown"}`))
      }
    })
  })

const waitForHealth = async (endpoint: string): Promise<void> => {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${endpoint}/health`)
      if (response.ok) {
        return
      }
    } catch {
      // process not listening yet
    }
    await delay(250)
  }
  throw new Error(`S2 Lite did not become healthy at ${endpoint}`)
}

const ensureCheckout = async (checkout: string): Promise<void> => {
  const ref = process.env["S2_LITE_REF"] ?? defaultS2Ref
  if (existsSync(join(checkout, ".git"))) {
    await run("git", ["-C", checkout, "fetch", "--depth", "1", "origin", ref])
    await run("git", ["-C", checkout, "checkout", "FETCH_HEAD"])
  } else {
    await run("git", ["clone", "--depth", "1", s2Repo, checkout])
    await run("git", ["-C", checkout, "fetch", "--depth", "1", "origin", ref])
    await run("git", ["-C", checkout, "checkout", "FETCH_HEAD"])
  }
}

const buildS2Lite = async (): Promise<string> => {
  if (!(await commandExists("cargo"))) {
    throw new Error("cargo is required to auto-run S2 Lite; set S2_LITE_ENDPOINT or S2_LITE_BIN instead")
  }

  const checkout = process.env["S2_LITE_CHECKOUT"] ?? join(tmpdir(), "fluent-firegrid-s2-lite-src")
  await ensureCheckout(checkout)
  await run("cargo", [
    "build",
    "--manifest-path",
    join(checkout, "Cargo.toml"),
    "-p",
    "s2-lite",
    "--bin",
    "server",
  ])
  return join(checkout, "target", "debug", "server")
}

const closeChild = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGTERM")
    await delay(500)
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL")
    }
  }
}

export const startS2Lite = async (root: string): Promise<S2LiteProcess> => {
  if (process.env["S2_LITE_ENDPOINT"] !== undefined) {
    return {
      endpoint: process.env["S2_LITE_ENDPOINT"],
      close: async () => {},
    }
  }

  const port = await freePort()
  const endpoint = `http://127.0.0.1:${port}`
  const localRoot = join(root, "s2-data")
  const explicitBin = process.env["S2_LITE_BIN"]
  const bin = explicitBin !== undefined && explicitBin.length > 0
    ? explicitBin
    : await buildS2Lite()

  const child = spawn(bin, ["--port", String(port), "--local-root", localRoot], {
    stdio: "inherit",
  })
  child.on("exit", (code) => {
    if (code !== null && code !== 0) {
      console.error(`S2 Lite exited with ${code}`)
    }
  })

  try {
    await waitForHealth(endpoint)
  } catch (error) {
    await closeChild(child)
    throw error
  }

  return {
    endpoint,
    close: () => closeChild(child),
  }
}
