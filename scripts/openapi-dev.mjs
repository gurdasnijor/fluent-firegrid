/* global console, process */
import { spawn } from "node:child_process"

const children = new Set()

function run(name, command, args) {
  const child = spawn(command, args, {
    stdio: "inherit",
    shell: process.platform === "win32",
  })

  children.add(child)
  child.on("exit", (code, signal) => {
    children.delete(child)
    if (signal === null && code !== 0) {
      console.error(`${name} exited with code ${code}`)
      process.exitCode = code ?? 1
      stop()
    }
  })

  return child
}

function stop() {
  for (const child of children) {
    child.kill("SIGTERM")
  }
}

process.on("SIGINT", () => {
  stop()
  process.exit(130)
})

process.on("SIGTERM", () => {
  stop()
  process.exit(143)
})

run("openapi:mock", "pnpm", ["run", "openapi:mock"])
run("openapi:view", "pnpm", ["run", "openapi:view"])
