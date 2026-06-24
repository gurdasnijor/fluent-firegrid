import fs from "node:fs"
import { execFileSync } from "node:child_process"
import path from "node:path"

import { createClient, type UserConfig } from "@hey-api/openapi-ts"
import { afterEach, beforeEach, describe, expect, it } from "vitest"

import { defaultConfig as clientEffect } from "../codegen/plugins/client-effect"
import { defaultConfig as effectSchema } from "../codegen/plugins/effect-schema"
import { S2_OPENAPI_URL } from "../openapi-ts.config"

const tmpDir = path.join(__dirname, ".tmp")
const snapshotsDir = path.join(__dirname, "__snapshots__")
const repoRoot = path.resolve(__dirname, "../../..")

const getFilePaths = (dirPath: string): Array<string> => {
  let filePaths: Array<string> = []
  for (const file of fs.readdirSync(dirPath)) {
    const filePath = path.join(dirPath, file)
    const stat = fs.statSync(filePath)
    filePaths = stat.isDirectory() ? filePaths.concat(getFilePaths(filePath)) : filePaths.concat(filePath)
  }
  return filePaths
}

const createConfig = (output: string): UserConfig => ({
  input: S2_OPENAPI_URL,
  logs: {
    level: "silent"
  },
  output: path.join(tmpDir, output),
  parser: {
    filters: {
      operations: {
        include: [
          "GET /basins",
          "GET /streams/{stream}/records",
          "POST /streams/{stream}/records",
          "GET /streams/{stream}/records/tail"
        ]
      }
    }
  },
  plugins: [
    effectSchema,
    clientEffect
  ]
} as UserConfig)

const formatGeneratedOutput = (output: string) => {
  execFileSync(
    "pnpm",
    [
      "--dir",
      repoRoot,
      "exec",
      "dprint",
      "fmt",
      `${path.relative(repoRoot, output)}/*.ts`
    ],
    { stdio: "pipe" }
  )
}

describe("effect-s2 HeyAPI generator", () => {
  beforeEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  it("generates Effect Schema and Effect HttpClient code from the pinned upstream S2 spec", async () => {
    const config = createConfig("s2-core")

    await createClient(config)

    const output = config.output as string
    formatGeneratedOutput(output)
    const filePaths = getFilePaths(output)

    await Promise.all(
      filePaths.map(async (filePath) => {
        const fileContent = fs.readFileSync(filePath, "utf8")
        await expect(fileContent).toMatchFileSnapshot(
          path.join(snapshotsDir, "s2-core", filePath.slice(output.length + 1))
        )
      })
    )
  })
})
