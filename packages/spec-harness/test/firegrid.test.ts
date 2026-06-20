import { execSync } from "node:child_process"
import { fileURLToPath } from "node:url"
import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { durableExecutionsSteps } from "../../../features/effect-s2-durable/durable-executions/durable-executions.steps.ts"
import { storagePrimitivesSteps } from "../../../features/effect-s2-stream-db/storage-primitives.steps.ts"
import { type FiregridResult, firstFailure, runFiregrid, statusesOf } from "../src/firegrid/run.ts"
import { WorldServicesLive } from "../src/firegrid/runtime.ts"
import type { SupportBundle } from "../src/durable/support.ts"

// The end objective: run the real firegrid-durable specs on the new runner, with
// their `@sql:` trace proofs evaluated against chDB over the production spans.

const featurePath = (relative: string): string => fileURLToPath(new URL(`../../../features/${relative}`, import.meta.url))

const hasS2 = (): boolean => {
  try {
    execSync("command -v s2", { stdio: "ignore" })
    return true
  } catch {
    return false
  }
}

const run = (feature: string, support: SupportBundle): Promise<FiregridResult> =>
  runFiregrid([featurePath(feature)], support).pipe(
    Effect.provide(WorldServicesLive),
    Effect.scoped,
    Effect.runPromise,
  )

const expectClean = (result: FiregridResult, expectedProofs: number): void => {
  expect(firstFailure(result), `first failing step: ${firstFailure(result)}`).toBeUndefined()
  expect(statusesOf(result).every((status) => status === "PASSED")).toBe(true)
  const failedProofs = result.proofs.filter((proof) => !proof.ok)
  expect(failedProofs, `failed proofs: ${JSON.stringify(failedProofs, null, 2)}`).toEqual([])
  expect(result.proofs.length).toBe(expectedProofs)
  expect(result.proofs.every((proof) => proof.ok)).toBe(true)
}

describe.skipIf(!hasS2())("firegrid specs on the durable runner (S2 + chDB)", () => {
  it("durable-executions: all scenarios pass and all @sql proofs hold", async () => {
    expectClean(await run("effect-s2-durable/durable-executions/durable-executions.feature", durableExecutionsSteps), 12)
  }, 120_000)

  it("storage-primitives: the checkpoint scenario passes and its @sql proof holds", async () => {
    expectClean(await run("effect-s2-stream-db/storage-primitives.feature", storagePrimitivesSteps), 1)
  }, 120_000)
})
