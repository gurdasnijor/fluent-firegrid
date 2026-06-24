import { describe, expect, it } from "@effect/vitest"
import {
  accessTokens,
  type AccessTokensApi,
  AppendInput,
  AppendRecord,
  basin,
  basins,
  type BasinsApi,
  locations,
  type LocationsApi,
  metrics,
  type MetricsApi,
  patterns,
  stream
} from "effect-s2"

const basinOperations = [
  "list",
  "listAll",
  "create",
  "getConfig",
  "delete",
  "ensure",
  "reconfigure"
] as const satisfies ReadonlyArray<keyof BasinsApi>

const accessTokenOperations = [
  "list",
  "listAll",
  "issue",
  "revoke"
] as const satisfies ReadonlyArray<keyof AccessTokensApi>

const locationOperations = [
  "list",
  "getDefault",
  "setDefault"
] as const satisfies ReadonlyArray<keyof LocationsApi>

const metricOperations = [
  "account",
  "basin",
  "stream"
] as const satisfies ReadonlyArray<keyof MetricsApi>

describe("effect-s2 SDK surface", () => {
  it("exposes grouped Effect accessors for every SDK operation family", () => {
    expect(typeof basin).toBe("function")
    expect(typeof stream).toBe("function")

    for (const operation of basinOperations) {
      expect(typeof basins[operation]).toBe("function")
    }
    for (const operation of accessTokenOperations) {
      expect(typeof accessTokens[operation]).toBe("function")
    }
    for (const operation of locationOperations) {
      expect(typeof locations[operation]).toBe("function")
    }
    for (const operation of metricOperations) {
      expect(typeof metrics[operation]).toBe("function")
    }
  })

  it("exposes upstream serialization patterns as Effect APIs", () => {
    expect(typeof patterns.u64.encode).toBe("function")
    expect(typeof patterns.u64.decode).toBe("function")
    expect(typeof patterns.chunkBytes).toBe("function")
    expect(typeof patterns.frameChunksToRecords).toBe("function")
    expect(typeof patterns.dedupeFilter).toBe("function")
  })

  it("re-exports SDK append constructors including command records", () => {
    const fence = AppendRecord.fence("token")
    const trim = AppendRecord.trim(2)
    const input = AppendInput.create([fence])

    expect(fence.headers).toEqual([["", "fence"]])
    expect(trim.headers?.[0]?.[0]).toBeInstanceOf(Uint8Array)
    expect(input.records).toEqual([fence])
  })
})
