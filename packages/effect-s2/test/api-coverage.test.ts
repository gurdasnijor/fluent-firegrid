import { describe, expect, it } from "@effect/vitest"
import { AppendInput, AppendRecord, S2Client, type S2ClientApi } from "effect-s2"

const accountOperations = [
  "listBasins",
  "listAllBasins",
  "createBasin",
  "getBasinConfig",
  "deleteBasin",
  "ensureBasin",
  "reconfigureBasin",
  "listAccessTokens",
  "listAllAccessTokens",
  "issueAccessToken",
  "revokeAccessToken",
  "listLocations",
  "getDefaultLocation",
  "setDefaultLocation",
  "accountMetrics",
  "basinMetrics",
  "streamMetrics"
] as const satisfies ReadonlyArray<keyof S2ClientApi>

const streamOperations = [
  "listStreams",
  "listAllStreams",
  "createStream",
  "getStreamConfig",
  "deleteStream",
  "ensureStream",
  "reconfigureStream",
  "checkTail",
  "readBatch",
  "readBatchBytes",
  "append",
  "read",
  "readBytes",
  "appendSession",
  "producer"
] as const satisfies ReadonlyArray<keyof S2ClientApi>

describe("effect-s2 SDK surface", () => {
  it("exposes Effect accessors for every SDK operation family", () => {
    for (const operation of [...accountOperations, ...streamOperations]) {
      expect(typeof S2Client[operation]).toBe("function")
    }
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
