import { describe, expect, it } from "vitest"
import { HttpApiClient } from "effect/unstable/httpapi"
import { DurableStreamsApi } from "../src/index.ts"

describe("DurableStreamsApi", () => {
  it("builds URLs around S2 stream names, not Durable Streams paths", () => {
    const client = HttpApiClient.urlBuilder(DurableStreamsApi, {
      baseUrl: "http://localhost:3000",
    })

    expect(client.Streams.ensureStream({ params: { stream: "events" } })).toBe(
      "http://localhost:3000/streams/events",
    )
    expect(
      client.Streams.read({
        params: { stream: "events-a" },
        query: { seqNum: 10, count: 5, ignoreCommandRecords: true },
      }),
    ).toBe("http://localhost:3000/streams/events-a/records?seqNum=10&count=5&ignoreCommandRecords=true")
    expect(
      client.State.readState({
        params: { stream: "state-users" },
        query: { seqNum: 0, count: 100 },
      }),
    ).toBe("http://localhost:3000/state/state-users/records?seqNum=0&count=100")
  })
})
