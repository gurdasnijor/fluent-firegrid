import { Effect } from "effect"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { describe, expect, it } from "vitest"
import { DurableStreamsApi } from "../src/index.ts"

describe("DurableStreamsApi", () => {
  it("derives a typed client from the HTTPAPI contract", async () => {
    const requests: Array<{
      readonly method: string
      readonly url: string
      readonly urlParams: ReadonlyArray<readonly [string, string]>
    }> = []
    const httpClient = HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push({
          method: request.method,
          url: request.url,
          urlParams: [...request.urlParams],
        })
        return HttpClientResponse.fromWeb(
          request,
          new Response(
            JSON.stringify({
              records: [
                {
                  seqNum: 10,
                  body: "aGVsbG8=",
                  headers: [["content-type", "text/plain"]],
                  kind: "data",
                  timestamp: "2026-06-13T00:00:00.000Z",
                },
              ],
              tail: {
                seqNum: 11,
                timestamp: "2026-06-13T00:00:01.000Z",
              },
            }),
            {
              headers: { "content-type": "application/json" },
              status: 200,
            },
          ),
        )
      }),
    )

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const client = yield* HttpApiClient.makeWith(DurableStreamsApi, {
          baseUrl: "http://localhost:3000",
          httpClient,
        })
        return yield* client.Streams.read({
          params: { stream: "events-a" },
          query: { seqNum: 10, count: 1, ignoreCommandRecords: true },
        })
      }),
    )

    expect(requests).toEqual([
      {
        method: "GET",
        url: "http://localhost:3000/streams/events-a/records",
        urlParams: [
          ["seqNum", "10"],
          ["count", "1"],
          ["ignoreCommandRecords", "true"],
        ],
      },
    ])
    expect(result.records).toHaveLength(1)
    expect(result.records[0]).toMatchObject({
      seqNum: 10,
      body: "aGVsbG8=",
      headers: [["content-type", "text/plain"]],
      kind: "data",
    })
    expect(result.tail?.seqNum).toBe(11)
  })
})
