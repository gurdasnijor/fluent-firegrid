import { Effect } from "effect"
import { HttpApiTest } from "effect/unstable/httpapi"
import { describe, expect, it } from "vitest"
import {
  DurableStreamsApi,
  layerConfig,
  layerProfile,
  StateLive,
  StreamsLive,
  type ReadBatch,
  type StateReadBatch,
} from "../src/index.ts"

const endpoint = process.env["S2_LITE_ENDPOINT"]
const describeLite = endpoint === undefined ? describe.skip : describe

describeLite("S2 Lite HTTPAPI", () => {
  it("appends and reads bytes through the HTTPAPI handlers backed by S2 Lite", async () => {
    const runnable = Effect.scoped(
      Effect.gen(function*() {
        const client = yield* HttpApiTest.groups(DurableStreamsApi, ["Streams"] as const)
        const stream = `httpapi-smoke-${Date.now().toString(36)}`

        yield* client.Streams.ensureStream({ params: { stream } })
        const ack = yield* client.Streams.appendRaw({
          params: { stream },
          query: {},
          payload: new TextEncoder().encode("hello"),
        })
        return yield* client.Streams.read({
          params: { stream },
          query: { seqNum: ack.start.seqNum, count: 1 },
        })
      }),
    ).pipe(
      Effect.provide(StreamsLive),
      Effect.provide(layerProfile),
      Effect.provide(
        layerConfig({
          accessToken: process.env["S2_ACCESS_TOKEN"] ?? "ignored",
          basin: process.env["S2_LITE_BASIN"] ?? "fluent-dev",
          endpoints: {
            account: endpoint!,
            basin: endpoint!,
          },
        }),
      ),
    ) as Effect.Effect<ReadBatch, unknown, never>

    const result = await Effect.runPromise(runnable)

    expect(result.records).toHaveLength(1)
    expect(atob(result.records[0]!.bodyBase64)).toBe("hello")
  })

  it("appends and reads state messages through the HTTPAPI handlers backed by S2 Lite", async () => {
    const runnable = Effect.scoped(
      Effect.gen(function*() {
        const client = yield* HttpApiTest.groups(DurableStreamsApi, ["State"] as const)
        const stream = `state-smoke-${Date.now().toString(36)}`

        const ack = yield* client.State.appendState({
          params: { stream },
          payload: {
            records: [
              {
                type: "user",
                key: "user:1",
                value: { name: "Ada" },
                headers: { operation: "insert", txid: "tx-1" },
              },
            ],
          },
        })
        return yield* client.State.readState({
          params: { stream },
          query: { seqNum: ack.start.seqNum, count: 1 },
        })
      }),
    ).pipe(
      Effect.provide(StateLive),
      Effect.provide(layerProfile),
      Effect.provide(
        layerConfig({
          accessToken: process.env["S2_ACCESS_TOKEN"] ?? "ignored",
          basin: process.env["S2_LITE_BASIN"] ?? "fluent-dev",
          endpoints: {
            account: endpoint!,
            basin: endpoint!,
          },
        }),
      ),
    ) as Effect.Effect<StateReadBatch, unknown, never>

    const result = await Effect.runPromise(runnable)

    expect(result.records).toHaveLength(1)
    expect(result.records[0]!.message).toMatchObject({
      type: "user",
      key: "user:1",
      value: { name: "Ada" },
      headers: { operation: "insert", txid: "tx-1" },
    })
  })
})
