// Test fixture: layers a fake `FetchHttpClient.Fetch` under the http client via two
// scoped provides — readable + correct; the production "combine provides" advice
// doesn't apply here.
// @effect-diagnostics effect/multipleEffectProvide:off
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, Layer, Schema, type Scope } from "effect"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { DurableStream } from "../../src/index.ts"
import { startTestServer, type TestServerHandle } from "./test-server.ts"

let server: TestServerHandle

beforeAll(async () => {
  server = await startTestServer()
})

afterAll(async () => {
  await server.stop()
})

const Message = Schema.Struct({
  id: Schema.String,
  text: Schema.String,
})

type Message = Schema.Schema.Type<typeof Message>
type Reqs = FetchHttpClient.Fetch | HttpClient.HttpClient | Scope.Scope

const runtime = <A, E>(eff: Effect.Effect<A, E, Reqs>) =>
  Effect.runPromise(
    Effect.scoped(eff.pipe(Effect.provide(FetchHttpClient.layer))) as Effect.Effect<
      A,
      E,
      never
    >,
  )

describe("classified producer append", () => {
  it("effect-durable-operators.BOUNDARIES.15 first producer append returns Appended and duplicate returns Duplicate", async () => {
    const url = server.streamUrl("classified-producer-append")
    const stream = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* stream.create({ contentType: "application/json" })
        const first = yield* DurableStream.appendWithProducer({
          endpoint: { url },
          schema: Message,
          event: { id: "m1", text: "first" },
          producerId: "classified-producer",
          producerEpoch: 0,
          producerSeq: 0,
        })
        const duplicate = yield* DurableStream.appendWithProducer({
          endpoint: { url },
          schema: Message,
          event: { id: "m1", text: "duplicate" },
          producerId: "classified-producer",
          producerEpoch: 0,
          producerSeq: 0,
        })
        const collected = yield* stream.collect

        expect(first._tag).toBe("Appended")
        expect(duplicate._tag).toBe("Duplicate")
        expect(collected).toEqual([{ id: "m1", text: "first" }])
      }),
    )
  })

  it("effect-durable-operators.BOUNDARIES.15 preserves endpoint headers and params", async () => {
    let captured: Request | undefined
    const fakeFetch: typeof globalThis.fetch = async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ) => {
      captured = input instanceof Request ? input : new Request(input, init)
      return new Response("", {
        status: 200,
        headers: { "stream-next-offset": "1_0" },
      })
    }

    await Effect.runPromise(
      Effect.scoped(
        DurableStream.appendWithProducer({
          endpoint: {
            url: "https://example.test/v1/stream/messages",
            headers: {
              "x-auth": "secret",
              "x-dynamic": () => Effect.succeed("resolved"),
            },
            params: {
              tenant: "acme",
              dynamic: () => Effect.succeed("param"),
            },
          },
          schema: Message,
          event: { id: "m1", text: "first" },
          producerId: "classified-producer-headers",
          producerEpoch: 0,
          producerSeq: 0,
        }).pipe(
          Effect.provide(FetchHttpClient.layer),
          Effect.provide(Layer.succeed(FetchHttpClient.Fetch, fakeFetch)),
        ),
      ),
    )

    expect(captured).toBeDefined()
    expect(captured?.headers.get("x-auth")).toBe("secret")
    expect(captured?.headers.get("x-dynamic")).toBe("resolved")
    expect(captured?.headers.get("producer-id")).toBe("classified-producer-headers")
    expect(new URL(captured?.url ?? "").searchParams.get("tenant")).toBe("acme")
    expect(new URL(captured?.url ?? "").searchParams.get("dynamic")).toBe("param")
  })

  it("effect-durable-operators.BOUNDARIES.15 producer sequence errors remain typed", async () => {
    const url = server.streamUrl("classified-producer-sequence-error")
    const stream = DurableStream.define({ endpoint: { url }, schema: Message })

    await runtime(
      Effect.gen(function* () {
        yield* stream.create({ contentType: "application/json" })
        const result = yield* DurableStream.appendWithProducer({
          endpoint: { url },
          schema: Message,
          event: { id: "m1", text: "gap" },
          producerId: "classified-producer-gap",
          producerEpoch: 0,
          producerSeq: 1,
        }).pipe(Effect.either)

        expect(result._tag).toBe("Left")
        if (result._tag === "Left") {
          expect(result.left).toBeInstanceOf(DurableStream.SequenceGap)
        }
      }),
    )
  })
})
