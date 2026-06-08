// Test fixture: layers a fake `FetchHttpClient.Fetch` under the http client via two
// scoped provides — readable + correct; the production "combine provides" advice
// doesn't apply here.
// @effect-diagnostics effect/multipleEffectProvide:off
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, Layer, Schema, type Scope, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { DurableStream } from "../../src/index.ts"

// ============================================================================
// SSE edge cases — ports of @durable-streams/client/test/sse.test.ts
// ============================================================================
//
// These tests drive the SSE pipeline with a mocked `fetch` returning canned
// `text/event-stream` bodies. The reference's `sse.test.ts` covers 18 cases;
// we port the ones that exercise behaviors our own pipeline owns (parser
// edge cases, reconnection, offset propagation). Cases tied to the
// reference's body() / bodyStream() / synthetic-Response surfaces are not
// applicable to our Stream-shaped API.

const Message = Schema.Struct({ n: Schema.Number })

type Reqs = FetchHttpClient.Fetch | HttpClient.HttpClient | Scope.Scope

const runtimeWith = <A, E>(
  fakeFetch: typeof globalThis.fetch,
  eff: Effect.Effect<A, E, Reqs>,
) =>
  Effect.runPromise(
    Effect.scoped(
      eff.pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provide(Layer.succeed(FetchHttpClient.Fetch, fakeFetch)),
      ),
    ),
  )

interface SseEvent {
  readonly event?: string
  readonly data: string
}

const encodeSse = (events: ReadonlyArray<SseEvent>): string => {
  let text = ""
  for (const e of events) {
    if (e.event !== undefined) text += `event: ${e.event}\n`
    // Per the SSE spec, multi-line data is split across multiple `data:` lines.
    for (const line of e.data.split("\n")) {
      text += `data: ${line}\n`
    }
    text += "\n"
  }
  return text
}

const sseBody = (events: ReadonlyArray<SseEvent>): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder()
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(encodeSse(events)))
      controller.close()
    },
  })
}

/**
 * Build a fake fetch that returns the supplied SSE responses in sequence.
 * Each entry is one HTTP round-trip; the LAST entry repeats indefinitely
 * (so a test can end every connection with a `streamClosed: true` control
 * to terminate the outer reconnect loop).
 */
const makeSseFetch = (
  responses: ReadonlyArray<{
    readonly events: ReadonlyArray<SseEvent>
    readonly status?: number
  }>,
): {
  readonly fetch: typeof globalThis.fetch
  readonly requests: Array<{ readonly url: string }>
} => {
  const requests: Array<{ url: string }> = []
  const fetchImpl: typeof globalThis.fetch = async (
    input: globalThis.RequestInfo | globalThis.URL,
    _init?: globalThis.RequestInit,
  ): Promise<Response> => {
    const url = typeof input === "string"
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url
    requests.push({ url })
    const r = responses[Math.min(requests.length - 1, responses.length - 1)]!
    return new Response(sseBody(r.events), {
      status: r.status ?? 200,
      headers: { "content-type": "text/event-stream" },
    })
  }
  return { fetch: fetchImpl, requests }
}

const closedControl = (offset = "0_0"): SseEvent => ({
  event: "control",
  data: JSON.stringify({
    streamNextOffset: offset,
    streamClosed: true,
  }),
})

describe("SSE edge cases", () => {
  it("emits items from a basic data event and terminates on streamClosed", async () => {
    const { fetch, requests } = makeSseFetch([
      {
        events: [
          { event: "data", data: JSON.stringify([{ n: 1 }, { n: 2 }, { n: 3 }]) },
          closedControl(),
        ],
      },
    ])

    const items = await runtimeWith(
      fetch,
      DurableStream.read({
        endpoint: { url: "http://example/v1/stream/x" },
        schema: Message,
        live: "sse",
      }).pipe(Stream.runCollect, Effect.map((c) => Array.from(c))),
    )

    expect(items.map((m) => m.n)).toEqual([1, 2, 3])
    expect(requests.length).toBe(1)
  })

  it("ignores unknown event types (does NOT error)", async () => {
    // Per the protocol, only `data` and `control` are recognized. Anything
    // else must be silently dropped — a future protocol extension shouldn't
    // crash this client.
    const { fetch } = makeSseFetch([
      {
        events: [
          { event: "ping", data: "irrelevant" },
          { event: "future-extension", data: "{}" },
          { event: "data", data: JSON.stringify([{ n: 42 }]) },
          closedControl(),
        ],
      },
    ])

    const items = await runtimeWith(
      fetch,
      DurableStream.read({
        endpoint: { url: "http://example/v1/stream/x" },
        schema: Message,
        live: "sse",
      }).pipe(Stream.runCollect, Effect.map((c) => Array.from(c))),
    )

    expect(items.map((m) => m.n)).toEqual([42])
  })

  it("surfaces a typed failure on invalid control event JSON", async () => {
    const { fetch } = makeSseFetch([
      {
        events: [
          { event: "data", data: JSON.stringify([{ n: 1 }]) },
          { event: "control", data: "{this-is-not-json" },
        ],
      },
    ])

    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.read({
          endpoint: { url: "http://example/v1/stream/x" },
          schema: Message,
          live: "sse",
        }).pipe(Stream.runCollect),
      ),
    )

    expect(exit._tag).toBe("Failure")
  })

  it("completes cleanly on an empty stream that immediately closes", async () => {
    const { fetch } = makeSseFetch([{ events: [closedControl()] }])

    const items = await runtimeWith(
      fetch,
      DurableStream.read({
        endpoint: { url: "http://example/v1/stream/x" },
        schema: Message,
        live: "sse",
      }).pipe(Stream.runCollect, Effect.map((c) => Array.from(c))),
    )

    expect(items).toEqual([])
  })

  it("reopens on disconnect and resumes from the updated offset", async () => {
    // First connection emits two data items + an offset advance (no close).
    // Stream ends — sseStream should reopen and the second request must
    // carry the new offset, NOT the original start offset.
    const { fetch, requests } = makeSseFetch([
      {
        events: [
          {
            event: "control",
            data: JSON.stringify({ streamNextOffset: "5_42" }),
          },
          { event: "data", data: JSON.stringify([{ n: 1 }, { n: 2 }]) },
        ],
      },
      {
        events: [
          { event: "data", data: JSON.stringify([{ n: 3 }]) },
          closedControl("5_99"),
        ],
      },
    ])

    const items = await runtimeWith(
      fetch,
      DurableStream.read({
        endpoint: { url: "http://example/v1/stream/x" },
        schema: Message,
        live: "sse",
      }).pipe(Stream.runCollect, Effect.map((c) => Array.from(c))),
    )

    expect(items.map((m) => m.n)).toEqual([1, 2, 3])
    expect(requests.length).toBe(2)
    // Second request MUST carry the updated offset from the control event.
    expect(requests[1]!.url).toContain("offset=5_42")
  })
})
