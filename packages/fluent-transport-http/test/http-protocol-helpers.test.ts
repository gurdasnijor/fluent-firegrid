import { Chunk, Effect, Stream } from "effect"
import { describe, expect, it } from "vitest"
import { cacheHeaders } from "../src/cache.ts"
import {
  appendHeaders,
  BASE64_ENCODING,
  JSON_CONTENT_TYPE,
  STREAM_CLOSED,
  STREAM_CURSOR,
  STREAM_SSE_DATA_ENCODING,
  sseDataEncodingHeaders,
} from "../src/headers.ts"
import { isReservedControlPath, readStreamUrl, streamUrl, subscriptionUrl } from "../src/routes.ts"
import {
  decodeBase64SseData,
  decodeSseControlEvent,
  encodeBase64DataEvent,
  encodeSseControlEvent,
  encodeSseDataEvent,
  parseSseText,
} from "../src/sse.ts"

describe("HTTP PROTOCOL.md helpers", () => {
  it("builds operation URLs from stream URLs and read query parameters", () => {
    expect(streamUrl("https://example.com/streams/", "events/a b")).toBe(
      "https://example.com/streams/events/a%20b",
    )
    expect(readStreamUrl("https://example.com/streams", "events/a", { offset: "-1" })).toBe(
      "https://example.com/streams/events/a?offset=-1",
    )
    expect(readStreamUrl("https://example.com/streams", "events/a", { offset: "now", live: "sse" })).toBe(
      "https://example.com/streams/events/a?offset=now&live=sse",
    )
    expect(readStreamUrl("https://example.com/streams", "events/a", {
      offset: "0001",
      live: "long-poll",
      cursor: "c1",
    })).toBe("https://example.com/streams/events/a?offset=0001&live=long-poll&cursor=c1")
  })

  it("reserves __ds as a stream-root control prefix", () => {
    expect(isReservedControlPath("__ds/subscriptions/a")).toBe(true)
    expect(isReservedControlPath("/__ds")).toBe(true)
    expect(isReservedControlPath("events/__ds")).toBe(false)
    expect(subscriptionUrl("https://example.com/streams", "events/a", "sub 1")).toBe(
      "https://example.com/streams/events/a/__ds/subscriptions/sub%201",
    )
  })

  it("uses stream content types and protocol stream headers", () => {
    expect(appendHeaders(JSON_CONTENT_TYPE, { closed: true })).toEqual({
      "content-type": JSON_CONTENT_TYPE,
      [STREAM_CLOSED]: "true",
    })
    expect(sseDataEncodingHeaders(BASE64_ENCODING)).toEqual({
      [STREAM_SSE_DATA_ENCODING]: BASE64_ENCODING,
    })
    expect(cacheHeaders({ etag: "\"1\"", cursor: "c1", maxAgeSeconds: 3 })).toEqual({
      etag: "\"1\"",
      [STREAM_CURSOR]: "c1",
      "cache-control": "public, max-age=3",
    })
  })

  it("encodes and decodes SSE data and control events", async () => {
    const control = {
      streamNextOffset: "0002",
      streamCursor: "cursor-2",
      upToDate: true,
    }
    const text = [
      encodeSseDataEvent("[{\"k\":\"v\"}]"),
      encodeSseControlEvent(control),
    ].join("")

    const events = await Effect.runPromise(
      parseSseText(text).pipe(
        Stream.runCollect,
        Effect.map(Chunk.toReadonlyArray),
      ),
    )
    expect(events.length).toBe(2)
    expect(events[0]).toMatchObject({ event: "data", data: "[{\"k\":\"v\"}]" })
    expect(events[1]).toMatchObject({ event: "control" })
    await expect(Effect.runPromise(decodeSseControlEvent(events[1]!.data))).resolves.toEqual(control)
  })

  it("base64-encodes binary SSE data and tolerates split data lines", async () => {
    expect(encodeBase64DataEvent(new Uint8Array([1, 2, 3, 4, 5, 6]))).toBe(
      "event: data\ndata: AQIDBAUG\n\n",
    )
    await expect(Effect.runPromise(decodeBase64SseData("AQID\nBAUG"))).resolves.toEqual(
      new Uint8Array([1, 2, 3, 4, 5, 6]),
    )
  })
})
