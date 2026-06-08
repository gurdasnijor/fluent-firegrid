// Test fixture: layers a fake `FetchHttpClient.Fetch` under the http client via two
// scoped provides — readable + correct; the production "combine provides" advice
// doesn't apply here.
// @effect-diagnostics effect/multipleEffectProvide:off
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, Layer, Schedule, Schema, type Scope } from "effect"
import { describe, expect, it } from "vitest"
import { DurableStream } from "../../src/index.ts"

// ============================================================================
// Mocked-fetch retry classification tests
// ============================================================================
//
// The reference `@durable-streams/client` test corpus pins down a precise
// HTTP-retry contract: 5xx and 429 retry through the backoff schedule, 4xx
// (other than 429) never retry, `Retry-After` is honored. Our reference
// `DurableStreamTestServer` doesn't synthesize 5xx/429 on demand, so we
// drive the HTTP layer with a fake `fetch` that returns canned responses
// per request. Each test asserts on the resulting CALL COUNT — the
// observable signal that the classifier did or did not retry.

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

/**
 * Build a fake fetch that emits the supplied responses in order. The LAST
 * response is repeated indefinitely so retry tests can use a short canned
 * sequence ending in 200. Returns both the fetch and a counter ref the
 * test can inspect to assert on attempt count.
 */
const makeFakeFetch = (
  responses: ReadonlyArray<{
    readonly status: number
    readonly body?: string
    readonly headers?: Record<string, string>
  }>,
): { fetch: typeof globalThis.fetch; calls: { count: number } } => {
  const calls = { count: 0 }
  const fetchImpl: typeof globalThis.fetch = async (
    _input: globalThis.RequestInfo | globalThis.URL,
    _init?: globalThis.RequestInit,
  ): Promise<Response> => {
    const r = responses[Math.min(calls.count, responses.length - 1)]!
    calls.count += 1
    return new Response(r.body ?? "", {
      status: r.status,
      headers: r.headers ?? {},
    })
  }
  return { fetch: fetchImpl, calls }
}

// A schedule that retries up to 3 times with effectively-zero backoff —
// keeps the test wall-clock under a few milliseconds.
const fastSchedule = Schedule.recurs(3)

describe("Phase 1 retry classification (mocked fetch)", () => {
  it("retries 5xx until success", async () => {
    const { fetch, calls } = makeFakeFetch([
      { status: 503 },
      { status: 503 },
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: { url: "http://example/v1/stream/x", retrySchedule: fastSchedule },
        schema: Message,
      }),
    )

    // 2 × 503 + 1 × 200 = 3 calls. Anything less means we didn't retry.
    expect(calls.count).toBe(3)
  })

  it("retries 429 (rate limit) until success", async () => {
    const { fetch, calls } = makeFakeFetch([
      { status: 429 },
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: { url: "http://example/v1/stream/x", retrySchedule: fastSchedule },
        schema: Message,
      }),
    )

    expect(calls.count).toBe(2)
  })

  it("does NOT retry 4xx other than 429 (404 maps to NotFound)", async () => {
    const { fetch, calls } = makeFakeFetch([{ status: 404 }])

    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: {
            url: "http://example/v1/stream/missing",
            retrySchedule: fastSchedule,
          },
          schema: Message,
        }),
      ),
    )

    expect(exit._tag).toBe("Failure")
    // Exactly one call — no retry on 4xx-not-429.
    expect(calls.count).toBe(1)
  })

  it("does NOT retry 4xx other than 429 (418 maps to TransportError)", async () => {
    const { fetch, calls } = makeFakeFetch([{ status: 418 }])

    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: { url: "http://example/v1/stream/teapot", retrySchedule: fastSchedule },
          schema: Message,
        }),
      ),
    )

    expect(exit._tag).toBe("Failure")
    expect(calls.count).toBe(1)
  })

  it("honors Retry-After (seconds form) on a 429 response", async () => {
    const { fetch, calls } = makeFakeFetch([
      { status: 429, headers: { "retry-after": "0" } },
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    const start = Date.now()
    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: { url: "http://example/v1/stream/x", retrySchedule: fastSchedule },
        schema: Message,
      }),
    )
    const elapsed = Date.now() - start

    expect(calls.count).toBe(2)
    // Retry-After: 0 means "retry immediately"; we only pay the schedule's
    // first delay (≤ 100ms). Anything under 500ms confirms we didn't stall.
    expect(elapsed).toBeLessThan(500)
  })

  it("retry schedule exhausts on persistent 5xx; surfaces typed failure", async () => {
    const { fetch, calls } = makeFakeFetch([{ status: 503 }])

    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: { url: "http://example/v1/stream/x", retrySchedule: fastSchedule },
          schema: Message,
        }),
      ),
    )

    expect(exit._tag).toBe("Failure")
    // recurs(3) = 1 initial + 3 retries = 4 calls before giving up.
    expect(calls.count).toBe(4)
  })

  it("maxRetries=0 (Schedule.stop) disables retries", async () => {
    const { fetch, calls } = makeFakeFetch([{ status: 503 }])

    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: { url: "http://example/v1/stream/x", retrySchedule: Schedule.stop },
          schema: Message,
        }),
      ),
    )

    expect(exit._tag).toBe("Failure")
    expect(calls.count).toBe(1)
  })

  // Property-style sweep: every 4xx-not-429 status results in exactly one
  // HTTP call (no retry); every 5xx status results in retries. Ports the
  // intent of `property-based.test.ts > HTTP Status Code Classification`.
  it.each([400, 401, 403, 404, 409, 410, 418, 422])(
    "does NOT retry on %i (any 4xx except 429 is final)",
    async (status) => {
      const { fetch, calls } = makeFakeFetch([{ status }])

      const exit = await runtimeWith(
        fetch,
        Effect.exit(
          DurableStream.collect({
            endpoint: {
              url: "http://example/v1/stream/x",
              retrySchedule: fastSchedule,
            },
            schema: Message,
          }),
        ),
      )

      expect(exit._tag).toBe("Failure")
      expect(calls.count).toBe(1)
    },
  )

  it.each([500, 502, 503, 504])(
    "DOES retry on %i (every 5xx is transient)",
    async (status) => {
      const { fetch, calls } = makeFakeFetch([
        { status },
        { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
      ])

      await runtimeWith(
        fetch,
        DurableStream.collect({
          endpoint: { url: "http://example/v1/stream/x", retrySchedule: fastSchedule },
          schema: Message,
        }),
      )

      expect(calls.count).toBe(2)
    },
  )
})

// ============================================================================
// onError handler — port of upstream @durable-streams/client/test/onError.test.ts
// ============================================================================
//
// Upstream pins down a precise contract for the post-retry recovery hook.
// We port the 7 cases that apply to our shape; the 4 params-related cases
// are NOT applicable (our `Endpoint` does not expose URL params today —
// callers carry params on the URL string itself).

describe("Phase 1 onError handler contract", () => {
  it("retries on error when handler returns an empty RetryOpts ({})", async () => {
    // Empty object is still a truthy `RetryOpts` — the contract is "any
    // non-void return means retry". Headers stay as-is.
    const { fetch, calls } = makeFakeFetch([
      { status: 500 },
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    let onErrorCalls = 0
    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: {
          url: "http://example/v1/stream/x",
          retrySchedule: Schedule.stop, // exhaust schedule on first 5xx
          onError: () => {
            onErrorCalls += 1
            return Effect.succeed({})
          },
        },
        schema: Message,
      }),
    )

    expect(onErrorCalls).toBe(1)
    expect(calls.count).toBe(2)
  })

  it("stops retrying when handler returns undefined (void)", async () => {
    const { fetch, calls } = makeFakeFetch([{ status: 500 }])

    let onErrorCalls = 0
    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: {
            url: "http://example/v1/stream/x",
            retrySchedule: Schedule.stop,
            onError: () => {
              onErrorCalls += 1
              return Effect.succeed(undefined as DurableStream.RetryOpts | undefined)
            },
          },
          schema: Message,
        }),
      ),
    )

    expect(exit._tag).toBe("Failure")
    expect(onErrorCalls).toBe(1)
    expect(calls.count).toBe(1)
  })

  it("supports an async error handler", async () => {
    // The handler may yield through Effect.sleep / external IO before
    // returning RetryOpts. Verify the retry still fires correctly.
    const { fetch, calls } = makeFakeFetch([
      { status: 500 },
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    let onErrorCalls = 0
    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: {
          url: "http://example/v1/stream/x",
          retrySchedule: Schedule.stop,
          onError: () =>
            Effect.gen(function* () {
              yield* Effect.sleep("10 millis")
              onErrorCalls += 1
              return { headers: { "x-refreshed": "1" } }
            }),
        },
        schema: Message,
      }),
    )

    expect(onErrorCalls).toBe(1)
    expect(calls.count).toBe(2)
  })

  it("is NOT called when no error occurs", async () => {
    const { fetch, calls } = makeFakeFetch([
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    let onErrorCalls = 0
    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: {
          url: "http://example/v1/stream/x",
          onError: () => {
            onErrorCalls += 1
            return Effect.succeed(undefined as DurableStream.RetryOpts | undefined)
          },
        },
        schema: Message,
      }),
    )

    expect(onErrorCalls).toBe(0)
    expect(calls.count).toBe(1)
  })

  it("propagates the original error when no onError handler is provided", async () => {
    const { fetch, calls } = makeFakeFetch([{ status: 404 }])

    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: { url: "http://example/v1/stream/x" },
          schema: Message,
        }),
      ),
    )

    expect(exit._tag).toBe("Failure")
    expect(calls.count).toBe(1)
  })

  it("is called for 4xx client errors (recovery hook fires on protocol failures)", async () => {
    // 404 doesn't go through the backoff schedule (it's a final answer) —
    // but the onError hook still gets a shot at recovery, e.g. URL refresh.
    const { fetch, calls } = makeFakeFetch([
      { status: 404 },
      { status: 200, body: "[]", headers: { "stream-up-to-date": "true" } },
    ])

    let onErrorCalls = 0
    await runtimeWith(
      fetch,
      DurableStream.collect({
        endpoint: {
          url: "http://example/v1/stream/x",
          onError: () => {
            onErrorCalls += 1
            return Effect.succeed({ headers: { "x-after-404": "1" } })
          },
        },
        schema: Message,
      }),
    )

    expect(onErrorCalls).toBe(1)
    expect(calls.count).toBe(2)
  })

  it("merges returned headers with existing endpoint headers", async () => {
    // The handler returns ONLY `x-refreshed`; the endpoint's existing
    // `x-tenant` must still be sent on the retry.
    const seenHeaders: Array<Record<string, string>> = []
    const fetchImpl: typeof globalThis.fetch = async (
      _input: globalThis.RequestInfo | globalThis.URL,
      init?: globalThis.RequestInit,
    ): Promise<Response> => {
      const hdrs = new Headers(init?.headers)
      const out: Record<string, string> = {}
      hdrs.forEach((v, k) => {
        out[k.toLowerCase()] = v
      })
      seenHeaders.push(out)
      // First call 500, second call 200.
      return new Response(seenHeaders.length === 1 ? "" : "[]", {
        status: seenHeaders.length === 1 ? 500 : 200,
        headers: seenHeaders.length === 1 ? {} : { "stream-up-to-date": "true" },
      })
    }

    await runtimeWith(
      fetchImpl,
      DurableStream.collect({
        endpoint: {
          url: "http://example/v1/stream/x",
          headers: { "x-tenant": "acme" },
          retrySchedule: Schedule.stop,
          onError: () => Effect.succeed({ headers: { "x-refreshed": "1" } }),
        },
        schema: Message,
      }),
    )

    expect(seenHeaders.length).toBe(2)
    // First call: tenant header only.
    expect(seenHeaders[0]!["x-tenant"]).toBe("acme")
    // Second call: BOTH the original tenant header AND the refresh header.
    expect(seenHeaders[1]!["x-tenant"]).toBe("acme")
    expect(seenHeaders[1]!["x-refreshed"]).toBe("1")
  })
})

// ============================================================================
// Per-call header overrides
// ============================================================================
//
// `ReadOpts`/`CollectOpts`/`AppendOpts`/`CreateOptions`/`CloseOptions` now
// accept a `headers?` field. Per-call values merge ON TOP of endpoint-level
// headers (call wins on collision). Function values are re-evaluated per
// request, same as endpoint-level headers.

describe("Phase 1 per-call header overrides", () => {
  it("collect: per-call headers merge over endpoint headers (call wins)", async () => {
    const seenHeaders: Array<Record<string, string>> = []
    const fetchImpl: typeof globalThis.fetch = async (
      _input: globalThis.RequestInfo | globalThis.URL,
      init?: globalThis.RequestInit,
    ): Promise<Response> => {
      const hdrs = new Headers(init?.headers)
      const out: Record<string, string> = {}
      hdrs.forEach((v, k) => {
        out[k.toLowerCase()] = v
      })
      seenHeaders.push(out)
      return new Response("[]", {
        status: 200,
        headers: { "stream-up-to-date": "true" },
      })
    }

    await runtimeWith(
      fetchImpl,
      DurableStream.collect({
        endpoint: {
          url: "http://example/v1/stream/x",
          headers: { "x-tenant": "acme", "x-shared": "endpoint-value" },
        },
        schema: Message,
        // Per-call: introduce a new header AND override an existing one.
        headers: {
          "x-request-id": "req-123",
          "x-shared": "call-value",
        },
      }),
    )

    expect(seenHeaders.length).toBe(1)
    expect(seenHeaders[0]!["x-tenant"]).toBe("acme")
    expect(seenHeaders[0]!["x-request-id"]).toBe("req-123")
    // Per-call override wins.
    expect(seenHeaders[0]!["x-shared"]).toBe("call-value")
  })

  it("append: per-call headers reach the wire", async () => {
    let seen: Record<string, string> = {}
    const fetchImpl: typeof globalThis.fetch = async (
      _input: globalThis.RequestInfo | globalThis.URL,
      init?: globalThis.RequestInit,
    ): Promise<Response> => {
      const hdrs = new Headers(init?.headers)
      seen = {}
      hdrs.forEach((v, k) => {
        seen[k.toLowerCase()] = v
      })
      return new Response("", {
        status: 200,
        headers: { "stream-next-offset": "0_42" },
      })
    }

    await runtimeWith(
      fetchImpl,
      DurableStream.append({
        endpoint: {
          url: "http://example/v1/stream/x",
          headers: { "x-tenant": "acme" },
        },
        schema: Message,
        event: { n: 1 },
        headers: { "x-idempotency-key": "abc-123" },
      }),
    )

    expect(seen["x-tenant"]).toBe("acme")
    expect(seen["x-idempotency-key"]).toBe("abc-123")
  })

  it("function-valued per-call headers are re-evaluated per request", async () => {
    // Live read over long-poll: a function header must fire on EVERY
    // poll, not just the first. Mock fetch reports two-then-close.
    let pollCount = 0
    let evalCount = 0
    const seenHeaders: Array<Record<string, string>> = []
    const fetchImpl: typeof globalThis.fetch = async (
      _input: globalThis.RequestInfo | globalThis.URL,
      init?: globalThis.RequestInit,
    ): Promise<Response> => {
      const hdrs = new Headers(init?.headers)
      const out: Record<string, string> = {}
      hdrs.forEach((v, k) => {
        out[k.toLowerCase()] = v
      })
      seenHeaders.push(out)
      pollCount += 1
      return new Response("[]", {
        status: 200,
        headers: pollCount >= 2
          ? { "stream-up-to-date": "true", "stream-closed": "true" }
          : {},
      })
    }

    await runtimeWith(
      fetchImpl,
      DurableStream.collect({
        endpoint: { url: "http://example/v1/stream/x" },
        schema: Message,
        headers: {
          "x-token": () => {
            evalCount += 1
            return `t-${evalCount}`
          },
        },
      }),
    )

    expect(pollCount).toBeGreaterThanOrEqual(2)
    expect(evalCount).toBeGreaterThanOrEqual(2)
    expect(seenHeaders[0]!["x-token"]).toBe("t-1")
    expect(seenHeaders[1]!["x-token"]).toBe("t-2")
  })
})

// ============================================================================
// 410 → Gone mapping
// ============================================================================

describe("Phase 1 410 Gone mapping", () => {
  it("read: 410 surfaces as Gone (not TransportError)", async () => {
    const { fetch } = makeFakeFetch([{ status: 410 }])
    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.collect({
          endpoint: { url: "http://example/v1/stream/dead" },
          schema: Message,
        }),
      ),
    )
    expect(exit._tag).toBe("Failure")
    // Walk the cause to find the typed Gone failure.
    if (exit._tag === "Failure") {
      const found = JSON.stringify(exit.cause).includes("DurableStream/Gone")
      expect(found).toBe(true)
    }
  })

  it("append: 410 surfaces as Gone (not TransportError)", async () => {
    const { fetch } = makeFakeFetch([{ status: 410 }])
    const exit = await runtimeWith(
      fetch,
      Effect.exit(
        DurableStream.append({
          endpoint: { url: "http://example/v1/stream/dead" },
          schema: Message,
          event: { n: 1 },
        }),
      ),
    )
    expect(exit._tag).toBe("Failure")
    if (exit._tag === "Failure") {
      const found = JSON.stringify(exit.cause).includes("DurableStream/Gone")
      expect(found).toBe(true)
    }
  })
})
