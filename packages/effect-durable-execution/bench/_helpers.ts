import { FetchHttpClient } from "@effect/platform"
import { Effect, Layer, ManagedRuntime } from "effect"
import type { HttpClient } from "@effect/platform"
import type { Scope } from "effect"

export const BENCH_SIZES = [10, 100, 1000] as const

export const BENCH_OPTS = {
  time: Number(process.env.DURABLE_EXECUTION_BENCH_MS ?? "1000"),
} as const

export const streamUrl = (name: string): string =>
  `https://journal.example/v1/stream/${name}-${globalThis.crypto.randomUUID()}`

type EffectReq = HttpClient.HttpClient | Scope.Scope

export const makeEffectRuntime = (fetch: typeof globalThis.fetch) =>
  ManagedRuntime.make(
    FetchHttpClient.layer.pipe(
      Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch)),
    ),
  )

export type EffectRuntime = ReturnType<typeof makeEffectRuntime>

export const runScoped = <A, E>(
  runtime: EffectRuntime,
  effect: Effect.Effect<A, E, EffectReq>,
): Promise<A> => runtime.runPromise(Effect.scoped(effect))

const lastOffset = (events: ReadonlyArray<unknown>): string =>
  events.length === 0 ? "-1" : String(events.length - 1)

const parseOffset = (raw: string | null): number =>
  raw === null || raw === "-1" ? -1 : Number(raw)

export const makeMemoryDurableStreamsFetch = (): typeof globalThis.fetch => {
  const streams = new Map<string, Array<unknown>>()

  return async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const request = input instanceof Request ? input : new Request(input, init)
    const url = new URL(request.url)
    const streamKey = url.pathname
    const method = request.method.toUpperCase()

    if (method === "PUT") {
      const exists = streams.has(streamKey)
      if (!exists) streams.set(streamKey, [])
      return new Response("", {
        status: exists ? 200 : 201,
        headers: {
          "content-type": "application/json",
          "stream-next-offset": lastOffset(streams.get(streamKey) ?? []),
        },
      })
    }

    const events = streams.get(streamKey)
    if (events === undefined) {
      return new Response("", { status: 404 })
    }

    if (method === "POST") {
      const body = await request.text()
      const parsed: unknown = body.trim() === "" ? [] : JSON.parse(body)
      const batch: ReadonlyArray<unknown> = Array.isArray(parsed)
        ? parsed
        : [parsed]
      for (let index = 0; index < batch.length; index += 1) {
        events.push(batch[index])
      }
      return new Response("", {
        status: 200,
        headers: { "stream-next-offset": lastOffset(events) },
      })
    }

    if (method === "GET") {
      const offset = parseOffset(url.searchParams.get("offset"))
      const items = events.slice(offset + 1)
      return new Response(JSON.stringify(items), {
        status: 200,
        headers: {
          "content-type": "application/json",
          "stream-next-offset": lastOffset(events),
          "stream-up-to-date": "true",
        },
      })
    }

    return new Response("", { status: 405 })
  }
}
