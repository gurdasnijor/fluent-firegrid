import { DurableStreamTestServer } from "@durable-streams/server"
import { FetchHttpClient, type HttpClient } from "@effect/platform"
import { Effect, ManagedRuntime, type Scope } from "effect"

/**
 * Boot the reference server once for the whole bench file. tinybench retains
 * the bench function's closure, so we can capture the URL here and reuse it
 * across iterations.
 */
export const startBenchServer = async (): Promise<{
  url: string
  streamUrl: (name: string) => string
  stop: () => Promise<void>
}> => {
  const server = new DurableStreamTestServer({ port: 0, host: "127.0.0.1" })
  const url = await server.start()
  return {
    url,
    streamUrl: (name) => `${url}/v1/stream/${name}-${crypto.randomUUID()}`,
    stop: () => server.stop(),
  }
}

/**
 * A long-lived runtime with FetchHttpClient pre-provided. Each bench
 * iteration runs `runtime.runPromise(...)` rather than re-building the
 * layer, so the measurement reflects steady-state cost.
 */
export const makeEffectRuntime = () => ManagedRuntime.make(FetchHttpClient.layer)

export type EffectRuntime = ReturnType<typeof makeEffectRuntime>

type EffectReq = HttpClient.HttpClient | Scope.Scope

export const runScoped = <A, E>(
  runtime: EffectRuntime,
  eff: Effect.Effect<A, E, EffectReq>,
): Promise<A> => runtime.runPromise(Effect.scoped(eff))
