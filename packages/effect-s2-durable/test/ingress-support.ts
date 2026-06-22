// @effect-diagnostics effect/nodeBuiltinImport:off effect/multipleEffectProvide:off -- edge integration support: node:http creates the server NodeHttpServer.layer adapts, node:child_process probes for the `s2` binary, and the chained provides are the intentional edge wiring (HttpClient → ingress → engine → server → S2).
import { execSync } from "node:child_process"
import { createServer } from "node:http"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as NodePath from "@effect/platform-node/NodePath"
import { Effect, Layer, Schema } from "effect"
import { HttpServer } from "effect/unstable/http"
import { primaryKey, Table } from "effect-s2-stream-db"
import { client, connect, type DurableIngressClient, durableIngress, object, objectClient, run, service, serviceLayer, state } from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"

// A small restate-style "catalog" of durable definitions, each exercising one
// capability, shared by every ingress test (so the edge provide-stack lives in
// exactly one place — `runIngress` — rather than being copy-pasted per test).

/** Stateless service: a plain durable step (`double`). */
export const Calculator = service({
  name: "ingress-calculator",
  handlers: {
    *double(input: number) {
      return yield* run("double", Effect.succeed(input * 2), { output: Schema.Number })
    },
  },
  schemas: { double: { input: Schema.Number, output: Schema.Number } },
})

class CounterRow extends Table<CounterRow>("ingress-counter")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

/** Keyed virtual object: per-key durable state (`add`). */
export const Counter = object({
  name: "ingress-counter-object",
  handlers: {
    *add(amount: number) {
      const current = yield* state(CounterRow).get("v")
      const next = (current._tag === "Some" ? current.value.value : 0) + amount
      yield* state(CounterRow).set({ id: "v", value: next })
      return next
    },
  },
  schemas: { add: { input: Schema.Number, output: Schema.Number } },
})

/**
 * A service that calls a keyed object via the **correct** in-handler surface
 * (`objectClient`). Proves the footgun guard does NOT misfire on the replay-safe
 * child-call path.
 */
export const Proxy = service({
  name: "ingress-proxy",
  handlers: {
    *bump(amount: number) {
      return yield* objectClient(Counter, "proxy-key").add(amount)
    },
  },
  schemas: { bump: { input: Schema.Number, output: Schema.Number } },
})

/**
 * A service that (wrongly) uses the **top-level** `client(...)` surface inside a
 * handler. The footgun guard must reject this (a fresh random id per replay is
 * not replay-safe), surfacing as a typed `DurableFailure` over the ingress.
 */
export const Footgun = service({
  name: "ingress-footgun",
  handlers: {
    *callTopLevel(amount: number) {
      return yield* client(Calculator).double(amount)
    },
  },
  schemas: { callTopLevel: { input: Schema.Number, output: Schema.Number } },
})

/** The whole catalog — passed explicitly to the engine + ingress (no global registry). */
export const catalog = [Calculator, Counter, Proxy, Footgun]

const EngineLive = serviceLayer(Calculator, Counter, Proxy, Footgun)

/** True when the `s2` CLI is on PATH (the S2-backed tests skip without it). */
export const hasS2 = (): boolean => {
  try {
    execSync("command -v s2", { stdio: "ignore" })
    return true
  } catch {
    return false
  }
}

/**
 * Stand up the full edge — a real Node HTTP server serving the catalog, an
 * out-of-process-style `connect()` client over HTTP, the engine, and s2-lite S2 —
 * run `program` against the connected ingress client, and return its result. The
 * one place the provide-stack is wired.
 */
export const runIngress = <A, E>(
  program: (ingress: DurableIngressClient) => Effect.Effect<A, E>,
): Promise<A> =>
  Effect.gen(function*() {
    const server = yield* HttpServer.HttpServer
    const port = server.address._tag === "TcpAddress" ? server.address.port : 0
    const ingress = yield* connect({ url: `http://127.0.0.1:${port}` })
    return yield* program(ingress)
  }).pipe(
    Effect.provide(NodeHttpClient.layerUndici),
    Effect.provide(durableIngress(catalog)),
    Effect.provide(EngineLive),
    Effect.provide(NodeHttpServer.layer(() => createServer(), { port: 0 })),
    Effect.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
    Effect.provide(S2LiteLive),
    Effect.scoped,
    Effect.runPromise,
  )
