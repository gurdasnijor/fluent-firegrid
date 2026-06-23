// @effect-diagnostics effect/nodeBuiltinImport:off effect/multipleEffectProvide:off -- edge integration support: node:child_process probes for the `s2` binary, and the chained provides are the intentional edge wiring (HttpClient → DurableHostLive (ingress → engine → server → S2)).
import { execSync } from "node:child_process"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer"
import { Effect, Schema } from "effect"
import { primaryKey, Table } from "effect-s2-stream-db"
import { DurableHostLive } from "../src/host.ts"
import { connect, type DurableIngressClient } from "../src/ingress/client.ts"
import { client, object, objectClient, run, service, state } from "../src/index.ts"
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

/** The whole catalog — passed explicitly to the host (no global registry). */
const catalog = [Calculator, Counter, Proxy, Footgun]

/** Allocate a free TCP port, then release it for the host's ingress to bind. */
const freePort = Effect.scoped(
  Effect.gen(function*() {
    const server = yield* NodeSocketServer.make({ port: 0, host: "127.0.0.1" })
    return server.address._tag === "TcpAddress" ? server.address.port : 0
  }),
).pipe(Effect.orDie)

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
 * Stand up the full edge by dogfooding the real host surface: `DurableHostLive`
 * serves the catalog over its own Node HTTP server (on a pre-allocated free port,
 * the realistic `INGRESS_PORT` path) backed by s2-lite, with an
 * out-of-process-style `connect()` client over HTTP. Run `program` against the
 * connected ingress client and return its result.
 */
export const runIngress = <A, E>(
  program: (ingress: DurableIngressClient) => Effect.Effect<A, E>,
): Promise<A> =>
  freePort.pipe(
    Effect.flatMap((port) =>
      Effect.gen(function*() {
        const ingress = yield* connect({ url: `http://127.0.0.1:${port}` })
        return yield* program(ingress)
      }).pipe(
        Effect.provide(NodeHttpClient.layerUndici),
        Effect.provide(
          DurableHostLive({
            catalog,
            namespace: "effect-s2-durable-test",
            ingress: { port },
            s2: S2LiteLive,
          }),
        ),
        Effect.scoped,
      )),
    Effect.runPromise,
  )
